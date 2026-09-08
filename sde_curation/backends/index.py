"""Index backends: run the WEB_COSMOS pipeline (sde-api-scrapers) for one exported run.

Both backends only *dispatch*; completion is always read from S3
`index_runs/{key}/{run_id}/status.json`, which WebPipeline.run() writes last and unconditionally.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol

from ..config import Settings
from ..engine.export import status_prefix
from ..models import Collection, IndexStatus, ValidationReport
from .s3 import S3

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]


class IndexError_(RuntimeError):
    pass


@dataclass
class Dispatch:
    external_ref: str  # pid | taskArn
    detail: dict[str, Any]


class IndexBackend(Protocol):
    name: str

    async def dispatch(self, c: Collection, run_id: str, target: str) -> Dispatch: ...

    async def still_running(self, d: Dispatch) -> bool | None:
        """True/False if knowable, None if the backend cannot tell."""
        ...


def indexer_command(c: Collection, run_id: str, target: str, *, python: str = "python3") -> list[str]:
    """Exactly what the ECS task definition / CLI expects (api_scraper.py::_run_web_cosmos)."""
    return [python, "api_scraper.py", "--source", "WEB_COSMOS", "--collection", c.collection_id,
            "--run-id", run_id, "--target", target]


# ── local subprocess ───────────────────────────────────────────────────


class LocalSubprocessIndexer:
    name = "local"

    def __init__(self, settings: Settings):
        self.s = settings
        self.root = settings.indexer_root
        self.python = settings.resolved_indexer_python
        self._procs: dict[str, asyncio.subprocess.Process] = {}

    def env(self) -> dict[str, str]:
        s = self.s
        env = {**os.environ, "COSMOS_INDEX_BUCKET": s.cosmos_index_bucket or "", "WEB_INDEX_NAME": s.web_index_name,
               "AWS_DEFAULT_REGION": s.aws_region}
        if s.opensearch_endpoint_test:
            env["OPENSEARCH_ENDPOINT_TEST"] = s.opensearch_endpoint_test
            env.setdefault("OPENSEARCH_ENDPOINT", s.opensearch_endpoint_test)
        if s.opensearch_endpoint_prod:
            env["OPENSEARCH_ENDPOINT_PROD"] = s.opensearch_endpoint_prod
        if s.sagemaker_endpoint_name:
            env["SAGEMAKER_ENDPOINT_NAME"] = s.sagemaker_endpoint_name
        return env

    async def dispatch(self, c: Collection, run_id: str, target: str) -> Dispatch:
        if not (self.root / "api_scraper.py").is_file():
            raise IndexError_(f"indexer not found: {self.root / 'api_scraper.py'} (INDEXER_ROOT)")
        if not self.python.is_file():
            raise IndexError_(f"indexer python not found: {self.python} (INDEXER_PYTHON)")
        log_dir = self.s.data_dir / "index_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log = await asyncio.to_thread(open, log_dir / f"{c.collection_id}-{run_id}.log", "wb")
        proc = await asyncio.create_subprocess_exec(
            *indexer_command(c, run_id, target, python=str(self.python)),
            cwd=self.root, env=self.env(), stdout=log, stderr=asyncio.subprocess.STDOUT,
        )
        self._procs[run_id] = proc
        return Dispatch(external_ref=f"pid:{proc.pid}", detail={"log": str(log.name)})

    async def still_running(self, d: Dispatch) -> bool | None:
        run_id = next((k for k, p in self._procs.items() if f"pid:{p.pid}" == d.external_ref), None)
        if run_id is None:
            return None
        proc = self._procs[run_id]
        if proc.returncode is None:
            return True
        d.detail["exit_code"] = proc.returncode
        return False

    async def kill(self, d: Dispatch) -> None:
        for p in self._procs.values():
            if f"pid:{p.pid}" == d.external_ref and p.returncode is None:
                p.kill()
                await p.wait()


# ── ECS run-task ───────────────────────────────────────────────────────


class EcsDispatchIndexer:
    """ecs:RunTask with the command override the task definition expects (no ENTRYPOINT in the
    image, so the full `python3 api_scraper.py …` command is passed). Assumes
    INDEXING_DISPATCH_ROLE_ARN when set; falls back to the ambient credentials otherwise."""

    name = "ecs"

    def __init__(self, settings: Settings, *, ecs=None, sts=None):
        import boto3

        self.s = settings
        if not settings.indexing_subnets:
            raise IndexError_("ECS backend needs INDEXING_SUBNETS (Fargate awsvpc networking)")
        self.sts = sts or boto3.client("sts", region_name=settings.aws_region)
        self._ecs = ecs
        self._creds_expire = 0.0

    def _client(self):
        import boto3

        if self._ecs is not None and (not self.s.indexing_dispatch_role_arn or time.time() < self._creds_expire):
            return self._ecs
        if self.s.indexing_dispatch_role_arn:
            try:
                r = self.sts.assume_role(RoleArn=self.s.indexing_dispatch_role_arn, RoleSessionName="sde-curation-engine")
                cr = r["Credentials"]
                self._ecs = boto3.client(
                    "ecs", region_name=self.s.aws_region, aws_access_key_id=cr["AccessKeyId"],
                    aws_secret_access_key=cr["SecretAccessKey"], aws_session_token=cr["SessionToken"],
                )
                self._creds_expire = time.time() + 45 * 60
                return self._ecs
            except Exception:  # noqa: BLE001 - fall back to ambient creds (logged via detail)
                self._assume_failed = True
        self._ecs = boto3.client("ecs", region_name=self.s.aws_region)
        self._creds_expire = float("inf")
        return self._ecs

    def run_task_args(self, c: Collection, run_id: str, target: str) -> dict[str, Any]:
        s = self.s
        return {
            "cluster": s.indexing_ecs_cluster,
            "taskDefinition": s.indexing_task_family,
            "launchType": "FARGATE",
            "count": 1,
            "networkConfiguration": {"awsvpcConfiguration": {
                "subnets": s.indexing_subnets,
                **({"securityGroups": s.indexing_security_groups} if s.indexing_security_groups else {}),
                "assignPublicIp": "ENABLED" if s.indexing_assign_public_ip else "DISABLED",
            }},
            "overrides": {"containerOverrides": [{
                "name": s.indexing_container_name,
                "command": indexer_command(c, run_id, target),
            }]},
            "startedBy": f"sde-curation-engine:{c.collection_id}"[:36],
        }

    async def dispatch(self, c: Collection, run_id: str, target: str) -> Dispatch:
        ecs = self._client()
        resp = await asyncio.to_thread(ecs.run_task, **self.run_task_args(c, run_id, target))
        if resp.get("failures"):
            raise IndexError_(f"ecs:RunTask failed: {resp['failures']}")
        task = resp["tasks"][0]
        return Dispatch(external_ref=task["taskArn"], detail={"cluster": self.s.indexing_ecs_cluster,
                                                               "assumed_role": not getattr(self, "_assume_failed", False)})

    async def still_running(self, d: Dispatch) -> bool | None:
        ecs = self._client()
        r = await asyncio.to_thread(ecs.describe_tasks, cluster=self.s.indexing_ecs_cluster, tasks=[d.external_ref])
        if not r.get("tasks"):
            return None
        t = r["tasks"][0]
        d.detail["last_status"] = t.get("lastStatus")
        if t.get("lastStatus") == "STOPPED":
            d.detail["stopped_reason"] = t.get("stoppedReason")
            d.detail["exit_code"] = (t.get("containers") or [{}])[0].get("exitCode")
            return False
        return True


def make_index_backend(settings: Settings) -> IndexBackend:
    if settings.index_backend == "ecs":
        return EcsDispatchIndexer(settings)
    return LocalSubprocessIndexer(settings)


# ── polling (backend-agnostic) ─────────────────────────────────────────


async def wait_for_status(
    s3: S3, settings: Settings, c: Collection, run_id: str, backend: IndexBackend, d: Dispatch,
    on_progress: ProgressCb, *, target: str,
) -> tuple[IndexStatus, ValidationReport | None]:
    """Poll status.json; treat a backend that has stopped without writing it as failed."""
    prefix = status_prefix(c.collection_id, run_id)
    t0 = time.monotonic()
    stopped_at: float | None = None
    while True:
        st = await s3.get_json(f"{prefix}/status.json")
        if st is not None:
            status = IndexStatus.model_validate(st)
            validation = None
            if target == "test":
                v = await s3.get_json(f"{prefix}/validation.json")
                validation = ValidationReport.model_validate(v) if v else None
            return status, validation
        running = await backend.still_running(d)
        if running is False:
            # give S3 a moment: status.json is written in a finally: block right before exit
            stopped_at = stopped_at or time.monotonic()
            if time.monotonic() - stopped_at > 30:
                raise IndexError_(f"indexer stopped without writing status.json: {d.detail}")
        elapsed = time.monotonic() - t0
        await on_progress({"waiting_s": int(elapsed), **{k: v for k, v in d.detail.items() if k != "log"}})
        if elapsed > settings.index_stall_timeout_s:
            raise IndexError_(f"indexer stalled: no status.json after {int(elapsed)}s ({d.detail})")
        await asyncio.sleep(settings.index_poll_interval_s)
