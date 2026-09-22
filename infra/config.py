"""Per-environment settings that are safe to commit: sizes, knobs, and the *names* of the SSM
parameters holding everything account-specific (instance ids, buckets, endpoints, role ARNs).

The values themselves live in SSM Parameter Store of the target account under
`/sde-curation-engine/<env>/<key>` and are resolved by CloudFormation at deploy time, so they never
appear in git, in the synthesized template, or in cdk.out. Seed them with `make infra-seed ENV=…`
(reads infra/envs/<env>.json, which is gitignored; infra/envs/example.json shows the shape).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

APP_NAME = "sde-curation-engine"


class Environment(str, Enum):
    DEV = "dev"
    TEST = "test"
    PROD = "prod"


# key → what it is. Every one is a plain String parameter; every one is required.
PARAMS: dict[str, str] = {
    "crawler_instance_id": "EC2 instance of the crawler (sde-crawl4ai-scraper, SdeCrawlerStack)",
    "crawler_bucket": "S3 bucket the crawler writes <crawler_s3_prefix>/scraped_collections/ and failure_logs/ to",
    "indexing_cluster_name": "ECS cluster of the WEB_COSMOS indexer (sde-api-scrapers)",
    "indexing_task_family": "task definition family of the WEB_COSMOS indexer",
    "indexing_task_role_arn": "task role ARN of the indexer task definition (iam:PassRole)",
    "indexing_execution_role_arn": "execution role ARN of the indexer task definition (iam:PassRole)",
    "cosmos_index_bucket": "S3 hand-off bucket (curated_collections/, index_runs/, vectorized/)",
    "aoss_collection_name": "OpenSearch Serverless collection holding the web index",
    "aoss_collection_id": "id of that collection (aoss:APIAccessAll resource)",
    "opensearch_endpoint_test": "AOSS endpoint the engine validates the 'test' target against",
    "opensearch_endpoint_prod": "AOSS endpoint 'Index to prod' publishes the test run's vectors to",
}
# Only where EnvConfig.prod_publish_via_role is set: the prod collection lives in another account.
ROLE_PARAMS: dict[str, str] = {
    "prod_index_role_arn": "role in the prod account the engine assumes to write the prod web index",
}


@dataclass(frozen=True)
class EnvConfig:
    env: Environment
    region: str = "us-east-1"
    # Public subnets are picked from these AZs only: Fargate is not offered in us-east-1e, and the
    # same subnets are handed to the indexer as INDEXING_SUBNETS.
    azs: tuple[str, ...] = ("us-east-1a", "us-east-1b")
    # None → the account's default VPC (Vpc.from_lookup(is_default=True))
    vpc_id: str | None = None

    crawler_inbox: str = "/opt/sde-crawler/jobs/incoming"
    # Folder inside crawler_bucket the crawler writes to ("" = bucket root).
    crawler_s3_prefix: str = "sde-curation-engine-prototype"
    # "Index to prod" writes the prod web index directly. The real engine runs only in test (there is
    # no prod engine), so test reaches the SMCE prod collection in the other account by assuming the
    # role in SSM `prod_index_role_arn` (True). False = "prod" is this account's own collection (dev,
    # where both targets are the dev collection) → the stack's AOSS data-access policy grants write.
    prod_publish_via_role: bool = False
    indexing_container_name: str = "WEB_COSMOSContainer"
    # dev indexes into a scratch subset; test and prod write the live sde-web index
    web_index_name: str = "sde-web-subset"
    openai_model: str = "gpt-5.6-luna"  # 1.05M-token window: the full page text always fits
    llm_provider: str = "openai"  # "fake": canned answers, no API calls (load tests — see stress_config)
    llm_workers: int = 16
    llm_pattern_batch_urls: int = 1000
    # Override for notification links; default = the stack's CloudFront URL.
    public_base_url: str | None = None
    cpu: int = 1024
    memory_mib: int = 2048
    waf_rate_limit_per_5min: int = 1000
    # RDS PostgreSQL (the state store). Storage starts at 20 GB gp3 and autoscales to
    # `db_max_storage_gib`, which is a hard stop: past it writes fail, mid-ingest, with a disk-full
    # error. The page text is what fills it — schema V9 made a collection cost one copy of its
    # crawl rather than two (the dump and the curated set share it), but a 100k-page site is still
    # GBs, and the V9 migration itself needs room for one extra copy of the text while it runs.
    # x86 dedicated compute, 2 vCPU / 8 GiB, no CPU-credit model. Graviton burstable (t4g) was
    # the first choice but hit "insufficient-capacity" twice in us-east-1 on 2026-09-11; the m6i
    # pools are far larger. Same class in every environment.
    db_instance_class: str = "m6i.large"
    db_max_storage_gib: int = 200
    # The DB subnet group spans every AZ where the class is orderable (not just `azs`, which is
    # limited by Fargate): RDS picks one with free capacity at create time, so more AZs = fewer
    # "insufficient-capacity" failures. us-east-1e offers no db.t3/t4g classes.
    db_azs: tuple[str, ...] = ("us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1f")
    db_multi_az: bool = False
    db_backup_days: int = 7
    db_deletion_protection: bool = False

    @property
    def name(self) -> str:
        return f"{APP_NAME}-{self.env.value}"

    @property
    def params(self) -> dict[str, str]:
        """Every SSM parameter this environment needs."""
        return {**PARAMS, **(ROLE_PARAMS if self.prod_publish_via_role else {})}

    def param_name(self, key: str) -> str:
        assert key in self.params, key
        return f"/{APP_NAME}/{self.env.value}/{key}"

    def secret_name(self, key: str) -> str:
        return f"/{APP_NAME}/{self.env.value}/{key}"


CONFIGS: dict[Environment, EnvConfig] = {
    # Same task size as test, so a change that works on dev will not run out of memory on test
    # (2 GB OOM-killed the ascl.net load on 2026-09-22 while test would have held it).
    Environment.DEV: EnvConfig(env=Environment.DEV, cpu=4096, memory_mib=16384),
    # The test crawler (SdeCrawlerStack in 119417011911) writes scraped_collections/ at the bucket root.
    # 4 vCPU / 16 GB: test is the engine that does the real curation and publishes to prod. Sized for
    # ~5 concurrent curators on 100k-URL collections, whose scrape ingest and test export hold the
    # full page text in memory; four of them peaked at 6.5 GB before the 2026-09-18 scale fixes
    # (docs/scale-audit-2026-09-18.md); re-measure before sizing down.
    # 500 GB: test holds every collection's crawl text at once and is where the big sites land
    # (ascl.net alone crawls to 6.7 GB of JSON). Autoscaling only ever raises the volume, and
    # gp3 is billed on what is allocated, not on the ceiling.
    Environment.TEST: EnvConfig(env=Environment.TEST, web_index_name="sde-web", crawler_s3_prefix="",
                                prod_publish_via_role=True, cpu=4096, memory_mib=16384,
                                db_max_storage_gib=500),
    Environment.PROD: EnvConfig(
        env=Environment.PROD, web_index_name="sde-web", cpu=2048, memory_mib=4096,
        db_multi_az=True, db_backup_days=35, db_deletion_protection=True,
    ),
}


def get_config(env_name: str) -> EnvConfig:
    return CONFIGS[Environment(env_name)]


def stress_config(cfg: EnvConfig) -> EnvConfig:
    """`cdk deploy -c stress=true` (dev only): the environment as a load test needs it: the fake
    LLM (a 100k-URL Suggest metadata is 100k paid API calls otherwise) and a WAF limit the test
    client's polling from one IP stays under. The task is already test's size. A plain deploy puts
    everything back."""
    if cfg.env is not Environment.DEV:
        raise ValueError("stress=true is for the dev environment only")
    return replace(cfg, llm_provider="fake", waf_rate_limit_per_5min=20_000)
