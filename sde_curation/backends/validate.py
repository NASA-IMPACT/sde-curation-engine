"""Direct validation of an index run against OpenSearch Serverless.

Same comparison as the indexer's web/validate.py (count + titles for `collection_key`), but run by
the engine *after* a refresh delay, so it is not fooled by AOSS eventual consistency.

Credentials: the default boto3 chain (task/instance role when deployed, your user locally), or
VALIDATION_ASSUME_ROLE_ARN for a role that already holds AOSS data access. A 403 means the
principal is not in the collection's data-access policy → the caller falls back to a second pass.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..config import Settings

log = logging.getLogger(__name__)

_MAX_REPORTED = 50


class NoIndexAccess(RuntimeError):
    """The engine's principal cannot read the index (AOSS data-access policy)."""


def web_id(collection_key: str, url: str) -> str:
    return f"/SDE/{collection_key}/|{url}"  # web/web_processor.py::make_web_id


def _client(settings: Settings, endpoint: str):
    import boto3
    from opensearchpy import AWSV4SignerAuth, OpenSearch, RequestsHttpConnection

    session = boto3.Session(region_name=settings.aws_region)
    if settings.validation_assume_role_arn:
        cr = session.client("sts").assume_role(
            RoleArn=settings.validation_assume_role_arn, RoleSessionName="sde-curation-validate"
        )["Credentials"]
        session = boto3.Session(
            aws_access_key_id=cr["AccessKeyId"], aws_secret_access_key=cr["SecretAccessKey"],
            aws_session_token=cr["SessionToken"], region_name=settings.aws_region,
        )
    host = endpoint.replace("https://", "").rstrip("/")
    return OpenSearch(
        hosts=[{"host": host, "port": 443}],
        http_auth=AWSV4SignerAuth(session.get_credentials(), settings.aws_region, "aoss"),
        use_ssl=True, verify_certs=True, connection_class=RequestsHttpConnection, timeout=60,
    )


def _scan_titles(client, index: str, collection_key: str) -> dict[str, str]:
    """{id: title} for the collection's visible documents (search_after paging)."""
    out: dict[str, str] = {}
    last = None
    query = {"bool": {"filter": [{"term": {"collection_key": collection_key}},
                                 {"term": {"public_visibility": True}}]}}
    while True:
        body: dict[str, Any] = {"size": 1000, "_source": ["id", "title"], "query": query, "sort": [{"id": "asc"}]}
        if last is not None:
            body["search_after"] = last
        r = client.search(index=index, body=body)
        hits = r["hits"]["hits"]
        if not hits:
            break
        for h in hits:
            src = h.get("_source") or {}
            if src.get("id"):
                out[src["id"]] = src.get("title") or ""
        last = hits[-1].get("sort")
        if len(hits) < 1000 or last is None:
            break
    return out


def compare(collection_key: str, run_id: str, expected: dict[str, str], indexed: dict[str, str]) -> dict[str, Any]:
    """expected/indexed are {id: title}. Mirrors web/validate.py's report shape."""
    missing = [i for i in expected if i not in indexed]
    extra = [i for i in indexed if i not in expected]
    mismatched = [i for i in expected if i in indexed and (expected[i] or "") != (indexed[i] or "")]
    comparable = len(expected)
    matched = comparable - len(missing) - len(mismatched)
    rate = (matched / comparable) if comparable else 1.0
    report: dict[str, Any] = {
        "run_id": run_id, "collection_key": collection_key,
        "expected_count": comparable, "indexed_count": len(indexed), "count_matches": len(indexed) == comparable,
        "titles_missing_in_index": [expected[i] for i in missing[:_MAX_REPORTED]],
        "titles_only_in_index": [indexed[i] for i in extra[:_MAX_REPORTED]],
        "titles_mismatched": [{"id": i, "exported": expected[i], "indexed": indexed[i]} for i in mismatched[:_MAX_REPORTED]],
        "title_match_rate": round(rate, 6),
    }
    if len(missing) > _MAX_REPORTED or len(extra) > _MAX_REPORTED or len(mismatched) > _MAX_REPORTED:
        report["truncated"] = {"missing_total": len(missing), "only_in_index_total": len(extra),
                               "mismatched_total": len(mismatched), "reported_limit": _MAX_REPORTED}
    return report


async def validate_direct(
    settings: Settings, *, collection_key: str, run_id: str, target: str, expected_titles: dict[str, str],
    client=None,
) -> dict[str, Any]:
    """Query the index and compare. Raises NoIndexAccess on 403 / missing endpoint."""
    endpoint = settings.opensearch_endpoint_prod if target == "prod" else settings.opensearch_endpoint_test
    if not endpoint:
        raise NoIndexAccess(f"OPENSEARCH_ENDPOINT_{target.upper()} is not set")
    from opensearchpy.exceptions import AuthorizationException, TransportError

    def run() -> dict[str, str]:
        c = client or _client(settings, endpoint)
        try:
            return _scan_titles(c, settings.web_index_name, collection_key)
        except AuthorizationException as e:
            raise NoIndexAccess(f"no AOSS data access for this principal on {settings.web_index_name}: {e}") from e
        except TransportError as e:
            if getattr(e, "status_code", None) in (401, 403):
                raise NoIndexAccess(str(e)) from e
            raise

    indexed = await asyncio.to_thread(run)
    expected = {web_id(collection_key, url): title for url, title in expected_titles.items()}
    return compare(collection_key, run_id, expected, indexed)
