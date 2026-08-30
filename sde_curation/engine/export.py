"""Curated set → the WEB_COSMOS export contract (sde-api-scrapers/web/cosmos_source.py).

  curated_collections/{collection_key}/{run_id}/documents.jsonl   one ExportLine per line
  curated_collections/{collection_key}/{run_id}/manifest.json     written LAST (= "export complete")

Only non-excluded curated URLs are exported. Title falls back to the scraped title; division and
document_type fall back to the collection defaults via the manifest (the indexer applies them).
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterator
from datetime import UTC, datetime

from ..models import Collection, CuratedUrl, ExportLine, ExportManifest


def mint_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(3)


def export_lines(curated: list[CuratedUrl], full_text: dict[str, str | None]) -> Iterator[ExportLine]:
    for r in sorted(curated, key=lambda r: r.url):
        if r.excluded:
            continue
        yield ExportLine(
            url=r.url,
            title=(r.title or r.scraped_title or "").strip() or None,
            full_text=full_text.get(r.url),
            document_type=r.document_type,
            division=r.division,
        )


def build_manifest(c: Collection, run_id: str, count: int, target: str) -> ExportManifest:
    return ExportManifest(
        collection_key=c.collection_id,
        run_id=run_id,
        document_count=count,
        collection_name=c.name,
        division=c.division,
        document_type=c.document_type,
        target=target,
    )


def write_jsonl(lines: Iterator[ExportLine], fh) -> int:
    """Stream lines to an open text file; returns the count. Never holds the set in memory."""
    n = 0
    for line in lines:
        fh.write(json.dumps(line.model_dump(exclude_none=True), ensure_ascii=False) + "\n")
        n += 1
    return n


def export_prefix(collection_key: str, run_id: str) -> str:
    return f"curated_collections/{collection_key}/{run_id}"


def status_prefix(collection_key: str, run_id: str) -> str:
    return f"index_runs/{collection_key}/{run_id}"
