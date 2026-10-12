"""Small pure helpers of the web layer (sde_curation/web/app.py, sde_curation/db.py) whose output a
curator reads or the browser acts on: the crawl-failure label, the htmx answer after an action, the
ORDER BY a table's ?sort= becomes, and the screenshots the handbook page shows."""

import json
import re
from pathlib import Path

import pytest
from starlette.requests import Request

from sde_curation.db import order_by
from sde_curation.models import NOT_VISITED
from sde_curation.web.app import failure_label, htmx_done

WEB = Path(failure_label.__code__.co_filename).parent
PAYLOAD = {"collection_id": "ex.org", "state": "failed"}


# ── crawl failures as the curator reads them ────────────────────────────


@pytest.mark.parametrize(("reason", "label"), [
    (None, "never seen by the crawl"),
    (NOT_VISITED, "not visited: the crawl stopped at its page cap"),
    ("http_404", "HTTP 404 not found"),
    ("http_403", "HTTP 403 forbidden"),
    ("challenge_cloudflare", "bot-challenge page instead of content"),
    ("some_new_reason", "some new reason"),
], ids=["absent from a complete crawl", "capped crawl", "gone", "blocked", "challenge page", "unknown code"])
def test_a_crawl_failure_reason_reads_as_plain_words(reason, label):
    """The Delta and Curated tables say why a page was removed or kept; an unknown code still reads."""
    assert failure_label(reason) == label


# ── the answer an htmx button gets ──────────────────────────────────────


def request(headers: dict[str, str] | None = None, query: str = "") -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "method": "POST", "path": "/", "headers": raw, "query_string": query.encode()})


def test_an_api_caller_gets_the_payload_itself():
    assert htmx_done(request(), PAYLOAD) is PAYLOAD


def test_an_htmx_button_gets_the_payload_and_a_page_refresh():
    """The Cancel button in the jobs strip: the strip may already be re-rendered by an SSE event, so
    the refresh is a response header, not a swap of the clicked element."""
    r = htmx_done(request({"HX-Request": "true"}), PAYLOAD)

    assert (r.headers.get("HX-Refresh"), r.headers.get("HX-Redirect"), json.loads(r.body)) == ("true", None, PAYLOAD)


def test_an_htmx_button_with_a_then_target_is_sent_there():
    r = htmx_done(request({"HX-Request": "true"}, "then=/jobs"), PAYLOAD, then="/ignored")

    assert (r.headers.get("HX-Redirect"), r.headers.get("HX-Refresh")) == ("/jobs", None)


# ── ?sort= → ORDER BY ───────────────────────────────────────────────────

SORTS = {"title": ("COALESCE(title, scraped_title)",), "url": ("host", "depth", "url")}
DEFAULT = "kind, url"


@pytest.mark.parametrize("sort", [None, "", "drop table x", "nonsense"])
def test_an_unknown_sort_key_gives_the_default_order_and_never_reaches_the_sql(sort):
    assert order_by(SORTS, sort, True, DEFAULT) == f" ORDER BY {DEFAULT}"


@pytest.mark.parametrize(("desc", "direction"), [(False, "ASC"), (True, "DESC")])
def test_a_sorted_column_orders_every_expression_nulls_last_then_by_the_default(desc, direction):
    """NULLS LAST in both directions, and the default order as the tiebreak so paging stays stable."""
    assert order_by(SORTS, "url", desc, DEFAULT) == (
        f" ORDER BY host {direction} NULLS LAST, depth {direction} NULLS LAST, url {direction} NULLS LAST, {DEFAULT}")


# ── the handbook ────────────────────────────────────────────────────────


def test_every_screenshot_the_handbook_shows_ships_with_the_app():
    """/manual shows screenshots from static/manual/; a missing file is a broken image for curators."""
    shown = set(re.findall(r"static_url\('([^']+)'\)", (WEB / "templates" / "manual.html").read_text()))

    assert shown, "the handbook shows no screenshots any more: update this test"
    assert sorted(n for n in shown if not (WEB / "static" / n).is_file()) == []
