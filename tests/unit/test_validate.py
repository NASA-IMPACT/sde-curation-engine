"""Phase 6: direct validation, 403 → second-pass fallback, gate → prod/live, notifications."""
import pytest

from sde_curation.backends.validate import NoIndexAccess, compare, validate_direct, web_id
from sde_curation.config import Settings
from sde_curation.models import IndexRun
from sde_curation.notify import Notifier


def test_compare_mirrors_indexer_report():
    exp = {web_id("k", "https://x/a"): "A", web_id("k", "https://x/b"): "B", web_id("k", "https://x/c"): "C"}
    idx = {web_id("k", "https://x/a"): "A", web_id("k", "https://x/b"): "B!", web_id("k", "https://x/z"): "Z"}
    r = compare("k", "r", exp, idx)
    assert r["expected_count"] == 3 and r["indexed_count"] == 3 and r["count_matches"] is True
    assert r["titles_missing_in_index"] == ["C"] and r["titles_only_in_index"] == ["Z"]
    assert r["titles_mismatched"][0]["exported"] == "B" and r["title_match_rate"] == round(1 / 3, 6)
    run = IndexRun(run_id="r", collection_id="k", target="test", validation=r)
    assert run.validation_passes(0.99) is False and run.validation_passes(0.3) is True


async def test_validate_direct_uses_client_and_maps_403():
    class Client:
        def search(self, index, body):
            assert index == "sde-web" and body["query"]["bool"]["filter"][0]["term"]["collection_key"] == "k"
            return {"hits": {"hits": [{"_source": {"id": web_id("k", "https://x/a"), "title": "A"}, "sort": [1]}]}}

    s = Settings(opensearch_endpoint_test="https://e.example", llm_provider="fake")
    r = await validate_direct(s, collection_key="k", run_id="r", target="test", expected_titles={"https://x/a": "A"}, client=Client())
    assert r["count_matches"] and r["title_match_rate"] == 1.0

    from opensearchpy.exceptions import AuthorizationException

    class Denied:
        def search(self, index, body):
            raise AuthorizationException(403, "security_exception", "Bad Authorization")

    with pytest.raises(NoIndexAccess, match="no AOSS data access"):
        await validate_direct(s, collection_key="k", run_id="r", target="test", expected_titles={}, client=Denied())
    with pytest.raises(NoIndexAccess, match="OPENSEARCH_ENDPOINT_TEST"):
        await validate_direct(Settings(llm_provider="fake"), collection_key="k", run_id="r", target="test", expected_titles={})


async def test_notifier_posts_and_never_raises():
    calls = []

    async def post(url, payload):
        calls.append((url, payload))
        raise RuntimeError("slack down")

    n = Notifier("https://hook", post=post, base_url="https://engine")
    await n.status_changed("ex.org", "curated", "live", "prod run r1")
    assert calls[0][0] == "https://hook" and "*live*" in calls[0][1]["text"] and "/collections/ex.org" in calls[0][1]["text"]
    assert n.sent[0]["new_status"] == "live"
    assert (await Notifier(None).status_changed("x", None, "backlog", None)) is None
