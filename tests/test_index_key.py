"""The collection_key everything is indexed under is the collection name slugified — the same rule
COSMOS gives its config_folder — not the engine's seed-derived collection_id."""

import json

from sde_curation.backends.index import indexer_command
from sde_curation.engine.export import build_manifest
from sde_curation.models import Collection, Division, collection_key_from_name
from tests.conftest import prepare, wait_job


def coll(name: str, **kw) -> Collection:
    return Collection(collection_id="science.nasa.gov", name=name, seed_url="https://science.nasa.gov",
                      division=Division.HELIOPHYSICS, connector="crawler2", max_pages=10, **kw)


def test_key_follows_the_cosmos_slug_rule():
    # sde_collections/models/collection.py::_compute_config_folder_name = slugify(name, separator="_")
    assert collection_key_from_name("NASA Applied Sciences") == "nasa_applied_sciences"
    assert collection_key_from_name("Earth & Space Science") == "earth_space_science"
    assert collection_key_from_name("PDS4: Data — Archive") == "pds4_data_archive"


def test_collection_keys_on_the_name_not_the_seed():
    c = coll("NASA Applied Sciences")
    assert c.collection_key == "nasa_applied_sciences" and c.collection_name == "NASA Applied Sciences"
    assert c.collection_id == "science.nasa.gov"  # the engine's own id is unchanged
    # the export contract and the indexer command carry the key, so both agree (deletion_guard checks)
    m = build_manifest(c, "run-1", 3, "test")
    assert m.collection_key == "nasa_applied_sciences" and m.collection_name == "NASA Applied Sciences"
    assert indexer_command(c, "run-1", "test") == ["python3", "api_scraper.py", "--source", "WEB_COSMOS",
                                                   "--collection", "nasa_applied_sciences", "--run-id", "run-1",
                                                   "--target", "test"]
    # a pinned/hand-set key wins, and a name that slugifies to nothing falls back to the id
    assert coll("NASA Applied Sciences", index_key="nasa_legacy", index_name="NASA Legacy").collection_key == "nasa_legacy"
    assert coll("!!!").collection_key == "science.nasa.gov"


# ── through the app (the fake indexer reads the export under the --collection key it is given) ──


def export_keys(c, key: str, run_id: str = "") -> list[str]:
    r = c.s3.list_objects_v2(Bucket="cosmos-idx", Prefix=f"curated_collections/{key}/{run_id}")
    return [o["Key"] for o in r.get("Contents") or []]


async def test_index_to_test_exports_under_the_name_key(index_client):
    c = index_client
    await c.post("/api/collections", json={"seed_url": "https://ex.org", "name": "NASA Applied Sciences",
                                           "max_pages": 10, "division": "Heliophysics"})
    await prepare(c, "ex.org", create=False)
    assert (await c.get("/api/collections/ex.org")).json()["index_key"] is None
    assert "<code>nasa_applied_sciences</code>" in (await c.get("/collections/ex.org?tab=overview")).text

    r = await c.post("/api/collections/ex.org/index?target=test")
    assert r.status_code == 202, r.text
    run_id = r.json()["run_id"]
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["state"] == "succeeded", job
    assert job["progress"]["index_key"] == "nasa_applied_sciences"
    # exported, indexed and validated as that collection — nothing under the engine's id
    assert export_keys(c, "ex.org", run_id) == []
    m = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"curated_collections/nasa_applied_sciences/{run_id}/manifest.json")["Body"].read())
    assert m["collection_key"] == "nasa_applied_sciences" and m["collection_name"] == "NASA Applied Sciences"
    assert job["progress"]["status"]["collection_key"] == "nasa_applied_sciences"
    # the first run pins it, so renaming the collection later cannot move it to a second collection
    col = (await c.get("/api/collections/ex.org")).json()
    assert col["index_key"] == "nasa_applied_sciences" and col["index_name"] == "NASA Applied Sciences"
    assert any(a["action"] == "index.key" for a in (await c.get("/api/collections/ex.org/audit")).json())


async def test_index_key_can_be_set_by_hand_and_prod_refuses_a_run_under_another_key(index_client):
    c = index_client
    await prepare(c)  # name "ex.org" → key "ex_org"
    r = await c.post("/api/collections/ex.org/index-key", json={"index_key": "bad key!"})
    assert r.status_code == 422
    r = await c.post("/api/collections/ex.org/index-key", json={"index_key": "nasa_legacy", "index_name": "NASA Legacy"})
    assert r.status_code == 200, r.text

    r = await c.post("/api/collections/ex.org/index?target=test")
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["state"] == "succeeded", job
    run_id = r.json()["run_id"]
    assert export_keys(c, "ex_org", run_id) == []
    m = json.loads(c.s3.get_object(Bucket="cosmos-idx", Key=f"curated_collections/nasa_legacy/{run_id}/manifest.json")["Body"].read())
    assert m["collection_key"] == "nasa_legacy" and m["collection_name"] == "NASA Legacy"

    # moving the key afterwards invalidates that test run for prod: the export prod would publish is
    # the one that was validated, under the old key
    r = await c.post("/api/collections/ex.org/index-key", json={"index_key": "nasa_other"})
    assert (await c.get("/api/collections/ex.org")).json()["index_name"] == "ex.org"  # new key: old name not kept
    c.app.state.settings.opensearch_endpoint_prod = "https://prod.aoss"
    c.app.state.jobs._publisher = lambda: object()
    assert (await c.post("/api/collections/ex.org/index?target=prod")).status_code == 202
    job = await wait_job(c, "ex.org", timeout=30)
    assert job["state"] == "failed" and "indexed as 'nasa_legacy'" in job["error"], job
    assert "index to test again" in job["error"]
