"""The collection_key everything is indexed under is the collection name slugified — the same rule
COSMOS gives its config_folder — not the engine's seed-derived collection_id."""
from sde_curation.backends.index import indexer_command
from sde_curation.engine.export import build_manifest
from sde_curation.models import Collection, Division, collection_key_from_name


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
