"""Who did it, in the places outside the database that say so: the status notification and the
git-trackable collection.yaml and patterns.yaml (sde_curation/notify.py, sde_curation/store.py)."""

import yaml

from sde_curation.models import (
    SYSTEM_ACTOR,
    Collection,
    ConnectorType,
    Pattern,
    PatternType,
    Status,
    StatusHistory,
)
from sde_curation.notify import Notifier
from sde_curation.store import write_collection_yaml, write_patterns_yaml

CID = "ex.org"
CREATOR, CURATOR = "admin", "alice"


async def test_a_status_notification_names_who_made_the_move():
    n = Notifier(None)  # no webhook: the payload is only recorded

    await n.status_changed(CID, "backlog", "scraped", "scrape ok: 8 documents", SYSTEM_ACTOR)

    assert n.sent[0]["actor"] == SYSTEM_ACTOR
    assert n.sent[0]["text"].split("\n")[0] == f"*{CID}*: backlog → *scraped* — scrape ok: 8 documents (by system)"


def test_collection_yaml_records_the_creator_and_every_status_move_with_its_actor(tmp_path):
    c = Collection(collection_id=CID, name="Ex", seed_url=f"https://{CID}", connector=ConnectorType.CRAWLER,
                   max_pages=10, created_by=CREATOR)
    history = [StatusHistory(collection_id=CID, old_status=None, new_status=Status.BACKLOG, note="created",
                             actor=CREATOR),
               StatusHistory(collection_id=CID, old_status=Status.BACKLOG, new_status=Status.SCRAPED, actor=SYSTEM_ACTOR),
               StatusHistory(collection_id=CID, old_status=Status.SCRAPED, new_status=Status.CURATING, actor=CURATOR)]

    data = yaml.safe_load(write_collection_yaml(tmp_path, c, history).read_text())

    assert data["created_by"] == CREATOR
    assert [(h["new_status"], h["actor"]) for h in data["history"]] == [
        ("backlog", CREATOR), ("scraped", SYSTEM_ACTOR), ("curating", CURATOR)]


def test_patterns_yaml_records_who_added_each_rule(tmp_path):
    rules = [Pattern(id=1, collection_id=CID, type=PatternType.EXCLUDE, match="*/p1", created_by=CURATOR),
             Pattern(id=2, collection_id=CID, type=PatternType.TITLE, match=f"https://{CID}/p3", value="T3",
                     created_by=CREATOR)]

    data = yaml.safe_load(write_patterns_yaml(tmp_path, CID, rules).read_text())

    assert [(r["match"], r["created_by"]) for r in data] == [("*/p1", CURATOR), (f"https://{CID}/p3", CREATOR)]
