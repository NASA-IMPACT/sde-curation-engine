"""The collection status rules, as pure functions.

Each function takes plain values (a Collection, counts, the action's arguments) and returns a
decision: a StatusMove to write, a refusal message for the route to send as a 409, or a yes/no.
Nothing here reads or writes the database, the request or the disk — the web layer does the I/O
before and after, so these rules can be tested on their own (tests/unit/test_rules.py)."""

from __future__ import annotations

from collections.abc import Container
from dataclasses import dataclass
from typing import Any

from .models import Collection, CurationStage, IndexRun, Status

# The statuses that have a promoted (curated) set behind them.
PROMOTED = (Status.CURATED, Status.CONFIG_GENERATED, Status.LIVE)
# The statuses before the first recompute.
PRE_CURATION = (Status.BACKLOG, Status.SCRAPED)


@dataclass(frozen=True)
class StatusMove:
    """A status change to write: `set_status(status, note=note, force=force)`, preceded by
    `set_flag(False)` (needs_recuration down) when `clear_flag`."""

    status: Status
    note: str
    force: bool = True
    clear_flag: bool = False


# ── status after a curation change ─────────────────────────────────────


def status_after_curation_change(c: Collection, total: int, curated_excluded: int,
                                 note: str | None = None) -> StatusMove | None:
    """Where a diff/pattern change leaves the collection. A change that produces delta URLs puts it
    in 'curating'. A recompute with nothing to review never demotes a curated/live collection
    (otherwise it would be stuck: nothing to promote, no way forward).

    `total`: the whole delta queue (DeltaSet.total). `curated_excluded`: how many curated URLs an
    exclude rule took out in place. `note`: what happened, for the status history (default: a
    recompute). None: the status stays as it is."""
    n = total
    pre = c.status in PRE_CURATION
    if pre and n == 0 and c.curated_rows:
        # re-crawl identical to the curated set: nothing to review
        return StatusMove(Status.CURATED, "re-crawl matches the curated URLs: no changes", clear_flag=True)
    if (pre and n) or (n and c.status in PROMOTED):
        return StatusMove(Status.CURATING, note or f"delta URLs recomputed: {n}")
    if curated_excluded and c.status in (Status.CONFIG_GENERATED, Status.LIVE):
        # an exclude rule took curated URLs out in place (no delta): the index is behind again
        k = curated_excluded
        return StatusMove(Status.CURATED, f"{k} curated URL{'s' if k != 1 else ''} excluded by rules: re-index to apply")
    if n == 0 and c.status is Status.CURATING and c.curated_rows:
        # nothing left to review on an already-promoted set → it is curated
        return StatusMove(Status.CURATED, note or "recomputed: no delta URLs")
    return None


def recompute_note(review_all: bool, n: int) -> str | None:
    """The status-history note of a recompute: a Re-curate everything says how many it queued."""
    return f"re-curating: {n} delta URLs queued for review" if review_all else None


def restarts_review_round(review_all: bool, n: int) -> bool:
    """A Re-curate everything that queued anything starts the walk-through again at exclusions and
    opens a review round (later recomputes keep the queue)."""
    return bool(review_all and n)


# ── manual status change ───────────────────────────────────────────────


def status_invariant_problem(c: Collection, new: Status) -> str | None:
    """Even a forced/manual status change must not contradict the data."""
    if new in (Status.SCRAPED, Status.CURATING) and c.dump_count == 0:
        return f"cannot be '{new}': no crawl dump yet — scrape first"
    if new in PROMOTED:
        if c.curated_rows == 0:
            return f"cannot be '{new}': nothing has been promoted to the curated set"
        if c.delta_count and c.status is not new:
            return f"cannot be '{new}': {c.delta_count} delta URLs are waiting — promote (or discard) them first"
    return None


# ── refusals ───────────────────────────────────────────────────────────


def busy_refusal(job: Any) -> str | None:
    """Mutating actions are refused while a job runs on the collection. `job`: the active job, or None."""
    if job:
        return f"{job.kind} job #{job.id} is running — wait for it or cancel it"
    return None


def recompute_refusal(c: Collection) -> str | None:
    if c.dump_count == 0:
        return "no dump ingested yet — scrape first"
    return None


def stage_refusal(c: Collection) -> str | None:
    """Stages (exclusions → metadata) exist only while curating."""
    if c.status is not Status.CURATING:
        return f"stages only apply while curating (status is {c.status})"
    return None


def metadata_stage_refusal(pending: int) -> str | None:
    """The metadata stage is gated on every pattern suggestion having been decided."""
    if pending:
        return (f"{pending} pattern suggestion{'s are' if pending != 1 else ' is'} pending"
                " — accept or reject them first")
    return None


def promote_refusal(c: Collection) -> str | None:
    """Promote (all, or the ticked URLs) only while curating. The metadata guards (blanks,
    duplicates, General division) are CurationService.promote's, raised as IncompleteMetadata."""
    if c.status is not Status.CURATING:
        return f"promote requires status 'curating' (is {c.status})"
    return None


def stale_selection_refusal(urls: list[str], found: Container[str]) -> str | None:
    """Promote the ticked URLs: every one must still be a delta URL."""
    stale = [u for u in urls if u not in found]
    if stale:
        return (f"{len(stale)} of the selected URLs are no longer delta URLs"
                f" (e.g. {stale[0]}) — reload the page and pick again")
    return None


def index_refusal(c: Collection) -> str | None:
    """Indexing needs a promoted set and an empty delta queue."""
    if c.status not in PROMOTED:
        return f"indexing requires a promoted (curated) set — status is '{c.status}'"
    if c.delta_count:
        return f"{c.delta_count} delta URLs are waiting — promote them first"
    return None


def export_refusal(export_count: int) -> str | None:
    if export_count == 0:
        return "nothing to export: every curated URL is excluded"
    return None


def prod_index_refusal(last_test_run: IndexRun | None, threshold: float) -> str | None:
    """Prod is published from the latest test run, which must have succeeded and passed validation."""
    if not last_test_run or last_test_run.state != "succeeded" or not last_test_run.validation_passes(threshold):
        return "prod indexing requires a successful, validated test run first"
    return None


def revalidate_refusal(last_run: IndexRun | None, target: str) -> str | None:
    if not last_run or last_run.state != "succeeded":
        return f"no successful {target} index run to validate"
    return None


def rename_refusal(c: Collection) -> str | None:
    """The name is locked by the first index run (test or prod, whatever its outcome)."""
    if c.last_run_id:
        return (f"'{c.name}' has been indexed (as '{c.collection_key}'), so its name can no"
                " longer change: it has to match the collection key and name it was indexed with")
    return None


def division_refusal(c: Collection) -> str | None:
    """Like the name, the division is locked by the first index run."""
    if c.last_run_id:
        return (f"'{c.name}' has been indexed (as '{c.collection_key}') with division"
                f" '{c.division}', so its division can no longer change")
    return None


def has_urls_to_recompute(c: Collection) -> bool:
    """A rename or a division change is applied to the URLs the collection already has (delta or
    curated) by a recompute; with none there is nothing to apply it to."""
    return bool(c.delta_count or c.curated_rows)


def suggest_patterns_refusal(c: Collection) -> str | None:
    if c.dump_count == 0:
        return "no crawl dump yet — scrape first"
    if c.delta_count == 0:
        return "no delta URLs — Start curating first, then suggest exclusions for the delta URLs"
    return None


def suggest_metadata_refusal(c: Collection) -> str | None:
    if c.delta_count == 0:
        return "no delta URLs — Start curating (recompute) first"
    return None


def suggest_metadata_pending_refusal(pending: int) -> str | None:
    """Exclusions first: excluded URLs are never classified, and titles depend on them."""
    if pending:
        return (f"{pending} pattern suggestion{'s are' if pending != 1 else ' is'} pending"
                " — accept or reject them before suggesting metadata")
    return None


def nothing_to_classify_refusal(count: int) -> str | None:
    if not count:
        return ("nothing to classify: every included delta URL already has suggestions"
                " — use ?all=true to redo them")
    return None


def moves_to_metadata_stage(c: Collection) -> bool:
    """Suggest metadata moves a curating collection to the metadata stage."""
    return c.status is Status.CURATING and c.curation_stage is not CurationStage.METADATA
