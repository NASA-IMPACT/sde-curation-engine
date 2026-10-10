"""Every database write that changes what a page counts must mark the collection changed.

Page counts are stored between changes (db._stored, collection_stats) and are recounted only after
a write marks the collection changed (@_touches). A writer without the mark leaves stale counts on
the page until some other write happens. This checks the code of every Database method: a method
that writes to a table the stored counts read must carry @_touches.
"""

import ast
import inspect
import re

import sde_curation.db as db_module

# The tables the stored page counts read (db._stored functions).
COUNTED = ("delta_urls", "curated_urls", "dump_urls", "patterns", "pattern_effects", "pattern_suggestions")
WRITE = re.compile(rf"\b(INSERT INTO|UPDATE|DELETE FROM|COPY)\s+({'|'.join(COUNTED)})\b")
# Writes that change nothing a count reads.
EXEMPT = {"_fill_keys": "fills canonical_key only (backfill_keys); no stored count reads it",
          "_add_pattern_suggestions": "a helper inside a transaction; its public callers carry the mark"}


def _writers() -> dict[str, list[str]]:
    tree = ast.parse(inspect.getsource(db_module))
    (cls,) = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Database"]
    out = {}
    for fn in cls.body:
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = ast.get_source_segment(inspect.getsource(db_module), fn) or ""
        if WRITE.search(body):
            out[fn.name] = [ast.unparse(d) for d in fn.decorator_list]
    return out


def test_the_check_finds_the_writers():
    writers = _writers()
    assert {"replace_deltas", "replace_curated", "insert_patterns", "set_delta_ai"} <= set(writers)


def test_every_writer_of_counted_tables_marks_the_change():
    unmarked = sorted(name for name, decos in _writers().items()
                      if name not in EXEMPT and not any(d.startswith("_touches") for d in decos))
    assert unmarked == []
