"""The test suite in three levels (the testing pyramid):

- tests/unit         one function or class, no database, no subprocess, no network. Seconds in all.
- tests/integration  the engine's parts working together against a real PostgreSQL: the API, the
                     database layer, jobs with the fake LLM and an in-process fake crawler.
- tests/e2e          whole user workflows with the outside processes the engine drives: the
                     crawler and indexer as real subprocesses, S3 (moto server), engine restarts.

Each test gets the marker of its directory (`-m unit`, `-m integration`, `-m e2e`); a test file
outside the three directories is an error. `make test-unit`, `make test-integration`,
`make test-e2e`; `make test` runs all three in that order.
"""

from pathlib import Path

import pytest

from sde_curation.config import Settings

# Tests must not depend on the developer's local .env (real bucket, instance, AOSS endpoints,
# scrape backend…): every Settings() built while the suite runs ignores the env file.
Settings.model_config["env_file"] = None

pytest_plugins = ["tests.support.fixtures", "tests.support.aws"]

LEVELS = ("unit", "integration", "e2e")
TESTS = Path(__file__).parent
UNIT_TIMEOUT_S = 5  # a unit test that takes longer is doing integration work


def pytest_collection_modifyitems(config, items):
    for item in items:
        level = Path(str(item.fspath)).resolve().relative_to(TESTS).parts[0]
        if level not in LEVELS:
            raise pytest.UsageError(f"{item.nodeid}: every test belongs in tests/unit, tests/integration or tests/e2e")
        item.add_marker(getattr(pytest.mark, level))
        if level == "unit":
            item.add_marker(pytest.mark.timeout(UNIT_TIMEOUT_S))
