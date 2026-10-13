"""End-to-end tests: whole workflows with the processes the engine drives for real: the crawler's
`run.py` and the indexer as subprocesses, S3 as a moto server, engine shutdowns and restarts.
PostgreSQL with empty tables for every test."""

import pytest


@pytest.fixture(autouse=True)
def _database(fresh_database):
    return fresh_database
