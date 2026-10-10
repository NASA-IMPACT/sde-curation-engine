"""Integration tests: the engine's parts together against a real PostgreSQL (empty tables for every
test). Crawls run the fake crawler in-process (tests/support/crawler.py InProcessCrawler): the
documents reach the database through the engine's own ingest, without a subprocess."""

import pytest

from tests.support.crawler import InProcessCrawler


@pytest.fixture(autouse=True)
def _database(fresh_database):
    return fresh_database


@pytest.fixture(autouse=True)
def _in_process_crawler(monkeypatch):
    import sde_curation.web.app as web_app

    monkeypatch.setattr(web_app, "make_scrape_backend", lambda settings: InProcessCrawler(settings))
