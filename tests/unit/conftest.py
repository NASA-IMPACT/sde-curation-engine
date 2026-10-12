"""Unit tests: one function or class at a time. No database, no subprocess, no network: anything
that reaches for one fails at once with a pointer to the right level."""

import asyncio
import socket
import subprocess

import psycopg
import pytest


def _refuse(what: str):
    def refuse(*_a, **_k):
        raise RuntimeError(f"a unit test must not {what}: put it in tests/integration or tests/e2e")
    return refuse


@pytest.fixture(autouse=True)
def no_outside_world(monkeypatch):
    monkeypatch.setattr(psycopg.Connection, "connect", classmethod(_refuse("open a database connection")))
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", classmethod(_refuse("open a database connection")))
    monkeypatch.setattr(psycopg, "connect", _refuse("open a database connection"))
    monkeypatch.setattr(asyncio, "create_subprocess_exec", _refuse("start a subprocess"))
    monkeypatch.setattr(subprocess, "Popen", _refuse("start a subprocess"))
    original_connect = socket.socket.connect

    def connect(self, address):
        if self.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError("a unit test must not open a network connection: put it in tests/integration or tests/e2e")
        return original_connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", connect)
    # A boto3 client resolves credentials when it is created. With none set (a CI runner) botocore
    # asks the instance metadata service, a network call; with a developer's own set it could reach
    # AWS. Fake ones, and no metadata lookup, make every machine behave the same.
    for name, value in (("AWS_ACCESS_KEY_ID", "testing"), ("AWS_SECRET_ACCESS_KEY", "testing"),
                        ("AWS_SESSION_TOKEN", "testing"), ("AWS_EC2_METADATA_DISABLED", "true")):
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
