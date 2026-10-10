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
