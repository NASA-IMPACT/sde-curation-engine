"""Settings defaults that something outside the engine relies on."""

from sde_curation.config import Settings

# infra/stacks/engine_stack.py ENGINE_MAX_DB_CONNECTIONS: the DbConnections alarm fires 12 above it
ENGINE_MAX_DB_CONNECTIONS = 28


def test_the_two_pools_open_at_most_the_connections_the_alarm_expects():
    s = Settings(llm_provider="fake")

    assert s.db_pool_size + s.db_read_pool_size == ENGINE_MAX_DB_CONNECTIONS, (
        "the pool defaults changed: update ENGINE_MAX_DB_CONNECTIONS in infra/stacks/engine_stack.py"
        " (the DbConnections alarm) and here")
