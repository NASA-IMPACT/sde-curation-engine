"""Login with local accounts: off by default; APP_PASSWORD seeds the bootstrap admin; sessions
die when a user is disabled or changes password; admins manage users, curators cannot."""

from sde_curation.web import auth

    # root2 is the last active admin now → nobody may demote or disable it


def test_sign_verify_pure():
    t = auth.sign("k", 7, 3, 1_000)
    assert auth.verify("k", t, now=999) == (7, 3) and auth.verify("k", t, now=1_000) is None
    assert auth.verify("k", "garbage") is None and auth.verify("k", None) is None
    assert auth.verify("other", t, now=999) is None


def test_hash_verify_password():
    h = auth.hash_password("correct horse")
    assert h.startswith("scrypt$14$8$1$") and h != auth.hash_password("correct horse")  # random salt
    assert auth.verify_password(h, "correct horse") and not auth.verify_password(h, "wrong")
    assert not auth.verify_password("garbage", "x") and not auth.verify_password("", "x")


async def test_a_role_change_during_a_login_lookup_is_seen_at_once():
    """The login check caches the user row for 30 s, and a role change clears the cache entry. A
    lookup that read the row just before the change must not put the old row back in the cache."""
    import asyncio

    from sde_curation.db import Database
    from sde_curation.models import Role, User

    db = Database("postgresql://unused")  # no connection: get_user is replaced below
    stored = {"role": Role.ADMIN}
    reading, release = asyncio.Event(), asyncio.Event()

    async def slow_get_user(uid):
        row = User(id=uid, username="bob", password_hash="x", role=stored["role"])  # read before the change
        reading.set()
        await release.wait()
        return row

    db.get_user = slow_get_user
    lookup = asyncio.create_task(db.session_user(7))
    await reading.wait()
    stored["role"] = Role.CURATOR  # an admin demotes bob: set_role commits, then clears the cache entry
    db._forget_session_user(7)
    release.set()
    await lookup

    async def get_user(uid):
        return User(id=uid, username="bob", password_hash="x", role=stored["role"])

    db.get_user = get_user
    assert (await db.session_user(7)).role is Role.CURATOR
