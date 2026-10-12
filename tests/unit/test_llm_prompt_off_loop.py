"""L16: building a prompt turns the page into tokens to fit it into the model's input budget. A
crawled page can be millions of characters (the ascl.net listing pages were 3.1M), and that work on
the event loop stops every other curator's request until it ends. It runs on a worker thread:
tiktoken lets other Python code run while it encodes."""

import asyncio
import random
import string
import time

from sde_curation.config import Settings
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import count_tokens, suggest_distinct_titles, suggest_metadata_one

SETTINGS = Settings(llm_provider="fake", data_dir="/tmp/unused")
BIG_PAGE_CHARS = 3_000_000  # above the 270K-token budget, so the page is counted and cut
TICK_S = 0.002
# The worst pause the loop may see, as a share of the time one encoding of the page takes on this
# machine: on the loop the pause is at least one whole encoding (share >= 1); on a thread it is a
# few milliseconds of Python around it (measured 12 ms against 150 ms on a laptop).
MAX_PAUSE_SHARE = 0.4


def big_page(seed: int) -> str:
    """Varied words, like real text: repeated text encodes several times faster than a real page."""
    rnd = random.Random(seed)
    words = ["".join(rnd.choices(string.ascii_lowercase, k=rnd.randint(2, 12))) for _ in range(20_000)]
    seps = [" ", " ", " ", ", ", ". ", "\n", "/", "-"]
    return "".join(rnd.choice(words) + rnd.choice(seps) for _ in range(BIG_PAGE_CHARS // 6))[:BIG_PAGE_CHARS]


async def worst_pause_during(work) -> float:
    """Run `work` and, beside it on the same loop, a ticker; the longest gap between ticks."""
    gaps, done = [], asyncio.Event()

    async def ticker():
        last = time.perf_counter()
        while True:  # the gap it waited through is recorded even when the work never let it run
            await asyncio.sleep(TICK_S)
            now = time.perf_counter()
            gaps.append(now - last)
            last = now
            if done.is_set():
                return

    tick = asyncio.create_task(ticker())
    await asyncio.sleep(0)  # the ticker starts its clock before the work does
    try:
        await work
    finally:
        done.set()
        await tick
    return max(gaps)


def one_encoding_s(text: str) -> float:
    count_tokens("load the tokenizer first")  # its first use loads the encoding (~90 ms): not encoding time
    t0 = time.perf_counter()
    count_tokens(text)
    return time.perf_counter() - t0


async def test_suggest_metadata_on_a_huge_page_does_not_stop_the_event_loop():
    page = big_page(1)
    encode_s = one_encoding_s(page)
    doc = {"url": "https://ex.org/big", "title": "Big", "text": page, "content_hash": "h"}

    pause = await worst_pause_during(suggest_metadata_one(FakeProvider(), doc, settings=SETTINGS))

    assert pause < MAX_PAUSE_SHARE * encode_s, (
        f"the loop stopped for {pause * 1000:.0f} ms; one encoding of the page takes {encode_s * 1000:.0f} ms")


async def test_regenerate_titles_on_huge_pages_does_not_stop_the_event_loop():
    pages = [big_page(2), big_page(3)]
    encode_s = one_encoding_s(pages[0])
    docs = [{"url": f"https://ex.org/{i}", "title": "Same", "text": p} for i, p in enumerate(pages)]

    pause = await worst_pause_during(suggest_distinct_titles(
        FakeProvider(), docs, shared_title="Same", sharing=2, settings=SETTINGS))

    assert pause < MAX_PAUSE_SHARE * encode_s, (
        f"the loop stopped for {pause * 1000:.0f} ms; one encoding of a page takes {encode_s * 1000:.0f} ms")
