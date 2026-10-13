"""Phase 4: provider contract, sanity filters, jobs, accept/reject — all offline (fake provider)."""
import asyncio

import pytest

from sde_curation.config import Settings
from sde_curation.llm.base import LLMError, make_llm
from sde_curation.llm.fake import FakeProvider
from sde_curation.llm.tasks import (
    count_tokens,
    fixed_tokens,
    share_budget,
    suggest_distinct_titles,
    suggest_metadata,
    suggest_metadata_one,
    suggest_patterns_batch,
)
from sde_curation.models import (
    Collection,
    DistinctTitles,
    Division,
    MetadataSuggestion,
    PatternSuggestions,
)

# division defaults to General = "not assigned", so the model is asked for one per page
COLL = Collection(collection_id="ex.org", name="Ex", seed_url="https://ex.org",
                  connector="crawler2", max_pages=10)


# ── pure task layer ───────────────────────────────────────────────────


async def test_suggest_patterns_keeps_only_globs_matching_the_batch_it_saw():
    canned = {"suggestions": [
        {"type": "exclude", "match": "*/privacy*", "rationale": "chrome"},
        {"type": "exclude", "match": "*/privacy*", "rationale": "dup"},
        {"type": "exclude", "match": "*/nothing-like-this*", "rationale": "hallucinated"},
        {"type": "exclude", "match": "*", "rationale": "too broad"},
        {"type": "exclude", "match": "*/login*", "rationale": "in the dump, but not in this batch"},
    ]}
    fake = FakeProvider(canned)
    kept, done = await suggest_patterns_batch(
        fake, COLL, [{"url": "https://ex.org/privacy"}, {"url": "https://ex.org/a"}],
        examples=["*/feed*"], batch_no=2, batches=3,
    )
    assert [(k.type, k.match) for k in kept] == [("exclude", "*/privacy*")]
    assert done.model == "fake" and done.tokens_in > 0
    assert "*/feed*" in fake.calls[0]["user"] and "2 of 3" in fake.calls[0]["user"]


async def test_suggest_patterns_drops_globs_the_applied_global_list_already_covers():
    canned = {"suggestions": [
        {"type": "exclude", "match": "*/login*", "rationale": "covered by */login*"},
        {"type": "exclude", "match": "*/account/*", "rationale": "only partly covered"},
    ]}
    batch = [{"url": u} for u in ("https://ex.org/login", "https://ex.org/account/login", "https://ex.org/account/me")]
    kept, _ = await suggest_patterns_batch(FakeProvider(canned), COLL, batch, examples=["*/login*"])
    assert [k.match for k in kept] == ["*/account/*"]


async def test_suggest_metadata_sends_the_collection_context():
    """A division the curator set goes along as context and is never asked for: the schema has no
    division field, the prompt says so, and the row carries no division suggestion. Without one,
    the model is asked for a division as before."""
    fake = FakeProvider()
    coll = COLL.model_copy(update={"name": "PDS", "division": Division.PLANETARY})
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "Proposers - PDS", "text": "t"},
                                     settings=SETTINGS, collection=coll)
    user = fake.calls[-1]["user"]
    assert '"collection": "PDS"' in user and '"collection_division": "Planetary Science"' in user
    assert "collection_document_type" not in user  # not set on the collection
    assert row["title"] == "Proposers"  # the model's title as written: no prefix added
    assert fake.calls[-1]["schema"] == "MetadataSuggestionNoDivision"
    assert "division" not in row and "division_conf" not in row
    assert "division — not asked for." in fake.calls[-1]["system"]
    # no division on the collection: the model decides one per page, as before
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "text": "t"}, settings=SETTINGS, collection=COLL)
    assert "collection_division" not in fake.calls[-1]["user"]
    assert fake.calls[-1]["schema"] == "MetadataSuggestion" and row["division"]


async def test_suggest_patterns_only_accepts_exclude_globs():
    for bad in (
        {"type": "division", "match": "*", "value": "Heliophysics", "rationale": "x"},
        {"type": "title", "match": "*", "value": "{title}", "rationale": "x"},
        {"type": "include", "match": "*/keep*", "rationale": "x"},
    ):
        with pytest.raises(LLMError, match="did not match PatternSuggestions"):
            await suggest_patterns_batch(FakeProvider({"suggestions": [bad]}), COLL, [{"url": "https://ex.org/a"}])
SETTINGS = Settings(llm_provider="fake", data_dir="/tmp/x")


async def test_suggest_metadata_is_one_call_per_document_with_full_text_and_confidence():
    fake = FakeProvider()
    docs = [{"url": f"https://ex.org/p{i}", "title": f"Aurora page {i} – Ex", "text": "some data " * 500}
            for i in range(45)]
    seen = []
    rows = await suggest_metadata(fake, docs, settings=SETTINGS,
                                  on_progress=lambda p: (seen.append(p), asyncio.sleep(0))[1])
    assert len(fake.calls) == 45 and seen[-1] == {"done": 45, "total": 45}
    assert "some data " * 500 in fake.calls[0]["user"]  # the whole text, not a 1200-char slice
    assert len(rows) == 45 and rows[0]["title"] == "Aurora page 0" and rows[0]["division"] == "Heliophysics"
    assert rows[0]["title_conf"] == "high" and rows[0]["division_conf"] == "high"  # "aurora" in the title
    assert rows[0]["document_type"] == "Data" and rows[0]["document_type_conf"] == "medium"  # "data" only in text
    assert rows[0]["model"] == "fake" and rows[0]["tokens_in"] > 0


def _prompt_tokens(call, schema=MetadataSuggestion):
    """What the API would count for a fake call: system + user + response schema + framing."""
    return fixed_tokens(call["system"], schema) + count_tokens(call["user"])


async def test_suggest_metadata_one_sends_a_page_that_fits_whole_and_records_the_model():
    fake = FakeProvider()
    page = "".join(f"paragraph {i} " for i in range(30_000))  # ~390K chars, ~60K tokens: fits
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": page,
                                            "content_hash": "h"}, settings=SETTINGS)
    assert fake.calls[-1]["model"] is None and row["model"] == "fake"  # the provider's default model
    assert fake.calls[-1]["user"].endswith(page) and '"text_cut"' not in fake.calls[-1]["user"]
    assert row["content_hash"] == "h" and "truncated" not in row and "large_model" not in row


async def test_suggest_metadata_one_cuts_a_page_to_fit_the_input_limit():
    """gpt-5-nano refuses a prompt over 272K tokens; a bigger page (ascl.net's listing URLs are
    ~780K) is cut from the end so the WHOLE prompt fits llm_max_input_tokens, and the model is told."""
    fake = FakeProvider()
    huge = "".join(f"paragraph {i} " for i in range(150_000))  # ~2.3M chars, ~450K tokens
    await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge,
                                      "content_hash": "h"}, settings=SETTINGS)
    call = fake.calls[-1]
    assert f'"text_chars": {len(huge)}, "text_cut": true' in call["user"]
    assert "Text:\nparagraph 0 paragraph 1 " in call["user"] and "paragraph 149999" not in call["user"]
    assert 269_000 < _prompt_tokens(call) <= SETTINGS.llm_max_input_tokens  # uses the room, stays under it
    await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge, "content_hash": "h"},
                               settings=Settings(llm_provider="fake", data_dir="/tmp/x", llm_max_input_tokens=50_000))
    assert _prompt_tokens(fake.calls[-1]) <= 50_000


def test_share_budget_cuts_only_the_biggest_pages():
    assert share_budget([10, 20, 30], 100) == [10, 20, 30]  # all fit
    assert share_budget([10, 500, 900], 410) == [10, 200, 200]  # the small one whole, the rest even
    assert share_budget([10, 100, 900], 410) == [10, 100, 300]
    assert sum(share_budget([7, 1_000, 3_000, 2], 999)) <= 999


async def test_suggest_distinct_titles_cuts_the_longest_pages_to_fit():
    fake = FakeProvider()
    small = "Volume 12 of the calibrated images. " * 50
    huge = "".join(f"row {i} " for i in range(250_000))  # ~500K tokens
    docs = [{"url": "https://ex.org/v12", "title": "Vol", "text": small},
            {"url": "https://ex.org/all", "title": "Vol", "text": huge}]
    await suggest_distinct_titles(fake, docs, shared_title="Vol", sharing=2, settings=SETTINGS)
    call = fake.calls[-1]
    assert _prompt_tokens(call, DistinctTitles) <= SETTINGS.llm_max_input_tokens
    assert small in call["user"] and "row 249999" not in call["user"]  # the small page whole
    assert f'"text_chars": {len(small)}}}' in call["user"] and f'"text_chars": {len(huge)}, "text_cut": true' in call["user"]


async def test_a_prompt_the_provider_still_refuses_as_too_long_is_cut_and_asked_again():
    """The local count is the GPT-5 tokenizer's; if the provider still says too long, the call is
    re-sent once, shorter by what its error message counted over the limit."""
    from sde_curation.llm.base import LLMInputTooLong

    class Strict(FakeProvider):
        async def complete(self, *, system, user, schema, model=None):
            if not self.calls:
                self.calls.append({"user": user})
                raise LLMInputTooLong("too long", got=300_000, limit=272_000)
            return await super().complete(system=system, user=user, schema=schema, model=model)

    fake = Strict()
    huge = "".join(f"paragraph {i} " for i in range(150_000))
    row = await suggest_metadata_one(fake, {"url": "https://ex.org/a", "title": "A", "text": huge,
                                            "content_hash": "h"}, settings=SETTINGS)
    first, second = count_tokens(fake.calls[0]["user"]), count_tokens(fake.calls[1]["user"])
    assert first - second >= 28_000 and row["title"]  # cut by the 28K over (+2%), and answered


async def test_schemas_reject_bad_enums():
    from pydantic import ValidationError

    ok = {"title_confidence": "high", "division_confidence": "low", "document_type_confidence": "low"}
    full = {"title": "T", "division": "Earth Science", "document_type": "Data", **ok}
    MetadataSuggestion.model_validate(full)
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "division": "Kitchen"})
    # "General" is not a division a page can be curated into, so it is not an answer the model
    # can give: it is absent from the schema the provider is handed, not merely rejected after
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "division": "General"})
    assert "General" not in str(MetadataSuggestion.model_json_schema())
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({"title": "T", "division": "Earth Science", "document_type": "Data"})  # confidence per field
    # every page gets every field: none may be missing or null
    for field in ("title", "division", "document_type"):
        with pytest.raises(ValidationError):
            MetadataSuggestion.model_validate({k: v for k, v in full.items() if k != field})
        with pytest.raises(ValidationError):
            MetadataSuggestion.model_validate({**full, field: None})
        schema = MetadataSuggestion.model_json_schema()
        assert field in schema["required"] and "null" not in str(schema["properties"][field])
    with pytest.raises(ValidationError):
        MetadataSuggestion.model_validate({**full, "title_confidence": "certain"})
    with pytest.raises(ValidationError):
        PatternSuggestions.model_validate({"suggestions": [{"type": "title", "match": "*", "rationale": "no value"}]})
    PatternSuggestions.model_validate({"suggestions": []})


def test_registry_and_missing_key(settings):
    assert make_llm(settings).name == "fake"
    with pytest.raises(LLMError, match="OPENAI_API_KEY"):
        make_llm(settings.model_copy(update={"llm_provider": "openai", "openai_api_key": None}))


# ── through the app (fake crawler + fake LLM) ─────────────────────────




    # nothing applied yet
    # accept → real exclude rule, recomputed: p9 is out of scope, so it can never reach the index
    # reject leaves nothing behind


    # global list hits come first, with match counts over the whole dump (http + https variants)
    # the model's own rows: it also proposed */login* and */tag* (chrome segments) — the global row
    # wins, and a model glob whose URLs the global list already covers is dropped


    # accept title → exact-URL pattern; ml cleared
    # reject doc type → cleared, effective untouched
    # second run: URLs still missing suggestions only → p2 (cleared) is the only candidate again
    # now nothing is left → 409 up front, no failed job










    # the next run only picks up the one that failed, and a success clears the error






    # an answer from before document_type was required: a title, no type, but a confidence for it
    # a type the SME dismissed is not re-asked


async def test_openai_provider_sends_temperature_only_when_configured():
    """gpt-5 / o-series reject any temperature but the default (400 unsupported_value): the
    request must omit it unless LLM_TEMPERATURE is set explicitly."""
    from types import SimpleNamespace

    from sde_curation.llm.openai import OpenAIProvider

    class Stub:
        def __init__(self):
            self.calls = []
            self.chat = SimpleNamespace(completions=SimpleNamespace(parse=self.parse))

        async def parse(self, **kw):
            self.calls.append(kw)
            answer = MetadataSuggestion(title="T", title_confidence="high", division="Earth Science", division_confidence="low", document_type="Data",
                                        document_type_confidence="low")
            msg = SimpleNamespace(parsed=answer, refusal=None, content=None)
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=self.usage)

        usage = None

    async def run(**over):
        p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", **over))
        p.client = Stub()
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
        return p.client.calls[0]

    assert "temperature" not in await run()
    assert (await run(llm_temperature=0))["temperature"] == 0

    # reasoning effort and service tier go only when set; the output budget always goes
    call = await run()
    assert "reasoning_effort" not in call and "service_tier" not in call
    assert call["max_completion_tokens"] == 4_000
    call = await run(llm_reasoning_effort="low", llm_service_tier="flex", llm_max_completion_tokens=900)
    assert (call["reasoning_effort"], call["service_tier"], call["max_completion_tokens"]) == ("low", "flex", 900)

    # page text is unique per call: only the system prompt may be written to the prompt cache
    call = await run()
    assert call["prompt_cache_options"] == {"mode": "explicit"}
    sys_msg, user_msg = call["messages"]
    assert sys_msg["content"] == [{"type": "text", "text": "s", "prompt_cache_breakpoint": {"mode": "explicit"}}]
    assert user_msg == {"role": "user", "content": "u"}
    call = await run(openai_prompt_cache="provider_default")
    assert "prompt_cache_options" not in call and call["messages"][0]["content"] == "s"


async def test_openai_provider_records_cache_writes_and_alarms_when_page_text_is_written(caplog):
    """Cache writes are billed at 1.25× input on gpt-5.6+: they are counted per call, and a call that
    wrote more than its system prompt (page text in the cache again) logs an error."""
    from types import SimpleNamespace

    from sde_curation.llm.openai import OpenAIProvider

    system = "x" * 8_000  # ~2k tokens, like the real system prompts
    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai"))

    async def answer(written):
        usage = SimpleNamespace(prompt_tokens=50_000, completion_tokens=10, prompt_tokens_details=SimpleNamespace(
            cached_tokens=0, cache_write_tokens=written))
        msg = SimpleNamespace(parsed=MetadataSuggestion(
            title="T", title_confidence="high", division="Earth Science", division_confidence="low",
            document_type="Data", document_type_confidence="low"), refusal=None, content=None)

        async def parse(**kw):
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=usage)
        p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
        return await p.complete(system=system, user="u", schema=MetadataSuggestion)

    caplog.clear()
    assert (await answer(1_848)).tokens_cache_write == 1_848  # the system prompt: expected
    assert not [r for r in caplog.records if r.levelname == "ERROR"]
    assert (await answer(48_000)).tokens_cache_write == 48_000  # the page too: alarm
    assert any("wrote 48000 prompt tokens to the cache" in r.getMessage() for r in caplog.records)
    # ← the engine went down with the job running

        # a job the curator cancelled stays cancelled across a restart


async def test_openai_provider_asks_again_when_the_output_budget_runs_out(caplog):
    """A reasoning model can spend the whole `max_completion_tokens` thinking and never write the
    JSON (the SDK raises LengthFinishReasonError, seen live on gpt-5.6-luna 2026-09-25). The call is
    asked once more with 4× the budget; both calls are billed, so both count. If that is cut too,
    the page fails with an LLMError (recorded on its row) instead of an uncaught SDK error."""
    from types import SimpleNamespace

    from openai import LengthFinishReasonError

    from sde_curation.llm.base import LLMError
    from sde_curation.llm.openai import OpenAIProvider

    def usage(out, reasoning):
        return SimpleNamespace(prompt_tokens=3_000, completion_tokens=out,
                               prompt_tokens_details=SimpleNamespace(cached_tokens=1_873, cache_write_tokens=0),
                               completion_tokens_details=SimpleNamespace(reasoning_tokens=reasoning))

    def provider(cut_calls):
        budgets = []

        async def parse(**kw):
            budgets.append(kw["max_completion_tokens"])
            if len(budgets) <= cut_calls:
                raise LengthFinishReasonError(completion=SimpleNamespace(usage=usage(kw["max_completion_tokens"],
                                                                                     kw["max_completion_tokens"])))
            msg = SimpleNamespace(parsed=MetadataSuggestion(
                title="T", title_confidence="high", division="Earth Science", division_confidence="low",
                document_type="Data", document_type_confidence="low"), refusal=None, content=None)
            return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=usage(90, 40))

        p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", llm_max_completion_tokens=500))
        p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
        return p, budgets

    p, budgets = provider(cut_calls=1)
    done = await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert done.parsed.title == "T" and budgets == [500, 2_000]
    assert (done.tokens_in, done.tokens_out, done.tokens_reasoning, done.tokens_cached) == (6_000, 590, 540, 3_746)
    assert any("output cut at 500 of 500 tokens" in r.getMessage() for r in caplog.records)

    p, budgets = provider(cut_calls=2)
    with pytest.raises(LLMError, match="did not fit in 2,000 output tokens"):
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert budgets == [500, 2_000]


@pytest.mark.parametrize("param", ["prompt_cache_options", "prompt_cache_breakpoint"])
async def test_openai_provider_drops_cache_options_on_a_model_that_rejects_them(param):
    """Models before gpt-5.6 answer 400 to the cache options (they have no cache-write charge):
    the call is re-sent without them, and that model is never sent them again. gpt-5-nano names
    `prompt_cache_breakpoint` in its 400 (live, 2026-09-25)."""
    from types import SimpleNamespace

    import httpx
    from openai import BadRequestError

    from sde_curation.llm.openai import OpenAIProvider

    calls = []

    async def parse(**kw):
        calls.append(kw)
        if "prompt_cache_options" in kw:
            raise BadRequestError(f"{param} is not supported on this model",
                                  response=httpx.Response(400, request=httpx.Request("POST", "https://x")),
                                  body={"message": "not supported", "param": param})
        msg = SimpleNamespace(parsed=MetadataSuggestion(
            title="T", title_confidence="high", division="Earth Science", division_confidence="low",
            document_type="Data", document_type_confidence="low"), refusal=None, content=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=msg)], model=kw["model"], usage=None)

    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", openai_model="gpt-5-nano"))
    p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
    assert (await p.complete(system="s", user="u", schema=MetadataSuggestion)).parsed.title == "T"
    assert ["prompt_cache_options" in c for c in calls] == [True, False]
    assert calls[1]["messages"][0] == {"role": "system", "content": "s"}
    await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert len(calls) == 3 and "prompt_cache_options" not in calls[2]  # remembered: one call, no 400


async def test_openai_provider_reports_a_too_long_prompt_with_its_token_counts():
    from types import SimpleNamespace

    import httpx
    from openai import BadRequestError

    from sde_curation.llm.base import LLMInputTooLong
    from sde_curation.llm.openai import OpenAIProvider

    async def parse(**kw):
        raise BadRequestError(
            "Error code: 400 - Input tokens exceed the configured limit of 272000 tokens. Your messages"
            " resulted in 300010 tokens. Please reduce the length of the messages.",
            response=httpx.Response(400, request=httpx.Request("POST", "https://x")),
            body={"message": "…", "param": "messages", "code": "context_length_exceeded"})

    p = OpenAIProvider(Settings(openai_api_key="k", llm_provider="openai", openai_prompt_cache="provider_default"))
    p.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(parse=parse)))
    with pytest.raises(LLMInputTooLong) as e:
        await p.complete(system="s", user="u", schema=MetadataSuggestion)
    assert (e.value.limit, e.value.got) == (272_000, 300_010)
