"""LLM-assisted curation tasks. Every result is Pydantic-validated, then *sanity-checked
against the collection* (a suggested URL must be one we sent; a pattern must match something)
before anything is written. Suggestions never touch effective fields — only *_ai / pending rows."""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable, Iterable
from typing import Any
from urllib.parse import parse_qsl, unquote, urlsplit

from ..config import Settings
from ..engine.patterns import glob_to_regex
from ..models import (
    Collection,
    DistinctTitles,
    MetadataSuggestion,
    MetadataSuggestionNoDivision,
    PatternSuggestion,
    PatternSuggestions,
    division_assigned,
)
from .base import Completion, LLMError, LLMInputTooLong, LLMProvider

ProgressCb = Callable[[dict[str, Any]], Awaitable[None]]

DOCUMENT_TYPE_DEFINITIONS = """- Data: resources that describe, provide access to, or support the use of scientific datasets
  produced from observations, experiments, models, or missions — raw, processed or derived data
  such as measurements, imagery and spectra. In the SDE, Data is represented through metadata
  and links to dataset landing pages or other access points; the data files themselves are not
  indexed.
- Software and Tools: webpages and metadata describing or providing access to software,
  applications, tools, algorithms and code developed to analyze, process, visualize, model,
  calculate or otherwise manipulate scientific data or support scientific research — command-line
  tools, GUI applications, numerical and computational models, model code and model interfaces.
  Includes project webpages, software and code repositories, and metadata about software.
- Documentation: written materials that provide information about scientific concepts, data
  products, software and tools, scientific methodologies or other scientific resources — manuals,
  guides, specifications, reports and technical documentation giving instructions, technical
  details, background or methodological context. Examples: Algorithm Theoretical Basis Documents
  (ATBDs), user guides, technical reports, data product documentation.
- Missions and Instruments: information about specific scientific missions, spacecraft, aircraft,
  instruments and payloads of NASA's Science Mission Directorate (SMD) — mission objectives,
  scientific goals, timelines, spacecraft and instrument capabilities, and related activities.
- Images: webpages and resources that contain, describe or provide access to scientific or
  educational visual content — photographs, videos, maps, diagrams, charts, illustrations and
  other graphics, depicting celestial objects, planetary surfaces including Earth, astronomical or
  atmospheric phenomena, spacecraft, instruments, laboratories, astronauts or scientists."""

DIVISION_DEFINITIONS = """- Astrophysics: the universe beyond the solar system — stars, galaxies, exoplanets, black holes,
  cosmology, dark matter and dark energy (e.g. Hubble, Webb, Chandra, TESS).
- Biological and Physical Sciences: life and physical science research in the space environment —
  space biology, microgravity fluids, combustion and materials, fundamental physics, ISS research
  (e.g. GeneLab, the Open Science Data Repository).
- Earth Science: Earth as a system — atmosphere, oceans, land, ice, biosphere, climate, weather
  and natural hazards (e.g. Terra, Aqua, Landsat, Earthdata and the DAACs).
- Heliophysics: the Sun and its influence through the solar system — solar activity, solar wind,
  space weather, magnetospheres, the ionosphere and aurora (e.g. SDO, Parker Solar Probe).
- Planetary Science: planets, moons, asteroids, comets and meteorites of the solar system,
  planetary defense and astrobiology (e.g. Mars rovers, Cassini, the Planetary Data System).
There is no "general", "other" or "unclear" division: every page belongs to one of the five. A page
that spans several — agency-wide science policy, cross-division education — takes the division it
serves most, and a page whose division you cannot tell takes the most likely one with low
confidence, which a reviewer checks."""

PATTERN_SYSTEM = f"""You help curate web crawls for NASA's Science Discovery Engine (SDE), a search engine over
NASA science content used by scientists, educators and the public. You are given one batch of
crawled URLs (with their scraped titles) from one collection, sorted by path. Propose exclude
globs for URLs that must NOT appear as SDE search results.

The default is to KEEP a page. A wrong exclude silently hides science content, so exclude only
what is plainly worthless as a search result. An empty list is a correct answer. Batches of the
same collection are processed separately and re-run later: apply the rules below literally so
the same URLs always give the same globs.

Always keep — never write a glob that matches any of these:
- Anything in the SDE's five document types (definitions below): dataset landing pages, archive
  volumes and data directories; software, tools, models and code pages; all documentation,
  including help, user guides, FAQs, tutorials, how-to pages, glossaries, standards, format
  specifications, and information for proposers or data providers; mission and instrument pages;
  image galleries and image or video pages.
- Search, query, browse and discovery interfaces themselves (the form or tool page a user starts
  from), data portals and viewers.
- Science news and feature articles, and pages about the project or site (about, overview, team,
  its nodes or sub-projects).

Exclude only these categories:
1. Account pages: sign-in, sign-up, logout, profile, password and session pages.
2. Search results: a search or filter already run, recognizable by parameters holding the terms
   (?q=, ?query=, ?keyword=, ?search=). The same search page without them is a tool: keep it.
3. Duplicate URLs of a page kept elsewhere: print views, share links, sort, order and view-mode
   variants, session or tracking parameters (utm_*, sessionid, jsessionid), and pages 2+ of a
   paginated listing.
4. Archive listings that only link to other pages: tag, category, author and date archives.
5. Machine formats and endpoints: feeds, JSON or XML API responses, sitemaps, CMS administration.
6. Site chrome: privacy, cookie, terms-of-use and accessibility statements, contact forms, error
   pages.
Anything outside these six categories stays, even if it looks low value.

Writing globs:
- * is the only wildcard and a glob must match the WHOLE URL. Never a bare "*".
- Host-agnostic: */login* or */catalog?sort=*, not https://host/login — the same page appears
  under http:// and https:// and with or without a trailing slash.
- Canonical forms, one glob per path segment, parameter or extension: everything under a
  segment → */segment/*; a segment that ends the URL → */segment*, only when no kept URL starts
  with the same letters; a query parameter → *?param=* (add *&param=* only when it also appears
  after another parameter); a file extension → *.ext. Do not combine or nest them.
- Check every glob against every URL in the batch: it must match at least one URL (globs that
  match nothing are discarded) and must not match any page you would keep.
- `global_excludes_already_applied` lists globs the engine applies already: do not repeat them,
  and do not propose a glob whose URLs they already cover.
- Rationale: one sentence starting with the category name, e.g. "Search results: /search?q=
  URLs are result lists of pages indexed at their own URLs."

The SDE document types:
{DOCUMENT_TYPE_DEFINITIONS}"""

# Shared by both prompts that write titles, so a re-title follows the same rules as the first answer.
TITLE_RULES = """- Descriptive and self-contained, typically 4–12 words: the page's real subject ("Information for
  Data Proposers", "Real-Time Geomagnetic Storm Tracker").
- Do not add the collection or site name to the title, as a prefix or a suffix ("PDS: …",
  "… | Aurorasaurus"). A mission, instrument, project or dataset name belongs in the title only
  when the page is about it ("Cassini ISS Calibrated Images"); keep such names and acronyms as the
  page writes them.
- Do not copy the site-wide scraped title; no slogans, "Welcome to" or "Home Page"."""

_METADATA_INTRO = f"""You classify one crawled web page at a time for NASA's Science Discovery Engine (SDE), a
search engine over NASA science content. You receive the collection (the website the page was
crawled from), the page URL, its scraped title and its full text (possibly long). The scraped
title is often the same site-wide string on every page, and the text usually starts with the
site's navigation menu, alerts and login links: skip that chrome and read the page's own content.
A page too long to send whole is cut from the end: `text_cut` is then true and `text_chars` is the
length of the whole page, so judge it from the part you have (a huge page is usually a listing).

Every page of a collection is classified in a separate call, and the answers must agree with
each other: apply the rules below the same way every time.

title — a free-form title for this page as it should read in a search result, judged by someone
who has not seen the site.
{TITLE_RULES}
- Never empty: a page with little content of its own still gets the best title its URL, scraped
  title and text support, with low confidence."""

_METADATA_DIVISION = f"""
division — the NASA Science Mission Directorate division, one of:
{DIVISION_DEFINITIONS}
Judge from the page's subject read in the context of the whole collection: the pages of one site
nearly always share a division, so a help page, data-format guide or proposer page on a planetary
data archive is Planetary Science like the rest of the archive.
"""

# What replaces the division section when the curator gave the collection a division: it is theirs,
# it is already on every page, and no division is asked for (the schema has no division field).
_METADATA_DIVISION_FIXED = """
division — not asked for. A subject-matter expert set `collection_division` for the whole
collection and it is already applied to this page; do not classify or comment on it. Read it as
context for the two fields below: the pages of one site nearly always belong to that division, so
judge the page as part of it.
"""

_METADATA_TYPE_AND_CONFIDENCE = f"""
document_type — exactly one of the five SDE document types, for every page; never null, because
the SDE cannot index a page without one:
{DOCUMENT_TYPE_DEFINITIONS}
Classify by the page's primary purpose — what a searcher would come to it for — not by incidental
elements: a page with a photo is not Images, a page with a download link is not Data. Between
close types:
- Gives access to a dataset or data holdings (dataset landing page, archive volume, data search
  or browse interface) → Data.
- A program, model, library or web application that analyzes, processes, visualizes or models
  data, or the page describing or distributing it → Software and Tools.
- Explains, specifies or instructs about data, software, missions or methods (user guides, ATBDs,
  help and FAQ pages, format standards, submission or proposal guidelines) → Documentation, even
  when it links to what it describes.
- An overview of a mission, spacecraft, aircraft, instrument or payload → Missions and
  Instruments, even when it shows pictures.
- A gallery, or an image, video or map page whose purpose is the visual itself → Images.
- A page that fits none of these well (news, feature articles, events, people, about or
  organization pages) → the type of what it is mainly about: a mission news story → Missions and
  Instruments, a feature on a dataset → Data, a tool announcement → Software and Tools; when it is
  about none of those → `collection_document_type` when given, otherwise Documentation. Its
  confidence is low.
When `collection_document_type` is given, an expert set it as the collection's usual type: use it
when the page fits it, but a page that clearly fits another type gets that type.

Confidence, one per field you answer:
- high: stated in the page text or title, or follows directly from the rules above (a dataset
  landing page is Data; a user guide is Documentation).
- medium: a reasonable inference where another answer is also defensible.
- low: a guess. No field is ever null or empty: when unsure, give the most likely value with low
  confidence — a reviewer checks every low-confidence answer.
Never invent facts that are not in the input."""


def metadata_system(ask_division: bool = True) -> str:
    """The Suggest metadata prompt. `ask_division` is False when the curator gave the collection a
    division: the division section becomes "already decided, here for context" and the answer
    schema drops the field, so the model neither spends tokens on it nor contradicts the SME."""
    middle = _METADATA_DIVISION if ask_division else _METADATA_DIVISION_FIXED
    return _METADATA_INTRO + "\n" + middle + _METADATA_TYPE_AND_CONFIDENCE


METADATA_SYSTEM = metadata_system()

TITLES_SYSTEM = f"""You write search-result titles for NASA's Science Discovery Engine (SDE), a search engine over
NASA science content. Several pages of one collection (the website they were crawled from) ended
up with the same title and the same document type, so a list of search results cannot tell them
apart. You receive the WHOLE GROUP in one call and rewrite it in one go: the collection, the title
and document type they share (the type stays as it is: you only write titles), and for every page
you must retitle its URL, its scraped title and its FULL text (a page too long to send whole is
cut from the end, with `text_cut` true and `text_chars` the whole length). The text usually starts with the
site's navigation menu, alerts and login links: skip that chrome and read the page's own content.

Also given:
- `settled_titles` — pages of the same group whose titles will NOT change. Your titles must differ
  from these too.
- `url_differs_at` — for each page, the parts of its URL that the other pages of the group do not
  have, worked out for you. When the text is thin this is usually what tells the pages apart.
- `previous_titles` — titles an earlier pass already gave these pages and that still did not tell
  them apart. Never return one of these, and never one of these with something appended.

Return ONE title for EVERY page you were asked about, and make them all different from each other
and from `settled_titles`. Compare the pages with each other — that is why they arrive together —
and name the thing that actually separates them: the specific volume, dataset or data product,
target, instrument, mission phase, version, date or date range, part or region the page states, or
that `url_differs_at` shows.
{TITLE_RULES}
- Tell pages apart with words a reader understands. No bare IDs, URL fragments, "Page 2" or
  "(copy)" unless that is truly all that differs; a volume or part number the page states is fine
  ("Cassini ISS Calibrated Images, Volume 12").
- There is no "leave this one as it was". Two pages a search result cannot tell apart is never an
  acceptable answer, so every page gets a title of its own even when you have to fall back on what
  its URL shows. Say so with low confidence rather than repeating a title.
- When two pages really are the same page served at two URLs, still give each a distinct title and
  list the URLs together in `same_page_groups`: excluding one of them is the curator's fix, not
  yours.

Confidence, per page:
- high: what sets the page apart is stated in its text or title.
- medium: a reasonable inference from the text.
- low: the page is told apart only by what its URL shows.
Never invent facts that are not in the input."""

# Settled titles listed as constraints in one call. A group is rewritten whole, so these are only
# the pages of it whose titles will not change (and, for a group too big for one call, the titles
# its earlier calls already handed out); the URL-order neighbours are the ones worth naming, because
# that is where the URLs differ by the least.
TITLE_SIBLINGS = 30


def title_siblings(members: list[dict[str, Any]], url: str, k: int = TITLE_SIBLINGS) -> list[dict[str, Any]]:
    """Up to `k` other members of a duplicate-title group (sorted by URL), nearest to `url` first
    in URL order, returned in URL order as {url, title, keeps_title}."""
    others = [m for m in members if m["url"] != url]
    if len(others) <= k:
        picked = others
    else:
        at = next((i for i, m in enumerate(others) if m["url"] > url), len(others))
        lo = max(0, min(at - k // 2, len(others) - k))
        picked = others[lo:lo + k]
    return [{"url": m["url"], "title": m.get("title"), "keeps_title": not m.get("rewrite", False)}
            for m in picked]


def norm_title(s: str) -> str:
    """Titles compared the way the index would see them: case and runs of whitespace do not count."""
    return " ".join((s or "").split()).lower()


def _url_tokens(url: str) -> list[str]:
    """A URL as the words that could name the page: its path segments, then its query values."""
    parts = urlsplit(url)
    toks = [unquote(s) for s in parts.path.split("/") if s]
    toks += [f"{k}={v}" for k, v in parse_qsl(parts.query)]
    return toks or [parts.netloc]


def url_distinctions(urls: Iterable[str]) -> dict[str, list[str]]:
    """What each URL has that the others in the group do not. Path depth differs from page to page,
    so this compares the URLs as sets of tokens rather than position by position: a token every URL
    of the group carries says nothing, the rest are what tells that page apart. Two URLs of one
    collection are never identical, so no URL comes back with nothing to say (the last segment is
    the fallback when the token sets themselves match, e.g. the same segments in another order)."""
    toks = {u: _url_tokens(u) for u in urls}
    if not toks:
        return {}
    common = set.intersection(*(set(t) for t in toks.values()))
    return {u: [t for t in ts if t not in common] or ts[-1:] for u, ts in toks.items()}


def humanise(tokens: Iterable[str]) -> str:
    """URL tokens as something readable in a search result: "ozone-2024_v2.html" -> "Ozone 2024 V2"."""
    words: list[str] = []
    for t in tokens:
        t = re.sub(r"\.(html?|php|aspx?|jsp|pdf)$", "", t, flags=re.IGNORECASE)
        words += [w for w in re.split(r"[-_+.\s]+", t) if w]
    out = " ".join(w if w.isupper() else w.capitalize() for w in words)
    return out[:80].strip()


# Recorded as the "model" of a title no model wrote: the pass resolved it from the URLs itself.
URL_DISAMBIGUATED = "rule:url-distinction"


def disambiguate(shared_title: str, urls: Iterable[str], *, taken: Iterable[str] = (),
                 is_taken: Callable[[str], bool] | None = None) -> dict[str, str]:
    """A distinct title for every URL without asking the model: the title they share plus what that
    URL has and the others do not. The guarantee behind the whole pass — URLs are unique within a
    collection, so this always resolves — and the floor under the model's answers, never the first
    choice: it is only as good as the URL is descriptive.

    `taken` are titles to stay off; `is_taken` is the same question asked of the whole collection,
    because a title that is new to this group can still be one another page already carries."""
    distinctions = url_distinctions(urls)
    used = {norm_title(t) for t in taken}

    def spoken_for(title: str) -> bool:
        return norm_title(title) in used or bool(is_taken and is_taken(title))

    out: dict[str, str] = {}
    for url in sorted(distinctions):
        label = humanise(distinctions[url])
        title = f"{shared_title} — {label}" if label else shared_title
        n = 2
        while spoken_for(title):  # nothing readable left: number them rather than collide
            title = f"{shared_title} — {label} ({n})" if label else f"{shared_title} ({n})"
            n += 1
        used.add(norm_title(title))
        out[url] = title
    return out


# ── fitting a prompt under llm_max_input_tokens ───────────────────────
# Counted with the GPT-5 tokenizer (o200k_base): it matched the API's prompt_tokens to within the 6
# tokens of chat framing on a 200K-token prompt. The response schema is part of the prompt too.
_ENCODING = None
FRAMING_TOKENS = 200  # chat roles / separators and slack for the count


def _enc():
    global _ENCODING
    if _ENCODING is None:
        import tiktoken
        _ENCODING = tiktoken.get_encoding("o200k_base")
    return _ENCODING


def count_tokens(s: str) -> int:
    return len(_enc().encode(s, disallowed_special=()))


def fit_text(text: str, budget: int) -> str:
    """`text` if it is at most `budget` tokens, else its first `budget` tokens."""
    if budget <= 0:
        return ""
    if len(text.encode("utf-8")) <= budget:  # a token is at least one byte: no need to count
        return text
    tokens = _enc().encode(text, disallowed_special=())
    return text if len(tokens) <= budget else _enc().decode(tokens[:budget])


def page_tokens(text: str) -> int:
    n = len(text.encode("utf-8"))
    return n if n <= 1_000 else count_tokens(text)  # a short page is not worth encoding


def share_budget(sizes: list[int], budget: int) -> list[int]:
    """Split `budget` tokens across pages of `sizes` tokens: pages smaller than an even share keep
    all of theirs, and what they leave over goes to the big ones — only the biggest are cut."""
    out = list(sizes)
    left, rest = max(budget, 0), sorted(range(len(sizes)), key=lambda i: sizes[i])
    while rest:
        share = left // len(rest)
        i = rest[0]
        if sizes[i] > share:
            for j in rest:
                out[j] = share
            break
        left -= sizes[i]
        rest.pop(0)
    return out


def fixed_tokens(system: str, schema: type) -> int:
    """What a call costs before any page text: the system prompt, the response schema, framing."""
    return count_tokens(system) + count_tokens(json.dumps(schema.model_json_schema())) + FRAMING_TOKENS


def after_refusal(budget: int, e: LLMInputTooLong) -> int:
    """The text budget to ask again with after the provider said the prompt was too long: short by
    what it counted over the limit (from its message), with 2% slack; a tenth less when it said none."""
    over = (e.got - e.limit) if e.got and e.limit else budget // 10
    return max(budget - over - budget // 50, 0)


def text_meta(total_chars: int, sent: str) -> dict[str, Any]:
    """`text_chars` (the whole page's length) plus `text_cut: true` when only its start is sent."""
    return {"text_chars": total_chars, **({"text_cut": True} if len(sent) < total_chars else {})}


async def suggest_distinct_titles(
    llm: LLMProvider, docs: list[dict[str, Any]], *, shared_title: str, sharing: int,
    settled: list[dict[str, Any]] | None = None, previous: Iterable[str] = (),
    document_type: str | None = None, collection: Collection | None = None, settings: Settings,
) -> dict[str, Any]:
    """ONE call for a whole group of pages {url, title, text} that would all be indexed under
    `shared_title` + `document_type`: the model sees them together, so it can tell them apart from
    each other instead of guessing page by page and colliding all over again. Every page's text goes
    in, whole unless the group would pass `settings.llm_max_input_tokens` — then the longest texts
    are cut from the end, evenly, until it fits; `settled` are the group's pages whose titles will not change (their titles only, as
    constraints) and `previous` are answers an earlier pass already gave and that did not work.

    Returns {"titles": {url: {"title", "title_conf"}}, "same_page_groups", "model", tokens…}. Only
    titles that are non-empty and differ from the shared title, the settled ones, `previous` and
    each other come back — the caller disambiguates the rest from their URLs."""
    settled = settled or []
    header: dict[str, Any] = {}
    if collection is not None:
        header["collection"] = collection.name
        header["collection_seed"] = collection.seed_url
    header |= {
        "shared_title": shared_title, "document_type": document_type, "pages_sharing_it": sharing,
        "pages_to_retitle": len(docs),
        "url_differs_at": url_distinctions([d["url"] for d in docs] + [s["url"] for s in settled]),
        "settled_titles": [{"url": s["url"], "title": s.get("title")} for s in settled],
    }
    if previous := [p for p in previous if p]:
        header["previous_titles"] = sorted(set(previous))
    texts = [d.get("text") or "" for d in docs]
    sizes = [page_tokens(t) for t in texts]

    def build(budget: int) -> str:
        """The prompt with the pages' texts cut to share `budget` tokens between them."""
        cut = [fit_text(t, n) for t, n in zip(texts, share_budget(sizes, budget), strict=True)]
        pages = "\n\n".join(
            f"Page {i} of {len(docs)}:\n"
            + json.dumps({"url": d["url"], "scraped_title": d.get("title"), **text_meta(len(t), c)},
                         ensure_ascii=False)
            + "\nText:\n" + c
            for i, (d, t, c) in enumerate(zip(docs, texts, cut, strict=True), 1)
        )
        return "Group:\n" + json.dumps(header, ensure_ascii=False) + "\n\n" + pages

    # everything but the texts, counted with every page marked cut (the longest header it can have)
    frame = build(0)
    budget = settings.llm_max_input_tokens - fixed_tokens(TITLES_SYSTEM, DistinctTitles) - count_tokens(frame)
    try:
        done = await llm.complete(system=TITLES_SYSTEM, user=build(budget), schema=DistinctTitles)
    except LLMInputTooLong as e:  # the count was off: cut by what the provider says, once
        done = await llm.complete(system=TITLES_SYSTEM, user=build(after_refusal(budget, e)), schema=DistinctTitles)

    asked = {d["url"] for d in docs}
    blocked = {norm_title(shared_title), *(norm_title(s.get("title") or "") for s in settled),
               *(norm_title(p) for p in header.get("previous_titles", []))} - {""}
    titles: dict[str, dict[str, Any]] = {}
    for item in done.parsed.items:
        title = (item.title or "").strip()
        if item.url not in asked or item.url in titles or not title or norm_title(title) in blocked:
            continue  # unasked, answered twice, empty, or a title that is already spoken for
        blocked.add(norm_title(title))
        titles[item.url] = {"title": title, "title_conf": item.title_confidence}
    return {
        "titles": titles, "same_page_groups": [g for g in done.parsed.same_page_groups if len(g) > 1],
        "model": done.model,
        "tokens_in": done.tokens_in, "tokens_out": done.tokens_out, "tokens_cached": done.tokens_cached,
        "tokens_cache_write": done.tokens_cache_write,
    }


async def suggest_patterns_batch(
    llm: LLMProvider, c: Collection, batch: list[dict[str, Any]], *,
    examples: list[str] = (), batch_no: int = 1, batches: int = 1,
) -> tuple[list[PatternSuggestion], Completion[PatternSuggestions]]:
    """One call over one batch of {url, scraped_title}. A suggestion is kept only if it matches
    a URL the model actually saw (this batch), is a real glob, is not a duplicate, and matches
    at least one URL that the already-applied globs in `examples` do not."""
    payload = {
        "collection": c.name, "seed": c.seed_url,
        "global_excludes_already_applied": list(examples),
        "batch": f"{batch_no} of {batches}",
        "urls": [{"url": s["url"], "title": s.get("scraped_title")} for s in batch],
    }
    done = await llm.complete(
        system=PATTERN_SYSTEM, user="Batch:\n" + json.dumps(payload, ensure_ascii=False), schema=PatternSuggestions
    )
    urls = [s["url"] for s in batch]
    applied = [glob_to_regex(g) for g in examples]
    kept: list[PatternSuggestion] = []
    seen: set[tuple[str, str]] = set()
    for s in done.parsed.suggestions:
        key = (s.type, s.match)
        if key in seen or s.match.strip() in ("", "*"):
            continue
        rx = glob_to_regex(s.match)
        hits = [u for u in urls if rx.match(u)]
        if not hits:
            continue  # hallucinated / over-specific: matches nothing the model was shown
        if applied and all(any(a.match(u) for a in applied) for u in hits):
            continue  # every URL it matches is already covered by a global exclude
        seen.add(key)
        kept.append(s)
    return kept, done


async def suggest_metadata_one(
    llm: LLMProvider, doc: dict[str, Any], *, settings: Settings, collection: Collection | None = None,
) -> dict[str, Any]:
    """One call for one document {url, title, text, content_hash}. Returns the row for
    `Database.set_delta_ai` plus the token usage. The collection's name and SME-set defaults go
    with the page so answers agree across the collection.

    A collection whose curator set a division is asked for the title and document type only: the
    division goes along as context (`collection_division`), the answer has no division field, and
    the returned row carries none — so no division suggestion ever turns up for review."""
    text = doc.get("text") or ""  # the whole page unless it does not fit llm_max_input_tokens
    # the curator's division, or None while the collection is still on the General placeholder
    division = collection.division if collection is not None and division_assigned(collection.division) else None
    header: dict[str, Any] = {}
    if collection is not None:
        header["collection"] = collection.name
        header["collection_seed"] = collection.seed_url
        if division is not None:  # the curator's, for the whole collection: context, not a question
            header["collection_division"] = division.value
        if collection.document_type is not None:
            header["collection_document_type"] = collection.document_type.value
    header |= {"url": doc["url"], "scraped_title": doc.get("title")}
    schema = MetadataSuggestion if division is None else MetadataSuggestionNoDivision
    system = metadata_system(division is None)

    def build(budget: int) -> str:
        sent = fit_text(text, budget)
        return ("Document:\n" + json.dumps(header | text_meta(len(text), sent), ensure_ascii=False)
                + "\n\nText:\n" + sent)

    budget = settings.llm_max_input_tokens - fixed_tokens(system, schema) - count_tokens(build(0))
    try:
        done = await llm.complete(system=system, user=build(budget), schema=schema)
    except LLMInputTooLong as e:  # the count was off: cut by what the provider says, once
        done = await llm.complete(system=system, user=build(after_refusal(budget, e)), schema=schema)
    r = done.parsed
    if not r.title.strip():  # recorded on the row as a failure; the next Suggest metadata asks again
        raise LLMError("the model returned an empty title")
    return {
        "url": doc["url"],
        "title": (r.title or "").strip() or None, "title_conf": r.title_confidence,
        # absent when the curator set the division: set_delta_ai then writes NULL, clearing any
        # division suggestion an earlier run (before the division was set) had left on the row, and
        # records `division_skipped` so the row can be asked for a division alone if it is cleared
        **({"division": r.division, "division_conf": r.division_confidence} if division is None
           else {"division_skipped": True}),
        "document_type": r.document_type, "document_type_conf": r.document_type_confidence,
        "model": done.model, "content_hash": doc.get("content_hash"),
        "tokens_in": done.tokens_in, "tokens_out": done.tokens_out, "tokens_cached": done.tokens_cached,
        "tokens_cache_write": done.tokens_cache_write,
    }


async def suggest_metadata(
    llm: LLMProvider, docs: list[dict[str, Any]], *, settings: Settings,
    collection: Collection | None = None, on_progress: ProgressCb | None = None,
) -> list[dict[str, Any]]:
    """Sequential convenience wrapper (tests, scripts): one call per document."""
    out: list[dict[str, Any]] = []
    for i, d in enumerate(docs, 1):
        out.append(await suggest_metadata_one(llm, d, settings=settings, collection=collection))
        if on_progress:
            await on_progress({"done": i, "total": len(docs)})
    return out
