"""LLM-assisted curation tasks. Every result is Pydantic-validated, then *sanity-checked
against the collection* (a suggested URL must be one we sent; a pattern must match something)
before anything is written. Suggestions never touch effective fields — only *_ai / pending rows."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from ..config import Settings
from ..engine.patterns import glob_to_regex
from ..models import (
    Collection,
    Division,
    MetadataSuggestion,
    PatternSuggestion,
    PatternSuggestions,
)
from .base import Completion, LLMProvider

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
- General: content that spans several divisions or belongs to none (agency-wide science policy,
  cross-division education). Not a fallback for a page whose division is unclear: use null."""

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

METADATA_SYSTEM = f"""You classify one crawled web page at a time for NASA's Science Discovery Engine (SDE), a
search engine over NASA science content. You receive the collection (the website the page was
crawled from), the page URL, its scraped title and its full text (possibly long). The scraped
title is often the same site-wide string on every page, and the text usually starts with the
site's navigation menu, alerts and login links: skip that chrome and read the page's own content.

Every page of a collection is classified in a separate call, and the answers must agree with
each other: apply the rules below the same way every time.

title — a free-form title for this page as it should read in a search result, judged by someone
who has not seen the site.
- Descriptive and self-contained, typically 4–12 words: the page's real subject ("Information for
  Data Proposers", "Real-Time Geomagnetic Storm Tracker").
- Do not add the collection or site name to the title, as a prefix or a suffix ("PDS: …",
  "… | Aurorasaurus"). A mission, instrument, project or dataset name belongs in the title only
  when the page is about it ("Cassini ISS Calibrated Images"); keep such names and acronyms as the
  page writes them.
- Do not copy the site-wide scraped title; no slogans, "Welcome to" or "Home Page".
- Null only if the page has no content of its own.

division — the NASA Science Mission Directorate division, one of:
{DIVISION_DEFINITIONS}
Judge from the page's subject read in the context of the whole collection: the pages of one site
nearly always share a division, so a help page, data-format guide or proposer page on a planetary
data archive is Planetary Science, not General. When `collection_division` is given, a subject-
matter expert set it for the whole collection: use it unless the page is clearly about another
division.

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

Confidence, one per field:
- high: stated in the page text or title, or follows directly from the rules above (a dataset
  landing page is Data; a page of a site devoted to one division has that division).
- medium: a reasonable inference where another answer is also defensible.
- low: a guess. For title and division, prefer a null value with low confidence over a wrong
  value; document_type is never null, so give the closest type with low confidence.
Never invent facts that are not in the input."""

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
    with the page so answers agree across the collection."""
    text = doc.get("text") or ""  # the whole page, never cut: an accurate title needs all of it
    header: dict[str, Any] = {}
    if collection is not None:
        header["collection"] = collection.name
        header["collection_seed"] = collection.seed_url
        if collection.division != Division.GENERAL:  # General is the untouched default
            header["collection_division"] = collection.division.value
        if collection.document_type is not None:
            header["collection_document_type"] = collection.document_type.value
    header |= {"url": doc["url"], "scraped_title": doc.get("title"), "text_chars": len(text)}
    user = "Document:\n" + json.dumps(header, ensure_ascii=False) + "\n\nText:\n" + text
    done = await llm.complete(system=METADATA_SYSTEM, user=user, schema=MetadataSuggestion)
    r = done.parsed
    return {
        "url": doc["url"],
        "title": (r.title or "").strip() or None, "title_conf": r.title_confidence,
        "division": r.division, "division_conf": r.division_confidence,
        "document_type": r.document_type, "document_type_conf": r.document_type_confidence,
        "model": done.model, "content_hash": doc.get("content_hash"),
        "tokens_in": done.tokens_in, "tokens_out": done.tokens_out, "tokens_cached": done.tokens_cached,
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
