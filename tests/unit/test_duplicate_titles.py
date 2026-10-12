"""Title + document type combinations two pages of one collection would both be indexed under:
counted, flagged per row, filterable, and sent back to the LLM (fake provider) for new titles only."""
from sde_curation.llm.tasks import disambiguate, title_siblings, url_distinctions

CID = "ex.org"
    # the SME types a title and a division for every page (promote below refuses blanks)
    # case and runs of whitespace do not make two titles different
    # a rule that gives a third page the same title joins the group
    # the same title with a different document type is not a duplicate
    # an excluded page is never indexed, so it shares nothing
    # a pending AI title counts as if accepted

    # promote refuses a collision outright: two pages a search cannot tell apart may not be indexed

    # a title of its own for one of them settles it (the curator's per-URL edit wins), and it promotes

    # promoted: the curated rows count too, and a new delta URL that collides with them is flagged
    # and it is the delta row, not the curated one it collides with, that promote holds back —
    # promoting it on its own does not get it past the gate either

    # the tables flag the row and filter on it; the Curate page says how many and offers the fix


    # every field is filled — and all three pages would be one row in a search
    # the metadata pass does not settle it by itself: it strips the site suffix and lands on one title

    # ↻ Regenerate duplicate titles answers it, but only an accepted answer is a value


    # ONE call for the group, both pages in it: the model tells them apart from each other rather
    # than guessing page by page and landing on the same title again
    # the differing part of each URL is worked out for the model, not left for it to spot







    # a rule gave every page the same title: no AI title pending, so every delta URL is asked

    # where some pages of a group have a pending AI title, those are asked first and the rest keep
    # theirs — but the pass does not stop there: the ones left sharing go round again, so one click
    # still ends at zero
    # the second pass picked up p0 and p1, which the first one left sharing "Portal"



    # sent back on demand: the new title is the suggestion, the one it shared is kept and shown

    # editing the original instead: accepted as edited, and the kept title goes with the suggestion
    # a fresh classification starts over




    # one suffix at most on any of them: a pass never builds on the answer the pass before it gave
    # only the pages that actually moved carry the title they shared; one may keep it and still be
    # distinct, which is the cheapest way out of a group and costs the SME nothing
    # a group asked twice is asked the SAME question — always from the title the pages shared, never
    # from the answer the pass before it gave, which is what used to grow the tail
    # a title the pass resolved from the URLs itself says so, so the SME can see which to check

    # and there is nothing left to send back




    # the curator puts them back on one title: now both flags apply to the same row

    # sent back again, the group is asked about the title it originally shared, and the answer that
    # put them here is named so the model does not offer it a second time



    # every page that changed is accounted for in exactly one of the two counters





    # on a URL table it still filters that table

    # every suggestion on /a accepted: nothing left to review there, but it still collides with /b

    # ?dup=title narrows the table to the colliding rows, whatever their suggestions are

    # an excluded page shares nothing: the collision is gone and so is the filtered table



    # /c is decided and tells itself apart, so hiding the decided rows leaves the collision


def test_url_distinctions_are_what_one_url_has_and_the_others_do_not():
    # only a token EVERY URL carries says nothing; one shared with some of them still narrows it
    u = ["https://ex.org/data/ozone/access", "https://ex.org/data/clouds/access",
         "https://ex.org/images/gallery/aurora"]
    d = url_distinctions(u)
    assert d[u[0]] == ["data", "ozone", "access"] and d[u[1]] == ["data", "clouds", "access"]
    assert d[u[2]] == ["images", "gallery", "aurora"]
    # drop the odd one out and "data" / "access" become common, leaving what really differs
    d = url_distinctions(u[:2])
    assert d[u[0]] == ["ozone"] and d[u[1]] == ["clouds"]
    # depths differ: comparing tokens, not positions, still finds the difference
    d = url_distinctions(["https://ex.org/data", "https://ex.org/data/ozone/2024"])
    assert d["https://ex.org/data"] == ["data"] and d["https://ex.org/data/ozone/2024"] == ["ozone", "2024"]
    # a query string is part of what tells two URLs apart
    d = url_distinctions(["https://ex.org/browse?year=2023", "https://ex.org/browse?year=2024"])
    assert d["https://ex.org/browse?year=2024"] == ["year=2024"]
    # the same tokens in another order: the last segment is the fallback, never nothing
    d = url_distinctions(["https://ex.org/a/b", "https://ex.org/b/a"])
    assert d["https://ex.org/a/b"] == ["b"] and d["https://ex.org/b/a"] == ["a"]


def test_disambiguate_always_resolves_a_group():
    """The floor under the whole pass: URLs are unique within a collection, so a distinct title can
    always be built from them. Nothing here asks a model, and nothing comes back colliding."""
    u = ["https://ex.org/data/ozone/access", "https://ex.org/data/clouds/access"]
    out = disambiguate("Data Access", u)
    assert out[u[0]] == "Data Access — Ozone" and out[u[1]] == "Data Access — Clouds"
    # a title already spoken for is stepped over, not written again
    out = disambiguate("Data Access", u, taken=["Data Access — Ozone"])
    assert out[u[0]] == "Data Access — Ozone (2)" and len(set(out.values())) == 2
    # extensions and separators come out readable
    assert disambiguate("Guide", ["https://ex.org/g/user-guide_v2.html", "https://ex.org/g/faq"]) == {
        "https://ex.org/g/faq": "Guide — Faq",
        "https://ex.org/g/user-guide_v2.html": "Guide — User Guide V2",
    }
    # 50 URLs that differ only in a number still come back 50 different titles
    many = [f"https://ex.org/v/{i}" for i in range(50)]
    assert len(set(disambiguate("Volume", many).values())) == 50


def test_title_siblings_are_the_url_order_neighbours():
    members = [{"url": f"u{i:03}", "rewrite": i % 2 == 0} for i in range(100)]
    near = title_siblings(members, "u050", k=4)
    assert [m["url"] for m in near] == ["u048", "u049", "u051", "u052"]
    assert [m["keeps_title"] for m in near] == [False, True, True, False]
    assert [m["url"] for m in title_siblings(members, "u000", k=3)] == ["u001", "u002", "u003"]
    assert [m["url"] for m in title_siblings(members, "u099", k=3)] == ["u096", "u097", "u098"]
    assert len(title_siblings(members[:3], "u001")) == 2
