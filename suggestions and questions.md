**SDE Curation Engine: Recommended Updates**

**1\. Truncation / LLM context, and what each job should suggest**

How it works today (two jobs, two prompts)

| Job | What’s sent | What the LLM is asked for |
| :---- | :---- | :---- |
| Suggest patterns | One call, up to 60 dump URLs: {url, scraped\_title} \+ collection name/seed. No full\_text. | exclude, include, and title / division / document\_type patterns |
| Suggest metadata | Sequential calls, up to 20 docs per prompt: {url, title, text\[:1200\]} | Per-URL title, division, document\_type |

Things to be resolved:

1. Incorrect split. Suggest patterns proposes title / division / document\_type without page content. That belongs on Suggest metadata.  
2. Patterns from a small sample, not grounded in it. The model sees ≤60 URLs. Prompt says “must match a sample URL,” but we only drop globs that match nothing in the dump. So suggestions can apply to URLs not in the sample.  
3. Truncation. Suggest metadata sends \~1.2k chars of full\_text per page so 20 URLs fit in one prompt. But actual full\_texts would be much larger.

Intended split

* Suggest patterns → include / exclude globs only. Prefer all dump URLs in one prompt; if too large, batch (\~10000 URLs per call) and ask for include/exclude among those URLs. Keep only patterns that match URLs in the batch that was sent.  
* Suggest metadata → one LLM call per URL: title \+ document\_type \+ division, with as much full\_text as the chosen model window allows. No batch of 20\.

Options for metadata context (per-URL call)

| Option | Idea | Recommendation |
| :---- | :---- | :---- |
| 1: Similarity based approach | Title \+ URL path to pick top chunks, then classify | Not recommended. May not be reliable enough, needs chunking/vectorization before curation. \~30% may later be excluded. |
| 2: Large-context model | One call per URL; send as much full\_text as fits | Primary. Simple; no pre-index embeddings. Bound by token budget and cost. No Batch of 20\. |

Open work

* Narrow Suggest patterns to include/exclude; send all URLs or \~1000-URL batches; filter against that input set.  
* Suggest metadata: 1 call per URL \+ scoped truncation (model, max tokens, cost vs quality).

Brief report on [Cheap, High-Reasoning Models for Very Large Documents and Small Structured Outputs](https://docs.google.com/document/u/0/d/1Ll3XWnk55dJVhkZUEXMYg2fYN3z4v7Yl6F6tP8rpZqE/edit)

**2\. Diff / delta calculation (It should be content aware)**

Current:

Dump vs curated: URL set \+ scraped title \+ effective curated metadata (title, division, document type, excluded). full\_text is not in the diff.

Proposed

1. On dump ingest: content\_hash \= hash(scraped full\_text)  
2. First cutover: also hash existing curated/indexed text  
3. Persist indexed\_hash on curated (or last successfully indexed) URLs  
4. On re-scrape: hash new text → compare to indexed\_hash → content delta

Priority:

Not first. On the first run after this lands, almost everything will look like a delta (Sinequa scrape vs new scrape). Hashes become meaningful from the second scrape onward.

**3\. Automation at scale (HC auto-pass / LC SME review)**

Problem:  
Reindex-everything by October cannot be fully manual.

Proposed:

* LLM returns confidence with each suggestion  
* High confidence (HC) → auto-accept  
* Low confidence (LC) → SME review

Calibration (small set):

1. Sample \~10 scraped docs  
2. LLM generates title \+ metadata \+ confidence  
3. SME reviews  
4. Find the confidence cutoff where SME agreement is near-certain  
5. Use that cutoff for HC auto-pass

Why this over random sampling of auto-accepts: Slightly more complexity, but a good quality gate for auto-curation.

Constraint: Keep complexity modest. Threshold should be data-driven from SME feedback.

**4\. LLM prompt refinement (especially exclude patterns)**

Current:  
Prompts include hardcoded examples of exclude-style patterns.

Proposed:

1. Primary: Leverage on the existing COSMOS data to find a global pattern.  
2. Secondary: SME authored exclude list to replace or extend prompt examples.

-  Should we pass full text for patterns? \- No \-  only URL and title \- how many urls can we pass in one call? Threshold \- prompt, instructions, (look at url patterns to determine document type/division) \-   
- For metadata we pass url+ title \+ full text to get suggested title, document type and division in the same call.  
-  First exclude patterns is done \- SME reviews or auto accepts and then generate metadata using LLMs. 
- Then allow suggest metadata. - Ensure multiple workers and async/parallel calls to the LLMs. And a tag to show llm calls in progress. 
- Make it clear how many urls/docs are being selected to suggest patterns
- If we are indexing multiple collections at the same time, how are we going to queue the indexing and show status on the curation engine?
- Gpt 5.6 luna for token heavy calls or Gpt 5.1 mini for everything else. 
