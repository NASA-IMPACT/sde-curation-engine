# Calibrating the confidence gate

Every AI metadata suggestion carries a per-field confidence (`high` / `medium` / `low`). The
October reindex will auto-accept the high-confidence ones and send the rest to an SME. The cutoff
must come from data, not from the model's word: this is how to collect it.

## 1. Draw a review sample

Per field (title, division, document type) you want on the order of **100–200 SME-reviewed
rows**, spread across several collections and stratified by confidence (roughly equal numbers of
high / medium / low, so the low tail is measured too). Ten rows are far too few: division has six
classes and document type five.

- Run **Suggest metadata** on 3–5 representative collections.
- In URLs › Delta URLs filter `AI · high confidence` / `medium` / `low` and export the CSV
  (`⇩ CSV`): the columns `title_ai`, `title_ai_conf`, `division_ai`, `division_ai_conf`,
  `document_type_ai`, `document_type_ai_conf`, `ai_model` are all there.
- Give the SME the rows; they accept (✓) or reject (✕) in the UI. Every decision is audited
  (`ai.accept` / `ai.reject` in the Activity tab), so agreement can be read back.

## 2. Read the agreement rate

For each field and each confidence level: `accepted / (accepted + rejected)`. The level at which
agreement is near-certain (say ≥ 97 %) is the auto-accept cutoff for that field. Expect the
three fields to land differently — titles are usually safe at `high`, division may not be.

While you are at it, compare a second, cheaper model (`OPENAI_MODEL`) on the same sample: if
agreement does not move, the cheaper model wins.

## 3. What comes next (not built yet)

Per-field thresholds as settings, an auto-accept job that applies them with
`created_by = "ai-auto"` and an audit row per URL, and a small ongoing random audit of
auto-accepted rows so drift after October stays visible.
