# cedwyn-graphrag

A statement graph of the Cedwyn records: every non-datro Cedwyn webapp repository, the central evidence catalogue and the public legal wiki, merged into one deduplicated set of statements. The dashboard shows them as a **statement cloud**: the more distinct sources carry a statement, the bigger it is. Text carried by only one source stays small.

**Dashboard:** https://unclehowell.github.io/cedwyn-graphrag/
**Tracking issue:** https://github.com/unclehowell/cedwyn/issues/523

## What it does

1. **Reads** every public `cedwyn-*` repository except the old `cedwyn` monorepo, whose webapp content now lives in the split repositories, and anything datro. It also reads the live evidence catalogue API and the legal wiki's public pages and posts. Only text files are read (md, html, json, txt, csv, yml, xml). Small repos are streamed as one archive. Large media repos are read through their file list, so no binary is downloaded and nothing is cloned to disk.
2. **Splits** the text into statements: sentences of 8 to 60 words with real content. Page furniture (navigation, shop lines, version notes) and anything carrying personal or contact details is left out.
3. **Merges near-duplicates.** Statements are first matched on their normalised wording, then by MinHash over word 3-shingles (Jaccard ≥ 0.6), so a statement reworded slightly in five places becomes one cluster.
4. **Counts support** as the number of **distinct sources** carrying the statement. A source is one webapp repo, the catalogue, or the legal wiki. A file that exactly mirrors another file is counted once, and repetition inside one source counts once. Text repeated on most pages of a source is set aside as template noise (`data/noise.json`).
5. **Builds the graph:** entities (bodies, places, people of record, record ids, title numbers, years) are linked to the statements that mention them. Entity co-occurrence is weighted by support (`data/graph.json`).

No model calls and zero spend: extraction is deterministic, so two runs on the same sources give the same result.

## Files

| file | what |
|---|---|
| `tools/ingest.py` | the whole pipeline |
| `data/statements-top.json` | the strongest statements with sources (feeds the cloud) |
| `data/graph.json` | entity / statement graph |
| `data/stats.json` | run counts and support histogram |
| `data/noise.json` | template text set aside |
| `data/sources.json` | what was read from each source |
| `index.html` | the dashboard |
| `.github/workflows/ingest.yml` | the re-runnable migration |
| `worker/trigger.js` | optional Cloudflare Worker that starts the migration |

## Re-running the migration

- **By hand:** open Actions › ingest › *Run workflow* (leave `only` blank for everything). It also runs weekly, on Sundays at 03:17 UTC.
- **From Cloudflare:** deploy `worker/` with `wrangler deploy`, set the secrets `GH_TOKEN` (a fine-grained token for this repo with Actions read/write) and `TRIGGER_KEY`, then `POST /run` with the header `X-Trigger-Key`. `GET /status` lists recent runs.
- **Locally:** `python3 tools/ingest.py --out data` (numpy needed; `--only cedwyn-wiki,cedwyn-research` for a subset).

The workflow waits for any running Cedwyn site build to finish and never cancels one. Output is static JSON committed here; no database writes.

## Reading the cloud

Size shows how often the Cedwyn sources repeat a point, not whether the point is right. A point carried by many webapps may simply have been copied widely, and a single primary record can outweigh twenty pages. Tap a statement and read its sources.

— Sion's Tab agent
