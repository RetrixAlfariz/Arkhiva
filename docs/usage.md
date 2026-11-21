# Arkhiva command guide

For a first-time setup with the default Parquet corpus, follow the [quickstart](../README.md#quickstart).

## Build

Use Rust 1.93 or later and a native C toolchain for bundled SQLite. On Linux, the TLS backend also requires OpenSSL development headers.

```powershell
cargo build --release --locked
cargo test --release --locked
```

The examples below use PowerShell on Windows. On Linux, use `target/release/arkhiva`.

## Run from any folder on Windows

Build in the repository root and copy the executable to a directory on your user PATH. Save the absolute path to the existing Parquet corpus beside the executable in `arkhiva.corpus`.

```powershell
cargo build --release --locked
$bin = Join-Path $env:USERPROFILE '.local/bin'
$corpus = (Resolve-Path data/parquet).Path
[IO.File]::WriteAllText((Join-Path $bin 'arkhiva.corpus'), $corpus + [char]10)
Copy-Item -LiteralPath target/release/arkhiva.exe -Destination (Join-Path $bin 'arkhiva.exe')
```

You can then run `arkhiva status`, `arkhiva sync`, or `arkhiva search "quantum coherence"` from any directory. A per-command `parquet --directory <path>` still takes precedence. Copy the rebuilt executable to the same location after updating Arkhiva.

## Append-only Parquet corpus

From the repository root, sync and check the default Parquet corpus with:

```powershell
$arkhiva = './target/release/arkhiva.exe'
& $arkhiva sync
& $arkhiva status
& $arkhiva parquet get 2611.18563
```

The local build defaults to `data/parquet` relative to the working directory. An installed executable can use an absolute corpus path configured next to it. For a separate corpus, use `parquet --directory <path> sync` or `parquet --directory <path> status`. Pass `--db <path>` to the top-level commands to select an existing SQLite corpus.

Use `arkhiva --db <validated.sqlite> parquet seed` to seed a Parquet corpus from a validated SQLite database. Seeding copies active papers and the OAI checkpoint, including any committed pages from an interrupted harvest. A seed without a checkpoint needs `sync --from YYYY-MM-DD` the first time.

New paper metadata is written into Zstd-compressed Parquet files. SQLite holds the normalized paper keys, file and row locations, the list of committed files, and the OAI checkpoint. Each page publishes its new Parquet file before the corresponding SQLite transaction commits. The transaction registers the file, its IDs, and the checkpoint together. If a process stops before the transaction commits, reopening cleans up the uncommitted file and sync can replay the page. An interrupted initial seed can be rerun with the same command.

Each Parquet record stores the first submission date as `first_submitted_on`. Categories are stored as a Parquet list of strings in `categories`. The OAI cursor lives in the SQLite index. Parquet files are written with 16 KiB data pages, so looking up one paper decompresses a few kilobytes instead of a whole megabyte-sized page; files carry an `arkhiva.layout` marker for this layout. Lookups and search results read the page locations (offset index) of the row groups they touch, not the whole file's. Run `arkhiva parquet migrate-schema` from the repository root to rewrite files from an older layout, convert legacy JSON categories, and remove obsolete date columns and unused deletion flags. The migration checks that legacy deletion flags are all false before dropping them.

Each sync fetches one OAI-PMH harvest and exits after the final page and Tantivy refresh. Progress goes to stderr, leaving the report JSON on stdout. It shows a progress bar when arXiv reports a usable size estimate and committed batch and record counts otherwise. An interrupted harvest can resume from its saved checkpoint.

Build the local phrase lexicon with `arkhiva parquet lexicon`. Parquet sync refreshes it after updating Tantivy. The initial build scans titles and abstracts with the corpus write lock. Later runs scan only added Parquet parts; a replaced or removed part, such as after `migrate-schema`, rebuilds the lexicon from scratch. A retry publishes counts atomically. Query the SQLite sidecar `lexicon-v2.sqlite` with `SELECT doc_freq FROM lexicon WHERE phrase = 'visual servoing'`.

The lexicon counts documents containing a contiguous phrase of one to three tokens. Case and common hyphens are normalized. Punctuation and numeric-only tokens break phrases. Function words may occur inside a phrase, but cannot start or end it. The filter also excludes selected generic entries. Only phrases found in at least three papers are kept, which keeps the sidecar a fraction of the corpus size. A phrase dropped earlier starts counting again from zero when a later sync mentions it, so its total can lag by up to two papers until the next full rebuild. The lexicon is computed independently of smart search.

Parquet sync keeps each paper's metadata as first collected after the seed. It skips known IDs and OAI deletion notices. The SQLite sync documented below continues to apply revisions and deletions.

Several read commands can use one corpus at once: `search` (including `--smart` and `--topic`), `parquet get`, `status`, and `topic list`. Writing commands take the corpus alone: `sync`, `tag`, `topic remove`, `parquet migrate-schema`, and `search --smart --save-topic`. A writer waits up to 10 seconds for running searches to finish. A read started during a sync gives up after 2 seconds and asks you to retry. If a seed or an interrupted sync left the Tantivy index behind, the next search briefly takes the corpus alone to bring it up to date.

For external reads, use the registered files rather than globbing while an ingest is in progress. The list is available from the small SQLite index:

```sql
SELECT printf('part-%08d.parquet', file_id) AS filename
FROM parquet_files ORDER BY file_id;
```

### Disk usage

`scripts/storage.ps1` lists folder and file sizes, largest first. Without arguments it covers the repository and `data/`; pass a folder to drill in.

```powershell
./scripts/storage.ps1
./scripts/storage.ps1 data/parquet
```

`data/parquet` is the only folder search needs. A downloaded Kaggle snapshot under `data/arxiv-v<release>/` and a seed database such as `data/arkhiva-v312.sqlite` can be deleted after seeding; keep them only to rebuild the corpus from scratch.

## Local search

```powershell
& $arkhiva search 2611.18563
& $arkhiva search 'https://arxiv.org/abs/2611.18563'
& $arkhiva search 'maritime data fusion' --category cs.LG --from 2026-01-01 --to 2026-11-16 --limit 10
& $arkhiva search --category quant-ph --from 1999-01-01 --to 1999-12-31
```

An arXiv ID (optionally prefixed `arxiv:`) or abstract/PDF link looks up one paper in the local index and accepts version suffixes and URL query strings. Text searches match every term across the title and abstract, give title matches more weight, and return snippets in relevance order. Browsing without search terms returns the newest papers first. The category matches a complete code, ignoring case. Both date bounds are inclusive and refer to the first submission date. Results are compact JSON with a default limit of 20. Each hit has `id` in `provider:external_id` form, such as `arxiv:1706.03762`, plus `title`, `first_submitted_on`, `categories`, and a `snippet` for the top text-search hits. Reading abstracts dominates search cost, so only the first five hits carry a snippet by default; set `--snippets <n>` to change that, or `--snippets 0` for titles only. Hits carry no URL; for arXiv it is `https://arxiv.org/abs/<external_id>`. Use `parquet get <id-or-link>` for the full abstract.

The Tantivy index lives in `tantivy-v2` and stores term frequencies without positions, since search uses term queries only. Opening a corpus with an older `tantivy-v1` index builds `tantivy-v2`, merges it into one segment, and removes the old directory. Later updates merge small segments automatically. A successful Parquet sync also brings the local Tantivy index up to date before returning. It indexes newly committed files and builds an initial index if needed. Parquet commits and the OAI checkpoint persist if the index refresh fails. The next sync or text search can resume the index update. Text search and category browsing can also build the initial index after a seed and read only the selected result rows when preparing output. Use `parquet --directory <path> search` to search another corpus.

Manual tags live in the corpus SQLite index and can be combined with text, category, and date filters. Tag names ignore surrounding spaces and case. Add and remove accept an arXiv ID or link.

```powershell
& $arkhiva tag add maritime-ai 2611.18563
& $arkhiva search --tag maritime-ai --category cs.LG --from 2026-01-01
& $arkhiva tag remove maritime-ai 2611.18563
```

### Search with a research prompt

Prompt search asks a model for one search plan, then searches the corpus itself. It needs the phrase lexicon (`arkhiva parquet lexicon`). The model can be a local `llama-server` or any OpenAI-compatible API. Small local models often fail the plan's JSON schema; Qwen3.5-9B worked in testing.

To start a local server on demand, put `arkhiva-llama.json` next to the corpus's `index.sqlite`. Paths can be absolute or relative to the corpus directory. [llama-server documentation](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md)

```json
{
  "port": 18117,
  "model_alias": "arkhiva-qwen",
  "server_executable": "C:/tools/llama.cpp/llama-server.exe",
  "model_file": "C:/models/Qwen3.5-9B-Q4_K_M.gguf"
}
```

The first smart search starts the server on loopback and waits for it to load. Later searches reuse the server. After five idle minutes it releases model memory and reloads on demand. Set `runtime_library_dir` if CUDA DLLs are in a separate directory. Arkhiva adds it to the server process's `PATH`. To use an already running server, configure its `port` and `model_alias` and omit both path fields. The default external endpoint is `127.0.0.1:18117` with alias `arkhiva-qwen`.

To use an API instead, add its settings to `arkhiva-llama.json` or set environment variables; Arkhiva then starts no local server. Only the plan instructions and your prompt are sent; the corpus and results stay local.

```json
{
  "api_url": "https://api.openai.com/v1",
  "api_model": "gpt-6-luna",
  "api_reasoning": "none",
  "api_temperature": 0
}
```

The key never goes in the file; set `OPENAI_API_KEY` or `ARKHIVA_LLM_KEY` in your environment. Each environment variable below overrides its file setting for that session.

To keep both setups in one file and pick one, add `"llm": "api"` or `"llm": "local"`. Without `llm`, Arkhiva uses the API when `api_url` is set and the local server otherwise. `ARKHIVA_LLM_URL` always selects the API.

| Variable | Meaning |
|---|---|
| `ARKHIVA_LLM_URL` | `api_url`: API base, such as `https://api.openai.com/v1` (https, or http on localhost) |
| `ARKHIVA_LLM_MODEL` | `api_model`: model name, required with the URL |
| `ARKHIVA_LLM_KEY` | API key; falls back to `OPENAI_API_KEY` |
| `ARKHIVA_LLM_REASONING` | `api_reasoning`: optional `reasoning_effort`, such as `none` |
| `ARKHIVA_LLM_TEMPERATURE` | `api_temperature`: optional temperature, for models that accept one |

```powershell
arkhiva search --smart "algoritma kompresi file; cepat; lightweight" --limit 10
arkhiva search --smart "Aku mau bikin drone yang terbang sendiri di gudang tanpa GPS" --all
```

The plan has an intent (`known`, `topic`, or `question`), guessed titles for a known paper, anchors (domain words such as `compression`), facets (sub-needs with English terms), constraints (requirements the prompt states, such as fast or real-time), and an avoid list (phrases of a different meaning). Arkhiva keeps a facet term only if the lexicon finds it in 5 to 20,000 papers (a phrase longer than three words counts as its rarest piece), drops terms more than ten times broader than their siblings, and never substitutes another phrase. Each term is searched on its own, paired with each anchor. A guessed title counts only if a real title shares at least 60% of its words.

Constraints never filter. A paper gets a bonus of half its score per constraint, up to double, when a constraint phrase or the prompt's own word (such as `fast`) appears in the same sentence of its title or full abstract as a facet term or anchor. A paper naming an avoid phrase keeps half its score. Hits are ordered by tier: `title` (the guessed paper), `contextual` (found with an anchor or by a rare term), `constraint` (found only through a constraint search), `extended` (a broad term without an anchor), and `expansion`.

Smart search JSON includes the model's `plan`, the `grounded` terms that survived, `total` and `contextual` candidate counts, `timings_ms`, and `hits`. Each hit adds `tier`, `matched_terms`, `constraints` (the evidence phrase per constraint, or null), and `avoid_hit` when one applies. `--limit` caps the hits; `--all` lists every candidate. Category, date, and tag filters apply to every search. Regular `arkhiva search` runs independently of the model.

### Add your own kamus and codec

Two JSON files teach smart search your own words. A kamus maps a word to the English terms papers use, and those terms are always searched. A codec explains a word or phrase to the model, so it understands the prompt; a codec searches nothing by itself. Examples: [kamus.json](examples/kamus.json), [codec.json](examples/codec.json).

```json
{ "kompresi": ["data compression", "lossless compression"], "YOLO": ["you only look once"] }
```

```json
{ "lightweight": "runs on a microcontroller or phone with under 1 MB of RAM and no GPU" }
```

```powershell
arkhiva import kamus docs/examples/kamus.json
arkhiva import codec docs/examples/codec.json
```

Entries are stored in the corpus's `index.sqlite`. Importing a word again replaces its entry; other entries stay. `arkhiva list kamus` and `arkhiva list codec` show every entry. Remove one entry with `arkhiva remove kamus <word>` or `arkhiva remove codec <word>`; type an all-capitals word in capitals. A file with any invalid entry imports nothing. Importing a kamus lists terms smart search would skip: those not found in the corpus, in fewer than 5 papers, or in more than 20,000. They are imported anyway.

An entry applies when the prompt contains its word as whole words. Punctuation is ignored, and so is case, except for a key written entirely in capitals: `IT` or `YOLO` is an acronym and matches only in capitals, so `IT` never matches the word "it". Write a lowercase key too if you type the acronym in lowercase. Up to 20 kamus and 20 codec entries are sent with the prompt to the model. Each kamus entry is also added to the plan as a facet, minus terms the model already chose, and grounded like the model's own terms. The output lists applied entries under `glossary`.

### Save custom topics

Save a smart search under a topic name to keep its prompt, plan, and filters in the local SQLite index. Opening a topic replays the saved plan on the current corpus, including eligible papers added since it was saved, without calling the model. Topics saved by older versions keep their keyword search. An explicit category, tag, or date bound on the opening command overrides the corresponding saved filter.

```powershell
arkhiva search --smart "Cari riset penggabungan AIS dan sinyal radio untuk pelacakan kapal" --save-topic maritime-ai
arkhiva search --topic maritime-ai --limit 50
arkhiva search --topic maritime-ai --from 2026-11-01
arkhiva topic list
arkhiva topic remove maritime-ai
```

Saving again under the same name replaces its plan.

## Create or resume the corpus

```powershell
$arkhiva = './target/release/arkhiva.exe'
$version = 312
$source = "data/arxiv-v$version/arxiv-metadata-oai-snapshot.json"
$db = "data/arkhiva-v$version.sqlite"

& $arkhiva snapshot fetch --version $version
& $arkhiva --db $db snapshot import --file $source --release $version
& $arkhiva --db $db validate --file $source
& $arkhiva --db $db status
```

Use a new database when bootstrapping a different snapshot release. The download and extracted JSON live under the Git-ignored `data/` directory. Extraction keeps a local, seekable copy for import recovery. To use an existing download, pass the extracted JSON directly to `snapshot import --file`.

After an interrupted import, rerun the same command. Arkhiva verifies the source checksum and resumes from the committed byte offset. A batch commits rows, rejects, counts, and the offset together. The first valid row for each normalized ID wins within the snapshot. Rejected lines are recorded in `snapshot_rejects` with their byte offsets and reasons.

Validation makes an independent pass over the source to count input records and verify its SHA-256. It compares the input count with distinct rows, duplicates, and rejects, reports missing fields, and runs SQLite's integrity check. The snapshot report is saved in the database at validation time. Later validation checks file identity and current database integrity while returning the original snapshot counts.

## Update with arXiv OAI-PMH

For release 312, the example below starts from November 1, 2026, approximately two weeks before Kaggle published the release. This overlap is an assumption about snapshot coverage.

```powershell
& $arkhiva --db $db sync --from 2026-11-01
& $arkhiva --db $db sync
& $arkhiva --db $db status
```

When verified historical coverage is required and the upstream snapshot boundary cannot be established, bootstrap a fresh snapshot database with `arkhiva --db <path> sync --full-reconciliation`. This starts at the repository's earliest OAI datestamp and can take substantially longer.

Each successfully parsed page commits with its next resumption token. After an interruption, rerun `arkhiva --db <path> sync` with the same database. An expired token restarts the same harvest from its original lower bound. The durable cursor advances only when the entire harvest finishes, using the first server response's UTC date at the repository's advertised granularity. Subsequent runs include that date again. Arkhiva uses one request at a time, spaces requests at least three seconds apart, and honors `Retry-After`.

For direct SQLite queries, filter `papers` by `is_deleted = 0` for active records. The paper key is `(provider, external_id)`. `first_submitted_on` and `last_revised_on` describe the paper's version dates. `metadata_updated_on` describes the source metadata's update date and can differ from the last revision. An OAI deletion is represented by a tombstone. Run one Arkhiva command at a time against a given database.

- [Cornell University snapshot on Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv)
- [arXiv OAI-PMH documentation](https://info.arxiv.org/help/oa/index.html)
- [arXiv API terms](https://info.arxiv.org/help/api/tou.html)

## Benchmark import

```powershell
& $arkhiva --db data/bench-v312.sqlite bench import --file $source --release $version --batch-size 4096
```

Choose a fresh database path for every run. The JSON report includes elapsed time and throughput, sampled parse latency, peak process resident memory on Windows and Linux, and SQLite size after truncating the WAL. The import timer includes SHA-256 fingerprinting, parsing, and transactional writes. It excludes downloads, extraction, and the final WAL checkpoint. Parse samples time JSON parsing and normalization for every 128th record.

See [the benchmarks](benchmarks.md) for the baseline environment and results.
