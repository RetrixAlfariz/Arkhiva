# Arkhiva

<img src="assets/arkhiva.svg" alt="Logo Arkhiva — Irisan Corpus" width="128">

Arkhiva is a local research companion for arXiv. It keeps a copy of arXiv's paper metadata (about 3 million titles and abstracts) on your machine, searches it offline in well under a second, and can turn a plain-language request, in English or Indonesian, into a precise search.

```powershell
arkhiva search "quantum error correction" --category quant-ph --limit 5
arkhiva search --smart "deteksi jatuh lansia pakai sensor wearable"
```

## What it does

- **Offline search** of titles and abstracts, or lookup by arXiv ID or link.
- **Smart search**: a language model writes a search plan, and Arkhiva checks every term against the corpus before using it, so invented terms never drive the results. Use a cloud API (any OpenAI-compatible one) or a local model through `llama-server`.
- **Your own vocabulary**: a *kamus* maps your words to English search terms (`kompresi` → `data compression`), and a *codec* tells the model what you mean by a word (`cepat` → "at least 100 MB/s on one core").
- **Saved topics** that rerun later, without the model, as the corpus grows.
- **Tags and filters** by category, first-submission date, and your own tags.
- **Stays current** by appending new papers from arXiv OAI-PMH.

Results are compact JSON, easy to read or pipe into other tools. The paper page for a hit is `https://arxiv.org/abs/<id>`.

## Install

You need Rust 1.93 or later and a C toolchain (for bundled SQLite). On Linux, also install the OpenSSL development headers and use `./target/release/arkhiva` below.

```powershell
git clone https://github.com/RetrixAlfariz/Arkhiva.git
cd Arkhiva
cargo build --release --locked
```

### Create the corpus (once)

This downloads Cornell's Kaggle snapshot (about 1.8 GB) and imports it. The import takes several minutes.

```powershell
$arkhiva = './target/release/arkhiva.exe'
$version = 312
$snapshot = "data/arxiv-v$version/arxiv-metadata-oai-snapshot.json"
$db = "data/arkhiva-v$version.sqlite"

& $arkhiva snapshot fetch --version $version
& $arkhiva --db $db snapshot import --file $snapshot --release $version
& $arkhiva --db $db validate --file $snapshot
& $arkhiva --db $db parquet seed        # build the searchable corpus in data/parquet
& $arkhiva parquet lexicon              # phrase counts, needed for smart search
& $arkhiva sync --from 2026-11-01       # add papers newer than the snapshot
```

After seeding, the snapshot and the SQLite database are no longer needed for search. Later `sync` runs resume from the saved checkpoint.

### Run it from any folder (Windows)

Copy the executable to a folder on your `PATH` and tell it where the corpus lives:

```powershell
$bin = Join-Path $env:USERPROFILE '.local/bin'
[IO.File]::WriteAllText((Join-Path $bin 'arkhiva.corpus'), (Resolve-Path data/parquet).Path + [char]10)
Copy-Item target/release/arkhiva.exe (Join-Path $bin 'arkhiva.exe')
```

The examples below assume this setup. Copy the executable again after each rebuild.

## Usage

### Search

```powershell
arkhiva search "visual servoing" --from 2024-01-01 --limit 10
arkhiva search arxiv:1706.03762                 # by ID or arXiv link
arkhiva parquet get 1706.03762                  # full record with abstract
arkhiva sync                                    # fetch new papers
arkhiva status
```

### Smart search

Describe what you need in your own words. Arkhiva asks the model for a plan (facets, constraints, phrases to avoid), keeps only terms the corpus actually contains, and ranks papers that match more of your request first.

```powershell
arkhiva search --smart "algoritma kompresi file; cepat; lightweight" --limit 10
arkhiva search --smart "paper YOLO yang asli"
```

Configure the model in `data/parquet/arkhiva-llama.json`, then pick one with `"llm"`:

```json
{
  "llm": "api",
  "api_url": "https://api.openai.com/v1",
  "api_model": "gpt-6-luna",

  "server_executable": "C:/tools/llama.cpp/llama-server.exe",
  "model_file": "C:/models/Qwen3.5-9B-Q4_K_M.gguf"
}
```

- `"llm": "api"` uses the API. Put the key in the `OPENAI_API_KEY` or `ARKHIVA_LLM_KEY` environment variable, never in the file.
- `"llm": "local"` starts `llama-server` on demand. Models around 9B work well; very small ones (under 1B) often fail to produce a plan.
- Only the plan instructions, your prompt and matching kamus/codec entries are sent to the model. The corpus and results stay local.

### Kamus and codec

```powershell
arkhiva import kamus docs/examples/kamus.json
arkhiva import codec docs/examples/codec.json
arkhiva list kamus
arkhiva remove kamus gudang
```

```json
{ "kompresi": ["data compression", "lossless compression"], "YOLO": ["you only look once"] }
```

Kamus terms are always searched when your prompt uses the word. Keys in all capitals, like `YOLO`, are acronyms and match only when written in capitals. See [`docs/examples`](docs/examples) for both formats.

### Topics and tags

```powershell
arkhiva search --smart "robot gudang yang hemat baterai" --save-topic warehouse
arkhiva search --topic warehouse                # rerun later, no model call
arkhiva topic list

arkhiva tag add reading-list 1706.03762
arkhiva search --tag reading-list
```

## Documentation

- [Command guide](docs/usage.md): every command and option, corpus layout and recovery, smart-search ranking rules, OAI-PMH sync details.
- [Changelog](CHANGELOG.md): what changed in each version.
- [Benchmarks](docs/benchmarks.md): measured import and search performance.

## Repository layout

| Path | Contents |
| --- | --- |
| `src/` | CLI, Parquet corpus, Tantivy search, smart search, OAI-PMH sync, snapshot import |
| `migrations/` | SQLite schema |
| `docs/` | Command guide, benchmarks, example kamus and codec files |
| `scripts/` | `storage.ps1` for disk usage |
| `assets/` | Logo |
| `data/` | Your local corpus, downloads, and model config; Git-ignored |

## License

Licensed under either the [MIT License](LICENSE-MIT) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option. Unless you state otherwise, any contribution you submit for inclusion is dual-licensed the same way, without additional terms.

## Data sources

Arkhiva seeds its corpus from the [arXiv Dataset on Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv), published by Cornell University, and then appends new papers through [arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html) under the [arXiv API terms](https://info.arxiv.org/help/api/tou.html).
