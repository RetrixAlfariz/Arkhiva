# Arkhiva

<img src="assets/arkhiva.svg" alt="Logo Arkhiva — Irisan Corpus" width="128">

Arkhiva is a local research companion for finding and organizing arXiv papers. It keeps a copy of arXiv's paper metadata on your machine and searches it offline. The Rust CLI stores metadata in Parquet, uses SQLite for lookups and sync state, and searches with Tantivy.

- **Search** titles and abstracts, or look up a paper by ID or arXiv link.
- **Filter** by category, first-submission date, and your own tags.
- **Ask in plain language**, including Indonesian, with optional prompt search through a local model.
- **Save topics** and rerun them as the corpus grows.
- **Stay current** by appending new papers from arXiv OAI-PMH.

## Quickstart

Requires Rust 1.93 or later and a native C toolchain for bundled SQLite. On Linux, also install the OpenSSL development headers and use `./target/release/arkhiva`.

Build the CLI and create the corpus once. The seed downloads Cornell's Kaggle snapshot of about 1.8 GB, and the import takes several minutes. From the repository root in PowerShell:

```powershell
cargo build --release --locked
$arkhiva = './target/release/arkhiva.exe'
$version = 312
$snapshot = "data/arxiv-v$version/arxiv-metadata-oai-snapshot.json"
$db = "data/arkhiva-v$version.sqlite"

& $arkhiva snapshot fetch --version $version
& $arkhiva --db $db snapshot import --file $snapshot --release $version
& $arkhiva --db $db validate --file $snapshot
& $arkhiva --db $db parquet seed
& $arkhiva sync --from 2026-11-01
```

The first sync overlaps the snapshot's expected coverage. Later runs of `sync` resume from the saved checkpoint. After seeding, the snapshot and the SQLite database are no longer needed for search.

## Everyday use

```powershell
& $arkhiva sync                                   # fetch new papers
& $arkhiva search 'quantum error correction' --category quant-ph --limit 5
& $arkhiva search arxiv:1706.03762                # ID or arXiv link
& $arkhiva parquet get 1706.03762                 # full record with abstract
& $arkhiva tag add reading-list 1706.03762
& $arkhiva search --tag reading-list
& $arkhiva status
```

Search prints compact JSON. Each hit has an `id` such as `arxiv:1706.03762`, plus `title`, `first_submitted_on`, and `categories`. Text searches add a `snippet` to the top five hits; change that with `--snippets <n>`. The paper page is `https://arxiv.org/abs/<id>`.

To run `arkhiva` from any folder, see [Run from any folder on Windows](docs/usage.md#run-from-any-folder-on-windows).

## Documentation

- [Command guide](docs/usage.md): corpus layout and recovery, search options, prompt search with a local model, saved topics, and OAI-PMH sync details.
- [Benchmarks](docs/benchmarks.md): measured snapshot import and search results.

## Repository layout

| Path | Contents |
| --- | --- |
| `src/` | CLI, Parquet corpus, Tantivy search, OAI-PMH sync, snapshot import, and prompt search |
| `migrations/` | SQLite schema |
| `docs/` | Command guide and benchmark results |
| `scripts/` | `storage.ps1` for disk usage |
| `assets/` | Logo |
| `data/` | Local corpus, downloads, and reports; Git-ignored |

## License

Licensed under either the [MIT License](LICENSE-MIT) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option. Unless you state otherwise, any contribution you submit for inclusion is dual-licensed the same way, without additional terms.

## Data sources

Arkhiva seeds its corpus from the [arXiv Dataset on Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv), published by Cornell University, and then appends new papers through [arXiv OAI-PMH](https://info.arxiv.org/help/oa/index.html) under the [arXiv API terms](https://info.arxiv.org/help/api/tou.html).
