# Arkhiva benchmarks

- [Snapshot import](#snapshot-import)
- [Live handoff](#live-handoff)
- [Search latency and I/O](#search-latency-and-io)

## Snapshot import

Measured on November 16, 2026 with Arkhiva 0.1.0, Rust 1.93.0 and bundled SQLite 3.51.1. Both runs used the same optimized executable on Windows 11 Pro build 26300, an AMD Ryzen 9 7950X, 63.1 GiB of visible RAM, and the ST2000DM008 SATA drive mounted as `E:`. Each run used a fresh database on that drive. OS cache state and background load were uncontrolled.

The input was Cornell's Kaggle arXiv release 312. Its extracted NDJSON was 5,707,139,903 bytes with SHA-256 `fd9d52c78984cba17b04c33fb083ec873fe9dfd28bdf887104dade76c25dd4b6`.

| Metric | Run 1 | Run 2 |
| --- | ---: | ---: |
| Input records | 3,252,452 | 3,252,452 |
| Distinct papers | 3,252,359 | 3,252,359 |
| Duplicate input records | 93 | 93 |
| Rejected input records | 0 | 0 |
| Import time | 452.0 s | 487.8 s |
| Input records per second | 7,196 | 6,667 |
| Sampled parse p50 | 2.4 µs | 2.5 µs |
| Sampled parse p95 | 4.5 µs | 4.9 µs |
| Peak process working set | 19.8 MiB | 19.6 MiB |
| SQLite bytes after WAL checkpoint | 10,625,683,456 | 10,625,683,456 |
| SQLite bytes per distinct paper | 3,267.1 | 3,267.1 |

The schema uses a composite primary key on a `WITHOUT ROWID` papers table and has no secondary indexes. SQLite used 4,096-byte pages, WAL mode and `synchronous=FULL`. Both runs committed up to 4,096 records per batch. The timer includes the initial SHA-256 scan, parsing and normalization, and committed writes. The storage measurement follows a WAL truncation checkpoint. Download, extraction, validation, and live syncing are outside the timer. Each run sampled parsing and normalization for 25,410 input records, every 128th record.

An independent validation pass reconciled the input count with the loaded rows and duplicates, found no missing fields or rejects, and confirmed SQLite integrity.

### Reproduce

```powershell
cargo build --release --locked
$arkhiva = './target/release/arkhiva.exe'
& $arkhiva snapshot fetch --version 312
$source = 'data/arxiv-v312/arxiv-metadata-oai-snapshot.json'

& $arkhiva --db data/bench-v312-a.sqlite bench import --file $source --release 312 --batch-size 4096
& $arkhiva --db data/bench-v312-b.sqlite bench import --file $source --release 312 --batch-size 4096
& $arkhiva --db data/bench-v312-a.sqlite validate --file $source
```

Use fresh paths for the two benchmark databases. Compare the input checksum, counts, runtime settings, and storage size before attributing a difference to changes in code or schema.

## Live handoff

On November 16, 2026 the validated database completed a 27-page catch-up from the assumed November 1 overlap and a two-page incremental run. The runs processed 33,876 and 2,175 OAI records respectively, including records already held by the corpus. The next inclusive cursor was November 16 UTC. The active corpus grew by 1,226 papers to 3,253,585. A comparison against the repeat snapshot database confirmed that an overlapping ID and a newly harvested ID each had one row.

## Search latency and I/O

Measured on November 17, 2026 on the same machine and `E:` drive, against the 3,253,585-paper Parquet corpus. Each cold run searches a fresh unbuffered copy of the corpus, so none of it is in the OS file cache; the warm run repeats the same queries on that copy. Values are medians of three runs and include process start. Reads and bytes come from the process I/O counters; Tantivy reads memory-mapped files, which those counters do not include.

"Before" is the layout with 1 MiB Parquet pages, snippets on every hit, and a `tantivy-v1` index with term positions in 10 segments. "After" is 16 KiB pages, snippets on the top five hits, and a single-segment `tantivy-v2` index without positions.

| Query | Cold before | Cold after | Warm before | Warm after | Bytes read before → after |
| --- | ---: | ---: | ---: | ---: | ---: |
| Text, 20 hits | 1,902 ms | 862 ms | 128 ms | 62 ms | 15.6 → 6.3 MB |
| Text, 100 hits | 1,921 ms | 924 ms | 284 ms | 80 ms | 59.0 → 7.5 MB |
| Category browse, 20 hits | 776 ms | 415 ms | 73 ms | 47 ms | 0.6 → 3.0 MB |
| ID lookup | 94 ms | 62 ms | 59 ms | 32 ms | 1.0 → 3.0 MB |

Smaller pages raised the Parquet file from 1.21 GB to 1.44 GB and enlarged its offset index to about 3 MB, which every query in this table still read in full. Dropping positions shrank the Tantivy index from 1.1 GB to 541 MB. With a warm cache, Tantivy itself answers a text query in about 3 ms; process start, opening files, and reading result rows make up the rest.

### Offset index per row group

Arkhiva now reads the Parquet footer without its page index and then loads offset index entries only for the row groups that hold results. Measured the same day, comparing the build that loaded the whole index ("full") with the one that loads it per row group. Warm values are medians of seven runs; cold values are two runs, each on a fresh unbuffered copy. Outputs were identical.

| Query | Warm full | Warm per group | Cold full | Cold per group | Bytes read full → per group |
| --- | ---: | ---: | ---: | ---: | ---: |
| Text, 20 hits | 84 ms | 40 ms | 918 / 897 ms | 853 / 846 ms | 6.3 → 1.1 MB |
| Text, 100 hits | 107 ms | 66 ms | 1,521 / 1,479 ms | 1,345 / 1,611 ms | 7.5 → 2.5 MB |
| Category browse, 20 hits | 72 ms | 34 ms | 421 / 454 ms | 404 / 354 ms | 3.0 → 0.3 MB |
| ID lookup | 64 ms | 24 ms | 205 / 214 ms | 121 / 137 ms | 3.0 → 0.3 MB |

Every query in this comparison ran on its own cold copy, including the Tantivy files, so cold times differ from the table above, where later queries shared a copy. Cold text search remains dominated by first reads of the memory-mapped Tantivy index, which the I/O counters do not show.
