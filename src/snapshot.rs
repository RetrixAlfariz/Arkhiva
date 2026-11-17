use crate::{
    arxiv::{PROVIDER, arxiv_id, arxiv_url, categories, nonempty},
    http::Http,
    model::{Paper, version_date},
    store::{Import, SnapshotRow, Store},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const FILE: &str = "arxiv-metadata-oai-snapshot.json";
const DOWNLOAD: &str = "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv";

#[derive(Deserialize)]
struct Entry {
    id: String,
    title: Option<String>,
    #[serde(rename = "abstract")]
    abstract_text: Option<String>,
    categories: Option<String>,
    update_date: Option<String>,
    versions: Option<Vec<Version>>,
}

#[derive(Deserialize)]
struct Version {
    version: String,
    created: String,
}

fn parse(line: &[u8]) -> Result<Paper> {
    let entry: Entry = serde_json::from_slice(line)?;
    let id = arxiv_id(&entry.id)?;
    let mut first_submitted = None;
    let mut latest = None;
    for version in entry.versions.into_iter().flatten() {
        if let Some(number) = version
            .version
            .strip_prefix('v')
            .and_then(|n| n.parse::<u32>().ok())
        {
            let date = version_date(&version.created);
            if number == 1 {
                first_submitted = date;
            }
            if latest.is_none_or(|(old, _)| number > old) {
                latest = Some((number, date));
            }
        }
    }
    Ok(Paper {
        url: arxiv_url(&id),
        id,
        title: entry.title.as_deref().and_then(nonempty),
        abstract_text: entry.abstract_text.as_deref().and_then(nonempty),
        categories: entry
            .categories
            .as_deref()
            .map(categories)
            .unwrap_or_default(),
        first_submitted,
        last_revised: latest.and_then(|(_, date)| date),
        metadata_updated: entry.update_date.as_deref().and_then(version_date),
    })
}

pub fn fetch(version: u64, directory: &Path) -> Result<PathBuf> {
    ensure!(version > 0, "Kaggle release must be positive");
    let folder = directory.join(format!("arxiv-v{version}"));
    fs::create_dir_all(&folder)?;
    let archive = folder.join("arxiv.zip");
    let output = folder.join(FILE);
    if !archive.exists() {
        let temporary = folder.join("arkhiva-download.part");
        let mut http = Http::new(Duration::ZERO)?;
        http.request(
            |client| {
                client
                    .get(DOWNLOAD)
                    .query(&[("datasetVersionNumber", version)])
                    .timeout(Duration::from_secs(3600))
            },
            |mut response| {
                let mut file = BufWriter::new(File::create(&temporary)?);
                let expected = response.content_length();
                let actual = io::copy(&mut response, &mut file)?;
                ensure!(
                    expected.is_none_or(|expected| expected == actual),
                    "incomplete Kaggle archive"
                );
                file.flush()?;
                let mut zip = zip::ZipArchive::new(File::open(&temporary)?)?;
                zip.by_name(FILE)?;
                Ok(())
            },
        )?;
        fs::rename(&temporary, &archive)?;
    }
    if !output.exists() {
        let temporary = folder.join("arkhiva-extract.part");
        let mut zip = zip::ZipArchive::new(File::open(&archive)?)?;
        let mut entry = zip.by_name(FILE)?;
        let mut file = BufWriter::new(File::create(&temporary)?);
        let bytes = io::copy(&mut entry, &mut file)?;
        ensure!(bytes == entry.size(), "incomplete snapshot extraction");
        file.flush()?;
        drop(file);
        fs::rename(&temporary, &output)?;
    }
    Ok(output)
}

pub struct Metrics {
    pub parse_ns: Vec<u64>,
}

pub fn import(
    store: &mut Store,
    path: &Path,
    release: Option<&str>,
    batch_size: usize,
    mut metrics: Option<&mut Metrics>,
) -> Result<Import> {
    ensure!(batch_size > 0, "batch size must be positive");
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let fingerprint = fingerprint(&mut file, false)?;
    ensure!(fingerprint.bytes > 0, "snapshot is empty");
    let mut state = store.begin_import(&fingerprint.sha256, fingerprint.bytes, release)?;
    if state.complete {
        return Ok(state);
    }
    file.seek(SeekFrom::Start(u64::try_from(state.next_offset)?))?;
    let mut reader = BufReader::with_capacity(256 * 1024, file);
    let mut offset = state.next_offset;
    let mut line = Vec::new();
    loop {
        let mut rows = Vec::with_capacity(batch_size);
        let mut eof = false;
        for i in 0..batch_size {
            line.clear();
            let bytes = reader.read_until(b'\n', &mut line)?;
            if bytes == 0 {
                eof = true;
                break;
            }
            let line_number = state.lines + i as i64 + 1;
            let start = (metrics.is_some() && line_number % 128 == 1).then(Instant::now);
            let record = parse(&line);
            if let (Some(start), Some(metrics)) = (start, metrics.as_deref_mut()) {
                metrics.parse_ns.push(start.elapsed().as_nanos() as u64);
            }
            rows.push(SnapshotRow {
                offset,
                line: line_number,
                record,
            });
            offset += bytes as i64;
        }
        if eof {
            ensure!(
                offset == fingerprint.bytes,
                "snapshot changed during import"
            );
        }
        store.snapshot_batch(&mut state, rows, offset, eof)?;
        if eof {
            return Ok(state);
        }
    }
}

struct Fingerprint {
    bytes: i64,
    lines: i64,
    sha256: String,
}

fn fingerprint(file: &mut File, count_lines: bool) -> Result<Fingerprint> {
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut hash = Sha256::new();
    let mut bytes = 0;
    let mut lines = 0;
    let mut last = None;
    loop {
        let chunk = reader.fill_buf()?;
        if chunk.is_empty() {
            break;
        }
        hash.update(chunk);
        if count_lines {
            lines += chunk.iter().filter(|&&b| b == b'\n').count() as i64;
            last = chunk.last().copied();
        }
        let len = chunk.len();
        bytes += len as i64;
        reader.consume(len);
    }
    if last.is_some_and(|byte| byte != b'\n') {
        lines += 1;
    }
    Ok(Fingerprint {
        bytes,
        lines,
        sha256: format!("{:x}", hash.finalize()),
    })
}

#[derive(Serialize)]
struct Missing {
    title: i64,
    abstract_text: i64,
    categories: i64,
    first_submitted_on: i64,
    last_revised_on: i64,
    metadata_updated_on: i64,
}

#[derive(Serialize)]
struct Validation {
    release: Option<String>,
    sha256: String,
    input_bytes: i64,
    input_records: i64,
    distinct_imported: i64,
    duplicates: i64,
    rejected: i64,
    missing: Missing,
    integrity: &'static str,
}

pub fn validate(store: &Store, path: &Path) -> Result<serde_json::Value> {
    let state = store.import()?.context("no snapshot import")?;
    ensure!(state.complete, "snapshot import is incomplete");
    let fingerprint = fingerprint(&mut File::open(path)?, true)?;
    ensure!(
        fingerprint.sha256 == state.sha256 && fingerprint.bytes == state.file_size,
        "snapshot differs from the imported file"
    );
    ensure!(
        fingerprint.lines == state.lines
            && state.lines == state.inserted + state.duplicates + state.rejected
            && state.next_offset == state.file_size,
        "snapshot counts or byte offset do not reconcile"
    );
    let rejects: i64 = store.conn.query_row(
        "SELECT count(*) FROM snapshot_rejects WHERE provider=?1",
        [store.provider],
        |r| r.get(0),
    )?;
    ensure!(rejects == state.rejected, "reject count does not reconcile");
    let integrity: String = store
        .conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    ensure!(
        integrity == "ok",
        "SQLite integrity check failed: {integrity}"
    );
    if state.validated {
        let report: String = store.conn.query_row(
            "SELECT validation_json FROM snapshot_imports WHERE provider=?1",
            [store.provider],
            |r| r.get(0),
        )?;
        return Ok(serde_json::from_str(&report)?);
    }
    let (distinct_imported, missing) = store.conn.query_row(
        "SELECT count(*), count(*) FILTER (WHERE title IS NULL), count(*) FILTER (WHERE abstract IS NULL),
            count(*) FILTER (WHERE categories_json='[]'), count(*) FILTER (WHERE first_submitted_on IS NULL),
            count(*) FILTER (WHERE last_revised_on IS NULL), count(*) FILTER (WHERE metadata_updated_on IS NULL)
         FROM papers WHERE provider=?1 AND is_deleted=0", [store.provider], |r| Ok((
            r.get::<_, i64>(0)?, Missing {
                title: r.get(1)?, abstract_text: r.get(2)?, categories: r.get(3)?,
                first_submitted_on: r.get(4)?, last_revised_on: r.get(5)?, metadata_updated_on: r.get(6)?,
            },
        )),
    )?;
    ensure!(
        distinct_imported == state.inserted && distinct_imported > 0,
        "SQLite paper count does not match a nonempty snapshot import"
    );
    let report = serde_json::to_value(Validation {
        release: state.release,
        sha256: state.sha256,
        input_bytes: fingerprint.bytes,
        input_records: fingerprint.lines,
        distinct_imported,
        duplicates: state.duplicates,
        rejected: state.rejected,
        missing,
        integrity: "ok",
    })?;
    store.conn.execute(
        "UPDATE snapshot_imports SET validation_json=?2 WHERE provider=?1",
        rusqlite::params![store.provider, report.to_string()],
    )?;
    Ok(report)
}

pub fn benchmark(
    path: &Path,
    db: &Path,
    batch_size: usize,
    release: Option<&str>,
) -> Result<serde_json::Value> {
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(db)
        .context("benchmark requires a fresh database file")?;
    let mut store = Store::open(db, PROVIDER)?;
    let mut metrics = Metrics {
        parse_ns: Vec::new(),
    };
    let start = Instant::now();
    let state = import(&mut store, path, release, batch_size, Some(&mut metrics))?;
    let seconds = start.elapsed().as_secs_f64();
    let (busy, _, _): (i64, i64, i64) =
        store
            .conn
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })?;
    ensure!(busy == 0, "benchmark WAL checkpoint was blocked");
    let bytes = fs::metadata(db)?.len();
    let peak = peak_rss_bytes();
    let percentile =
        |rank: usize, sorted: &[u64]| sorted.len().checked_sub(1).map(|n| sorted[n * rank / 100]);
    metrics.parse_ns.sort_unstable();
    Ok(serde_json::json!({
        "arkhiva_version": env!("CARGO_PKG_VERSION"), "os": std::env::consts::OS,
        "architecture": std::env::consts::ARCH, "sqlite_version": rusqlite::version(),
        "sqlite_journal_mode": "wal", "sqlite_synchronous": "FULL",
        "batch_size": batch_size, "elapsed_seconds": seconds, "rows_per_second": state.lines as f64 / seconds,
        "parse_sample_every": 128, "parse_samples": metrics.parse_ns.len(),
        "parse_p50_ns": percentile(50, &metrics.parse_ns), "parse_p95_ns": percentile(95, &metrics.parse_ns),
        "peak_process_rss_bytes": peak, "database_bytes": bytes,
        "bytes_per_paper": (state.inserted > 0).then(|| bytes as f64 / state.inserted as f64),
        "snapshot": state
    }))
}

#[cfg(windows)]
fn peak_rss_bytes() -> Option<u64> {
    use windows_sys::Win32::System::{
        ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::GetCurrentProcess,
    };
    let mut counters = PROCESS_MEMORY_COUNTERS::default();
    let size = std::mem::size_of_val(&counters) as u32;
    // The pseudo-handle is valid for this process and the buffer has the required size.
    let ok = unsafe { GetProcessMemoryInfo(GetCurrentProcess(), &mut counters, size) };
    (ok != 0).then_some(counters.PeakWorkingSetSize as u64)
}

#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    let line = status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?;
    Some(line.split_whitespace().next()?.parse::<u64>().ok()? * 1024)
}

#[cfg(not(any(windows, target_os = "linux")))]
fn peak_rss_bytes() -> Option<u64> {
    None
}
