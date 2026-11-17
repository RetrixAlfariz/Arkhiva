use crate::model::{Change, Paper};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    path::Path,
    time::Duration,
};

const PARQUET_APPLICATION_ID: i32 = 0x4152_5051;

pub type OaiChanges = Vec<(String, Change)>;

pub struct PageCheckpoint<'a> {
    pub feed: &'a str,
    pub from: &'a str,
    pub first_response: &'a str,
    pub next_from: &'a str,
    pub token: Option<&'a str>,
}

pub struct Store {
    pub conn: Connection,
    pub provider: &'static str,
    _lock: File,
}

#[derive(Debug, Default, Serialize)]
pub struct Import {
    pub release: Option<String>,
    pub sha256: String,
    pub file_size: i64,
    pub next_offset: i64,
    pub lines: i64,
    pub inserted: i64,
    pub duplicates: i64,
    pub rejected: i64,
    pub complete: bool,
    pub validated: bool,
}

#[derive(Debug, Serialize)]
pub struct SyncState {
    pub bootstrap_from: String,
    pub bootstrap_basis: String,
    pub next_from: String,
    pub active_from: Option<String>,
    pub first_response: Option<String>,
    pub token: Option<String>,
    pub completed_harvests: i64,
    pub committed_pages: i64,
    pub committed_changes: i64,
}

pub struct SnapshotRow {
    pub offset: i64,
    pub line: i64,
    pub record: Result<Paper>,
}

impl Store {
    pub fn open(path: &Path, provider: &'static str) -> Result<Self> {
        Self::open_with_application(path, provider, 0)
    }

    pub(crate) fn open_parquet_index(path: &Path, provider: &'static str) -> Result<Self> {
        Self::open_with_application(path, provider, PARQUET_APPLICATION_ID)
    }

    fn open_with_application(
        path: &Path,
        provider: &'static str,
        application: i32,
    ) -> Result<Self> {
        let absolute = if path.exists() {
            path.canonicalize()?
        } else {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            parent
                .canonicalize()?
                .join(path.file_name().context("database file name missing")?)
        };
        let mut lock_path = absolute.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock()
            .context("database is locked by another Arkhiva process")?;
        let mut conn = Connection::open(absolute)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let version: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
        let id: i32 = conn.pragma_query_value(None, "application_id", |row| row.get(0))?;
        let uninitialized = version == 0
            && id == 0
            && conn.query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM sqlite_schema)",
                [],
                |row| row.get::<_, bool>(0),
            )?;
        ensure!(
            uninitialized || (version != 0 && id == application),
            "database belongs to a different storage layout"
        );
        let parquet = application == PARQUET_APPLICATION_ID;
        let latest = if parquet { 2 } else { 1 };
        ensure!(version <= latest, "unsupported schema version {version}");
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        if uninitialized || version < latest {
            let tx = conn.transaction()?;
            if uninitialized {
                tx.execute_batch(include_str!("../migrations/001.sql"))?;
                if parquet {
                    tx.execute_batch(include_str!("../migrations/parquet.sql"))?;
                }
                tx.pragma_update(None, "application_id", application)?;
            }
            if parquet {
                tx.execute_batch(include_str!("../migrations/paper_tags.sql"))?;
            }
            tx.pragma_update(None, "user_version", latest)?;
            tx.commit()?;
        }
        Ok(Self {
            conn,
            provider,
            _lock: lock,
        })
    }

    pub fn import(&self) -> Result<Option<Import>> {
        Ok(self
            .conn
            .query_row(
                "SELECT release, sha256, file_size, next_offset, lines, inserted, duplicates,
                    rejected, complete, validation_json IS NOT NULL
             FROM snapshot_imports WHERE provider=?1",
                [self.provider],
                |r| {
                    Ok(Import {
                        release: r.get(0)?,
                        sha256: r.get(1)?,
                        file_size: r.get(2)?,
                        next_offset: r.get(3)?,
                        lines: r.get(4)?,
                        inserted: r.get(5)?,
                        duplicates: r.get(6)?,
                        rejected: r.get(7)?,
                        complete: r.get(8)?,
                        validated: r.get(9)?,
                    })
                },
            )
            .optional()?)
    }

    pub fn begin_import(&self, sha256: &str, bytes: i64, release: Option<&str>) -> Result<Import> {
        self.conn.execute(
            "INSERT INTO snapshot_imports(provider, release, sha256, file_size)
             VALUES(?1, ?2, ?3, ?4) ON CONFLICT(provider) DO NOTHING",
            params![self.provider, release, sha256, bytes],
        )?;
        let state = self.import()?.context("snapshot import state missing")?;
        ensure!(
            state.sha256 == sha256 && state.file_size == bytes,
            "snapshot differs from the file recorded for this database"
        );
        ensure!(
            state.release.as_deref() == release || release.is_none(),
            "snapshot release differs from the recorded release"
        );
        Ok(state)
    }

    pub fn snapshot_batch(
        &mut self,
        state: &mut Import,
        rows: Vec<SnapshotRow>,
        next_offset: i64,
        complete: bool,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        let mut inserted = 0;
        let mut rejected = 0;
        let lines = rows.len() as i64;
        {
            let mut insert = tx.prepare_cached(INSERT_PAPER)?;
            let mut reject = tx.prepare_cached(
                "INSERT INTO snapshot_rejects(provider, byte_offset, line, reason) VALUES(?1, ?2, ?3, ?4)",
            )?;
            for row in rows {
                match row.record {
                    Ok(paper) => {
                        inserted += write_paper(&mut insert, self.provider, &paper, None)? as i64
                    }
                    Err(error) => {
                        reject.execute(params![
                            self.provider,
                            row.offset,
                            row.line,
                            error.to_string()
                        ])?;
                        rejected += 1;
                    }
                }
            }
        }
        let duplicates = lines - inserted - rejected;
        tx.execute(
            "UPDATE snapshot_imports SET next_offset=?2, complete=?3, lines=lines+?4, inserted=inserted+?5,
              duplicates=duplicates+?6, rejected=rejected+?7 WHERE provider=?1",
            params![self.provider, next_offset, complete, lines, inserted, duplicates, rejected],
        )?;
        tx.commit()?;
        state.next_offset = next_offset;
        state.complete = complete;
        state.lines += lines;
        state.inserted += inserted;
        state.duplicates += duplicates;
        state.rejected += rejected;
        Ok(())
    }

    pub fn sync_state(&self, feed: &str) -> Result<Option<SyncState>> {
        Ok(self.conn.query_row(
            "SELECT bootstrap_from, bootstrap_basis, next_from, active_from, first_response, token,
                    completed_harvests, committed_pages, committed_changes FROM oai_sync_state WHERE feed=?1",
            [feed],
            |r| Ok(SyncState {
                bootstrap_from: r.get(0)?, bootstrap_basis: r.get(1)?, next_from: r.get(2)?,
                active_from: r.get(3)?, first_response: r.get(4)?, token: r.get(5)?,
                completed_harvests: r.get(6)?, committed_pages: r.get(7)?, committed_changes: r.get(8)?,
            }),
        ).optional()?)
    }

    pub fn start_sync(&self, feed: &str, from: &str, basis: &str) -> Result<()> {
        self.conn.execute(
            "INSERT INTO oai_sync_state(feed, bootstrap_from, bootstrap_basis, next_from) VALUES(?1, ?2, ?3, ?2)",
            params![feed, from, basis],
        )?;
        Ok(())
    }

    pub fn restart_sync(&self, feed: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE oai_sync_state SET first_response=NULL, token=NULL WHERE feed=?1",
            [feed],
        )?;
        Ok(())
    }

    pub fn commit_page(
        &mut self,
        page: PageCheckpoint<'_>,
        changes: OaiChanges,
        apply: impl FnOnce(&Transaction<'_>, &str, OaiChanges) -> Result<()>,
    ) -> Result<()> {
        let PageCheckpoint {
            feed,
            from,
            first_response,
            next_from,
            token,
        } = page;
        let tx = self.conn.transaction()?;
        let count = changes.len() as i64;
        apply(&tx, self.provider, changes)?;
        if let Some(token) = token {
            tx.execute(
                "UPDATE oai_sync_state SET active_from=?2, first_response=?3, token=?4,
                    committed_pages=committed_pages+1, committed_changes=committed_changes+?5 WHERE feed=?1",
                params![feed, from, first_response, token, count],
            )?;
        } else {
            tx.execute(
                "UPDATE oai_sync_state SET next_from=?2, active_from=NULL, first_response=NULL, token=NULL,
                    completed_harvests=completed_harvests+1, committed_pages=committed_pages+1,
                    committed_changes=committed_changes+?3 WHERE feed=?1",
                params![feed, next_from, count],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
}

const INSERT_PAPER: &str = "INSERT INTO papers(provider, external_id, url, title, abstract,
    categories_json, first_submitted_on, last_revised_on, metadata_updated_on, oai_datestamp)
    VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
    ON CONFLICT(provider, external_id) DO NOTHING";

const UPSERT_PAPER: &str = "INSERT INTO papers(provider, external_id, url, title, abstract,
    categories_json, first_submitted_on, last_revised_on, metadata_updated_on, oai_datestamp)
    VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
    ON CONFLICT(provider, external_id) DO UPDATE SET url=excluded.url, title=excluded.title,
    abstract=excluded.abstract, categories_json=excluded.categories_json,
    first_submitted_on=coalesce(excluded.first_submitted_on, papers.first_submitted_on),
    last_revised_on=coalesce(excluded.last_revised_on, papers.last_revised_on),
    metadata_updated_on=excluded.metadata_updated_on, oai_datestamp=excluded.oai_datestamp,
    is_deleted=0 WHERE papers.oai_datestamp IS NULL OR excluded.oai_datestamp >= papers.oai_datestamp";

fn write_paper(
    statement: &mut rusqlite::Statement<'_>,
    provider: &str,
    paper: &Paper,
    datestamp: Option<&str>,
) -> Result<usize> {
    Ok(statement.execute(params![
        provider,
        paper.id,
        paper.url,
        paper.title,
        paper.abstract_text,
        serde_json::to_string(&paper.categories)?,
        paper.first_submitted.map(|date| date.to_string()),
        paper.last_revised.map(|date| date.to_string()),
        paper.metadata_updated.map(|date| date.to_string()),
        datestamp,
    ])?)
}
pub fn apply_sqlite_page(tx: &Transaction<'_>, provider: &str, changes: OaiChanges) -> Result<()> {
    for (datestamp, change) in changes {
        apply_oai(tx, provider, &datestamp, change)?;
    }
    Ok(())
}

fn apply_oai(tx: &Transaction<'_>, provider: &str, datestamp: &str, change: Change) -> Result<()> {
    match change {
        Change::Upsert(paper) => {
            let mut statement = tx.prepare_cached(UPSERT_PAPER)?;
            write_paper(&mut statement, provider, &paper, Some(datestamp))?;
        }
        Change::Delete { id, url } => {
            tx.prepare_cached(
                "INSERT INTO papers(provider, external_id, url, categories_json, metadata_updated_on, oai_datestamp, is_deleted)
                 VALUES(?1, ?2, ?3, '[]', substr(?4,1,10), ?4, 1)
                 ON CONFLICT(provider, external_id) DO UPDATE SET is_deleted=1, metadata_updated_on=excluded.metadata_updated_on,
                    oai_datestamp=excluded.oai_datestamp
                 WHERE papers.oai_datestamp IS NULL OR excluded.oai_datestamp >= papers.oai_datestamp",
            )?.execute(params![provider, id, url, datestamp])?;
        }
    }
    Ok(())
}
