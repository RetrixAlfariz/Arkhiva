use crate::{
    arxiv::{PROVIDER, arxiv_lookup_id},
    model::{Change, Paper},
    oai::{self, Bootstrap},
    store::{OaiChanges, Store, SyncState},
};
use anyhow::{Context, Result, bail, ensure};
use arrow_array::{
    Array, ArrayRef, BooleanArray, ListArray, RecordBatch, StringArray,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{
            ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder, RowSelection,
        },
    },
    basic::{Compression, ZstdLevel},
    file::{
        metadata::{KeyValue, PageIndexPolicy, ParquetMetaData, page_index::PageIndexBuilder},
        page_index::index_reader::decode_offset_index,
        reader::ChunkReader,
        properties::{EnabledStatistics, WriterProperties},
    },
    schema::types::ColumnPath,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Row, Transaction, params, params_from_iter,
    types::Value,
};
use serde_json::{Map, json};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

const INDEX: &str = "index.sqlite";
const ROW_GROUP_ROWS: usize = 8192;
// A search hit decompresses one whole page per column, so small pages keep point reads cheap.
// Tune DATA_PAGE_BYTES for the compression-vs-read-size trade-off, then bump LAYOUT_VERSION.
const DATA_PAGE_BYTES: usize = 16 * 1024;
// The writer checks page limits only between batches of this many rows.
const WRITE_BATCH_ROWS: usize = 16;
const LAYOUT_KEY: &str = "arkhiva.layout";
const LAYOUT_VERSION: &str = "2";
const REMOVED_COLUMNS: [&str; 4] = [
    "last_revised_on",
    "metadata_updated_on",
    "oai_datestamp",
    "is_deleted",
];
const CATEGORIES: &str = "categories";
const LEGACY_CATEGORIES: &str = "categories_json";
const TEXT_COLUMNS: [&str; 6] = [
    "provider",
    "external_id",
    "url",
    "title",
    "abstract",
    "first_submitted_on",
];
const INSERT_KEY: &str = "INSERT INTO paper_index VALUES (?1, ?2, ?3, ?4)
    ON CONFLICT(provider, external_id) DO NOTHING";

struct CorpusRow {
    text: [Option<String>; TEXT_COLUMNS.len()],
    categories: Vec<String>,
}

impl CorpusRow {
    fn from_sql(row: &Row<'_>) -> Result<Self> {
        let mut text = std::array::from_fn(|_| None);
        for (index, value) in text.iter_mut().enumerate() {
            *value = row.get(index)?;
        }
        let categories: String = row.get(TEXT_COLUMNS.len())?;
        Ok(Self {
            text,
            categories: serde_json::from_str(&categories).context("invalid SQLite categories")?,
        })
    }

    fn from_oai(provider: &str, paper: Paper) -> Self {
        Self {
            text: [
                Some(provider.to_owned()),
                Some(paper.id),
                Some(paper.url),
                paper.title,
                paper.abstract_text,
                paper.first_submitted.map(|d| d.to_string()),
            ],
            categories: paper.categories,
        }
    }
}

pub struct ParquetStore {
    directory: PathBuf,
    index: Store,
}

impl ParquetStore {
    pub fn open(directory: &Path) -> Result<Self> {
        let directory = directory.canonicalize()?;
        let index_path = directory.join(INDEX);
        ensure!(
            index_path.is_file(),
            "missing Parquet index; run parquet seed first"
        );
        let index = Store::open_parquet_index(&index_path, PROVIDER)?;
        let corpus = Self { directory, index };
        corpus.check_files()?;
        let orphan = part_path(&corpus.directory, next_file_id(&corpus.index.conn)?);
        remove_if_present(&orphan)?;
        remove_if_present(&orphan.with_extension("parquet.part"))?;
        Ok(corpus)
    }

    pub fn seed(directory: &Path, source: &Path) -> Result<Self> {
        let mut source_conn = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("opening source corpus {}", source.display()))?;
        let source = source_conn.transaction()?;
        let validated = source.query_row(
            "SELECT EXISTS(SELECT 1 FROM snapshot_imports
                WHERE provider=?1 AND complete=1 AND validation_json IS NOT NULL)",
            [PROVIDER],
            |row| row.get::<_, bool>(0),
        )?;
        ensure!(validated, "validate the source corpus before seeding");

        fs::create_dir_all(directory)?;
        let directory = directory.canonicalize()?;
        let index_path = directory.join(INDEX);
        ensure!(
            index_path.exists() || fs::read_dir(&directory)?.next().is_none(),
            "seed requires an empty directory or an interrupted Arkhiva seed"
        );
        let mut index = Store::open_parquet_index(&index_path, PROVIDER)?;
        let tx = index.conn.transaction()?;
        let files: i64 =
            tx.query_row("SELECT count(*) FROM parquet_files", [], |row| row.get(0))?;
        ensure!(files == 0, "Parquet corpus is already seeded");

        let file_id = 1;
        tx.execute("INSERT INTO parquet_files VALUES (?1, 0, 0)", [file_id])?;
        let mut source_query = source.prepare(&format!(
            "SELECT {}, {LEGACY_CATEGORIES} FROM papers WHERE is_deleted=0 ORDER BY provider, external_id",
            TEXT_COLUMNS.join(", ")
        ))?;
        let (rows, bytes) = materialize(&directory, file_id, |writer, schema| {
            let mut source_rows = source_query.query([])?;
            let mut insert = tx.prepare(INSERT_KEY)?;
            let mut position: i64 = 0;
            let mut batch = Vec::with_capacity(ROW_GROUP_ROWS);
            while let Some(row) = source_rows.next()? {
                let row = CorpusRow::from_sql(row)?;
                insert.execute(params![row.text[0], row.text[1], file_id, position])?;
                batch.push(row);
                position += 1;
                if batch.len() == ROW_GROUP_ROWS {
                    write_batch(writer, schema, &batch)?;
                    batch.clear();
                }
            }
            if !batch.is_empty() {
                write_batch(writer, schema, &batch)?;
            }
            Ok(())
        })?;
        tx.execute(
            "UPDATE parquet_files SET rows=?1, bytes=?2 WHERE file_id=?3",
            params![rows, bytes, file_id],
        )?;
        for name in ["snapshot_imports", "snapshot_rejects", "oai_sync_state"] {
            copy_table(&source, &tx, name)?;
        }
        tx.commit()?;
        Ok(Self { directory, index })
    }

    pub fn migrate_schema(&mut self) -> Result<usize> {
        let files = {
            let mut query = self
                .index
                .conn
                .prepare("SELECT file_id, rows FROM parquet_files ORDER BY file_id")?;
            query
                .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut migrated = 0;
        for (old_id, expected_rows) in files {
            let old_path = part_path(&self.directory, old_id);
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&old_path)?)?;
            // Compare fields only: the reader copies file key-value metadata into the schema.
            if reader.schema().fields() == schema().fields() && has_current_layout(reader.metadata())
            {
                continue;
            }
            let projection = record_projection(reader.schema())?;
            let reader = reader.with_batch_size(ROW_GROUP_ROWS).build()?;
            let tx = self.index.conn.transaction()?;
            let new_id = next_file_id(&tx)?;
            let (rows, bytes) = materialize(&self.directory, new_id, |writer, schema| {
                for batch in reader {
                    let batch = batch?;
                    if let Some(flags) = legacy_deletion_flags(&batch)? {
                        ensure!(
                            flags.null_count() == 0 && flags.true_count() == 0,
                            "legacy Parquet contains deleted rows or invalid deletion flags"
                        );
                    }
                    writer.write(&project_record_batch(&batch, &projection, schema)?)?;
                }
                Ok(())
            })?;
            ensure!(
                rows == expected_rows,
                "Parquet row count changed during migration"
            );
            tx.execute(
                "INSERT INTO parquet_files VALUES (?1, ?2, ?3)",
                params![new_id, rows, bytes],
            )?;
            let moved = tx.execute(
                "UPDATE paper_index SET file_id=?1 WHERE file_id=?2",
                params![new_id, old_id],
            )?;
            ensure!(
                i64::try_from(moved)? == rows,
                "index row count changed during migration"
            );
            tx.execute("DELETE FROM parquet_files WHERE file_id=?1", [old_id])?;
            tx.commit()?;
            remove_if_present(&old_path)?;
            migrated += 1;
        }
        self.remove_retired_parts()?;
        Ok(migrated)
    }

    fn remove_retired_parts(&self) -> Result<()> {
        let mut registered = self
            .index
            .conn
            .prepare("SELECT EXISTS(SELECT 1 FROM parquet_files WHERE file_id=?1)")?;
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id) = name.to_str().and_then(|name| {
                name.strip_prefix("part-")?
                    .strip_suffix(".parquet")?
                    .parse::<i64>()
                    .ok()
            }) else {
                continue;
            };
            if id > 0
                && part_path(&self.directory, id).file_name() == Some(name.as_os_str())
                && entry.file_type()?.is_file()
                && !registered.query_row([id], |row| row.get::<_, bool>(0))?
            {
                remove_if_present(&entry.path())?;
            }
        }
        Ok(())
    }

    pub fn sync(&mut self, bootstrap: Option<Bootstrap>) -> Result<SyncState> {
        let directory = &self.directory;
        let state = oai::sync_with(
            &mut self.index,
            oai::ENDPOINT,
            bootstrap,
            |tx, provider, changes| append_page(directory, tx, provider, changes),
        )?;
        self.refresh_search_index().context(
            "OAI changes are saved, but the search index refresh failed; rerun sync to retry",
        )?;
        Ok(state)
    }

    pub fn status(&self) -> Result<serde_json::Value> {
        let (files, papers, bytes): (i64, i64, i64) = self.index.conn.query_row(
            "SELECT count(*), coalesce(sum(rows), 0), coalesce(sum(bytes), 0) FROM parquet_files",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok(json!({
            "storage": "parquet", "papers": papers, "parquet_files": files, "parquet_bytes": bytes,
            "index_bytes": fs::metadata(self.directory.join(INDEX))?.len(),
            "snapshot": self.index.import()?,
            "oai": self.index.sync_state(&oai::feed(oai::ENDPOINT))?,
        }))
    }

    fn paper_location(&self, id: &str) -> Result<Option<(i64, i64)>> {
        Ok(self
            .index
            .conn
            .query_row(
                "SELECT file_id, row_no FROM paper_index WHERE provider=?1 AND external_id=?2",
                params![PROVIDER, id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?)
    }

    pub fn get(&self, id: &str) -> Result<Option<serde_json::Value>> {
        let id = arxiv_lookup_id(id)?;
        let Some((file_id, row)) = self.paper_location(&id)? else {
            return Ok(None);
        };
        let mut offset = usize::try_from(row)?;
        let (file, metadata) = open_part(&part_path(&self.directory, file_id))?;
        let projection = record_projection(metadata.schema())?;
        let (group, offset) = metadata
            .metadata()
            .row_groups()
            .iter()
            .enumerate()
            .find_map(|(n, group)| {
                let rows = group.num_rows() as usize;
                if offset < rows {
                    Some((n, offset))
                } else {
                    offset -= rows;
                    None
                }
            })
            .context("indexed row is outside its Parquet file")?;
        let rows = usize::try_from(metadata.metadata().row_group(group).num_rows())?;
        let batch = with_offset_index(file, &metadata, &[group])?
            .with_batch_size(1)
            .with_row_groups(vec![group])
            .with_row_selection(RowSelection::from_consecutive_ranges(
                std::iter::once(offset..offset + 1),
                rows,
            ))
            .build()?
            .next()
            .context("missing Parquet row group")??;
        ensure!(batch.num_rows() == 1, "missing indexed Parquet row");
        let offset = 0;
        if let Some(flags) = legacy_deletion_flags(&batch)? {
            ensure!(
                !flags.is_null(offset),
                "invalid legacy Parquet deletion flag"
            );
            if flags.value(offset) {
                return Ok(None);
            }
        }
        let batch = project_record_batch(&batch, &projection, &schema())?;
        let mut result = Map::new();
        for (index, name) in TEXT_COLUMNS.iter().enumerate() {
            let column = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("invalid Parquet text column")?;
            let value = (!column.is_null(offset)).then(|| column.value(offset));
            result.insert((*name).to_owned(), json!(value));
        }
        let categories = batch
            .column(TEXT_COLUMNS.len())
            .as_any()
            .downcast_ref::<ListArray>()
            .context("invalid Parquet categories list")?
            .value(offset);
        let categories = categories
            .as_any()
            .downcast_ref::<StringArray>()
            .context("invalid Parquet category values")?;
        result.insert(
            CATEGORIES.to_owned(),
            json!(categories.iter().collect::<Vec<_>>()),
        );
        ensure!(
            result["provider"] == PROVIDER && result["external_id"] == id,
            "indexed row has a different paper ID"
        );
        Ok(Some(serde_json::Value::Object(result)))
    }

    fn check_files(&self) -> Result<()> {
        let mut query = self
            .index
            .conn
            .prepare("SELECT file_id, rows, bytes FROM parquet_files")?;
        let mut rows = query.query([])?;
        ensure!(
            self.index.import()?.is_some_and(|s| s.validated),
            "Parquet seed is incomplete; rerun parquet seed"
        );
        let mut files = 0;
        while let Some(row) = rows.next()? {
            let file_id: i64 = row.get(0)?;
            let count: i64 = row.get(1)?;
            let bytes: u64 = row.get::<_, i64>(2)?.try_into()?;
            let path = part_path(&self.directory, file_id);
            ensure!(
                count > 0
                    && bytes > 0
                    && fs::metadata(&path).is_ok_and(|meta| meta.is_file() && meta.len() == bytes),
                "missing or changed Parquet file {}",
                path.display()
            );
            files += 1;
        }
        ensure!(files > 0, "Parquet seed is incomplete; rerun parquet seed");
        Ok(())
    }
}

fn append_page(
    directory: &Path,
    tx: &Transaction<'_>,
    provider: &str,
    changes: OaiChanges,
) -> Result<()> {
    let file_id = next_file_id(tx)?;
    tx.execute("INSERT INTO parquet_files VALUES (?1, 0, 0)", [file_id])?;
    let mut pending = Vec::new();
    {
        let mut index = tx.prepare_cached(INSERT_KEY)?;
        for (_, change) in changes {
            if let Change::Upsert(paper) = change
                && index.execute(params![provider, paper.id, file_id, pending.len() as i64])? > 0
            {
                pending.push(CorpusRow::from_oai(provider, paper));
            }
        }
    }
    if pending.is_empty() {
        tx.execute("DELETE FROM parquet_files WHERE file_id=?1", [file_id])?;
        return Ok(());
    }
    let (rows, bytes) = materialize(directory, file_id, |writer, schema| {
        write_batch(writer, schema, &pending)
    })?;
    tx.execute(
        "UPDATE parquet_files SET rows=?1, bytes=?2 WHERE file_id=?3",
        params![rows, bytes, file_id],
    )?;
    Ok(())
}

fn next_file_id(conn: &Connection) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT coalesce(max(file_id), 0) + 1 FROM parquet_files",
        [],
        |row| row.get(0),
    )?)
}

fn copy_table(source: &Transaction<'_>, target: &Transaction<'_>, table: &str) -> Result<()> {
    let mut select = source.prepare(&format!("SELECT * FROM {table}"))?;
    let columns = select.column_count();
    let values = vec!["?"; columns].join(", ");
    let mut insert = target.prepare(&format!("INSERT INTO {table} VALUES ({values})"))?;
    let mut rows = select.query([])?;
    while let Some(row) = rows.next()? {
        let values = (0..columns)
            .map(|n| row.get::<_, Value>(n))
            .collect::<rusqlite::Result<Vec<_>>>()?;
        insert.execute(params_from_iter(values))?;
    }
    Ok(())
}

fn category_field() -> Field {
    Field::new_list_field(DataType::Utf8, false)
}

fn schema() -> SchemaRef {
    let mut fields: Vec<_> = TEXT_COLUMNS
        .iter()
        .enumerate()
        .map(|(index, name)| Field::new(*name, DataType::Utf8, index > 2))
        .collect();
    fields.push(Field::new_list(CATEGORIES, category_field(), false));
    Arc::new(Schema::new(fields))
}

fn record_projection(source: &SchemaRef) -> Result<Vec<usize>> {
    let expected = schema();
    let mut projection = TEXT_COLUMNS
        .iter()
        .map(|name| source.index_of(name))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        projection
            .iter()
            .enumerate()
            .all(|(n, index)| source.field(*index) == expected.field(n))
            && source.fields().iter().all(|field| {
                expected.index_of(field.name()).is_ok()
                    || field.name() == LEGACY_CATEGORIES
                    || REMOVED_COLUMNS.contains(&field.name().as_str())
            }),
        "unexpected Parquet schema"
    );
    let category = match (
        source.index_of(CATEGORIES),
        source.index_of(LEGACY_CATEGORIES),
    ) {
        (Ok(index), Err(_)) if source.field(index) == expected.field(TEXT_COLUMNS.len()) => index,
        (Err(_), Ok(index))
            if source.field(index) == &Field::new(LEGACY_CATEGORIES, DataType::Utf8, false) =>
        {
            index
        }
        _ => bail!("invalid Parquet categories column"),
    };
    projection.push(category);
    Ok(projection)
}

fn project_record_batch(
    batch: &RecordBatch,
    projection: &[usize],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut columns: Vec<_> = projection
        .iter()
        .map(|&n| Arc::clone(batch.column(n)))
        .collect();
    let category = &mut columns[TEXT_COLUMNS.len()];
    *category = native_categories(category)?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

fn native_categories(category: &ArrayRef) -> Result<ArrayRef> {
    let Some(json) = category.as_any().downcast_ref::<StringArray>() else {
        return Ok(Arc::clone(category));
    };
    let decoded = json
        .iter()
        .map(|value| {
            serde_json::from_str::<Vec<String>>(value.context("missing legacy categories")?)
                .context("invalid legacy Parquet categories")
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(build_categories(decoded.iter().map(Vec::as_slice)))
}

fn build_categories<'a>(rows: impl IntoIterator<Item = &'a [String]>) -> ArrayRef {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(category_field());
    for categories in rows {
        for category in categories {
            builder.values().append_value(category);
        }
        builder.append(true);
    }
    Arc::new(builder.finish())
}

fn legacy_deletion_flags(batch: &RecordBatch) -> Result<Option<&BooleanArray>> {
    batch
        .column_by_name("is_deleted")
        .map(|column| {
            column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("invalid legacy Parquet deletion flag")
        })
        .transpose()
}

fn write_batch(
    writer: &mut ArrowWriter<&mut File>,
    schema: &SchemaRef,
    rows: &[CorpusRow],
) -> Result<()> {
    let mut columns: Vec<ArrayRef> = (0..TEXT_COLUMNS.len())
        .map(|n| {
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.text[n].as_deref()),
            )) as ArrayRef
        })
        .collect();
    columns.push(build_categories(
        rows.iter().map(|row| row.categories.as_slice()),
    ));
    writer.write(&RecordBatch::try_new(Arc::clone(schema), columns)?)?;
    Ok(())
}

/// Opens a part's footer without its page index; see [`with_offset_index`].
fn open_part(path: &Path) -> Result<(File, ArrowReaderMetadata)> {
    let file = File::open(path)?;
    let metadata = ArrowReaderMetadata::load(
        &file,
        ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip),
    )?;
    Ok((file, metadata))
}

/// Builds a reader that knows the page locations of `row_groups` only.
/// Small pages make the whole file's offset index several megabytes, and a point read
/// needs just the entries of the row groups it touches.
fn with_offset_index(
    file: File,
    metadata: &ArrowReaderMetadata,
    row_groups: &[usize],
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let parquet = metadata.metadata();
    let mut index = PageIndexBuilder::default();
    index.allocate_offset_indexes(
        parquet.num_row_groups(),
        parquet.file_metadata().schema_descr().num_columns(),
    );
    for &group in row_groups {
        let ranges = parquet
            .row_group(group)
            .columns()
            .iter()
            .map(|column| column.offset_index_range().context("missing Parquet offset index"))
            .collect::<Result<Vec<_>>>()?;
        // The writer stores a row group's offset indexes back to back, so one read covers them.
        let start = ranges.iter().map(|range| range.start).min().unwrap_or(0);
        let end = ranges.iter().map(|range| range.end).max().unwrap_or(0);
        let bytes = file.get_bytes(start, usize::try_from(end - start)?)?;
        for (column, range) in ranges.into_iter().enumerate() {
            let slice = &bytes[usize::try_from(range.start - start)?..usize::try_from(range.end - start)?];
            index.put_offset_index(decode_offset_index(slice)?, group, column);
        }
    }
    let parquet = Arc::unwrap_or_clone(Arc::clone(parquet))
        .into_builder()
        .set_page_index(Some(Arc::new(index.build())))
        .build();
    let metadata = ArrowReaderMetadata::try_new(Arc::new(parquet), ArrowReaderOptions::new())?;
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
        file, metadata,
    ))
}

fn has_current_layout(metadata: &ParquetMetaData) -> bool {
    metadata
        .file_metadata()
        .key_value_metadata()
        .is_some_and(|pairs| {
            pairs
                .iter()
                .any(|kv| kv.key == LAYOUT_KEY && kv.value.as_deref() == Some(LAYOUT_VERSION))
        })
}

fn part_path(directory: &Path, id: i64) -> PathBuf {
    directory.join(format!("part-{id:08}.parquet"))
}

fn materialize(
    directory: &Path,
    file_id: i64,
    write: impl FnOnce(&mut ArrowWriter<&mut File>, &SchemaRef) -> Result<()>,
) -> Result<(i64, i64)> {
    let destination = part_path(directory, file_id);
    let staging = destination.with_extension("parquet.part");
    remove_if_present(&staging)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)?;
    let schema = schema();
    let mut properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(false)
        .set_data_page_size_limit(DATA_PAGE_BYTES)
        .set_write_batch_size(WRITE_BATCH_ROWS)
        .set_key_value_metadata(Some(vec![KeyValue::new(
            LAYOUT_KEY.to_owned(),
            LAYOUT_VERSION.to_owned(),
        )]));
    for column in ["provider", "first_submitted_on"] {
        properties = properties.set_column_dictionary_enabled(ColumnPath::from(column), true);
    }
    for column in ["provider", "external_id", "first_submitted_on"] {
        properties = properties
            .set_column_statistics_enabled(ColumnPath::from(column), EnabledStatistics::Chunk);
    }
    let mut writer =
        ArrowWriter::try_new(&mut file, Arc::clone(&schema), Some(properties.build()))?;
    write(&mut writer, &schema)?;
    let rows = writer.close()?.file_metadata().num_rows();
    ensure!(rows > 0, "refusing to publish an empty Parquet file");
    file.sync_all()?;
    let bytes = file.metadata()?.len().try_into()?;
    drop(file);

    // The file ID was allocated in the current SQLite transaction.
    // An existing destination for that ID is left over from an uncommitted write.
    remove_if_present(&destination)?;
    fs::rename(&staging, &destination)?;
    #[cfg(unix)]
    File::open(directory)?.sync_all()?;
    Ok((rows, bytes))
}

fn remove_if_present(path: &Path) -> std::io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[path = "parquet_store_search.rs"]
mod search;
#[path = "parquet_store_topics.rs"]
mod topics;
pub use search::SearchArgs;

#[cfg(test)]
#[path = "parquet_store_tests.rs"]
mod tests;
