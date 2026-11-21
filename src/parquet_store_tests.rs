use super::{
    ParquetStore, REMOVED_COLUMNS, RowSelection, SearchArgs, append_page, has_current_layout,
    next_file_id, open_part, part_path, topics::TopicPlan, with_offset_index,
};
use crate::{
    arxiv::PROVIDER,
    model::{Change, Paper},
    oai, snapshot,
    store::{OaiChanges, PageCheckpoint, Store},
};
use anyhow::{Context, Result};
use arrow_array::{BooleanArray, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use rusqlite::params;
use std::{fs, sync::Arc};
use tempfile::tempdir;

#[test]
fn new_only_page_replays_after_file_publication_before_checkpoint() -> Result<()> {
    let temporary = tempdir()?;
    let snapshot_path = temporary.path().join("snapshot.json");
    let sqlite_path = temporary.path().join("corpus.sqlite");
    let destination = temporary.path().join("parquet");
    fs::write(
        &snapshot_path,
        "{\"id\":\"2611.00001\",\"title\":\"Original\",\"abstract\":\"Example β\",\"categories\":\"cs.AI\"}\n",
    )?;
    let feed = oai::feed(oai::ENDPOINT);
    let mut sqlite = Store::open(&sqlite_path, PROVIDER)?;
    snapshot::import(&mut sqlite, &snapshot_path, Some("fixture"), 4, None)?;
    snapshot::validate(&sqlite, &snapshot_path)?;
    sqlite.start_sync(&feed, "2026-11-15", "fixture")?;
    drop(sqlite);

    let mut corpus = ParquetStore::seed(&destination, &sqlite_path)?;
    assert_eq!(corpus.status()?["papers"], 1);
    assert_eq!(search(&corpus, &["original"])?[0]["id"], "arxiv:2611.00001");
    assert_eq!(search(&corpus, &["new"])?, serde_json::json!([]));
    let topic_args = search_args(&["--topic", "new-papers"])?;
    corpus.save_topic(
        "new-papers",
        "new",
        &TopicPlan {
            keywords: vec!["new".into()],
            preferred_categories: vec!["cs.AI".into()],
            smart: None,
            category: None,
            tag: None,
            from: Some("2026-11-15".parse()?),
            to: None,
        },
    )?;
    assert_eq!(
        corpus.search_topic(&topic_args, "new-papers")?["hits"],
        serde_json::json!([])
    );
    assert_eq!(corpus.build_lexicon()?["parts_scanned"], 1);
    assert_eq!(corpus.build_lexicon()?["parts_scanned"], 0);
    let original = corpus.get("2611.00001v2")?.unwrap();
    assert_eq!(original["title"], "Original");
    assert_eq!(original["abstract"], "Example β");
    assert_eq!(original["categories"], serde_json::json!(["cs.AI"]));
    assert_eq!(
        corpus.index.sync_state(&feed)?.unwrap().next_from,
        "2026-11-15"
    );

    let checkpoint = || PageCheckpoint {
        feed: &feed,
        from: "2026-11-15",
        first_response: "2026-11-16T07:00:00+00:00",
        next_from: "2026-11-16",
        token: None,
    };
    let commit = |corpus: &mut ParquetStore| {
        corpus
            .index
            .commit_page(checkpoint(), changes(), |tx, provider, changes| {
                append_page(&destination, tx, provider, changes)
            })
    };

    corpus.index.conn.execute_batch(
        "CREATE TRIGGER interrupt BEFORE UPDATE ON oai_sync_state
         BEGIN SELECT RAISE(ABORT, 'simulated failure after file publication'); END;",
    )?;
    let failure = commit(&mut corpus).unwrap_err();
    assert!(failure.to_string().contains("simulated failure"));
    assert!(part_path(&destination, 2).is_file());
    drop(corpus);

    let mut corpus = ParquetStore::open(&destination)?;
    assert!(!part_path(&destination, 2).exists());
    assert!(corpus.get("2611.00002")?.is_none());
    assert_eq!(corpus.status()?["parquet_files"], 1);
    assert_eq!(corpus.status()?["oai"]["next_from"], "2026-11-15");
    corpus.index.conn.execute_batch("DROP TRIGGER interrupt;")?;

    commit(&mut corpus)?;
    assert_eq!(corpus.status()?["papers"], 2);
    assert_eq!(corpus.status()?["parquet_files"], 2);
    assert_eq!(corpus.status()?["oai"]["next_from"], "2026-11-16");
    assert_eq!(corpus.get("2611.00001")?, Some(original));
    assert_eq!(corpus.build_lexicon()?["parts_scanned"], 1);
    let lexicon = rusqlite::Connection::open(destination.join("lexicon-v2.sqlite"))?;
    let df: i64 =
        lexicon.query_row("SELECT doc_freq FROM lexicon WHERE phrase='new'", [], |r| {
            r.get(0)
        })?;
    assert_eq!(df, 1);
    drop(lexicon);
    let legacy_search = destination.join("tantivy-v1");
    fs::create_dir_all(&legacy_search)?;
    corpus.refresh_search_index()?;
    assert!(!legacy_search.exists());
    let search_directory = destination.join("tantivy-v2");
    let index = tantivy::Index::open_in_dir(&search_directory)?;
    assert_eq!(index.reader()?.searcher().num_docs(), 2);
    // A sync merges every segment except the largest, so at most two remain.
    assert!(index.searchable_segment_ids()?.len() <= 2);
    let segment_files = fs::read_dir(&search_directory)?
        .filter(|entry| {
            entry
                .as_ref()
                .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "idx"))
        })
        .count();
    assert_eq!(segment_files, index.searchable_segment_ids()?.len());
    let metadata_path = search_directory.join("meta.json");
    let metadata = fs::read(&metadata_path)?;
    assert_eq!(search(&corpus, &["new"])?[0]["id"], "arxiv:2611.00002");
    let current = corpus.search_topic(&topic_args, "new-papers")?;
    assert_eq!(current["category_source"], "saved");
    assert_eq!(current["hits"][0]["id"], "arxiv:2611.00002");
    let constrained = search_args(&["--topic", "new-papers", "--category", "cs.LG"])?;
    assert_eq!(
        corpus.search_topic(&constrained, "new-papers")?["hits"],
        serde_json::json!([])
    );
    assert_eq!(corpus.list_topics()?.len(), 1);
    assert_eq!(corpus.remove_topic(" NEW-PAPERS ")?["changed"], true);
    assert!(corpus.search_topic(&topic_args, "new-papers").is_err());
    assert_eq!(
        search(&corpus, &["--category", "cs.ai"])?
            .as_array()
            .unwrap()
            .len(),
        2
    );
    corpus.refresh_search_index()?;
    assert_eq!(fs::read(metadata_path)?, metadata);
    let added = corpus.get("2611.00002")?.unwrap();
    assert_eq!(added["title"], "New");
    assert_eq!(added["categories"], serde_json::json!(["cs.AI"]));
    assert!(added.get("categories_json").is_none());
    assert_eq!(added["first_submitted_on"], "2026-11-15");
    for column in REMOVED_COLUMNS {
        assert!(added.get(column).is_none());
    }
    assert!(corpus.get("2611.00003")?.is_none());

    commit(&mut corpus)?;
    assert_eq!(corpus.status()?["papers"], 2);
    assert_eq!(corpus.status()?["parquet_files"], 2);
    drop(corpus);
    assert!(Store::open(&destination.join("index.sqlite"), PROVIDER).is_err());

    fs::remove_file(part_path(&destination, 2))?;
    assert!(ParquetStore::open(&destination).is_err());
    Ok(())
}

#[test]
fn schema_migration_keeps_payload_and_checkpoint_after_replay() -> Result<()> {
    let temporary = tempdir()?;
    let snapshot_path = temporary.path().join("snapshot.json");
    let sqlite_path = temporary.path().join("corpus.sqlite");
    let destination = temporary.path().join("parquet");
    fs::write(
        &snapshot_path,
        "{\"id\":\"2611.00001\",\"title\":\"Original\",\"abstract\":\"Complete abstract β\",\
          \"categories\":\"cs.LG cs.CR\",\"versions\":[{\"version\":\"v1\",\"created\":\"2026-11-14\"}]}\n",
    )?;
    let feed = oai::feed(oai::ENDPOINT);
    let mut sqlite = Store::open(&sqlite_path, PROVIDER)?;
    snapshot::import(&mut sqlite, &snapshot_path, Some("fixture"), 4, None)?;
    snapshot::validate(&sqlite, &snapshot_path)?;
    sqlite.start_sync(&feed, "2026-11-15", "fixture")?;
    drop(sqlite);

    let mut corpus = ParquetStore::seed(&destination, &sqlite_path)?;
    let original = corpus.get("2611.00001")?.unwrap();
    assert_eq!(
        original["categories"],
        serde_json::json!(["cs.LG", "cs.CR"])
    );
    let checkpoint = corpus.status()?["oai"].clone();
    let old = part_path(&destination, 1);
    let batch = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&old)?)?
        .build()?
        .next()
        .context("fixture batch missing")??;
    let mut columns = batch.columns().to_vec();
    let mut fields = batch.schema().fields().to_vec();
    columns.pop();
    fields.pop();
    fields.push(Arc::new(Field::new(
        "categories_json",
        DataType::Utf8,
        false,
    )));
    columns.push(Arc::new(StringArray::from(vec![r#"["cs.LG","cs.CR"]"#])));
    for name in REMOVED_COLUMNS
        .into_iter()
        .filter(|name| *name != "is_deleted")
    {
        fields.push(Arc::new(Field::new(name, DataType::Utf8, true)));
        columns.push(Arc::new(StringArray::from(vec!["2026-11-16"])));
    }
    fields.push(Arc::new(Field::new("is_deleted", DataType::Boolean, false)));
    let legacy_schema = Arc::new(Schema::new(fields));
    let write_legacy = |corpus: &ParquetStore, deleted| -> Result<()> {
        let mut columns = columns.clone();
        columns.push(Arc::new(BooleanArray::from(vec![deleted])));
        let legacy = RecordBatch::try_new(Arc::clone(&legacy_schema), columns)?;
        let mut writer = ArrowWriter::try_new(fs::File::create(&old)?, legacy.schema(), None)?;
        writer.write(&legacy)?;
        writer.close()?;
        corpus.index.conn.execute(
            "UPDATE parquet_files SET bytes=?1 WHERE file_id=1",
            params![i64::try_from(fs::metadata(&old)?.len())?],
        )?;
        Ok(())
    };
    write_legacy(&corpus, true)?;
    assert!(corpus.get("2611.00001")?.is_none());
    assert!(
        corpus
            .migrate_schema()
            .unwrap_err()
            .to_string()
            .contains("deleted rows")
    );
    assert_eq!(corpus.status()?["papers"], 1);
    assert!(old.is_file());
    write_legacy(&corpus, false)?;
    assert_eq!(corpus.build_lexicon()?["parts_scanned"], 1);
    corpus
        .index
        .conn
        .execute_batch("DROP TABLE paper_tags; PRAGMA user_version=1;")?;
    drop(corpus);
    let mut corpus = ParquetStore::open(&destination)?;
    corpus.set_tag("local", "2611.00001", true)?;
    assert_eq!(
        search(&corpus, &["complete", "--tag", "local"])?[0]["id"],
        "arxiv:2611.00001"
    );

    corpus.index.conn.execute_batch(
        "CREATE TRIGGER interrupt BEFORE UPDATE ON paper_index
         BEGIN SELECT RAISE(ABORT, 'simulated migration interruption'); END;",
    )?;
    let error = corpus.migrate_schema().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("simulated migration interruption")
    );
    drop(corpus);

    let mut corpus = ParquetStore::open(&destination)?;
    assert_eq!(corpus.get("2611.00001")?, Some(original.clone()));
    assert!(old.is_file());
    corpus.index.conn.execute_batch("DROP TRIGGER interrupt;")?;
    assert_eq!(corpus.migrate_schema()?, 1);
    assert_eq!(corpus.migrate_schema()?, 0);
    assert!(!old.exists());
    assert_eq!(corpus.build_lexicon()?["parts_scanned"], 1);
    let lexicon = rusqlite::Connection::open(destination.join("lexicon-v2.sqlite"))?;
    let frequency: i64 = lexicon.query_row(
        "SELECT doc_freq FROM lexicon WHERE phrase='complete abstract'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(frequency, 1);
    drop(lexicon);

    // A current-schema file written before the small-page layout is rewritten as well.
    let current_id: i64 =
        corpus
            .index
            .conn
            .query_row("SELECT max(file_id) FROM parquet_files", [], |row| {
                row.get(0)
            })?;
    let current = part_path(&destination, current_id);
    let reader = ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&current)?)?;
    assert!(has_current_layout(reader.metadata()));
    let batch = reader.build()?.next().context("migrated batch missing")??;
    let mut writer = ArrowWriter::try_new(fs::File::create(&current)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    corpus.index.conn.execute(
        "UPDATE parquet_files SET bytes=?1 WHERE file_id=?2",
        params![i64::try_from(fs::metadata(&current)?.len())?, current_id],
    )?;
    assert_eq!(corpus.migrate_schema()?, 1);
    assert_eq!(corpus.migrate_schema()?, 0);
    assert!(!current.exists());
    assert_eq!(corpus.get("2611.00001")?, Some(original));
    let hits = search(&corpus, &["complete", "--tag", "local"])?;
    assert_eq!(hits.as_array().unwrap().len(), 1);
    assert_eq!(hits[0]["id"], "arxiv:2611.00001");
    assert_eq!(corpus.status()?["oai"], checkpoint);
    assert_eq!(corpus.status()?["papers"], 1);
    Ok(())
}

#[test]
fn readers_share_the_corpus_while_writers_wait_for_them() -> Result<()> {
    let temporary = tempdir()?;
    let snapshot_path = temporary.path().join("snapshot.json");
    let sqlite_path = temporary.path().join("corpus.sqlite");
    let destination = temporary.path().join("parquet");
    fs::write(
        &snapshot_path,
        "{\"id\":\"2611.00001\",\"title\":\"Shared reading\",\"abstract\":\"Concurrent lookup\",\
          \"categories\":\"cs.DB\",\"versions\":[{\"version\":\"v1\",\"created\":\"2026-11-14\"}]}\n",
    )?;
    let mut sqlite = Store::open(&sqlite_path, PROVIDER)?;
    snapshot::import(&mut sqlite, &snapshot_path, Some("fixture"), 4, None)?;
    snapshot::validate(&sqlite, &snapshot_path)?;
    drop(sqlite);
    drop(ParquetStore::seed(&destination, &sqlite_path)?);

    // The first search after a seed builds the index under the write lock, then reads shared.
    let first = ParquetStore::open_for_search(&destination)?;
    assert!(first.search_index_is_current()?);
    let second = ParquetStore::open_for_search(&destination)?;
    for reader in [&first, &second] {
        assert_eq!(
            search(reader, &["concurrent"])?[0]["id"],
            "arxiv:2611.00001"
        );
    }

    // Readers never remove an unregistered part: a concurrent writer may be publishing it.
    let pending = part_path(&destination, next_file_id(&first.index.conn)?);
    fs::write(&pending, b"in progress")?;
    drop(ParquetStore::open_shared(&destination)?);
    assert!(pending.exists());

    assert!(ParquetStore::open(&destination).is_err());
    drop(first);
    drop(second);
    let writer = ParquetStore::open(&destination)?;
    assert!(!pending.exists());
    assert!(ParquetStore::open_shared(&destination).is_err());
    drop(writer);
    Ok(())
}

#[test]
fn point_reads_load_offset_indexes_for_selected_row_groups_only() -> Result<()> {
    let temporary = tempdir()?;
    let path = temporary.path().join("rows.parquet");
    let values: Vec<String> = (0..12)
        .map(|n| format!("value-{n}-{}", "x".repeat(40)))
        .collect();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(values.clone()))],
    )?;
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(4))
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .build();
    let mut writer = ArrowWriter::try_new(fs::File::create(&path)?, schema, Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let (file, metadata) = open_part(&path)?;
    assert!(metadata.metadata().page_index().is_none());
    let reader = with_offset_index(file, &metadata, &[2])?;
    let index = reader
        .metadata()
        .page_index()
        .context("page index missing")?;
    assert!(index.offset_index(2, 0).is_some());
    assert!(index.offset_index(0, 0).is_none() && index.offset_index(1, 0).is_none());
    let batch = reader
        .with_row_groups(vec![2])
        .with_row_selection(RowSelection::from_consecutive_ranges(
            std::iter::once(1..2),
            4,
        ))
        .build()?
        .next()
        .context("selected row missing")??;
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .context("string column")?;
    assert_eq!(column.value(0), values[9]);
    Ok(())
}

fn changes() -> OaiChanges {
    let paper = |id: &str, title: &str| {
        Change::Upsert(Paper {
            id: id.into(),
            url: format!("https://arxiv.org/abs/{id}"),
            title: Some(title.into()),
            abstract_text: None,
            categories: vec!["cs.AI".into()],
            first_submitted: chrono::NaiveDate::from_ymd_opt(2026, 11, 15),
            last_revised: None,
            metadata_updated: None,
        })
    };
    let deletion = |id: &str| Change::Delete {
        id: id.into(),
        url: format!("https://arxiv.org/abs/{id}"),
    };
    [
        paper("2611.00001", "Revision to a known ID"),
        paper("2611.00002", "New"),
        paper("2611.00002", "Duplicate within page"),
        deletion("2611.00001"),
        deletion("2611.00003"),
    ]
    .into_iter()
    .map(|change| ("2026-11-16".to_owned(), change))
    .collect()
}

fn search(corpus: &ParquetStore, args: &[&str]) -> Result<serde_json::Value> {
    Ok(serde_json::to_value(corpus.search(&search_args(args)?)?)?)
}

fn search_args(args: &[&str]) -> Result<SearchArgs> {
    use clap::Parser;
    let cli = crate::Cli::try_parse_from(
        ["arkhiva", "search"]
            .into_iter()
            .chain(args.iter().copied()),
    )?;
    let crate::Command::Search(args) = cli.command else {
        unreachable!();
    };
    Ok(args)
}
