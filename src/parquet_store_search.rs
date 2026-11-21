use super::topics::{TopicPlan, normalize_topic};
use super::{
    ParquetRecordBatchReaderBuilder, ParquetStore, ROW_GROUP_ROWS, RowSelection, TEXT_COLUMNS,
    legacy_deletion_flags, native_categories, open_part, part_path, record_projection,
    with_offset_index,
};
use crate::{
    arxiv::{PROVIDER, arxiv_lookup_id},
    llama_server::LlamaServer,
    progress::{Progress, bar},
    smart_search,
};
use anyhow::{Context, Result, ensure};
use arrow_array::{Array, ArrayRef, BooleanArray, ListArray, RecordBatch, StringArray};
use chrono::{Datelike, NaiveDate};
use clap::Args;
use parquet::arrow::ProjectionMask;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    num::NonZeroUsize,
    ops::Bound,
    path::Path,
    time::Instant,
};
use tantivy::{
    DocAddress, Index, IndexWriter, Order, Searcher, TantivyDocument, Term,
    collector::{
        TopDocs,
        sort_key::{SortByStaticFastValue, SortByString},
    },
    directory::MmapDirectory,
    indexer::NoMergePolicy,
    query::{
        AllQuery, BooleanQuery, BoostQuery, Occur, Query, RangeQuery, TermQuery, TermSetQuery,
    },
    schema::{
        FAST, Field, INDEXED, IndexRecordOption, STRING, Schema, TextFieldIndexing, TextOptions,
    },
    tokenizer::TokenStream,
};

// v2 drops term positions: search only runs term queries, and snippets come from Parquet.
const SEARCH_DIRECTORY: &str = "tantivy-v2";
const LEGACY_SEARCH_DIRECTORY: &str = "tantivy-v1";
const TITLE_WEIGHT: f32 = 3.0;

#[derive(Args, Clone)]
pub struct SearchArgs {
    /// An arXiv ID, link, or case-insensitive terms from the title and abstract.
    #[arg(value_name = "QUERY")]
    query: Option<String>,
    /// Let a model plan the search: facets, anchors and constraints, grounded in the corpus.
    /// Set ARKHIVA_LLM_URL and ARKHIVA_LLM_MODEL to use an OpenAI-compatible API instead of
    /// the local llama-server.
    #[arg(long, requires = "query")]
    smart: bool,
    /// Smart search: list every candidate, not just the top --limit.
    #[arg(long, requires = "smart")]
    all: bool,
    /// Save this smart search as a reusable custom topic.
    #[arg(long, requires = "smart")]
    save_topic: Option<String>,
    /// Search the current corpus using a saved topic.
    #[arg(long, conflicts_with_all = ["query", "smart"])]
    topic: Option<String>,
    /// Match a complete category code. In smart mode this overrides the model's categories.
    #[arg(long)]
    category: Option<String>,
    /// Match a local paper tag.
    #[arg(long)]
    tag: Option<String>,
    /// Earliest first-submission date (YYYY-MM-DD, inclusive).
    #[arg(long)]
    from: Option<NaiveDate>,
    /// Latest first-submission date (YYYY-MM-DD, inclusive).
    #[arg(long)]
    to: Option<NaiveDate>,
    /// Maximum results, ranked by relevance for text or newest first for browsing.
    #[arg(long, default_value = "20")]
    limit: NonZeroUsize,
    /// Text searches attach snippets to this many top hits; the rest skip reading abstracts.
    #[arg(long, default_value_t = 5)]
    snippets: usize,
}

impl SearchArgs {
    pub fn report(&self, directory: &Path) -> Result<serde_json::Value> {
        if let Some(name) = &self.topic {
            let name = normalize_topic(name)?;
            return ParquetStore::open_for_search(directory)?.search_topic(self, &name);
        }
        if !self.smart {
            // An ID lookup reads Parquet directly; only text search needs a current Tantivy index.
            let id_lookup = self
                .query
                .as_deref()
                .is_some_and(|query| arxiv_lookup_id(query).is_ok());
            let store = if id_lookup {
                ParquetStore::open_shared(directory)?
            } else {
                ParquetStore::open_for_search(directory)?
            };
            return Ok(serde_json::to_value(store.search(self)?)?);
        }
        let prompt = self
            .query
            .as_deref()
            .context("--smart requires a research prompt")?;
        let prompt = smart_search::validate_prompt(prompt)?;
        let save_name = self
            .save_topic
            .as_deref()
            .map(normalize_topic)
            .transpose()?;
        // Saving a topic writes to the index database; a plain smart search only reads.
        let store = if save_name.is_some() {
            ParquetStore::open(directory)?
        } else {
            ParquetStore::open_for_search(directory)?
        };
        // Imported kamus and codec entries the prompt names travel with it to the model;
        // kamus terms are also searched whatever the plan says.
        let glossary = store.glossary_for(prompt)?;
        let request = if glossary.lines.is_empty() {
            prompt.to_owned()
        } else {
            format!(
                "{prompt}\n\nUser glossary (treat as correct; prefer these terms):\n{}",
                glossary.lines.join("\n")
            )
        };
        // Fail on a missing lexicon here, before the prompt is sent to the model.
        drop(store.lexicon_df()?);
        let started = Instant::now();
        let server = LlamaServer::connect(directory)?;
        let mut plan = smart_search::plan(&server, &request)?;
        let llm_ms = started.elapsed().as_millis();
        super::glossary::add_kamus(&mut plan, glossary.kamus);
        let started = Instant::now();
        let results = store.search_planned(self, plan.clone())?;
        let mut report = serde_json::to_value(&results)?;
        report["timings_ms"] = json!({"llm": llm_ms, "search": started.elapsed().as_millis()});
        if !glossary.lines.is_empty() {
            report["glossary"] = json!(glossary.lines);
        }
        if let Some(name) = save_name {
            store.save_topic(
                &name,
                prompt,
                &TopicPlan {
                    keywords: Vec::new(),
                    preferred_categories: Vec::new(),
                    smart: Some(plan),
                    category: self.category.clone(),
                    tag: self.tag.clone(),
                    from: self.from,
                    to: self.to,
                },
            )?;
            report["topic"] = json!(name);
        }
        Ok(report)
    }

    fn validated_tag(&self) -> Result<Option<String>> {
        ensure!(
            self.from.zip(self.to).is_none_or(|(from, to)| from <= to),
            "--from must be on or before --to"
        );
        ensure!(
            self.category
                .as_deref()
                .is_none_or(|code| !code.trim().is_empty()),
            "--category must not be empty"
        );
        self.tag.as_deref().map(tag_name).transpose()
    }

    fn accepts_exact_hit(&self, hit: &SearchHit) -> bool {
        self.from
            .is_none_or(|from| hit.first_submitted_on.is_some_and(|date| date >= from))
            && self
                .to
                .is_none_or(|to| hit.first_submitted_on.is_some_and(|date| date <= to))
            && self.category.as_deref().is_none_or(|category| {
                hit.categories
                    .iter()
                    .any(|code| code.eq_ignore_ascii_case(category.trim()))
            })
    }
}

// `id` is `provider:external_id`; readers derive the URL from it, which keeps output small.
#[derive(Clone, Serialize)]
pub struct SearchHit {
    id: String,
    title: Option<String>,
    first_submitted_on: Option<NaiveDate>,
    categories: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snippet: Option<String>,
    /// Smart search judges constraints on the whole abstract; never printed.
    #[serde(skip)]
    abstract_text: Option<String>,
}

#[derive(Deserialize)]
struct StoredPaper {
    provider: String,
    external_id: String,
    title: Option<String>,
    first_submitted_on: Option<NaiveDate>,
    categories: Vec<String>,
}

impl From<StoredPaper> for SearchHit {
    fn from(paper: StoredPaper) -> Self {
        Self {
            id: format!("{}:{}", paper.provider, paper.external_id),
            title: paper.title,
            first_submitted_on: paper.first_submitted_on,
            categories: paper.categories,
            snippet: None,
            abstract_text: None,
        }
    }
}

#[derive(Serialize)]
struct SmartSearchReport<'a> {
    keywords: &'a [String],
    categories: Vec<String>,
    category_mode: &'static str,
    category_source: &'static str,
    hits: Vec<SearchHit>,
}

#[derive(Default)]
struct RankedHits {
    rows: Vec<(i64, usize)>,
    terms: Vec<String>,
}

enum SearchProjection {
    Metadata,
    WithAbstract,
}

#[derive(Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
struct Part {
    rows: i64,
    bytes: i64,
}

#[derive(Clone, Copy)]
struct Fields {
    title: Field,
    abstract_text: Field,
    category: Field,
    day: Field,
    paper_key: Field,
    file_id: Field,
    row_no: Field,
}

impl Fields {
    fn schema() -> (Schema, Self) {
        let mut schema = Schema::builder();
        let text = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqs),
        );
        let fields = Self {
            title: schema.add_text_field("title", text.clone()),
            abstract_text: schema.add_text_field("abstract", text),
            category: schema.add_text_field("category", STRING),
            day: schema.add_i64_field("day", INDEXED | FAST),
            paper_key: schema.add_text_field("paper_key", STRING | FAST),
            file_id: schema.add_i64_field("file_id", INDEXED | FAST),
            row_no: schema.add_u64_field("row_no", FAST),
        };
        (schema.build(), fields)
    }
}

impl ParquetStore {
    pub fn set_tag(&self, name: &str, paper: &str, add: bool) -> Result<serde_json::Value> {
        let tag = tag_name(name)?;
        let id = arxiv_lookup_id(paper)?;
        ensure!(
            self.paper_location(&id)?.is_some(),
            "paper {id} is not in this corpus"
        );
        let statement = if add {
            "INSERT INTO paper_tags(tag, provider, external_id) VALUES (?1, ?2, ?3) ON CONFLICT DO NOTHING"
        } else {
            "DELETE FROM paper_tags WHERE tag=?1 AND provider=?2 AND external_id=?3"
        };
        let changed = self
            .index
            .conn
            .execute(statement, params![tag, PROVIDER, id])?
            != 0;
        Ok(json!({"tag": tag, "provider": PROVIDER, "external_id": id, "changed": changed}))
    }

    pub fn search(&self, args: &SearchArgs) -> Result<Vec<SearchHit>> {
        self.search_with(args, args.query.as_deref(), &[])
    }

    pub(super) fn search_topic(&self, args: &SearchArgs, name: &str) -> Result<serde_json::Value> {
        let topic = self.topic(name)?;
        let plan = topic.plan;
        let mut args = args.clone();
        args.smart = true;
        args.category = args.category.or(plan.category);
        args.tag = args.tag.or(plan.tag);
        args.from = args.from.or(plan.from);
        args.to = args.to.or(plan.to);
        if let Some(smart) = plan.smart {
            // Topics saved by the planned search replay their plan without calling the model.
            let mut report = serde_json::to_value(self.search_planned(&args, smart)?)?;
            report["topic"] = json!(topic.name);
            return Ok(report);
        }
        let (categories, inferred, mode) = if let Some(code) = &args.category {
            (vec![code.trim().to_owned()], &[][..], "filter")
        } else if plan.preferred_categories.is_empty() {
            (Vec::new(), &[][..], "none")
        } else {
            (
                plan.preferred_categories.clone(),
                plan.preferred_categories.as_slice(),
                "preferred",
            )
        };
        let hits = self.search_with(&args, Some(&plan.keywords.join(" ")), inferred)?;
        let mut report = serde_json::to_value(SmartSearchReport {
            keywords: &plan.keywords,
            categories,
            category_mode: mode,
            category_source: "saved",
            hits,
        })?;
        report["topic"] = json!(topic.name);
        Ok(report)
    }

    fn search_with(
        &self,
        args: &SearchArgs,
        query: Option<&str>,
        inferred: &[String],
    ) -> Result<Vec<SearchHit>> {
        let tag = args.validated_tag()?;
        if !args.smart
            && let Some(id) = query.and_then(|query| arxiv_lookup_id(query).ok())
        {
            if let Some(tag) = tag.as_deref() {
                let tagged: bool = self.index.conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM paper_tags WHERE tag=?1 AND provider=?2 AND external_id=?3)",
                    params![tag, PROVIDER, id],
                    |row| row.get(0),
                )?;
                if !tagged {
                    return Ok(Vec::new());
                }
            }
            let hit = self
                .get(&id)?
                .map(serde_json::from_value::<StoredPaper>)
                .transpose()?;
            return Ok(hit
                .into_iter()
                .map(SearchHit::from)
                .filter(|hit| args.accepts_exact_hit(hit))
                .collect());
        }

        let ranked = self.rank_hits(args, query, inferred, tag.as_deref())?;
        self.hydrate_hits(&ranked, args.snippets)
    }

    fn rank_hits(
        &self,
        args: &SearchArgs,
        query: Option<&str>,
        inferred: &[String],
        tag: Option<&str>,
    ) -> Result<RankedHits> {
        let category = args.category.as_deref().map(str::trim);
        let (index, fields) = self.open_search_index()?;
        let mut tokenizer = index.tokenizer_for_field(fields.title)?;
        let query = query.unwrap_or_default();
        let mut stream = tokenizer.token_stream(query);
        let mut terms = Vec::new();
        while stream.advance() {
            let term = &stream.token().text;
            if !terms.contains(term) {
                terms.push(term.clone());
            }
        }
        if terms.is_empty() && !query.trim().is_empty() {
            return Ok(RankedHits::default());
        }

        let mut clauses: Vec<Box<dyn Query>> = terms
            .iter()
            .map(|text| {
                Box::new(BooleanQuery::union(vec![
                    Box::new(BoostQuery::new(
                        term_query(fields.title, text, IndexRecordOption::WithFreqs),
                        TITLE_WEIGHT,
                    )),
                    term_query(fields.abstract_text, text, IndexRecordOption::WithFreqs),
                ])) as Box<dyn Query>
            })
            .collect();
        if args.smart && clauses.len() > 1 {
            let minimum_matches = if clauses.len() > 2 { 2 } else { 1 };
            let terms = clauses
                .into_iter()
                .enumerate()
                .map(|(i, term)| {
                    let query: Box<dyn Query> = if i == 0 {
                        Box::new(BoostQuery::new(term, 2.0))
                    } else {
                        term
                    };
                    (Occur::Should, query)
                })
                .collect();
            clauses = vec![Box::new(BooleanQuery::with_minimum_required_clauses(
                terms,
                minimum_matches,
            ))];
        }
        if let Some(code) = category {
            clauses.push(term_query(
                fields.category,
                &code.to_ascii_lowercase(),
                IndexRecordOption::Basic,
            ));
        }
        if args.from.is_some() || args.to.is_some() {
            let bound = |date: Option<NaiveDate>| {
                date.map_or(Bound::Unbounded, |date| {
                    Bound::Included(Term::from_field_i64(fields.day, day(date)))
                })
            };
            clauses.push(Box::new(RangeQuery::new(bound(args.from), bound(args.to))));
        }
        if let Some(tag) = tag {
            let mut statement = self
                .index
                .conn
                .prepare("SELECT provider, external_id FROM paper_tags WHERE tag=?1")?;
            let tagged = statement.query_map([tag], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut keys = Vec::new();
            for row in tagged {
                let (provider, id) = row?;
                keys.push(Term::from_field_text(
                    fields.paper_key,
                    &paper_key(&provider, &id),
                ));
            }
            if keys.is_empty() {
                return Ok(RankedHits::default());
            }
            clauses.push(Box::new(TermSetQuery::new(keys)));
        }

        let query: Box<dyn Query> = if clauses.is_empty() {
            Box::new(AllQuery)
        } else if inferred.is_empty() {
            Box::new(BooleanQuery::intersection(clauses))
        } else {
            let categories = inferred
                .iter()
                .map(|code| Term::from_field_text(fields.category, &code.to_ascii_lowercase()));
            Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(BooleanQuery::intersection(clauses))),
                (
                    Occur::Should,
                    Box::new(BoostQuery::new(
                        Box::new(TermSetQuery::new(categories)),
                        2.0,
                    )),
                ),
            ]))
        };
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let limit = args.limit.get().min(usize::try_from(searcher.num_docs())?);
        if limit == 0 {
            return Ok(RankedHits::default());
        }
        let top = TopDocs::with_limit(limit);
        let documents: Vec<DocAddress> = if terms.is_empty() {
            searcher
                .search(
                    query.as_ref(),
                    &top.order_by((
                        (SortByStaticFastValue::<i64>::for_field("day"), Order::Desc),
                        (SortByString::for_field("paper_key"), Order::Asc),
                    )),
                )?
                .into_iter()
                .map(|(_, doc)| doc)
                .collect()
        } else {
            searcher
                .search(query.as_ref(), &top.order_by_score())?
                .into_iter()
                .map(|(_, doc)| doc)
                .collect()
        };
        Ok(RankedHits {
            rows: ranked_rows(&searcher, &documents)?,
            terms,
        })
    }

    /// Opens for searching alongside other readers. A seed or interrupted sync can leave the
    /// index behind its Parquet parts; that refresh runs once under the write lock.
    pub fn open_for_search(directory: &Path) -> Result<Self> {
        let store = Self::open_shared(directory)?;
        if store.search_index_is_current()? {
            return Ok(store);
        }
        drop(store);
        Self::open(directory)?.refresh_search_index()?;
        Self::open_shared(directory)
    }

    pub(super) fn refresh_search_index(&self) -> Result<()> {
        self.open_search_index()?;
        Ok(())
    }

    /// True when the Tantivy index covers exactly the registered Parquet parts, so a shared
    /// reader can search without writing to it.
    pub(super) fn search_index_is_current(&self) -> Result<bool> {
        let directory = self.directory.join(SEARCH_DIRECTORY);
        if !directory.join("meta.json").is_file() {
            return Ok(false);
        }
        let index = Index::open_in_dir(&directory)?;
        let (parts, indexed) = self.search_parts(&index)?;
        Ok(parts == indexed)
    }

    fn search_parts(&self, index: &Index) -> Result<(BTreeMap<i64, Part>, BTreeMap<i64, Part>)> {
        let mut statement = self
            .index
            .conn
            .prepare("SELECT file_id, rows, bytes FROM parquet_files")?;
        let parts = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    Part {
                        rows: row.get(1)?,
                        bytes: row.get(2)?,
                    },
                ))
            })?
            .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
        let metadata = index.load_metas()?;
        let indexed: BTreeMap<i64, Part> = match metadata.payload {
            Some(payload) => {
                serde_json::from_str(&payload).context("invalid search index manifest")?
            }
            None => {
                ensure!(
                    metadata.segments.is_empty(),
                    "search index lacks its corpus manifest"
                );
                BTreeMap::new()
            }
        };
        Ok((parts, indexed))
    }

    fn open_search_index(&self) -> Result<(Index, Fields)> {
        let (schema, fields) = Fields::schema();
        let directory = self.directory.join(SEARCH_DIRECTORY);
        if !self.writable {
            ensure!(
                self.search_index_is_current()?,
                "the search index is out of date; reopen the corpus for writing to refresh it"
            );
            return Ok((Index::open_in_dir(&directory)?, fields));
        }
        fs::create_dir_all(&directory)?;
        let index = Index::open_or_create(MmapDirectory::open(&directory)?, schema)?;
        let (parts, indexed) = self.search_parts(&index)?;
        if parts != indexed {
            let pending: Vec<_> = parts
                .iter()
                .filter(|(id, part)| indexed.get(id) != Some(*part))
                .collect();
            let total = pending.len() as u64;
            let rows: i64 = pending.iter().map(|(_, part)| part.rows).sum();
            let mut progress = Progress::new("Tantivy");
            progress.update(format_args!(
                "updating index | {rows} rows in {total} Parquet files"
            ));
            let mut writer = index.writer_with_num_threads(4, 192 * 1024 * 1024)?;
            for (&file_id, part) in &indexed {
                if parts.get(&file_id) != Some(part) {
                    writer.delete_term(Term::from_field_i64(fields.file_id, file_id));
                }
            }
            for (number, (&file_id, &part)) in pending.into_iter().enumerate() {
                self.index_search_part(&writer, fields, file_id, part)?;
                let done = number as u64 + 1;
                if done < total {
                    progress.update(format_args!(
                        "{} | {done}/{total} files read",
                        bar(done, total)
                    ));
                }
            }
            progress.update("committing index");
            // The file inventory and the indexed documents become visible in the same commit.
            let mut commit = writer.prepare_commit()?;
            commit.set_payload(&serde_json::to_string(&parts)?);
            commit.commit()?;
            writer.wait_merging_threads()?;
            // Every segment costs a term lookup per query. A full build merges everything;
            // a sync merges all but the largest segment so it does not rewrite the whole index.
            let mut metas = index.searchable_segment_metas()?;
            metas.sort_by_key(|meta| std::cmp::Reverse(meta.num_docs()));
            let skip = usize::from(!indexed.is_empty());
            let targets: Vec<_> = metas.iter().skip(skip).map(|meta| meta.id()).collect();
            if targets.len() > 1 {
                progress.update("merging segments");
                let mut writer: IndexWriter = index.writer_with_num_threads(1, 64 * 1024 * 1024)?;
                writer.set_merge_policy(Box::new(NoMergePolicy));
                writer.merge(&targets).wait()?;
                writer.wait_merging_threads()?;
            }
            progress.finish(format_args!("ready | {rows} rows indexed"));
        }
        // Merged-away segment files survive a merge on Windows while this process still maps
        // them, so a later run removes them before any segment is mapped.
        let live = index.searchable_segment_ids()?.len();
        let on_disk = fs::read_dir(&directory)?
            .filter(|entry| {
                entry
                    .as_ref()
                    .is_ok_and(|entry| entry.path().extension().is_some_and(|ext| ext == "idx"))
            })
            .count();
        if on_disk > live {
            let writer: IndexWriter = index.writer_with_num_threads(1, 50 * 1024 * 1024)?;
            writer.garbage_collect_files().wait()?;
        }
        let legacy = self.directory.join(LEGACY_SEARCH_DIRECTORY);
        if legacy.is_dir() {
            fs::remove_dir_all(legacy)?;
        }
        Ok((index, fields))
    }

    fn index_search_part(
        &self,
        writer: &IndexWriter,
        fields: Fields,
        file_id: i64,
        part: Part,
    ) -> Result<()> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(part_path(
            &self.directory,
            file_id,
        ))?)?;
        let layout = record_projection(reader.schema())?;
        let category = reader
            .schema()
            .field(layout[TEXT_COLUMNS.len()])
            .name()
            .clone();
        let reader = reader.with_batch_size(ROW_GROUP_ROWS).build()?;
        let mut offset = 0;
        for batch in reader {
            let batch = batch?;
            let data = PaperColumns::new(&batch, &category, true)?;
            for row in 0..batch.num_rows() {
                if !data.deleted.is_some_and(|flags| flags.value(row)) {
                    let mut doc = TantivyDocument::new();
                    if let Some(title) = text_at(data.title, row) {
                        doc.add_text(fields.title, title);
                    }
                    if let Some(body) = data.body.and_then(|column| text_at(column, row)) {
                        doc.add_text(fields.abstract_text, body);
                    }
                    for category in data.categories(row)? {
                        doc.add_text(fields.category, category.to_ascii_lowercase());
                    }
                    if let Some(date) = data.date(row)? {
                        doc.add_i64(fields.day, day(date));
                    }
                    let (provider, id) = data.key(row)?;
                    doc.add_text(fields.paper_key, paper_key(provider, id));
                    doc.add_i64(fields.file_id, file_id);
                    doc.add_u64(fields.row_no, u64::try_from(offset + row)?);
                    writer.add_document(doc)?;
                }
            }
            offset += batch.num_rows();
        }
        ensure!(
            i64::try_from(offset)? == part.rows,
            "search index row count differs for Parquet part {file_id}"
        );
        Ok(())
    }

    /// Abstracts dominate read cost, so only the top `snippets` hits read them.
    fn hydrate_hits(&self, ranked: &RankedHits, snippets: usize) -> Result<Vec<SearchHit>> {
        let terms = &ranked.terms;
        let split = if terms.is_empty() {
            0
        } else {
            snippets.min(ranked.rows.len())
        };
        let (head, tail) = ranked.rows.split_at(split);
        let decode = |batch: &RecordBatch, category: &str, terms: &[String]| {
            let data = PaperColumns::new(batch, category, !terms.is_empty())?;
            (0..batch.num_rows())
                .map(|row| data.hit(row, terms))
                .collect()
        };
        let mut hits = self.read_search_rows(head, SearchProjection::WithAbstract, |b, c| {
            decode(b, c, terms)
        })?;
        hits.extend(
            self.read_search_rows(tail, SearchProjection::Metadata, |b, c| decode(b, c, &[]))?,
        );
        Ok(hits)
    }

    fn read_search_rows<T>(
        &self,
        rows: &[(i64, usize)],
        projection: SearchProjection,
        decode: impl Fn(&RecordBatch, &str) -> Result<Vec<T>>,
    ) -> Result<Vec<T>> {
        let mut parts: BTreeMap<i64, Vec<(usize, usize)>> = BTreeMap::new();
        let mut output: Vec<Option<T>> = Vec::with_capacity(rows.len());
        for (rank, &(file, row)) in rows.iter().enumerate() {
            parts.entry(file).or_default().push((row, rank));
            output.push(None);
        }

        for (file_id, mut hits) in parts {
            hits.sort_unstable();
            let (file, reader) = open_part(&part_path(&self.directory, file_id))?;
            let mut layout = record_projection(reader.schema())?;
            let category_index = layout[TEXT_COLUMNS.len()];
            let category = reader.schema().field(category_index).name().clone();
            match projection {
                SearchProjection::Metadata => {
                    layout.retain(|&index| reader.schema().field(index).name() != "abstract");
                }
                SearchProjection::WithAbstract => {}
            }
            let mask = ProjectionMask::roots(reader.parquet_schema(), layout);
            let mut groups = Vec::new();
            let mut positions = Vec::with_capacity(hits.len());
            let mut file_offset = 0;
            let mut selection_offset = 0;
            for (group, metadata) in reader.metadata().row_groups().iter().enumerate() {
                let len = usize::try_from(metadata.num_rows())?;
                let group_end = file_offset + len;
                let first_hit = positions.len();
                for &(row, _) in &hits[first_hit..] {
                    if row >= group_end {
                        break;
                    }
                    let pos = selection_offset + (row - file_offset);
                    positions.push(pos..pos + 1);
                }
                if positions.len() != first_hit {
                    groups.push(group);
                    selection_offset += len;
                }
                file_offset = group_end;
            }
            ensure!(
                positions.len() == hits.len(),
                "search row is outside its Parquet file"
            );
            let selection =
                RowSelection::from_consecutive_ranges(positions.into_iter(), selection_offset);
            let batch = with_offset_index(file, &reader, &groups)?
                .with_projection(mask)
                .with_row_groups(groups)
                .with_row_selection(selection)
                .with_batch_size(hits.len())
                .build()?
                .next()
                .context("missing Parquet search hits")??;
            ensure!(
                batch.num_rows() == hits.len(),
                "Parquet returned the wrong number of search hits"
            );
            let records = decode(&batch, &category)?;
            ensure!(
                records.len() == hits.len(),
                "wrong number of decoded search hits"
            );
            for (record, (_, rank)) in records.into_iter().zip(hits) {
                output[rank] = Some(record);
            }
        }
        output
            .into_iter()
            .map(|hit| hit.context("missing Parquet search hit"))
            .collect()
    }
}

fn ranked_rows(searcher: &Searcher, documents: &[DocAddress]) -> Result<Vec<(i64, usize)>> {
    documents
        .iter()
        .map(|&doc| {
            let fields = searcher.segment_reader(doc.segment_ord).fast_fields();
            let file = fields
                .i64("file_id")?
                .first(doc.doc_id)
                .context("missing search file locator")?;
            let row = fields
                .u64("row_no")?
                .first(doc.doc_id)
                .context("missing search row locator")?;
            Ok((file, usize::try_from(row)?))
        })
        .collect()
}

#[path = "parquet_store_smart.rs"]
mod smart;

fn term_query(field: Field, text: &str, option: IndexRecordOption) -> Box<dyn Query> {
    Box::new(TermQuery::new(Term::from_field_text(field, text), option))
}

fn tag_name(name: &str) -> Result<String> {
    let name = name.trim();
    ensure!(
        !name.is_empty() && !name.chars().any(char::is_control),
        "tag must have a nonempty name without control characters"
    );
    Ok(name.to_lowercase())
}

fn paper_key(provider: &str, id: &str) -> String {
    format!("{provider}\0{id}")
}

fn day(date: NaiveDate) -> i64 {
    i64::from(date.num_days_from_ce())
}

struct PaperColumns<'a> {
    provider: &'a StringArray,
    id: &'a StringArray,
    title: &'a StringArray,
    body: Option<&'a StringArray>,
    submitted: &'a StringArray,
    categories: ArrayRef,
    deleted: Option<&'a BooleanArray>,
}

impl<'a> PaperColumns<'a> {
    fn new(batch: &'a RecordBatch, category: &str, include_abstract: bool) -> Result<Self> {
        let text = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|column| column.as_any().downcast_ref::<StringArray>())
                .with_context(|| format!("invalid Parquet text column {name}"))
        };
        let deleted = legacy_deletion_flags(batch)?;
        ensure!(
            deleted.is_none_or(|flags| flags.null_count() == 0),
            "invalid legacy Parquet deletion flags"
        );
        Ok(Self {
            provider: text("provider")?,
            id: text("external_id")?,
            title: text("title")?,
            body: include_abstract.then(|| text("abstract")).transpose()?,
            submitted: text("first_submitted_on")?,
            categories: category_column(batch, category)?,
            deleted,
        })
    }

    fn key(&self, row: usize) -> Result<(&'a str, &'a str)> {
        Ok((
            text_at(self.provider, row).context("missing paper provider")?,
            text_at(self.id, row).context("missing paper ID")?,
        ))
    }

    fn date(&self, row: usize) -> Result<Option<NaiveDate>> {
        text_at(self.submitted, row)
            .map(str::parse)
            .transpose()
            .context("invalid first submission date")
    }

    fn categories(&self, row: usize) -> Result<impl Iterator<Item = &str>> {
        paper_categories(&self.categories, row)
    }

    fn hit(&self, row: usize, terms: &[String]) -> Result<SearchHit> {
        let (provider, id) = self.key(row)?;
        let title = text_at(self.title, row);
        let body = self
            .body
            .and_then(|column| text_at(column, row))
            .filter(|body| !body.is_empty());
        let snippet = if terms.is_empty() {
            None
        } else {
            body.or(title).map(|text| snippet(text, terms))
        };
        Ok(SearchHit {
            id: format!("{provider}:{id}"),
            title: title.map(str::to_owned),
            first_submitted_on: self.date(row)?,
            categories: self.categories(row)?.map(str::to_owned).collect(),
            snippet,
            abstract_text: body.map(str::to_owned),
        })
    }
}

fn category_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    native_categories(
        batch
            .column_by_name(name)
            .context("missing Parquet categories")?,
    )
}

fn paper_categories(column: &ArrayRef, row: usize) -> Result<impl Iterator<Item = &str>> {
    let lists = column
        .as_any()
        .downcast_ref::<ListArray>()
        .context("invalid Parquet categories")?;
    let values = lists
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .context("invalid Parquet category values")?;
    let offsets = lists.value_offsets();
    Ok((offsets[row] as usize..offsets[row + 1] as usize).map(|n| values.value(n)))
}

fn text_at(column: &StringArray, row: usize) -> Option<&str> {
    column.is_valid(row).then(|| column.value(row))
}

fn snippet(text: &str, terms: &[String]) -> String {
    let folded = text.to_ascii_lowercase();
    let anchor = terms
        .iter()
        .filter_map(|term| folded.find(term))
        .min()
        .unwrap_or(0);
    let start = text.floor_char_boundary(anchor.saturating_sub(65));
    let end = text.floor_char_boundary((start + 260).min(text.len()));
    format!(
        "{}{}{}",
        if start > 0 { "…" } else { "" },
        &text[start..end],
        if end < text.len() { "…" } else { "" }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{arxiv::PROVIDER, snapshot, store::Store};
    use serde_json::json;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn text_category_dates_and_tags_compose_before_limit() -> Result<()> {
        let directory = tempdir()?;
        let source = directory.path().join("snapshot.json");
        let rows = [
            (
                "2501.00001",
                "Quantum\ncoherence in a cavity",
                "quant-ph",
                Some("2025-01-01"),
            ),
            (
                "2502.00001",
                "COHERENCE in QUANTUM systems",
                "cs.LG quant-ph",
                Some("2025-02-01"),
            ),
            (
                "2502.00002",
                "Quantum coherence reanalysis",
                "quant-ph",
                Some("2025-02-01"),
            ),
            (
                "2502.00003",
                "Quantum coherence review",
                "quant-physics",
                Some("2025-02-01"),
            ),
            (
                "2502.00004",
                "Quantum dynamics",
                "quant-ph",
                Some("2025-02-01"),
            ),
            (
                "2502.00005",
                "Quantum coherence after the cutoff",
                "quant-ph",
                Some("2025-02-02"),
            ),
            (
                "2502.00006",
                "Quantum coherence with unknown date",
                "quant-ph",
                None,
            ),
        ];
        let ndjson = rows
            .map(|(id, title, categories, date)| {
                json!({
                    "id": id,
                    "title": title,
                    "abstract": (id == "2502.00004").then_some("We describe beam splitter calibration."),
                    "categories": categories,
                    "versions": date.map(|day| [json!({"version":"v1", "created":day})])
                })
                .to_string()
            })
            .join("\n");
        fs::write(&source, ndjson)?;
        let db = directory.path().join("archive.sqlite");
        let mut store = Store::open(&db, PROVIDER)?;
        snapshot::import(&mut store, &source, Some("fixture"), 4, None)?;
        snapshot::validate(&store, &source)?;
        drop(store);
        let parquet_directory = directory.path().join("parquet");
        let parquet = ParquetStore::seed(&parquet_directory, &db)?;
        drop(parquet);

        let mut query = SearchArgs {
            query: Some("coherence QUANTUM".to_owned()),
            smart: false,
            all: false,
            save_topic: None,
            topic: None,
            category: Some("QUANT-PH".to_owned()),
            tag: None,
            from: Some("2025-01-01".parse()?),
            to: Some("2025-02-01".parse()?),
            limit: NonZeroUsize::new(3).unwrap(),
            snippets: 5,
        };
        // An ID lookup reads Parquet without building the full-text index.
        let mut lookup = query.clone();
        (lookup.query, lookup.category, lookup.from, lookup.to) =
            (Some("arxiv:2502.00004".into()), None, None, None);
        assert_eq!(
            lookup.report(&parquet_directory)?[0]["id"],
            "arxiv:2502.00004"
        );
        assert!(!parquet_directory.join(SEARCH_DIRECTORY).exists());
        // Smart search stops on a missing lexicon before contacting any model.
        (lookup.query, lookup.smart) = (Some("beam splitter calibration".into()), true);
        let error = lookup.report(&parquet_directory).unwrap_err();
        assert!(
            format!("{error:#}").contains("arkhiva parquet lexicon"),
            "{error:#}"
        );

        let parquet = ParquetStore::open(&parquet_directory)?;
        let hits = parquet.search(&query)?;
        let mut ids: Vec<_> = hits.iter().map(|hit| hit.id.as_str()).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            ["arxiv:2501.00001", "arxiv:2502.00001", "arxiv:2502.00002"]
        );
        assert!(hits.iter().all(|hit| hit.snippet.is_some()));

        query.snippets = 1;
        let lazy = parquet.search(&query)?;
        assert_eq!(
            lazy.iter().map(|hit| &hit.id).collect::<Vec<_>>(),
            hits.iter().map(|hit| &hit.id).collect::<Vec<_>>()
        );
        assert!(lazy[0].snippet.is_some() && lazy[1..].iter().all(|hit| hit.snippet.is_none()));
        query.snippets = 5;

        query.query = Some("splitter calibration".into());
        let hits = parquet.search(&query)?;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, "arxiv:2502.00004");
        assert!(hits[0].snippet.as_deref().unwrap().contains("splitter"));

        query.smart = true;
        query.category = None;
        let smart_hits =
            parquet.search_with(&query, Some("splitter mismatch"), &["cs.CV".into()])?;
        assert_eq!(smart_hits.len(), 1);
        assert_eq!(smart_hits[0].id, "arxiv:2502.00004");

        // Planned search: constraint evidence binds to the topic sentence; avoid only flags.
        parquet.build_lexicon()?;
        let mut df = parquet.lexicon_df()?;
        // Five words: bounded by the rarest three-word piece.
        assert_eq!(df("quantum coherence after the cutoff")?, 1);
        assert_eq!(df("quantum coherence after the xylophone")?, 0);
        drop(df);
        let group = |name: &str, terms: &[&str]| smart_search::Group {
            name: name.into(),
            terms: terms.iter().map(|term| (*term).into()).collect(),
        };
        let plan = smart_search::Plan {
            intent: "topic".into(),
            anchors: vec!["quantum".into()],
            facets: vec![group("optics", &["beam splitter", "coherence"])],
            constraints: vec![group("calibration", &[])],
            avoid: vec!["dynamics".into()],
            ..smart_search::Plan::default()
        };
        let (from, to) = (query.from.take(), query.to.take());
        let report = serde_json::to_value(parquet.search_planned(&query, plan.clone())?)?;
        assert_eq!(report["total"], 7);
        assert_eq!(report["hits"].as_array().unwrap().len(), 3);
        query.all = true;
        let report = serde_json::to_value(parquet.search_planned(&query, plan.clone())?)?;
        let hits = report["hits"].as_array().unwrap();
        assert_eq!(hits.len(), 7);
        let splitter = hits
            .iter()
            .find(|hit| hit["id"] == "arxiv:2502.00004")
            .unwrap();
        assert_eq!(splitter["tier"], "contextual");
        assert_eq!(splitter["constraints"]["calibration"], "calibration");
        assert_eq!(splitter["avoid_hit"], "dynamics");
        assert!(hits.iter().all(|hit| hit.get("abstract_text").is_none()));
        assert!(splitter.get("snippet").is_some());
        query.snippets = 0;
        let quiet = serde_json::to_value(parquet.search_planned(&query, plan)?)?;
        let quiet = quiet["hits"].as_array().unwrap();
        assert_eq!(quiet.len(), 7);
        assert!(quiet.iter().all(|hit| hit.get("snippet").is_none()));
        query.snippets = 5;
        (query.from, query.to, query.all) = (from, to, false);

        let kamus = directory.path().join("kamus.json");
        fs::write(
            &kamus,
            r#"{"cahaya": ["beam splitter"], "kuantum": ["quantum"]}"#,
        )?;
        let imported = parquet.import_glossary(crate::parquet_store::Glossary::Kamus, &kamus)?;
        assert_eq!(
            (imported["imported"].as_u64(), imported["replaced"].as_u64()),
            (Some(2), Some(0))
        );
        fs::write(&kamus, r#"{"cahaya": ["coherence"]}"#)?;
        assert_eq!(
            parquet.import_glossary(crate::parquet_store::Glossary::Kamus, &kamus)?["replaced"],
            1
        );
        assert_eq!(
            parquet.glossary_for("riset cahaya")?.lines,
            ["- \"cahaya\" = English terms: coherence"]
        );
        let kamus_kind = crate::parquet_store::Glossary::Kamus;
        assert_eq!(
            parquet.list_glossary(kamus_kind)?,
            json!({"kamus": {"cahaya": ["coherence"], "kuantum": ["quantum"]}})
        );
        assert_eq!(
            parquet.list_glossary(crate::parquet_store::Glossary::Codec)?,
            json!({"codec": {}})
        );
        assert_eq!(
            parquet.remove_glossary(kamus_kind, "Cahaya!")?["removed"],
            true
        );
        assert_eq!(
            parquet.remove_glossary(kamus_kind, "cahaya")?["removed"],
            false
        );
        assert!(parquet.glossary_for("riset cahaya")?.lines.is_empty());
        query.smart = false;
        query.category = Some("QUANT-PH".into());

        query.query = None;
        query.limit = NonZeroUsize::new(1).unwrap();
        let hits = parquet.search(&query)?;
        assert_eq!(hits[0].id, "arxiv:2502.00001");
        assert_eq!(hits[0].categories, ["cs.LG", "quant-ph"]);
        assert!(hits[0].snippet.is_none());

        assert_eq!(
            parquet.set_tag("  Reading List ", "2501.00001", true)?["changed"],
            true
        );
        assert_eq!(
            parquet.set_tag("reading list", "2501.00001", true)?["changed"],
            false
        );
        parquet.set_tag("reading list", "2502.00003", true)?;
        parquet.set_tag("reading list", "2502.00005", true)?;
        parquet.set_tag("reading list", "2502.00006", true)?;
        assert!(parquet.set_tag("reading list", "2502.99999", true).is_err());
        query.tag = Some("READING LIST".into());
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        query.from = None;
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        query.from = Some("2025-01-01".parse()?);
        query.query = Some("quantum coherence".into());
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        query.query = Some("2501.00001".to_owned());
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        query.query = Some("arxiv:2501.00001".to_owned());
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        query.query =
            Some("https://arxiv.org/pdf/2501.00001v2.pdf?source=example#page=1".to_owned());
        assert_eq!(parquet.search(&query)?[0].id, "arxiv:2501.00001");
        assert_eq!(
            parquet.set_tag("reading list", "2501.00001", false)?["changed"],
            true
        );
        assert!(parquet.search(&query)?.is_empty());
        query.tag = None;
        query.category = Some("cs.LG".to_owned());
        assert!(parquet.search(&query)?.is_empty());
        query.category = None;
        query.from = Some("2025-01-02".parse()?);
        assert!(parquet.search(&query)?.is_empty());
        query.query = Some("2501.99999".to_owned());
        assert!(parquet.search(&query)?.is_empty());
        query.from = Some("2025-02-02".parse()?);
        assert!(parquet.search(&query).is_err());
        Ok(())
    }
}
