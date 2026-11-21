mod arxiv;
mod http;
mod llama_server;
mod model;
mod oai;
mod parquet_store;
mod progress;
mod smart_search;
mod snapshot;
mod store;

use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use clap::{Parser, Subcommand};
use parquet_store::{Glossary, ParquetStore, SearchArgs};
use std::{
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};
use store::Store;

const DEFAULT_BATCH: usize = 4096;
const DEFAULT_PARQUET_DIRECTORY: &str = "data/parquet";
const CORPUS_PATH_FILE: &str = "arkhiva.corpus";
const DEFAULT_SQLITE_DB: &str = "arkhiva.sqlite";

#[derive(Parser)]
#[command(version, about = "Local academic paper metadata corpus")]
struct Cli {
    /// Select a SQLite database. Sync and status otherwise use the default Parquet corpus.
    #[arg(long, global = true)]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Parquet {
        /// Corpus directory. Defaults to the installed corpus or data/parquet.
        #[arg(long, global = true)]
        directory: Option<PathBuf>,
        #[command(subcommand)]
        command: ParquetCommand,
    },
    Snapshot {
        #[command(subcommand)]
        command: SnapshotCommand,
    },
    Sync {
        #[arg(long, conflicts_with = "full_reconciliation")]
        from: Option<NaiveDate>,
        #[arg(long)]
        full_reconciliation: bool,
    },
    Status,
    /// Search by text, arXiv ID, or arXiv link in the local Parquet corpus.
    Search(SearchArgs),
    /// Assign and remove local paper tags.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Manage custom topics saved from smart search.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    /// Import your own word lists for smart search into the corpus index.
    Import {
        #[command(subcommand)]
        command: ImportCommand,
    },
    /// List the kamus or codec entries in the corpus index.
    List {
        #[command(subcommand)]
        command: ListCommand,
    },
    /// Remove one kamus or codec entry from the corpus index.
    Remove {
        #[command(subcommand)]
        command: RemoveCommand,
    },
    Validate {
        #[arg(long)]
        file: PathBuf,
    },
    Bench {
        #[command(subcommand)]
        command: BenchCommand,
    },
}

#[derive(Subcommand)]
enum ParquetCommand {
    /// Compute document frequencies of local corpus phrases.
    Lexicon,
    /// Seed a Parquet corpus from the validated SQLite database selected by --db.
    Seed,
    /// Upgrade existing Parquet files to the current schema.
    #[command(alias = "migrate-dates")]
    MigrateSchema,
    /// Harvest and append papers whose arXiv IDs are unseen.
    Sync {
        #[arg(long, conflicts_with = "full_reconciliation")]
        from: Option<NaiveDate>,
        #[arg(long)]
        full_reconciliation: bool,
    },
    Status,
    /// Search by text, arXiv ID, or arXiv link in this Parquet corpus.
    Search(SearchArgs),
    /// Assign and remove local paper tags.
    Tag {
        #[command(subcommand)]
        command: TagCommand,
    },
    /// Manage custom topics saved from smart search in this corpus.
    Topic {
        #[command(subcommand)]
        command: TopicCommand,
    },
    /// Look up a paper by its arXiv ID or link.
    Get {
        id: String,
    },
}

#[derive(Subcommand)]
enum TagCommand {
    /// Assign a tag to an arXiv ID or link.
    Add { name: String, paper: String },
    /// Remove a tag from an arXiv ID or link.
    Remove { name: String, paper: String },
}

#[derive(Subcommand)]
enum TopicCommand {
    /// List saved topics and their search plans.
    List,
    /// Remove a saved topic.
    Remove { name: String },
}

#[derive(Subcommand)]
enum ImportCommand {
    /// Word -> English search terms, as a JSON object: {"kompresi": ["data compression"]}.
    Kamus { file: PathBuf },
    /// Word or phrase -> definition, as a JSON object: {"lightweight": "runs on a phone"}.
    Codec { file: PathBuf },
}

#[derive(Subcommand)]
enum ListCommand {
    /// Every kamus word with its English terms.
    Kamus,
    /// Every codec word or phrase with its definition.
    Codec,
}

#[derive(Subcommand)]
enum RemoveCommand {
    /// Remove a kamus word; all-capitals words (acronyms) must be typed in capitals.
    Kamus { word: String },
    /// Remove a codec word or phrase; all-capitals words must be typed in capitals.
    Codec { word: String },
}

#[derive(Subcommand)]
enum SnapshotCommand {
    Fetch {
        #[arg(long)]
        version: u64,
        #[arg(long, default_value = "data")]
        directory: PathBuf,
    },
    Import(ImportArgs),
}

#[derive(Subcommand)]
enum BenchCommand {
    Import(ImportArgs),
}

#[derive(clap::Args)]
struct ImportArgs {
    #[arg(long)]
    file: PathBuf,
    #[arg(long)]
    release: Option<String>,
    #[arg(long, default_value_t = DEFAULT_BATCH)]
    batch_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let sqlite_db = cli.db.as_deref().unwrap_or(Path::new(DEFAULT_SQLITE_DB));
    // Search output is mostly read by models, so it is printed compactly to save tokens.
    let compact = matches!(
        cli.command,
        Command::Search(_)
            | Command::Parquet {
                command: ParquetCommand::Search(_),
                ..
            }
    );
    let report = match cli.command {
        Command::Parquet { directory, command } => {
            let directory = parquet_directory(directory)?;
            match command {
                ParquetCommand::Lexicon => ParquetStore::open(&directory)?.build_lexicon()?,
                ParquetCommand::Seed => ParquetStore::seed(&directory, sqlite_db)?.status()?,
                ParquetCommand::MigrateSchema => {
                    let mut corpus = ParquetStore::open(&directory)?;
                    let migrated = corpus.migrate_schema()?;
                    let mut report = corpus.status()?;
                    report["migrated_files"] = migrated.into();
                    report
                }
                ParquetCommand::Sync {
                    from,
                    full_reconciliation,
                } => sync_parquet(&directory, bootstrap(from, full_reconciliation))?,
                ParquetCommand::Status => ParquetStore::open_shared(&directory)?.status()?,
                ParquetCommand::Search(args) => {
                    search_parquet(&directory, &args, cli.db.is_some())?
                }
                ParquetCommand::Tag { command } => {
                    tag_parquet(&directory, command, cli.db.is_some())?
                }
                ParquetCommand::Topic { command } => {
                    topic_parquet(&directory, command, cli.db.is_some())?
                }
                ParquetCommand::Get { id } => {
                    serde_json::to_value(ParquetStore::open_shared(&directory)?.get(&id)?)?
                }
            }
        }
        Command::Snapshot {
            command: SnapshotCommand::Fetch { version, directory },
        } => {
            let file = snapshot::fetch(version, &directory)?;
            serde_json::json!({ "kaggle_release": version, "file": file })
        }
        Command::Snapshot {
            command: SnapshotCommand::Import(args),
        } => {
            let mut store = Store::open(sqlite_db, arxiv::PROVIDER)?;
            serde_json::to_value(snapshot::import(
                &mut store,
                &args.file,
                args.release.as_deref(),
                args.batch_size,
                None,
            )?)?
        }
        Command::Sync {
            from,
            full_reconciliation,
        } => {
            let bootstrap = bootstrap(from, full_reconciliation);
            if cli.db.is_some() {
                ensure!(sqlite_db.is_file(), "database does not exist");
                let mut store = Store::open(sqlite_db, arxiv::PROVIDER)?;
                serde_json::to_value(oai::sync(&mut store, oai::ENDPOINT, bootstrap)?)?
            } else {
                sync_parquet(&parquet_directory(None)?, bootstrap)?
            }
        }
        Command::Status if cli.db.is_none() => {
            ParquetStore::open_shared(&parquet_directory(None)?)?.status()?
        }
        Command::Status => {
            ensure!(sqlite_db.is_file(), "database does not exist");
            let store = Store::open(sqlite_db, arxiv::PROVIDER)?;
            let (active, deleted): (i64, i64) = store.conn.query_row(
                "SELECT count(*) FILTER (WHERE is_deleted=0), count(*) FILTER (WHERE is_deleted=1) FROM papers",
                [], |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            serde_json::json!({
                "active_papers": active, "deleted_papers": deleted, "snapshot": store.import()?,
                "oai": store.sync_state(&oai::feed(oai::ENDPOINT))?
            })
        }
        Command::Search(args) => {
            search_parquet(&parquet_directory(None)?, &args, cli.db.is_some())?
        }
        Command::Tag { command } => {
            tag_parquet(&parquet_directory(None)?, command, cli.db.is_some())?
        }
        Command::Topic { command } => {
            topic_parquet(&parquet_directory(None)?, command, cli.db.is_some())?
        }
        Command::Import { command } => {
            ensure!(
                cli.db.is_none(),
                "imports use the Parquet corpus index; omit --db"
            );
            let (kind, file) = match command {
                ImportCommand::Kamus { file } => (Glossary::Kamus, file),
                ImportCommand::Codec { file } => (Glossary::Codec, file),
            };
            ParquetStore::open(&parquet_directory(None)?)?.import_glossary(kind, &file)?
        }
        Command::List { command } => {
            ensure!(
                cli.db.is_none(),
                "lists use the Parquet corpus index; omit --db"
            );
            let kind = match command {
                ListCommand::Kamus => Glossary::Kamus,
                ListCommand::Codec => Glossary::Codec,
            };
            ParquetStore::open_shared(&parquet_directory(None)?)?.list_glossary(kind)?
        }
        Command::Remove { command } => {
            ensure!(
                cli.db.is_none(),
                "removals use the Parquet corpus index; omit --db"
            );
            let (kind, word) = match command {
                RemoveCommand::Kamus { word } => (Glossary::Kamus, word),
                RemoveCommand::Codec { word } => (Glossary::Codec, word),
            };
            ParquetStore::open(&parquet_directory(None)?)?.remove_glossary(kind, &word)?
        }
        Command::Validate { file } => {
            ensure!(sqlite_db.is_file(), "database does not exist");
            snapshot::validate(&Store::open(sqlite_db, arxiv::PROVIDER)?, &file)?
        }
        Command::Bench {
            command: BenchCommand::Import(args),
        } => snapshot::benchmark(
            &args.file,
            sqlite_db,
            args.batch_size,
            args.release.as_deref(),
        )?,
    };
    let output = if compact {
        serde_json::to_string(&report)?
    } else {
        serde_json::to_string_pretty(&report)?
    };
    println!("{output}");
    Ok(())
}

fn parquet_directory(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = override_path {
        return Ok(path);
    }
    let settings = std::env::current_exe()?.with_file_name(CORPUS_PATH_FILE);
    match fs::read_to_string(&settings) {
        Ok(contents) => {
            let path = PathBuf::from(contents.trim());
            ensure!(
                path.is_absolute(),
                "{} must contain the absolute path to a Parquet corpus",
                settings.display()
            );
            Ok(path)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(DEFAULT_PARQUET_DIRECTORY.into()),
        Err(error) => Err(error).with_context(|| format!("reading {}", settings.display())),
    }
}

fn search_parquet(
    directory: &Path,
    args: &SearchArgs,
    sqlite_selected: bool,
) -> Result<serde_json::Value> {
    ensure!(
        !sqlite_selected,
        "search uses the Parquet corpus; omit --db"
    );
    args.report(directory)
}

fn tag_parquet(
    directory: &Path,
    command: TagCommand,
    sqlite_selected: bool,
) -> Result<serde_json::Value> {
    ensure!(
        !sqlite_selected,
        "tags use the Parquet corpus index; omit --db"
    );
    let corpus = ParquetStore::open(directory)?;
    match command {
        TagCommand::Add { name, paper } => corpus.set_tag(&name, &paper, true),
        TagCommand::Remove { name, paper } => corpus.set_tag(&name, &paper, false),
    }
}

fn sync_parquet(directory: &Path, bootstrap: Option<oai::Bootstrap>) -> Result<serde_json::Value> {
    let mut corpus = ParquetStore::open(directory)?;
    corpus.sync(bootstrap)?;
    corpus.status()
}

fn topic_parquet(
    directory: &Path,
    command: TopicCommand,
    sqlite_selected: bool,
) -> Result<serde_json::Value> {
    ensure!(
        !sqlite_selected,
        "topics use the Parquet corpus index; omit --db"
    );
    match command {
        TopicCommand::List => Ok(serde_json::to_value(
            ParquetStore::open_shared(directory)?.list_topics()?,
        )?),
        TopicCommand::Remove { name } => ParquetStore::open(directory)?.remove_topic(&name),
    }
}

fn bootstrap(from: Option<NaiveDate>, full_reconciliation: bool) -> Option<oai::Bootstrap> {
    from.map(oai::Bootstrap::From)
        .or(full_reconciliation.then_some(oai::Bootstrap::Full))
}

#[cfg(test)]
mod tests;
