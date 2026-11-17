use crate::{
    arxiv::{arxiv_id, arxiv_url, categories, nonempty},
    http::Http,
    model::{Change, Paper, version_date},
    progress::{Progress, bar},
    store::{OaiChanges, PageCheckpoint, Store, SyncState, apply_sqlite_page},
};
use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, NaiveDate, Utc};
use reqwest::blocking::Response;
use roxmltree::{Document, Node};
use rusqlite::Transaction;
use std::{fs::OpenOptions, io::Read, time::Duration};

pub const ENDPOINT: &str = "https://oaipmh.arxiv.org/oai";
const OAI: &str = "http://www.openarchives.org/OAI/2.0/";
const RAW: &str = "http://arxiv.org/OAI/arXivRaw/";
const MAX_PAGE_BYTES: u64 = 64 * 1024 * 1024;

pub enum Bootstrap {
    From(NaiveDate),
    Full,
}

#[derive(Clone, Copy)]
enum Granularity {
    Day,
    Second,
}

struct Repository {
    granularity: Granularity,
    earliest: String,
    server_date: DateTime<Utc>,
}

struct Page {
    response_date: DateTime<Utc>,
    token: Option<String>,
    estimate: Option<(u64, u64)>,
    changes: Vec<(String, Change)>,
}

enum Reply {
    Page(Page),
    Empty(DateTime<Utc>),
    Expired,
}

pub fn sync(store: &mut Store, endpoint: &str, bootstrap: Option<Bootstrap>) -> Result<SyncState> {
    sync_with(store, endpoint, bootstrap, apply_sqlite_page)
}

pub fn feed(endpoint: &str) -> String {
    format!("{endpoint}#arXivRaw")
}

pub fn sync_with(
    store: &mut Store,
    endpoint: &str,
    bootstrap: Option<Bootstrap>,
    apply: impl FnMut(&Transaction<'_>, &str, OaiChanges) -> Result<()>,
) -> Result<SyncState> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(std::env::temp_dir().join("arkhiva-arxiv-oai.lock"))?;
    lock.try_lock()
        .context("another Arkhiva process is syncing arXiv")?;
    sync_inner(store, endpoint, bootstrap, Duration::from_secs(3), apply)
}

pub(crate) fn sync_inner(
    store: &mut Store,
    endpoint: &str,
    bootstrap: Option<Bootstrap>,
    interval: Duration,
    mut apply: impl FnMut(&Transaction<'_>, &str, OaiChanges) -> Result<()>,
) -> Result<SyncState> {
    ensure!(
        store.import()?.is_some_and(|s| s.validated),
        "validate the imported snapshot before syncing"
    );
    let feed = feed(endpoint);
    let mut state = store.sync_state(&feed)?;
    ensure!(
        state.is_none() || bootstrap.is_none(),
        "this feed already has a checkpoint; run sync without a bootstrap option"
    );
    ensure!(
        state.is_some() || bootstrap.is_some(),
        "first sync requires --from DATE or --full-reconciliation"
    );
    let mut progress = Progress::new("OAI-PMH");
    progress.update("connecting to arXiv");
    let mut http = Http::new(interval)?;
    // POST avoids cached OAI responses carrying an outdated harvest clock.
    let repository = http.request(
        |client| client.post(endpoint).form(&[("verb", "Identify")]),
        |response| identify(&read_xml(response)?),
    )?;
    if let Some(bootstrap) = bootstrap {
        let (from, basis) = match bootstrap {
            Bootstrap::From(date) => {
                ensure!(
                    date <= repository.server_date.date_naive(),
                    "bootstrap date is after the repository's current UTC date"
                );
                let from = repository
                    .granularity
                    .format(date.and_hms_opt(0, 0, 0).unwrap().and_utc());
                (
                    from.max(repository.earliest.clone()),
                    "operator-selected overlap",
                )
            }
            Bootstrap::Full => (repository.earliest.clone(), "repository earliest datestamp"),
        };
        store.start_sync(&feed, &from, basis)?;
        state = store.sync_state(&feed)?;
    }
    let mut state = state.context("OAI checkpoint missing")?;
    let from = state
        .active_from
        .clone()
        .unwrap_or_else(|| state.next_from.clone());
    let mut restarted = false;
    let mut pages = 0;
    let mut records = 0;
    loop {
        let reply = http.request(
            |client| {
                let request = client.post(endpoint);
                match &state.token {
                    Some(token) => request
                        .form(&[("verb", "ListRecords"), ("resumptionToken", token.as_str())]),
                    None => request.form(&[
                        ("verb", "ListRecords"),
                        ("metadataPrefix", "arXivRaw"),
                        ("from", &from),
                    ]),
                }
            },
            |response| page(&read_xml(response)?, repository.granularity),
        )?;
        let page = match reply {
            Reply::Page(page) => page,
            Reply::Empty(date) => {
                ensure!(
                    state.token.is_none(),
                    "OAI returned noRecordsMatch while paging"
                );
                Page {
                    response_date: date,
                    token: None,
                    estimate: None,
                    changes: Vec::new(),
                }
            }
            Reply::Expired if state.token.is_some() && !restarted => {
                store.restart_sync(&feed)?;
                state.token = None;
                state.first_response = None;
                restarted = true;
                progress.update("resuming from checkpoint after token expiry");
                continue;
            }
            Reply::Expired => {
                bail!("OAI rejected the resumption token after restarting the harvest")
            }
        };
        ensure!(
            page.token.is_none() || page.token != state.token,
            "OAI returned the same resumption token"
        );
        let first = match &state.first_response {
            Some(saved) => timestamp(saved)?,
            None => page.response_date,
        };
        let next = repository.granularity.format(first);
        ensure!(
            next >= from,
            "OAI responseDate precedes the requested starting boundary"
        );
        let first = first.to_rfc3339();
        let batch_size = page.changes.len() as u64;
        store.commit_page(
            PageCheckpoint {
                feed: &feed,
                from: &from,
                first_response: &first,
                next_from: &next,
                token: page.token.as_deref(),
            },
            page.changes,
            &mut apply,
        )?;
        pages += u64::from(batch_size > 0);
        records += batch_size;
        let Some(token) = page.token else {
            let state = store.sync_state(&feed)?.context("OAI checkpoint missing")?;
            progress.finish(format_args!("done | {pages} batches, {records} records"));
            return Ok(state);
        };
        if let Some((cursor, total)) = page.estimate
            && let Some(done) = cursor.checked_add(batch_size)
            && done < total
        {
            progress.update(format_args!(
                "{} | batch {pages}, {done}/~{total} records",
                bar(done, total),
            ));
        } else {
            progress.update(format_args!("batch {pages} | {records} records processed"));
        }
        state.token = Some(token);
        state.first_response = Some(first);
    }
}

impl Granularity {
    fn format(self, date: DateTime<Utc>) -> String {
        match self {
            Self::Day => date.date_naive().to_string(),
            Self::Second => date.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        }
    }

    fn datestamp(self, raw: &str) -> Result<String> {
        match self {
            Self::Day => Ok(NaiveDate::parse_from_str(raw, "%Y-%m-%d")?.to_string()),
            Self::Second => Ok(self.format(timestamp(raw)?)),
        }
    }
}

fn identify(xml: &str) -> Result<Repository> {
    let doc = Document::parse(xml)?;
    let root = root(&doc)?;
    let server_date = timestamp(required(root, OAI, "responseDate")?)?;
    let identify = child(root, OAI, "Identify").context("missing OAI Identify")?;
    ensure!(
        required(identify, OAI, "protocolVersion")? == "2.0",
        "unsupported OAI-PMH protocol version"
    );
    let granularity = match required(identify, OAI, "granularity")? {
        "YYYY-MM-DD" => Granularity::Day,
        "YYYY-MM-DDThh:mm:ssZ" => Granularity::Second,
        value => bail!("unsupported OAI granularity {value}"),
    };
    Ok(Repository {
        granularity,
        earliest: granularity.datestamp(required(identify, OAI, "earliestDatestamp")?)?,
        server_date,
    })
}

fn page(xml: &str, granularity: Granularity) -> Result<Reply> {
    let doc = Document::parse(xml)?;
    let root = root(&doc)?;
    let response_date = timestamp(required(root, OAI, "responseDate")?)?;
    if let Some(error) = child(root, OAI, "error") {
        return match error.attribute("code") {
            Some("noRecordsMatch") => Ok(Reply::Empty(response_date)),
            Some("badResumptionToken") => Ok(Reply::Expired),
            code => bail!(
                "OAI error {}: {}",
                code.unwrap_or("unknown"),
                error.text().unwrap_or_default()
            ),
        };
    }
    let list = child(root, OAI, "ListRecords").context("missing ListRecords")?;
    let token_node = child(list, OAI, "resumptionToken");
    let estimate = token_node.and_then(|node| {
        Some((
            node.attribute("cursor")?.parse().ok()?,
            node.attribute("completeListSize")?.parse().ok()?,
        ))
    });
    let token = token_node
        .and_then(|node| node.text())
        .filter(|token| !token.trim().is_empty())
        .map(str::to_owned);
    let changes = list
        .children()
        .filter(|node| node.has_tag_name((OAI, "record")))
        .map(|node| record(node, granularity))
        .collect::<Result<Vec<_>>>()?;
    ensure!(!changes.is_empty(), "OAI ListRecords contains no records");
    Ok(Reply::Page(Page {
        response_date,
        token,
        estimate,
        changes,
    }))
}

fn record(node: Node<'_, '_>, granularity: Granularity) -> Result<(String, Change)> {
    let header = child(node, OAI, "header").context("missing record header")?;
    let id = arxiv_id(required(header, OAI, "identifier")?)?;
    let datestamp = granularity.datestamp(required(header, OAI, "datestamp")?)?;
    if header.attribute("status") == Some("deleted") {
        return Ok((
            datestamp,
            Change::Delete {
                url: arxiv_url(&id),
                id,
            },
        ));
    }
    let metadata = child(node, OAI, "metadata").context("missing OAI metadata")?;
    let raw = child(metadata, RAW, "arXivRaw").context("missing arXivRaw payload")?;
    ensure!(
        arxiv_id(required(raw, RAW, "id")?)? == id,
        "OAI header and payload IDs differ"
    );
    let mut first_submitted = None;
    let mut latest = None;
    for version in raw
        .children()
        .filter(|node| node.has_tag_name((RAW, "version")))
    {
        if let Some(number) = version
            .attribute("version")
            .and_then(|v| v.strip_prefix('v'))
            .and_then(|n| n.parse::<u32>().ok())
        {
            let date = child(version, RAW, "date")
                .and_then(|n| n.text())
                .and_then(version_date);
            if number == 1 {
                first_submitted = date;
            }
            if latest.is_none_or(|(old, _)| number > old) {
                latest = Some((number, date));
            }
        }
    }
    let paper = Paper {
        url: arxiv_url(&id),
        id,
        title: child(raw, RAW, "title")
            .and_then(|n| n.text())
            .and_then(nonempty),
        abstract_text: child(raw, RAW, "abstract")
            .and_then(|n| n.text())
            .and_then(nonempty),
        categories: child(raw, RAW, "categories")
            .and_then(|n| n.text())
            .map(categories)
            .unwrap_or_default(),
        first_submitted,
        last_revised: latest.and_then(|(_, date)| date),
        metadata_updated: version_date(&datestamp[..10]),
    };
    Ok((datestamp, Change::Upsert(paper)))
}

fn read_xml(response: Response) -> Result<String> {
    let mut bytes = Vec::new();
    response.take(MAX_PAGE_BYTES + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= MAX_PAGE_BYTES,
        "OAI page exceeds the 64 MiB limit"
    );
    Ok(String::from_utf8(bytes)?)
}

fn root<'a, 'input>(doc: &'a Document<'input>) -> Result<Node<'a, 'input>> {
    let node = doc.root_element();
    ensure!(
        node.has_tag_name((OAI, "OAI-PMH")),
        "response is not OAI-PMH XML"
    );
    Ok(node)
}

fn child<'a, 'input>(node: Node<'a, 'input>, ns: &str, name: &str) -> Option<Node<'a, 'input>> {
    node.children().find(|child| child.has_tag_name((ns, name)))
}

fn required<'a>(node: Node<'a, '_>, ns: &str, name: &str) -> Result<&'a str> {
    child(node, ns, name)
        .and_then(|node| node.text())
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .with_context(|| format!("missing {name}"))
}

fn timestamp(text: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
}
