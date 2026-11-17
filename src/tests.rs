use crate::{
    arxiv::{PROVIDER, arxiv_id},
    oai::{self, Bootstrap},
    snapshot,
    store::{Store, apply_sqlite_page},
};
use anyhow::Result;
use chrono::NaiveDate;
use serde_json::json;
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    thread,
    time::Duration,
};
use tempfile::tempdir;

fn fixture() -> String {
    let versions = json!([
        {"version":"v2","created":"Wed, 3 May 2023 12:00:00 GMT"},
        {"version":"v1","created":"Tue, 2 May 2023 12:00:00 GMT"}
    ]);
    [
        json!({"id":"2305.01234","title":"First title","abstract":"Abstract","categories":"cs.AI cs.CL","versions":versions,"update_date":"2023-05-04"}).to_string(),
        json!({"id":"2305.01234v2","title":"Duplicate"}).to_string(),
        "{broken".into(),
        json!({"id":"hep-th/9901001","title":"Legacy paper"}).to_string(),
    ].join("\n")
}

#[test]
fn snapshot_offsets_and_rows_commit_together() -> Result<()> {
    let temporary = tempdir()?;
    let file = temporary.path().join("snapshot.json");
    let data = fixture();
    fs::write(&file, &data)?;

    let mut clean = Store::open(&temporary.path().join("clean.sqlite"), PROVIDER)?;
    snapshot::import(&mut clean, &file, Some("fixture"), 2, None)?;
    let expected = snapshot::validate(&clean, &file)?;
    assert_eq!(expected["input_records"], 4);
    assert_eq!(expected["distinct_imported"], 2);
    assert_eq!(expected["duplicates"], 1);
    assert_eq!(expected["rejected"], 1);
    assert_eq!(expected["missing"]["abstract_text"], 1);
    assert_eq!(expected["missing"]["first_submitted_on"], 1);
    let fields: (String, String, String, String) = clean.conn.query_row(
        "SELECT title, first_submitted_on, last_revised_on, metadata_updated_on FROM papers WHERE external_id='2305.01234'",
        [], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    assert_eq!(
        fields,
        (
            "First title".into(),
            "2023-05-02".into(),
            "2023-05-03".into(),
            "2023-05-04".into()
        )
    );

    let db = temporary.path().join("resumed.sqlite");
    let mut resumed = Store::open(&db, PROVIDER)?;
    assert!(Store::open(&db, PROVIDER).is_err());
    resumed.conn.execute_batch(
        "CREATE TRIGGER interrupt BEFORE UPDATE OF next_offset ON snapshot_imports
         BEGIN SELECT RAISE(ABORT,'simulated commit interruption'); END;",
    )?;
    assert!(snapshot::import(&mut resumed, &file, Some("fixture"), 2, None).is_err());
    assert_eq!(resumed.import()?.unwrap().lines, 0);
    assert_eq!(
        resumed
            .conn
            .query_row("SELECT count(*) FROM papers", [], |r| r.get::<_, u32>(0))?,
        0
    );
    resumed.conn.execute_batch(
        "DROP TRIGGER interrupt;
         CREATE TRIGGER interrupt BEFORE UPDATE OF next_offset ON snapshot_imports
         WHEN OLD.next_offset > 0 BEGIN SELECT RAISE(ABORT,'interrupted after first batch'); END;",
    )?;
    assert!(snapshot::import(&mut resumed, &file, Some("fixture"), 2, None).is_err());
    let state = resumed.import()?.unwrap();
    assert_eq!(state.lines, 2);
    assert_eq!(
        state.next_offset as usize,
        data.match_indices('\n').nth(1).unwrap().0 + 1
    );
    resumed.conn.execute_batch("DROP TRIGGER interrupt;")?;
    drop(resumed);

    let mut resumed = Store::open(&db, PROVIDER)?;
    fs::write(&file, data.replacen("First title", "Wrong title", 1))?;
    assert!(snapshot::import(&mut resumed, &file, Some("fixture"), 2, None).is_err());
    assert_eq!(resumed.import()?.unwrap().lines, 2);
    fs::write(&file, &data)?;
    snapshot::import(&mut resumed, &file, None, 2, None)?;
    assert_eq!(snapshot::validate(&resumed, &file)?, expected);
    Ok(())
}

#[test]
fn normalizes_modern_and_legacy_ids() -> Result<()> {
    assert_eq!(arxiv_id("oai:arXiv.org:2305.01234v2")?, "2305.01234");
    assert_eq!(arxiv_id("arxiv:2305.01234")?, "2305.01234");
    assert_eq!(
        arxiv_id("https://arxiv.org/abs/hep-th/9901001v1")?,
        "hep-th/9901001"
    );
    assert!(arxiv_id("hep-th/../9901001").is_err());
    Ok(())
}

fn xml(date: &str, payload: &str) -> String {
    format!(
        "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"><responseDate>{date}T12:00:00Z</responseDate>{payload}</OAI-PMH>"
    )
}

fn record(id: &str, date: &str, title: &str) -> String {
    format!("<record><header><identifier>oai:arXiv.org:{id}</identifier><datestamp>{date}</datestamp></header>
       <metadata><arXivRaw xmlns=\"http://arxiv.org/OAI/arXivRaw/\"><id>{id}</id>
       <version version=\"v1\"><date>Tue, 2 May 2023 12:00:00 GMT</date></version>
       <version version=\"v2\"><date>Wed, 3 May 2023 12:00:00 GMT</date></version>
       <title>{title}</title><categories>cs.AI</categories><abstract>Text &amp; math</abstract>
       </arXivRaw></metadata></record>")
}

fn list(records: &str, token: &str) -> String {
    format!("<ListRecords>{records}<resumptionToken>{token}</resumptionToken></ListRecords>")
}

#[test]
fn oai_pages_resume_and_replay_without_advancing_early() -> Result<()> {
    let temp = tempdir()?;
    let file = temp.path().join("snapshot.json");
    fs::write(&file, fixture())?;
    let mut store = Store::open(&temp.path().join("corpus.sqlite"), PROVIDER)?;
    snapshot::import(&mut store, &file, None, 2, None)?;
    let validation = snapshot::validate(&store, &file)?;

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let endpoint = format!("http://{address}/oai");
    let feed = format!("{endpoint}#arXivRaw");
    let identify = xml(
        "2025-05-10",
        "<Identify><protocolVersion>2.0</protocolVersion><granularity>YYYY-MM-DD</granularity><earliestDatestamp>2005-09-16</earliestDatestamp></Identify>",
    );
    let first = xml(
        "2025-05-10",
        &list(
            &record("2305.01234", "2025-05-09", "OAI original"),
            "old&amp;opaque",
        ),
    );
    let token_error = xml("2025-05-10", "<error code=\"badResumptionToken\"/>");
    let replay = xml(
        "2025-05-10",
        &list(
            &(record("2305.01234", "2025-05-09", "OAI corrected")
                + &record("2505.00001", "2025-05-10", "New paper")),
            "new",
        )
        .replace(
            "<resumptionToken>",
            r#"<resumptionToken cursor="0" completeListSize="4">"#,
        ),
    );
    let deletion = "<record><header status=\"deleted\"><identifier>oai:arXiv.org:hep-th/9901001</identifier><datestamp>2025-05-10</datestamp></header></record>";
    let final_page = xml(
        "2025-05-11",
        &list(
            &(record("2305.01234", "2025-05-08", "Stale OAI") + deletion),
            "",
        ),
    );
    let empty = xml("2025-05-11", "<error code=\"noRecordsMatch\"/>");
    let server = thread::spawn(move || {
        let replies = [
            ("verb=Identify", "200 OK", identify.clone()),
            ("from=2025-05-09", "503 Service Unavailable", "".into()),
            ("from=2025-05-09", "200 OK", first),
            (
                "verb=ListRecords&resumptionToken=old%26opaque",
                "400 Bad Request",
                "".into(),
            ),
            ("verb=Identify", "200 OK", identify.clone()),
            (
                "verb=ListRecords&resumptionToken=old%26opaque",
                "200 OK",
                token_error,
            ),
            ("from=2025-05-09", "200 OK", replay),
            (
                "verb=ListRecords&resumptionToken=new",
                "200 OK",
                "<truncated>".into(),
            ),
            ("verb=ListRecords&resumptionToken=new", "200 OK", final_page),
            ("verb=Identify", "200 OK", identify),
            ("from=2025-05-10", "200 OK", empty),
        ];
        for (expected, status, body) in replies {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut reader = BufReader::new(&mut stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            assert!(line.starts_with("POST /oai "));
            let mut body_len = 0;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" || line.is_empty() {
                    break;
                }
                if let Some(value) = line.strip_prefix("content-length:") {
                    body_len = value.trim().parse().unwrap();
                }
            }
            let mut request_body = vec![0; body_len];
            reader.read_exact(&mut request_body).unwrap();
            let request_body = String::from_utf8(request_body).unwrap();
            assert!(
                request_body.contains(expected),
                "unexpected request {request_body:?}, wanted {expected:?}"
            );
            write!(stream, "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\nRetry-After: 0\r\n\r\n{body}", body.len()).unwrap();
        }
    });

    let date = NaiveDate::from_ymd_opt(2025, 5, 9).unwrap();
    assert!(
        oai::sync_inner(
            &mut store,
            &endpoint,
            Some(Bootstrap::From(date)),
            Duration::ZERO,
            apply_sqlite_page
        )
        .is_err()
    );
    let incomplete = store.sync_state(&feed)?.unwrap();
    assert_eq!(incomplete.completed_harvests, 0);
    assert_eq!(incomplete.token.as_deref(), Some("old&opaque"));
    assert_eq!(incomplete.next_from, "2025-05-09");

    let complete = oai::sync_inner(
        &mut store,
        &endpoint,
        None,
        Duration::ZERO,
        apply_sqlite_page,
    )?;
    assert_eq!(complete.completed_harvests, 1);
    assert_eq!(complete.next_from, "2025-05-10");
    assert!(complete.token.is_none());
    let (title, abstract_text): (String, String) = store.conn.query_row(
        "SELECT title,abstract FROM papers WHERE external_id='2305.01234'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(title, "OAI corrected");
    assert_eq!(abstract_text, "Text & math");
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM papers", [], |r| r.get::<_, u32>(0))?,
        3
    );
    assert_eq!(
        store.conn.query_row(
            "SELECT is_deleted FROM papers WHERE external_id='hep-th/9901001'",
            [],
            |r| r.get::<_, u32>(0)
        )?,
        1
    );

    let next = oai::sync_inner(
        &mut store,
        &endpoint,
        None,
        Duration::ZERO,
        apply_sqlite_page,
    )?;
    assert_eq!(next.completed_harvests, 2);
    assert_eq!(next.next_from, "2025-05-11");
    assert_eq!(snapshot::validate(&store, &file)?, validation);
    server.join().unwrap();
    Ok(())
}

#[test]
fn snapshot_fetch_extracts_the_pinned_archive() -> Result<()> {
    let temporary = tempdir()?;
    let directory = temporary.path().join("arxiv-v312");
    fs::create_dir(&directory)?;
    let archive = fs::File::create(directory.join("arxiv.zip"))?;
    let mut archive = zip::ZipWriter::new(archive);
    archive.start_file(
        "arxiv-metadata-oai-snapshot.json",
        zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated),
    )?;
    let data = fixture();
    archive.write_all(data.as_bytes())?;
    archive.finish()?;

    let path = snapshot::fetch(312, temporary.path())?;
    assert_eq!(fs::read_to_string(&path)?, data);
    assert_eq!(snapshot::fetch(312, temporary.path())?, path);
    Ok(())
}
