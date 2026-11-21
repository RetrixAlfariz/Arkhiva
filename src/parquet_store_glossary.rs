//! User glossaries for `--smart`. A kamus maps a word to English terms that are always
//! searched; a codec defines a word or phrase for the model. Both apply only when the prompt
//! names the entry's key.
use super::ParquetStore;
use crate::smart_search::{Group, MAX_DF, MIN_DF, Plan, contains, phrase};
use anyhow::{Context, Result, bail, ensure};
use rusqlite::params;
use serde_json::{Value, json};
use std::{collections::BTreeMap, fs, path::Path};

const MAX_DEFINITION_CHARS: usize = 1_000;
const MAX_ENTRIES_PER_PROMPT: usize = 20;

#[derive(Clone, Copy)]
pub enum Glossary {
    Kamus,
    Codec,
}

impl Glossary {
    fn name(self) -> &'static str {
        match self {
            Self::Kamus => "kamus",
            Self::Codec => "codec",
        }
    }
}

/// Entries a prompt names: lines for the model, and kamus groups to search.
#[derive(Default)]
pub(super) struct Matched {
    pub(super) lines: Vec<String>,
    pub(super) kamus: Vec<Group>,
}

/// Words with case kept, split at anything but letters and digits.
fn cased(text: &str) -> String {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// An all-capitals key ("IT", "YOLO") is an acronym and keeps its case, so it never matches
/// the everyday word "it". Other keys ignore case and punctuation.
fn key_of(raw: &str) -> String {
    let acronym = raw.chars().any(char::is_alphabetic) && !raw.chars().any(char::is_lowercase);
    if acronym { cased(raw) } else { phrase(raw) }
}

fn named_in(prompt: &str, key: &str) -> bool {
    if key.chars().any(char::is_uppercase) {
        contains(&cased(prompt), key)
    } else {
        contains(&phrase(prompt), key)
    }
}

/// Validates every entry first, so a bad entry imports nothing.
fn parse(kind: Glossary, text: &str) -> Result<Vec<(String, String)>> {
    let entries: BTreeMap<String, Value> =
        serde_json::from_str(text).context("expected a JSON object of entries")?;
    ensure!(!entries.is_empty(), "the file has no entries");
    let mut rows: Vec<(String, String)> = Vec::new();
    for (raw, value) in entries {
        let key = key_of(&raw);
        ensure!(!key.is_empty(), "entry {raw:?} has no word");
        let stored = match (kind, value) {
            (Glossary::Kamus, Value::Array(items)) => {
                let mut terms: Vec<String> = Vec::new();
                for item in items {
                    let term = item
                        .as_str()
                        .map(phrase)
                        .filter(|term| !term.is_empty())
                        .with_context(|| format!("{raw:?}: every term must be nonempty text"))?;
                    if !terms.contains(&term) {
                        terms.push(term);
                    }
                }
                ensure!(!terms.is_empty(), "{raw:?} needs at least one term");
                serde_json::to_string(&terms)?
            }
            (Glossary::Codec, Value::String(definition)) => {
                let definition = definition.split_whitespace().collect::<Vec<_>>().join(" ");
                ensure!(!definition.is_empty(), "{raw:?} needs a definition");
                ensure!(
                    definition.chars().count() <= MAX_DEFINITION_CHARS,
                    "{raw:?}: definition exceeds {MAX_DEFINITION_CHARS} characters"
                );
                definition
            }
            (Glossary::Kamus, _) => bail!("{raw:?}: a kamus entry is a list of English terms"),
            (Glossary::Codec, _) => bail!("{raw:?}: a codec entry is a definition string"),
        };
        ensure!(
            !rows.iter().any(|(old, _)| *old == key),
            "{raw:?} repeats another entry once punctuation (and case, unless all capitals) is ignored"
        );
        rows.push((key, stored));
    }
    Ok(rows)
}

fn matched(prompt: &str, kamus: &[(String, String)], codec: &[(String, String)]) -> Matched {
    let named = |rows: &[(String, String)]| -> Vec<(String, String)> {
        rows.iter()
            .filter(|(key, _)| named_in(prompt, key))
            .take(MAX_ENTRIES_PER_PROMPT)
            .cloned()
            .collect()
    };
    let mut out = Matched::default();
    for (word, terms) in named(kamus) {
        let terms: Vec<String> = serde_json::from_str(&terms).unwrap_or_default();
        out.lines.push(format!(
            "- \"{word}\" = English terms: {}",
            terms.join(", ")
        ));
        out.kamus.push(Group { name: word, terms });
    }
    for (term, definition) in named(codec) {
        out.lines.push(format!("- \"{term}\" means: {definition}"));
    }
    out
}

/// Adds each kamus entry as a facet, so its terms are searched whatever the model wrote.
/// Terms the model already used are left out, so no paper is counted twice.
pub(super) fn add_kamus(plan: &mut Plan, kamus: Vec<Group>) {
    let used: Vec<String> = plan
        .facets
        .iter()
        .flat_map(|facet| facet.terms.iter().map(|term| phrase(term)))
        .collect();
    for mut group in kamus {
        group.terms.retain(|term| !used.contains(term));
        if !group.terms.is_empty() && !plan.facets.iter().any(|f| f.name == group.name) {
            plan.facets.push(group);
        }
    }
}

/// Explains why smart search would skip a term, or None when it is searchable.
fn skip_reason(papers: i64) -> Option<String> {
    if papers == 0 {
        Some("not found in the corpus".into())
    } else if papers < MIN_DF {
        Some(format!(
            "in only {papers} papers; smart search needs {MIN_DF}"
        ))
    } else if papers > MAX_DF {
        Some(format!(
            "in {papers} papers; smart search skips terms in more than {MAX_DF}"
        ))
    } else {
        None
    }
}

impl ParquetStore {
    pub fn import_glossary(&self, kind: Glossary, file: &Path) -> Result<Value> {
        let text =
            fs::read_to_string(file).with_context(|| format!("reading {}", file.display()))?;
        let rows = parse(kind, &text).with_context(|| format!("importing {}", file.display()))?;
        self.init_glossary()?;
        let table = kind.name();
        let transaction = self.index.conn.unchecked_transaction()?;
        let mut replaced = 0;
        for (key, value) in &rows {
            // Re-importing a key replaces its entry; other entries stay.
            replaced += transaction.execute(&format!("DELETE FROM {table} WHERE key=?1"), [key])?;
            transaction.execute(
                &format!("INSERT INTO {table}(key, value) VALUES (?1, ?2)"),
                params![key, value],
            )?;
        }
        transaction.commit()?;
        let mut report = json!({table: file.display().to_string(), "imported": rows.len(), "replaced": replaced});
        if let Glossary::Kamus = kind {
            report["warnings"] = json!(self.check_kamus(&rows));
        }
        Ok(report)
    }

    /// Removes one entry; the word matches as on import (case kept only for all capitals).
    pub fn remove_glossary(&self, kind: Glossary, word: &str) -> Result<Value> {
        let key = key_of(word);
        ensure!(!key.is_empty(), "give the word to remove");
        self.init_glossary()?;
        let table = kind.name();
        let removed = self
            .index
            .conn
            .execute(&format!("DELETE FROM {table} WHERE key=?1"), [&key])?;
        Ok(json!({table: key, "removed": removed != 0}))
    }

    /// Terms smart search would skip; imported anyway, since the corpus grows.
    fn check_kamus(&self, rows: &[(String, String)]) -> Vec<String> {
        let mut df = match self.lexicon_df() {
            Ok(df) => df,
            Err(_) => return vec!["no phrase lexicon yet; terms were not checked".into()],
        };
        let mut warnings = Vec::new();
        for (word, terms) in rows {
            for term in serde_json::from_str::<Vec<String>>(terms).unwrap_or_default() {
                match df(&term) {
                    Ok(papers) => {
                        if let Some(reason) = skip_reason(papers) {
                            warnings.push(format!("{word}: \"{term}\" is {reason}"));
                        }
                    }
                    Err(error) => warnings.push(format!("{word}: \"{term}\" unchecked: {error}")),
                }
            }
        }
        warnings
    }

    /// Kamus and codec entries the prompt names.
    pub(super) fn glossary_for(&self, prompt: &str) -> Result<Matched> {
        Ok(matched(
            prompt,
            &self.rows(Glossary::Kamus)?,
            &self.rows(Glossary::Codec)?,
        ))
    }

    /// Every entry, sorted: kamus words with their term lists, codec words with definitions.
    pub fn list_glossary(&self, kind: Glossary) -> Result<Value> {
        let mut entries = serde_json::Map::new();
        for (key, value) in self.rows(kind)? {
            let value = match kind {
                Glossary::Kamus => serde_json::from_str(&value)?,
                Glossary::Codec => Value::String(value),
            };
            entries.insert(key, value);
        }
        Ok(json!({kind.name(): entries}))
    }

    fn rows(&self, kind: Glossary) -> Result<Vec<(String, String)>> {
        self.init_glossary()?;
        let mut statement = self.index.conn.prepare(&format!(
            "SELECT key, value FROM {} ORDER BY key",
            kind.name()
        ))?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<_>>()?;
        Ok(rows)
    }

    fn init_glossary(&self) -> Result<()> {
        // Additive tables, like search_topics.
        self.index
            .conn
            .execute_batch(include_str!("../migrations/glossary.sql"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_validates_and_matches_whole_words() -> Result<()> {
        let kamus = parse(
            Glossary::Kamus,
            r#"{"Kompresi": ["Data Compression", "data compression", "lossless compression"],
               "gudang": ["warehouse"]}"#,
        )?;
        assert_eq!(
            kamus[0],
            (
                "kompresi".into(),
                r#"["data compression","lossless compression"]"#.into()
            )
        );
        let codec = parse(
            Glossary::Codec,
            r#"{"lightweight": "runs  on a\nmicrocontroller with under 1 MB RAM"}"#,
        )?;
        let found = matched("Algoritma KOMPRESI file; lightweight", &kamus, &codec);
        assert_eq!(
            found.lines,
            [
                "- \"kompresi\" = English terms: data compression, lossless compression",
                "- \"lightweight\" means: runs on a microcontroller with under 1 MB RAM",
            ]
        );
        assert_eq!(found.kamus[0].name, "kompresi");
        assert!(matched("kompresigudang", &kamus, &codec).lines.is_empty());

        assert!(parse(Glossary::Kamus, r#"{"a": []}"#).is_err());
        assert!(parse(Glossary::Kamus, r#"{"a": "text"}"#).is_err());
        assert!(parse(Glossary::Codec, r#"{"a": ["x"]}"#).is_err());
        assert!(parse(Glossary::Codec, r#"{"Ab": "x", "ab!": "y"}"#).is_err());
        assert!(parse(Glossary::Codec, "[]").is_err());
        Ok(())
    }

    #[test]
    fn all_capitals_keys_match_only_in_capitals() -> Result<()> {
        let kamus = parse(
            Glossary::Kamus,
            r#"{"IT": ["information technology"], "it": ["x"], "YOLO": ["you only look once"], "YOLO-v3": ["yolov3"]}"#,
        )?;
        assert_eq!(kamus[0].0, "IT");
        let names = |prompt: &str| -> Vec<String> {
            matched(prompt, &kamus, &[])
                .kamus
                .into_iter()
                .map(|g| g.name)
                .collect()
        };
        assert_eq!(names("is it fast"), ["it"]);
        assert_eq!(names("IT audit"), ["IT", "it"]);
        // Any lowercase letter makes a key case-insensitive.
        assert_eq!(names("paper YOLO v3"), ["YOLO", "yolo v3"]);
        assert_eq!(names("paper yolo v3"), ["yolo v3"]);
        Ok(())
    }

    #[test]
    fn kamus_facets_skip_terms_the_model_used() {
        let group = |name: &str, terms: &[&str]| Group {
            name: name.into(),
            terms: terms.iter().map(|term| (*term).into()).collect(),
        };
        let mut plan = Plan {
            facets: vec![group("compression", &["Data Compression"])],
            ..Plan::default()
        };
        add_kamus(
            &mut plan,
            vec![
                group("kompresi", &["data compression", "lossless compression"]),
                group("zip", &["data compression"]),
            ],
        );
        assert_eq!(plan.facets.len(), 2);
        assert_eq!(plan.facets[1].terms, ["lossless compression"]);
        assert_eq!(skip_reason(0).as_deref(), Some("not found in the corpus"));
        assert!(skip_reason(MIN_DF).is_none());
        assert!(skip_reason(MAX_DF + 1).is_some());
    }
}
