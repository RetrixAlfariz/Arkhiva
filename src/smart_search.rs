//! `--smart`: one model call turns a request into a search plan; Rust grounds every term in the
//! corpus lexicon, so a hallucinated term can only miss, never invent results.
use crate::llama_server::LlamaServer;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const PLAN_PROMPT: &str = "You turn a user's request (any language) into a search plan for an English corpus of academic papers (titles and abstracts only).
intent: 'known' if the user wants one specific existing paper or named method; 'topic' if they want papers about a subject or an idea to build; 'question' if they ask why/how.
titles: only for 'known': up to 3 exact English titles of the original paper, most likely first. Else [].
anchors: 0-3 single English words naming the domain or object that every relevant paper would mention (good: battery, lensing, compression, speaker). Never generic words like image, model, method, data, fast.
facets: 1-4 sub-needs the user actually asked for, each with 3-5 English terms (1-3 words) that paper titles and abstracts use. Translate everyday words to field terminology (file compression -> lossless compression, data compression). Keep meaning direction: speech to text is speech recognition, not speech synthesis. For a question, use the key concepts of a short answer as facets.
constraints: only requirements the user wrote (fast, lightweight, real-time, without GPU, lossless...). Each gets 2-4 phrases an abstract would use as evidence (compression speed, low memory, inference latency). Never invent constraints.
expansions: methods or directions the user did not ask for; usually [].
avoid: 0-4 English phrases naming a different meaning the search could drift into: the opposite direction or another field's sense of the same words (gene therapy vectors -> avoid: vector database). Usually [].
A constraint never becomes a facet of its own. A technical problem the user names stays a facet even when phrased as 'without X'. Example: 'deteksi lansia jatuh tanpa kamera; real-time; hemat baterai' -> facets: fall detection [fall detection, wearable sensors, accelerometer], camera-free sensing [wifi sensing, radar sensing]; constraints: real-time [real time, low latency], battery [low power, energy efficient].";

/// A term in fewer papers is noise; in more, it names a whole field.
// Test corpora hold only a few papers.
pub(crate) const MIN_DF: i64 = if cfg!(test) { 1 } else { 5 };
pub(crate) const MAX_DF: i64 = 20_000;
/// "image" (149k papers) anchors nothing; "llm" (59k) still does.
pub(crate) const ANCHOR_MAX_DF: i64 = 100_000;
/// Below this a phrase rarely drifts out of its field, so it counts as on topic unanchored.
pub(crate) const SPECIFIC_DF: i64 = 1_000;
const TERMS_PER_FACET: usize = 4;
const TERMS_PER_CONSTRAINT: usize = 6;

#[derive(Clone, Default, Deserialize, Serialize)]
pub(crate) struct Group {
    pub(crate) name: String,
    pub(crate) terms: Vec<String>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub(crate) struct Plan {
    pub(crate) intent: String,
    pub(crate) titles: Vec<String>,
    pub(crate) anchors: Vec<String>,
    pub(crate) facets: Vec<Group>,
    pub(crate) constraints: Vec<Group>,
    pub(crate) expansions: Vec<Group>,
    pub(crate) avoid: Vec<String>,
}

/// The plan after grounding: only phrases the corpus uses at a useful rate.
#[derive(Default, Serialize)]
pub(crate) struct Grounded {
    pub(crate) anchors: Vec<String>,
    pub(crate) facets: Vec<Group>,
    pub(crate) constraints: Vec<Group>,
    pub(crate) expansions: Vec<Group>,
    pub(crate) avoid: Vec<String>,
}

pub(crate) fn validate_prompt(prompt: &str) -> Result<&str> {
    let prompt = prompt.trim();
    ensure!(!prompt.is_empty(), "--smart requires a research prompt");
    ensure!(
        prompt.len() <= 4096,
        "--smart prompt exceeds 4096 UTF-8 bytes"
    );
    Ok(prompt)
}

pub(crate) fn plan(server: &LlamaServer, prompt: &str) -> Result<Plan> {
    let strings = json!({"type": "array", "items": {"type": "string"}});
    let groups = json!({"type": "array", "items": {
        "type": "object",
        "properties": {"name": {"type": "string"}, "terms": strings},
        "required": ["name", "terms"],
        "additionalProperties": false,
    }});
    let request = json!({
        "messages": [
            {"role": "system", "content": PLAN_PROMPT},
            {"role": "user", "content": prompt},
        ],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "arkhiva_plan",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string", "enum": ["known", "topic", "question"]},
                    "titles": strings, "anchors": strings, "facets": groups,
                    "constraints": groups, "expansions": groups, "avoid": strings,
                },
                "required": ["intent", "titles", "anchors", "facets", "constraints", "expansions", "avoid"],
                "additionalProperties": false,
            }
        }},
        "stream": false,
    });
    parse_plan(&server.chat(request, 800)?)
}

fn parse_plan(reply: &Value) -> Result<Plan> {
    let choice = &reply["choices"][0];
    ensure!(
        choice["finish_reason"] != "length",
        "the model reached its output limit before finishing the search plan"
    );
    let content = choice["message"]["content"]
        .as_str()
        .context("the model returned no search plan")?;
    serde_json::from_str(content).context("invalid model search plan JSON")
}

/// Marks punctuation inside word-joined text, so a phrase cannot match across it.
const BREAK: &str = "|";

/// Lowercase words, hyphens splitting words. Other punctuation becomes a `BREAK`, matching how
/// the lexicon never counts a phrase across punctuation ("calibration." keeps its word).
pub(crate) fn words(text: &str) -> Vec<String> {
    let spaced: String = text
        .to_lowercase()
        .chars()
        .map(|ch| match ch {
            '-' | '‐' | '‑' => " ".to_owned(),
            ch if ch.is_alphanumeric() || ch.is_whitespace() => ch.to_string(),
            _ => format!(" {BREAK} "),
        })
        .collect();
    let mut out: Vec<String> = Vec::new();
    for word in spaced.split_whitespace() {
        let repeated = word == BREAK && out.last().is_none_or(|last| last == BREAK);
        if !repeated {
            out.push(word.to_owned());
        }
    }
    if out.last().is_some_and(|last| last == BREAK) {
        out.pop();
    }
    out
}

pub(crate) fn phrase(text: &str) -> String {
    words(text).join(" ")
}

/// Keeps the model's own terms within `MIN_DF..=max_df`; no substitution (it drifted).
/// Facet terms far broader than their siblings drag in a whole field, so they are dropped.
fn ground_groups(
    groups: &[Group],
    limit: usize,
    max_df: i64,
    df: &mut impl FnMut(&str) -> Result<i64>,
) -> Result<Vec<Group>> {
    let mut kept = Vec::new();
    for group in groups {
        let mut terms: Vec<(String, i64)> = Vec::new();
        for term in &group.terms {
            let phrase = phrase(term);
            if phrase.is_empty() || terms.iter().any(|(old, _)| *old == phrase) {
                continue;
            }
            let count = df(&phrase)?;
            if (MIN_DF..=max_df).contains(&count) {
                terms.push((phrase, count));
            }
        }
        if terms.len() > 1 && max_df == MAX_DF {
            let mut counts: Vec<i64> = terms.iter().map(|(_, count)| *count).collect();
            counts.sort_unstable();
            let middle = counts.len() / 2;
            let median = if counts.len().is_multiple_of(2) {
                (counts[middle - 1] + counts[middle]) as f64 / 2.0
            } else {
                counts[middle] as f64
            };
            terms.retain(|(_, count)| *count as f64 <= 10.0 * median);
        }
        if !terms.is_empty() {
            kept.push(Group {
                name: group.name.clone(),
                terms: terms
                    .into_iter()
                    .take(limit)
                    .map(|(term, _)| term)
                    .collect(),
            });
        }
    }
    Ok(kept)
}

pub(crate) fn ground(plan: &Plan, mut df: impl FnMut(&str) -> Result<i64>) -> Result<Grounded> {
    let mut facets: Vec<Group> = Vec::new();
    for facet in &plan.facets {
        if !facets.iter().any(|old| old.name == facet.name) {
            facets.push(facet.clone());
        }
    }
    // The user's own words ("fast", "lightweight") are evidence too: abstracts say "fast",
    // not "compression speed". Constraint terms are evidence, not queries, so a common
    // phrase such as "real time" is fine there.
    let constraints: Vec<Group> = plan
        .constraints
        .iter()
        .map(|group| Group {
            name: group.name.clone(),
            terms: group
                .name
                .replace(',', " and ")
                .split(" and ")
                .map(str::to_owned)
                .chain(group.terms.iter().cloned())
                .collect(),
        })
        .collect();
    let mut anchors = Vec::new();
    for anchor in plan.anchors.iter().take(3) {
        if let Some(word) = words(anchor).into_iter().next()
            && !anchors.contains(&word)
            && (1..=ANCHOR_MAX_DF).contains(&df(&word)?)
        {
            anchors.push(word);
        }
    }
    let mut avoid = Vec::new();
    for text in &plan.avoid {
        let phrase = phrase(text);
        if !phrase.is_empty() && !avoid.contains(&phrase) && df(&phrase)? > 0 {
            avoid.push(phrase);
        }
    }
    Ok(Grounded {
        anchors,
        facets: ground_groups(&facets, TERMS_PER_FACET, MAX_DF, &mut df)?,
        constraints: ground_groups(&constraints, TERMS_PER_CONSTRAINT, i64::MAX, &mut df)?,
        expansions: ground_groups(&plan.expansions, TERMS_PER_FACET, MAX_DF, &mut df)?,
        avoid,
    })
}

/// Word-joined sentences of a paper: the title, then each abstract sentence.
pub(crate) fn sentences(title: &str, abstract_text: &str) -> Vec<String> {
    std::iter::once(phrase(title))
        .chain(
            abstract_text
                .replace(['!', '?'], ".")
                .split(". ")
                .map(phrase),
        )
        .filter(|sentence| !sentence.is_empty())
        .collect()
}

/// Whole-word containment on word-joined text: "fast" must not match "breakfast".
pub(crate) fn contains(text: &str, phrase: &str) -> bool {
    format!(" {text} ").contains(&format!(" {phrase} "))
}

/// Constraint evidence counts only in a sentence that also names the topic, so "high
/// throughput" from a sequencing paper does not reward it in a compression search.
pub(crate) fn binds(
    sentences: &[String],
    term: &str,
    topic: &[String],
    anchors: &[String],
) -> bool {
    sentences.iter().any(|sentence| {
        contains(sentence, term)
            && (topic.iter().any(|t| contains(sentence, t))
                || anchors.iter().any(|a| contains(sentence, a)))
    })
}

/// Word-set Jaccard similarity; a guessed title counts only if a real title is this close.
pub(crate) fn similar(left: &str, right: &str) -> f64 {
    let set = |text| -> std::collections::BTreeSet<String> {
        words(text)
            .into_iter()
            .filter(|word| word != BREAK)
            .collect()
    };
    let (left, right) = (set(left), set(right));
    left.intersection(&right).count() as f64 / left.union(&right).count().max(1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group(name: &str, terms: &[&str]) -> Group {
        Group {
            name: name.into(),
            terms: terms.iter().map(|term| (*term).into()).collect(),
        }
    }

    #[test]
    fn grounds_terms_by_corpus_frequency() -> Result<()> {
        let counts = [
            ("lossless compression", 3_000),
            ("data compression", 9_000),
            ("compression", 90_000),
            ("compress", 0),
            ("image", 150_000),
            ("real time", 53_800),
            ("fast", 60_000),
            ("compression speed", 31),
            ("speech synthesis", 4_000),
        ];
        let df = |phrase: &str| {
            Ok(counts
                .iter()
                .find(|(known, _)| *known == phrase)
                .map_or(0, |(_, count)| *count))
        };
        let plan = Plan {
            anchors: vec!["Compression".into(), "image".into(), "compress".into()],
            facets: vec![
                group(
                    "files",
                    &["Lossless-compression", "data compression", "rare thing"],
                ),
                group("files", &["ignored duplicate"]),
                group("broad", &["compression"]),
            ],
            constraints: vec![group("fast", &["compression speed", "real-time"])],
            avoid: vec!["speech synthesis".into(), "made up".into()],
            ..Plan::default()
        };
        let grounded = ground(&plan, df)?;
        assert_eq!(grounded.anchors, ["compression"]);
        assert_eq!(grounded.facets.len(), 1);
        assert_eq!(
            grounded.facets[0].terms,
            ["lossless compression", "data compression"]
        );
        assert_eq!(
            grounded.constraints[0].terms,
            ["fast", "compression speed", "real time"]
        );
        assert_eq!(grounded.avoid, ["speech synthesis"]);
        Ok(())
    }

    #[test]
    fn binds_evidence_to_the_topic_sentence() {
        let text = sentences(
            "Compression of high-throughput sequencing data",
            "We sequence genomes at high throughput. Our lossless compression is fast! Breakfast.",
        );
        let topic = vec!["lossless compression".to_owned()];
        assert!(binds(&text, "fast", &topic, &[]));
        assert!(!binds(&text, "high throughput", &topic, &[]));
        assert!(binds(
            &text,
            "high throughput",
            &topic,
            &["compression".into()]
        ));
        assert!(!contains("breakfast", "fast"));
        assert_eq!(
            phrase("Speed, memory: LZ4-fast."),
            "speed | memory | lz4 fast"
        );
        assert!(!contains(&phrase("speed, memory"), "speed memory"));
        assert!(similar("Attention Is All You Need", "attention is all you need") > 0.99);
    }

    #[test]
    fn parses_a_complete_plan_and_rejects_truncation() -> Result<()> {
        let reply = |content: &str, finish: &str| json!({"choices": [{"finish_reason": finish, "message": {"content": content}}]});
        let content = r#"{"intent":"topic","titles":[],"anchors":["battery"],"facets":[{"name":"soh","terms":["state of health"]}],"constraints":[],"expansions":[],"avoid":[]}"#;
        assert_eq!(parse_plan(&reply(content, "stop"))?.anchors, ["battery"]);
        assert!(parse_plan(&reply(content, "length")).is_err());
        assert!(parse_plan(&reply("{", "stop")).is_err());
        Ok(())
    }
}
