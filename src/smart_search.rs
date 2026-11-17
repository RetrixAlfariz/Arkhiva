use crate::llama_server::LlamaServer;
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

const TAXONOMY: &str = include_str!("arxiv_categories.tsv");
const KEYWORD_PROMPT: &str = r#"Read the user's research description. Identify the underlying publication or scientific concept.
For a known original paper, use 3–6 distinctive words from the canonical English title.
For a broad topic, use 2–4 translated technical terms.
Use standard acronyms when relevant: radio-frequency signals are RF; Automatic Identification System is AIS.
Use one vocabulary word per item. Preserve specified author or algorithm names and avoid inventing terms.
Examples: 'paper pertama BERT yang pretrain representasi dua arah' → ["BERT","bidirectional","transformers"]; 'gabung sensor RF dengan GPS' → ["RF","GPS","fusion"]."#;

pub(crate) fn validate_prompt(prompt: &str) -> Result<&str> {
    let prompt = prompt.trim();
    ensure!(!prompt.is_empty(), "--smart requires a research prompt");
    ensure!(
        prompt.len() <= 4096,
        "--smart prompt exceeds 4096 UTF-8 bytes"
    );
    Ok(prompt)
}

pub(crate) fn keywords(server: &LlamaServer, prompt: &str) -> Result<Vec<String>> {
    let words = complete(
        server,
        KEYWORD_PROMPT,
        prompt,
        "keywords",
        6,
        json!({
            "type": "string",
            "pattern": "^[A-Za-z0-9][A-Za-z0-9_-]{0,35}$"
        }),
    )?;
    validate_keywords(words)
}

fn validate_keywords(words: Vec<String>) -> Result<Vec<String>> {
    let mut valid = Vec::new();
    ensure!(
        (1..=6).contains(&words.len()),
        "model returned the wrong number of search keywords"
    );
    for word in words {
        ensure!(
            (1..=36).contains(&word.len())
                && word.as_bytes()[0].is_ascii_alphanumeric()
                && word
                    .bytes()
                    .all(|ch| ch.is_ascii_alphanumeric() || b"-_".contains(&ch)),
            "model returned an invalid search keyword"
        );
        if !valid
            .iter()
            .any(|old: &String| old.eq_ignore_ascii_case(&word))
        {
            valid.push(word);
        }
    }
    Ok(valid)
}

pub(crate) fn category_name(code: &str) -> Option<&'static str> {
    TAXONOMY
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .find_map(|(known, name)| (known == code).then_some(name))
}

pub(crate) fn categories(
    server: &LlamaServer,
    prompt: &str,
    keywords: &[String],
    candidates: &[&str],
) -> Result<Vec<String>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let options = candidates
        .iter()
        .filter_map(|code| category_name(code).map(|name| format!("{code}: {name}")))
        .collect::<Vec<_>>()
        .join("\n");
    let system = format!(
        "Choose 1 to 3 arXiv categories most relevant to the user's research question.         The user may write in Indonesian. Select only from the observed categories below.         Prefer direct topical matches and select only one if it covers the request.\n{options}"
    );
    let input = format!(
        "Research request: {}\nSearch keywords: {}",
        prompt.trim(),
        keywords.join(" ")
    );
    let codes = complete(
        server,
        &system,
        &input,
        "categories",
        candidates.len().min(3),
        json!({
            "type": "string", "enum": candidates
        }),
    )?;
    ensure!(
        (1..=3).contains(&codes.len()),
        "model returned the wrong number of categories"
    );
    let mut valid = Vec::new();
    for code in codes {
        ensure!(
            candidates.contains(&code.as_str()),
            "model returned an unknown arXiv category"
        );
        if !valid.contains(&code) {
            valid.push(code);
        }
    }
    Ok(valid)
}

fn complete(
    server: &LlamaServer,
    system: &str,
    input: &str,
    field: &str,
    max_items: usize,
    items: Value,
) -> Result<Vec<String>> {
    let request = json!({
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": input},
        ],
        "response_format": {"type": "json_schema", "json_schema": {
            "name": "arkhiva_search",
            "strict": true,
            "schema": {
                "type": "object",
                "properties": {
                    (field): {"type": "array", "minItems": 1, "maxItems": max_items, "items": items}
                },
                "required": [field],
                "additionalProperties": false,
            }
        }},
        "temperature": 0.0,
        "max_tokens": 128,
        "stream": false,
        "reasoning_effort": "none",
        "chat_template_kwargs": {"enable_thinking": false},
    });
    parse_reply(&server.chat(request)?, field, max_items)
}

fn parse_reply(reply: &Value, field: &str, max_items: usize) -> Result<Vec<String>> {
    let choice = &reply["choices"][0];
    ensure!(
        choice["finish_reason"] != "length",
        "local model reached its output limit"
    );
    let content = choice["message"]["content"]
        .as_str()
        .context("local model returned no search plan")?;
    let mut plan: serde_json::Map<String, Value> =
        serde_json::from_str(content).context("invalid model search plan JSON object")?;
    ensure!(plan.len() == 1, "unexpected fields in model search plan");
    let values: Vec<String> = serde_json::from_value(
        plan.remove(field)
            .context("missing model search plan field")?,
    )
    .context("invalid model search plan field")?;
    ensure!(
        (1..=max_items).contains(&values.len()),
        "model search plan array has the wrong length"
    );
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_keyword_anchors_and_recognizes_official_categories() -> Result<()> {
        let words = validate_keywords(vec![
            "AIS".into(),
            "ais".into(),
            "RF".into(),
            "low-rank".into(),
        ])?;
        assert_eq!(words, ["AIS", "RF", "low-rank"]);
        assert!(validate_keywords(vec!["AIS fusion".into()]).is_err());
        assert!(validate_keywords(vec![]).is_err());
        assert_eq!(category_name("eess.SP"), Some("Signal Processing"));
        assert!(category_name("cs.MADEUP").is_none());
        Ok(())
    }

    #[test]
    fn requires_the_requested_structured_json_field() -> Result<()> {
        let reply = |content, finish_reason| {
            json!({"choices":[{
                "finish_reason": finish_reason,
                "message": {"content": content}
            }]})
        };
        let response = reply(r#"{"categories":["eess.SP"]}"#, "stop");
        assert_eq!(parse_reply(&response, "categories", 1)?, ["eess.SP"]);
        assert!(parse_reply(&response, "keywords", 1).is_err());
        assert!(
            parse_reply(
                &reply(r#"{"categories":["eess.SP"],"extra":1}"#, "stop"),
                "categories",
                1
            )
            .is_err()
        );
        assert!(parse_reply(&reply(r#"{"categories":[]}"#, "stop"), "categories", 1).is_err());
        assert!(parse_reply(&reply("", "length"), "categories", 1).is_err());
        Ok(())
    }
}
