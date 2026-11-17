use anyhow::{Context, Result, ensure};

pub const PROVIDER: &str = "arxiv";

pub fn arxiv_id(value: &str) -> Result<String> {
    let value = value.trim();
    let id = ["oai:arXiv.org:", "arXiv:", "arxiv:", "https://arxiv.org/abs/"]
        .iter()
        .find_map(|prefix| value.strip_prefix(prefix))
        .unwrap_or(value);
    let id = match id.rsplit_once('v') {
        Some((base, suffix)) if digits(suffix) => base,
        _ => id,
    };
    let valid = match id.split_once('/') {
        Some((archive, number)) => {
            !archive.is_empty()
                && archive
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
                && number.len() == 7
                && digits(number)
        }
        None => id.split_once('.').is_some_and(|(month, number)| {
            month.len() == 4 && digits(month) && (4..=5).contains(&number.len()) && digits(number)
        }),
    };
    ensure!(valid, "invalid arXiv ID {value:?}");
    Ok(id.to_owned())
}

pub fn arxiv_lookup_id(value: &str) -> Result<String> {
    if let Ok(id) = arxiv_id(value) {
        return Ok(id);
    }
    let url = reqwest::Url::parse(value.trim()).context("expected an arXiv ID or link")?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && matches!(
                url.host_str(),
                Some("arxiv.org" | "www.arxiv.org" | "export.arxiv.org")
            ),
        "expected an arXiv link"
    );
    if let Some(id) = url.path().strip_prefix("/abs/") {
        return arxiv_id(id);
    }
    let id = url
        .path()
        .strip_prefix("/pdf/")
        .context("expected an arXiv abstract or PDF link")?;
    arxiv_id(id.strip_suffix(".pdf").unwrap_or(id))
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

pub fn nonempty(text: &str) -> Option<String> {
    let text = text.trim();
    (!text.is_empty()).then(|| text.to_owned())
}

pub fn categories(text: &str) -> Vec<String> {
    text.split_whitespace().map(str::to_owned).collect()
}

pub fn arxiv_url(id: &str) -> String {
    format!("https://arxiv.org/abs/{id}")
}
