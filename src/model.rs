use chrono::NaiveDate;

#[derive(Debug)]
pub struct Paper {
    pub id: String,
    pub url: String,
    pub title: Option<String>,
    pub abstract_text: Option<String>,
    pub categories: Vec<String>,
    pub first_submitted: Option<NaiveDate>,
    pub last_revised: Option<NaiveDate>,
    pub metadata_updated: Option<NaiveDate>,
}

#[derive(Debug)]
pub enum Change {
    Upsert(Paper),
    Delete { id: String, url: String },
}

pub fn version_date(text: &str) -> Option<NaiveDate> {
    let text = text.trim();
    chrono::DateTime::parse_from_rfc2822(text)
        .map(|date| date.date_naive())
        .or_else(|_| NaiveDate::parse_from_str(text, "%Y-%m-%d"))
        .ok()
}
