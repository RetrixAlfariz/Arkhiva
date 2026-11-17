use super::ParquetStore;
use anyhow::{Context, Result, ensure};
use chrono::NaiveDate;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub(super) struct TopicPlan {
    pub(super) keywords: Vec<String>,
    pub(super) preferred_categories: Vec<String>,
    pub(super) category: Option<String>,
    pub(super) tag: Option<String>,
    pub(super) from: Option<NaiveDate>,
    pub(super) to: Option<NaiveDate>,
}

#[derive(Serialize)]
pub struct SearchTopic {
    pub(super) name: String,
    pub(super) prompt: String,
    #[serde(flatten)]
    pub(super) plan: TopicPlan,
}

impl ParquetStore {
    pub(super) fn save_topic(&self, name: &str, prompt: &str, plan: &TopicPlan) -> Result<()> {
        self.init_topics()?;
        self.index.conn.execute(
            "INSERT INTO search_topics(name, prompt, plan_json) VALUES (?1, ?2, ?3)
            ON CONFLICT(name) DO UPDATE SET prompt=excluded.prompt, plan_json=excluded.plan_json",
            params![name, prompt, serde_json::to_string(plan)?],
        )?;
        Ok(())
    }

    pub(super) fn topic(&self, name: &str) -> Result<SearchTopic> {
        self.init_topics()?;
        let (prompt, plan): (String, String) = self
            .index
            .conn
            .query_row(
                "SELECT prompt, plan_json FROM search_topics WHERE name=?1",
                [name],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .with_context(|| format!("unknown topic {name:?}; use arkhiva topic list"))?;
        Ok(SearchTopic {
            name: name.to_owned(),
            prompt,
            plan: serde_json::from_str(&plan)?,
        })
    }

    pub fn list_topics(&self) -> Result<Vec<SearchTopic>> {
        self.init_topics()?;
        let mut statement = self
            .index
            .conn
            .prepare("SELECT name, prompt, plan_json FROM search_topics ORDER BY name")?;
        let mut rows = statement.query([])?;
        let mut topics = Vec::new();
        while let Some(row) = rows.next()? {
            topics.push(SearchTopic {
                name: row.get(0)?,
                prompt: row.get(1)?,
                plan: serde_json::from_str(&row.get::<_, String>(2)?)?,
            });
        }
        Ok(topics)
    }

    pub fn remove_topic(&self, name: &str) -> Result<serde_json::Value> {
        let name = normalize_topic(name)?;
        self.init_topics()?;
        let changed = self
            .index
            .conn
            .execute("DELETE FROM search_topics WHERE name=?1", [&name])?;
        Ok(serde_json::json!({"topic": name, "changed": changed != 0}))
    }

    fn init_topics(&self) -> Result<()> {
        // This additive table leaves the existing corpus schema readable by benchmark binaries.
        self.index
            .conn
            .execute_batch(include_str!("../migrations/search_topics.sql"))?;
        Ok(())
    }
}

pub(super) fn normalize_topic(name: &str) -> Result<String> {
    let name = name.trim();
    ensure!(
        !name.is_empty() && !name.chars().any(char::is_control),
        "topic must have a nonempty name without control characters"
    );
    Ok(name.to_lowercase())
}
