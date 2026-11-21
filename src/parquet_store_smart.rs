//! Runs a grounded `--smart` plan: each term searched on its own, papers tiered by how they
//! were found, constraints applied as a bounded bonus on full abstracts, never as a filter.
use super::super::lexicon;
use super::{SearchArgs, SearchHit};
use crate::parquet_store::ParquetStore;
use crate::smart_search::{
    self, Grounded, Plan, SPECIFIC_DF, binds, contains, phrase, sentences, similar, words,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    num::NonZeroUsize,
};

/// Longest phrase the lexicon counts.
const LEXICON_WORDS: usize = 3;
const PER_QUERY: usize = 50;
const TITLE_HITS_PER_GUESS: usize = 3;
const TITLE_SIMILARITY: f64 = 0.6;
const CONSTRAINT_BONUS: f64 = 0.5;
const MAX_BONUS: f64 = 2.0;
const AVOID_PENALTY: f64 = 0.5;
/// A broad term ("environmental noise") also matches other fields (LIGO detectors). A paper
/// whose field group holds under this share of the leading results' weight is demoted.
const FIELD_SHARE: f64 = 0.15;
const FIELD_WINDOW: usize = 50;
const OFF_FIELD_PENALTY: f64 = 0.5;

#[derive(Serialize)]
pub(super) struct SmartHit {
    #[serde(flatten)]
    hit: SearchHit,
    tier: &'static str,
    matched_terms: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    constraints: BTreeMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    avoid_hit: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    off_field: bool,
}

#[derive(Serialize)]
pub(super) struct SmartReport {
    plan: Plan,
    grounded: Grounded,
    total: usize,
    contextual: usize,
    hits: Vec<SmartHit>,
}

struct Candidate {
    hit: SearchHit,
    text: String,
    sentences: Vec<String>,
    matched: Vec<String>,
    facets: BTreeSet<usize>,
    score: f64,
    contextual: bool,
    via_constraint: bool,
    evidence: BTreeMap<String, Option<String>>,
    avoided: Option<String>,
    off_field: bool,
}

impl Candidate {
    fn new(hit: SearchHit) -> Self {
        let title = hit.title.clone().unwrap_or_default();
        let body = hit.abstract_text.clone().unwrap_or_default();
        Self {
            text: phrase(&format!("{title} {body}")),
            sentences: sentences(&title, &body),
            hit,
            matched: Vec::new(),
            facets: BTreeSet::new(),
            score: 0.0,
            contextual: false,
            via_constraint: false,
            evidence: BTreeMap::new(),
            avoided: None,
            off_field: false,
        }
    }

    fn met(&self) -> usize {
        self.evidence.values().filter(|term| term.is_some()).count()
    }

    /// Topic relevance stays primary; constraints are a capped bonus and an avoid phrase a
    /// penalty (training-free soft-constraint reranking, as in SoFT and CoDeR).
    fn rank_score(&self) -> f64 {
        let bonus = (1.0 + CONSTRAINT_BONUS * self.met() as f64).min(MAX_BONUS);
        let penalty = if self.avoided.is_some() {
            AVOID_PENALTY
        } else {
            1.0
        };
        let field = if self.off_field {
            OFF_FIELD_PENALTY
        } else {
            1.0
        };
        self.score * bonus * penalty * field
    }
}

#[derive(PartialEq)]
enum Kind {
    Facet,
    Expansion,
    Constraint,
}

impl ParquetStore {
    /// Opens the phrase lexicon read-only and returns a cached document-frequency lookup.
    pub(in crate::parquet_store) fn lexicon_df(&self) -> Result<impl FnMut(&str) -> Result<i64>> {
        let path = self.directory.join(lexicon::DB);
        let conn = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| {
                format!(
                    "--smart needs {}; build it with arkhiva parquet lexicon",
                    path.display()
                )
            })?;
        let mut cache: HashMap<String, i64> = HashMap::new();
        Ok(move |phrase: &str| {
            if let Some(&count) = cache.get(phrase) {
                return Ok(count);
            }
            let lookup = |key: &str| -> Result<i64> {
                Ok(conn
                    .query_row(
                        "SELECT doc_freq FROM lexicon WHERE phrase=?1",
                        [key],
                        |row| row.get(0),
                    )
                    .optional()?
                    .unwrap_or(0))
            };
            // The lexicon stops at three words and at stopword edges. A longer phrase appears
            // in at most as many papers as its rarest stored piece ("you only look once").
            let words: Vec<&str> = phrase.split(' ').collect();
            let count = if words.len() > LEXICON_WORDS {
                let mut least = 0;
                for size in 1..=LEXICON_WORDS {
                    for piece in words.windows(size) {
                        if lexicon::boundary(piece[0]) || lexicon::boundary(piece[size - 1]) {
                            continue;
                        }
                        let count = lookup(&piece.join(" "))?;
                        least = if least == 0 { count } else { least.min(count) };
                        if count == 0 {
                            return Ok(0);
                        }
                    }
                }
                least
            } else {
                lookup(phrase)?
            };
            cache.insert(phrase.to_owned(), count);
            Ok(count)
        })
    }

    /// One plain AND search with the user's filters; every hit carries its full abstract.
    fn plain_search(&self, args: &SearchArgs, query: &str, limit: usize) -> Result<Vec<SearchHit>> {
        let mut args = args.clone();
        args.smart = false;
        args.limit = NonZeroUsize::new(limit).context("search limit must be nonzero")?;
        let tag = args.validated_tag()?;
        let ranked = self.rank_hits(&args, Some(query), &[], tag.as_deref())?;
        self.hydrate_hits(&ranked, limit)
    }

    pub(super) fn search_planned(&self, args: &SearchArgs, plan: Plan) -> Result<SmartReport> {
        let mut df = self.lexicon_df()?;
        let grounded = smart_search::ground(&plan, &mut df)?;
        let anchors = &grounded.anchors;

        // Known item: a guessed title counts only if a real title closely matches it.
        let mut title_hits: Vec<SearchHit> = Vec::new();
        if plan.intent == "known" {
            for guess in plan.titles.iter().take(3) {
                for hit in self.plain_search(args, guess, TITLE_HITS_PER_GUESS)? {
                    let close = similar(hit.title.as_deref().unwrap_or_default(), guess)
                        >= TITLE_SIMILARITY;
                    if close && !title_hits.iter().any(|old| old.id == hit.id) {
                        title_hits.push(hit);
                    }
                }
            }
        }

        let mut papers: HashMap<String, Candidate> = HashMap::new();
        let mut found: Vec<String> = Vec::new(); // insertion order, for the expansion tier
        let topic: Vec<String> = grounded
            .facets
            .iter()
            .flat_map(|facet| facet.terms.iter().cloned())
            .collect();
        let passes = [
            (Kind::Facet, &grounded.facets),
            (Kind::Expansion, &grounded.expansions),
            (Kind::Constraint, &grounded.constraints),
        ];
        for (kind, groups) in passes {
            for (group_index, group) in groups.iter().enumerate() {
                for term in &group.terms {
                    let term_df = df(term)?;
                    if kind == Kind::Constraint && term_df > smart_search::MAX_DF {
                        continue; // too common to search; it still counts as evidence
                    }
                    // Anchored queries are "contextual"; the bare term is "extended".
                    let queries: Vec<(String, bool)> = if anchors.is_empty()
                        || words(term).iter().any(|word| anchors.contains(word))
                    {
                        vec![(term.clone(), true)]
                    } else {
                        anchors
                            .iter()
                            .map(|anchor| (format!("{term} {anchor}"), true))
                            .chain([(term.clone(), false)])
                            .collect()
                    };
                    let mut seen = BTreeSet::new();
                    for (query, anchored) in queries {
                        for (rank, hit) in self
                            .plain_search(args, &query, PER_QUERY)?
                            .into_iter()
                            .enumerate()
                        {
                            if !seen.insert(hit.id.clone()) {
                                continue;
                            }
                            let rank_weight = 1.0 / (10.0 + rank as f64);
                            let id = hit.id.clone();
                            if kind == Kind::Constraint {
                                // A paper found via a constraint joins only if the constraint
                                // binds to the topic in one sentence and it names a facet term.
                                let candidate = Candidate::new(hit);
                                if !anchored || !binds(&candidate.sentences, term, &topic, anchors)
                                {
                                    continue;
                                }
                                for (facet_index, facet) in grounded.facets.iter().enumerate() {
                                    let named: Vec<&String> = facet
                                        .terms
                                        .iter()
                                        .filter(|t| contains(&candidate.text, t))
                                        .collect();
                                    if named.is_empty() {
                                        continue;
                                    }
                                    let entry = papers.entry(id.clone()).or_insert_with(|| {
                                        found.push(id.clone());
                                        let mut fresh = Candidate::new(candidate.hit.clone());
                                        fresh.via_constraint = true;
                                        fresh
                                    });
                                    entry.contextual = true;
                                    for t in named {
                                        if !entry.matched.contains(t) {
                                            entry.matched.push(t.clone());
                                        }
                                    }
                                    entry.facets.insert(facet_index);
                                    entry.score += rank_weight;
                                }
                                continue;
                            }
                            let entry = papers.entry(id.clone()).or_insert_with(|| {
                                found.push(id.clone());
                                Candidate::new(hit)
                            });
                            // The index has no word positions: check phrase order in the text.
                            let weight = if term.contains(' ') && !contains(&entry.text, term) {
                                0.25
                            } else if contains(
                                &phrase(entry.hit.title.as_deref().unwrap_or_default()),
                                term,
                            ) {
                                2.0
                            } else {
                                1.0
                            };
                            let on_topic =
                                anchored || on_topic_by_rarity(term_df, &entry.text, anchors);
                            entry.contextual |= on_topic;
                            entry.matched.push(term.clone());
                            if kind == Kind::Facet {
                                entry.facets.insert(group_index);
                                entry.score +=
                                    weight * rank_weight * if on_topic { 1.0 } else { 0.3 };
                            }
                        }
                    }
                }
            }
        }

        for candidate in papers.values_mut() {
            candidate.evidence = grounded
                .constraints
                .iter()
                .map(|group| {
                    let term = group
                        .terms
                        .iter()
                        .find(|term| binds(&candidate.sentences, term, &topic, anchors))
                        .cloned();
                    (group.name.clone(), term)
                })
                .collect();
            candidate.avoided = grounded
                .avoid
                .iter()
                .find(|phrase| contains(&candidate.text, phrase))
                .cloned();
        }

        let first: Vec<String> = title_hits.iter().map(|hit| hit.id.clone()).collect();
        let facet_count = grounded.facets.len();
        let contextual_count = found
            .iter()
            .filter(|id| !papers[*id].facets.is_empty() && papers[*id].contextual)
            .count();
        let mut order = ordered(&papers, &found, &first, facet_count);
        mark_off_field(&mut papers, &order);
        order = ordered(&papers, &found, &first, facet_count);

        let total = order.len();
        let shown = if args.all {
            total
        } else {
            args.limit.get().min(total)
        };
        let mut title_lookup: HashMap<String, SearchHit> = title_hits
            .into_iter()
            .map(|hit| (hit.id.clone(), hit))
            .collect();
        let mut hits = Vec::with_capacity(shown);
        for id in order.into_iter().take(shown) {
            let is_title = first.contains(&id);
            let mut hit = match papers.remove(&id) {
                Some(paper) => {
                    let tier = if is_title {
                        "title"
                    } else if paper.facets.is_empty() {
                        "expansion"
                    } else if paper.via_constraint {
                        "constraint"
                    } else if paper.contextual {
                        "contextual"
                    } else {
                        "extended"
                    };
                    SmartHit {
                        hit: paper.hit,
                        tier,
                        matched_terms: paper.matched,
                        constraints: paper.evidence,
                        avoid_hit: paper.avoided,
                        off_field: paper.off_field,
                    }
                }
                None => SmartHit {
                    hit: title_lookup.remove(&id).context("missing title hit")?,
                    tier: "title",
                    matched_terms: Vec::new(),
                    constraints: BTreeMap::new(),
                    avoid_hit: None,
                    off_field: false,
                },
            };
            // Ranking read every abstract; output still honors --snippets.
            if hits.len() >= args.snippets {
                hit.hit.snippet = None;
            }
            hits.push(hit);
        }
        Ok(SmartReport {
            plan,
            grounded,
            total,
            contextual: contextual_count,
            hits,
        })
    }
}

/// A rare term found without its anchor is on topic only if the paper still names an anchor:
/// "data encoding" (under 1,000 papers) otherwise pulls quantum encoding into compression.
fn on_topic_by_rarity(term_df: i64, text: &str, anchors: &[String]) -> bool {
    term_df <= SPECIFIC_DF
        && (anchors.is_empty() || anchors.iter().any(|anchor| contains(text, anchor)))
}

/// Tiers: title hits, then contextual papers, then the rest with a facet, then expansions.
/// Inside a tier, papers covering more facets lead; single-facet papers alternate by facet.
fn ordered(
    papers: &HashMap<String, Candidate>,
    found: &[String],
    first: &[String],
    facet_count: usize,
) -> Vec<String> {
    let by_score = |left: &&String, right: &&String| {
        papers[*right]
            .rank_score()
            .partial_cmp(&papers[*left].rank_score())
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.cmp(right))
    };
    let layer = |ids: Vec<&String>| -> Vec<String> {
        let mut multi: Vec<&String> = ids
            .iter()
            .copied()
            .filter(|id| papers[*id].facets.len() > 1)
            .collect();
        multi.sort_by(|left, right| {
            papers[*right]
                .facets
                .len()
                .cmp(&papers[*left].facets.len())
                .then_with(|| by_score(left, right))
        });
        let mut queues: Vec<Vec<&String>> = (0..facet_count)
            .map(|facet| {
                let mut queue: Vec<&String> = ids
                    .iter()
                    .copied()
                    .filter(|id| {
                        let facets = &papers[*id].facets;
                        facets.len() == 1 && facets.contains(&facet)
                    })
                    .collect();
                queue.sort_by(by_score);
                queue.reverse(); // pop from the end
                queue
            })
            .collect();
        let mut order: Vec<String> = multi.into_iter().cloned().collect();
        while queues.iter().any(|queue| !queue.is_empty()) {
            for queue in &mut queues {
                if let Some(id) = queue.pop() {
                    order.push(id.clone());
                }
            }
        }
        order
    };
    let core = |contextual: bool| -> Vec<&String> {
        found
            .iter()
            .filter(|id| {
                let paper = &papers[*id];
                !paper.facets.is_empty() && paper.contextual == contextual
            })
            .collect()
    };
    let mut order = first.to_vec();
    for id in layer(core(true))
        .into_iter()
        .chain(layer(core(false)))
        .chain(
            found
                .iter()
                .filter(|id| papers[*id].facets.is_empty())
                .cloned(),
        )
    {
        if !first.contains(&id) {
            order.push(id);
        }
    }
    order
}

/// arXiv cross-lists cs, eess and stat so heavily that they act as one field here; other
/// archives (physics.*, astro-ph, quant-ph, ...) are their own.
fn field(category: &str) -> &str {
    let archive = category.split('.').next().unwrap_or(category);
    match archive {
        "cs" | "eess" | "stat" => "cs",
        other => other,
    }
}

/// Flags papers whose fields carry little of the weight of the leading results.
fn mark_off_field(papers: &mut HashMap<String, Candidate>, order: &[String]) {
    let fields_of = |paper: &Candidate| -> BTreeSet<String> {
        paper
            .hit
            .categories
            .iter()
            .map(|c| field(c).to_owned())
            .collect()
    };
    let mut mass: HashMap<String, f64> = HashMap::new();
    let leading = order
        .iter()
        .filter_map(|id| papers.get(id))
        .take(FIELD_WINDOW);
    for (rank, paper) in leading.enumerate() {
        let fields = fields_of(paper);
        for name in &fields {
            *mass.entry(name.clone()).or_default() +=
                1.0 / (10.0 + rank as f64) / fields.len() as f64;
        }
    }
    let total: f64 = mass.values().sum();
    if total == 0.0 {
        return;
    }
    for paper in papers.values_mut() {
        let share = fields_of(paper)
            .iter()
            .map(|name| mass.get(name).copied().unwrap_or(0.0) / total)
            .fold(0.0, f64::max);
        paper.off_field = share < FIELD_SHARE;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rare_term_needs_an_anchor_in_the_paper() {
        let anchors = ["compression".to_owned()];
        let quantum = phrase("Quantum topological data encoding for variational circuits");
        let lz = phrase("Fast data encoding: a Lempel-Ziv compression scheme");
        assert!(!on_topic_by_rarity(400, &quantum, &anchors));
        assert!(on_topic_by_rarity(400, &lz, &anchors));
        assert!(!on_topic_by_rarity(SPECIFIC_DF + 1, &lz, &anchors));
        assert!(on_topic_by_rarity(400, &quantum, &[]));
    }

    #[test]
    fn groups_cross_listed_archives_into_one_field() {
        assert_eq!(field("cs.CL"), "cs");
        assert_eq!(field("eess.AS"), "cs");
        assert_eq!(field("stat.ME"), "cs");
        assert_eq!(field("astro-ph.IM"), "astro-ph");
        assert_eq!(field("quant-ph"), "quant-ph");
    }
}
