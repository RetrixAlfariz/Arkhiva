# Changelog

All notable changes to Arkhiva are listed here. Versions follow [Semantic Versioning](https://semver.org); while Arkhiva is below 1.0, a minor version (0.x.0) may change commands, file formats or the corpus index.

## 0.2.0 — 2026-11-20

Highlights: smart search now plans the search with a model and checks every term against your corpus; choose a cloud API or a local model; teach Arkhiva your own words with kamus and codec.

### Added

- **Planned smart search.** `arkhiva search --smart` asks a model for one search plan (intent, guessed titles, anchors, facets, constraints, avoid phrases), then searches the corpus itself. Prompts can be in Indonesian, English, or both.
  - Facet terms are kept only if the phrase lexicon finds them in 5 to 20,000 papers, so invented terms never drive results. Guessed titles count only when a real title shares at least 60% of their words.
  - Results are ordered by tier: `title`, `contextual`, `constraint`, `extended`, `expansion`. Papers covering more facets lead.
  - Constraints never filter. They add a bonus, capped at double, only when they appear in the same sentence as a facet term or anchor.
  - Papers naming an avoid phrase, or from a field that barely appears among the leading results, keep half their score.
  - Ranking reads every abstract, but `--snippets` still limits how many hits show one.
- **Cloud or local model.** `arkhiva-llama.json` accepts `"llm": "api"` or `"llm": "local"`, plus `api_url`, `api_model`, `api_reasoning` and `api_temperature` for any OpenAI-compatible API. `ARKHIVA_LLM_*` environment variables override the file. The API key is read only from `ARKHIVA_LLM_KEY` or `OPENAI_API_KEY`; an `api_key` in the file is rejected. Because the key is sent as a bearer token, the API URL must use `https`; plain `http` is accepted only for `localhost`, `127.0.0.1` and `[::1]`. Only the plan instructions, the prompt and matching glossary entries are sent; the corpus and results stay local.
- **Kamus and codec glossaries**, stored in the corpus `index.sqlite`:
  - `arkhiva import kamus|codec <file.json>` (all-or-nothing; returns imported and replaced counts), `arkhiva list kamus|codec`, `arkhiva remove kamus|codec <word>`.
  - A kamus entry maps a word to English terms. Matching entries are always searched as an extra facet, and import warns about terms that are missing from the corpus or too broad.
  - A codec entry maps a word or phrase to a definition that only guides the model.
  - Keys written entirely in capitals (`YOLO`, `IT`) are acronyms and match case-sensitively.
  - Examples: `docs/examples/kamus.json`, `docs/examples/codec.json`.
- **Saved smart topics.** `--save-topic` stores the plan, and `--topic` replays it without calling the model. Topics saved by 0.1.0 still load.
- Searches open the corpus index with a shared lock, so several can run at once. Writers wait briefly for readers.

### Changed

- Phrases longer than three words get a corpus frequency from their rarest stored piece, instead of being reported as missing.
- A rare facet term found without an anchor counts as contextual only if the paper names an anchor. This stops terms such as "data encoding" pulling unrelated fields into results.
- README rewritten as a project overview with install and usage; `CHANGELOG.md` added.

### Upgrading from 0.1.0

- No migration step: the kamus and codec tables are created in `index.sqlite` on first use.
- Smart search needs the phrase lexicon. If `lexicon-v2.sqlite` is missing, smart search stops before calling the model and asks you to run `arkhiva parquet lexicon`.
- To keep using a local model while `api_url` is set, add `"llm": "local"` to `arkhiva-llama.json`.

### Removed

- The keyword and category smart search, and `src/arxiv_categories.tsv`.

### Known limitations

- Off-topic papers from the same field can still rank high, such as KV-cache compression in a file-compression search.
- The model sometimes puts a needed concept in the avoid list.
- Small local models can fail to produce a usable plan: a 0.8B model failed in testing, and 9B models worked.

## 0.1.0

- First release: offline arXiv metadata corpus (Parquet, SQLite, Tantivy), search with category, date and tag filters, keyword-based prompt search, saved topics, and OAI-PMH sync.
