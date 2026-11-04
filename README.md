# comp_ml_project — film release, box office, festival, award and ratings data for 378,654 films

A snapshot of the NEON/ADP release-events dataset, assembled for a machine-learning
class project. Everything here is a **copy**; the live pipeline in
`~/NEON/ADP_Project/release_events/` is untouched and keeps running.

```
comp_ml_project/
  README.md                    this file
  data/csv/                    flattened exports, one CSV per table (load with pandas)
  data/db/release_events.db    the full SQLite database every CSV was exported from (~5 GB)
  code/                        the three scripts that built the database, their fixtures, README, notebook
  inputs/                      what the scripts read: the IMDb<->TMDb crosswalk, the IMDb metadata database
  docs/                        the pipeline handoff (what was built, lessons) and SNAPSHOT_INFO.txt (when, sweep state)
```


## Large files are not in this repo

GitHub refuses files over 100 MB, so six files are shared separately —
download link and where each one goes: see `data/LARGE_FILES.md`.

| file | size | why it matters |
|---|---|---|
| `data/csv/films.csv` | 327 MB | the spine: one row per film, every other file joins to it on `tmdb_id` |
| `data/csv/events.csv` | 114 MB | every TMDb release date |
| `data/csv/events_reconciled.csv` | 156 MB | release dates + Box Office Mojo grosses |
| `data/csv/01_events_tmdb_compat.csv` | 140 MB | the same in the comp engine's legacy layout |
| `data/db/release_events.db` | 4.7 GB | the full SQLite database (optional) |
| `inputs/imdb.db` | 1.8 GB | IMDb metadata input (optional) |

Everything else in `data/csv/` is in the repo and works on its own; you need
`films.csv` for most analyses and the two events files for anything date-based.

## How the data was made (short version)

The unit is a film identified by `tmdb_id` (TMDb) and `tconst` (IMDb); every file
joins on `tmdb_id`. The universe is the 378,654 films where an IMDb title and a TMDb
title were matched with confidence (`inputs/28_imdb_tmdb_crosswalk.csv`).

1. `code/release_events.py` pulled every release date TMDb holds for each film, in
   every country (1,104,049 dates), classified the free-text notes (festival name,
   platform, format, version), matched festival names to a 16,106-festival registry
   built from Wikidata, and derived per-country *windows* (premiere → festival →
   theatrical → digital → physical) and a one-row-per-film summary. It also joined
   IMDb metadata (`inputs/imdb.db`) and TMDb credits into `films.csv`.
2. `code/box_office.py` scraped Box Office Mojo for every film with a theatrical
   release anywhere since 2000 (153,908 in scope; 52,255 have a page): headline
   grosses, per-territory opening and total, US weekly runs, UK weekends, distributor,
   budget, rating. It then matched BOM dates to TMDb dates and produced a reconciled
   event list where BOM fills dates TMDb lacked.
3. `code/enrich.py` added awards and nominations (Wikidata, and a scrape of TMDb's
   awards pages), Rotten Tomatoes critic **and audience** scores (page scrape),
   OMDb ratings (Metacritic, IMDb), and distributors with territory and format
   (Wikidata).

Each script has an offline self-test (`python3 <script> selftest`) and a `report`
command. `code/README.md` has the full run order; `docs/` has the handoff with
results and every lesson learned.

## The files in `data/csv/` — grain and what they're for

| file | one row per | notes |
|---|---|---|
| `films.csv` | film | the spine: ids, title, year, runtime, genres, countries, languages, IMDb rating and votes, TMDb vote count and popularity, director/writer/cast from TMDb credits. Start here. |
| `film_summary.csv` | film | earliest date anywhere, first festival, first theatrical (+ country), US premiere / theatrical / digital / physical dates |
| `windows.csv` | film × country | the release path in that country: premiere, festival, theatrical (limited, wide, first), digital, physical, TV, days theatrical→digital and →physical; `first_theatrical_source` says whether the date came from TMDb or BOM |
| `events.csv` | release entry | every TMDb release date: country, type 1–6 (premiere, limited, theatrical, digital, physical, TV), date, raw note, normalised note, note class, certification |
| `events_reconciled.csv` | release entry | `events.csv` plus BOM: `source` (tmdb / bom), opening and gross in USD where matched, `match_status`, and the resolved festival for festival notes |
| `01_events_tmdb_compat.csv` | release entry | the same thing in the comp engine's legacy `01_events` column layout |
| `festival_appearances.csv` | film × festival × edition | de-duplicated festival appearances with the registry festival id, edition year, section, the entry types seen and the countries that reported it |
| `box_office_summary.csv` | film (with a BOM page) | domestic / international / worldwide gross, domestic opening and theater count, widest release, distributor, budget, MPAA rating, UK totals |
| `box_office_territory.csv` | film × release group × market | release date, opening, total gross per territory, plus lifetime rows |
| `weekly_domestic.csv` | film × week | the US weekly run: week, rank, gross, theaters, per-theater average, change |
| `weekend_uk.csv` | film × weekend | the UK weekend run |
| `box_office_event_matches.csv` | BOM territory row | which TMDb event it matched and the day gap |
| `awards.csv` | award / nomination | from both sources (`source` = wikidata or tmdb): body, ceremony or edition, year, category, won/nominated, award family (oscars, bafta, festival, critics …), festival id when the body is a festival |
| `ratings.csv` | film | RT critic score and review count, RT audience score and count, Certified Fresh / Verified Hot flags, Metacritic, IMDb rating; `source` columns say where each number came from. **Blank means not available, never zero.** |
| `distributors.csv` | film × distributor × territory × format | Wikidata and BOM distributors |
| `enrichment_coverage.csv` | source | how many films each enrichment source covered |
| `registry.csv` | festival | the 16,106-festival registry: label, aliases, country, usual month |
| `review_queue.csv`, `unlisted_festival_notes.csv` | note | festival notes the resolver was unsure about / couldn't match — useful as an NLP exercise |
| `coverage.csv` | stage | pipeline coverage counts |

`data/db/release_events.db` holds all of the above as tables plus the raw payloads
(compressed) and the run log. `sqlite3 data/db/release_events.db ".tables"` lists them;
`pandas.read_sql` works directly.

## Things to know before modelling

- **Missing is not zero.** A film with no BOM page has no gross, not a $0 gross; a
  film with fewer than five RT reviews has a review count but no score. Filter on
  presence explicitly.
- **Coverage is uneven by design.** Box office exists for ~15k films with US grosses
  and ~51k with any gross. Rotten Tomatoes pages were fetched for the 69k films that
  have an RT id on Wikidata. OMDb and TMDb awards were fetched in *priority order*
  (US theatrical releases first, most-voted first), so at snapshot time they cover
  the well-known end of the catalogue — see `docs/SNAPSHOT_INFO.txt` for how far the
  awards sweep had got. Treat "not yet fetched" differently from "fetched, none".
- **Leakage.** Anything that happens after release (grosses, audience scores,
  awards, review counts) is an outcome, not a feature, if you're predicting
  performance. Pre-release features are: metadata, credits, festival premiere and
  section, distributor, release pattern (limited vs wide), budget, rating, and
  critic reception where it precedes wide release.
- **Dates** are as TMDb/BOM report them (local, never timezone-shifted). A P585
  award date at year precision is stored as a year, not January 1st.
- **Festival resolution** is probabilistic: `festival_appearances.csv` holds rows
  the resolver accepted (score ≥ 80); the review queue holds the uncertain ones.
  About 8% of accepted appearances are "type-1 defects" (TMDb calling a festival
  screening a premiere) — fine for most purposes, worth knowing.
- **Terms of use.** Box Office Mojo, Rotten Tomatoes and TMDb's website were scraped
  politely (single connection, paused, cached) for internal research. Use this data
  for the class project; don't republish the raw scraped tables.

## Ideas that fit the data

Predicting US opening or total gross from pre-release signals (compare with the
Comp Atlas's LightGBM baseline: R² 0.71 acquisition-stage, 0.84 release-plan stage);
survival analysis of the theatrical→digital window; classifying which festival a
film will premiere at from metadata; whether a festival premiere converts to a US
release; critic vs audience score divergence; nearest-neighbour comp finding over
the credits/genre/festival features; sequence models over the weekly run shapes.
