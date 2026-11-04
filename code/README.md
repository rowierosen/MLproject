# release_events — every TMDb release date, every country, every film

Scales `festival/Festival_Pipeline_Test.ipynb` (Parasite / Monster) to the whole
IMDb↔TMDb crosswalk (378,721 TMDb ids as of 2026-09-07).

```
release_events/
  release_events.py          the module: every stage is a function and a CLI sub-command
  Release_Events.ipynb       thin driver: run stages with limits, read the ledger, look at films
  fixtures/
    festival_seed.json       the 40 verified festivals + 35 edition windows from the test notebook
    tmdb_496243.json         real TMDb payload, Parasite  (63 countries, 138 entries)
    tmdb_1050035.json        real TMDb payload, Monster   (37 countries,  62 entries)
    wdqs_editions_sample.json  real Wikidata edition response (shape fixture for tests)
  release_events.db          created on first run (SQLite, WAL)
  out/                       CSV exports
```

## Run

```bash
export TMDB_API_KEY=...                      # v3 key; or TMDB_READ_TOKEN=... for a v4 bearer token
cd ADP_Project/release_events
python release_events.py selftest            # 158 checks, no key, no network — run this first
python release_events.py universe            # crosswalk -> films
python release_events.py fetch --limit 500   # read the ok/gone/error split and the rate, then
python release_events.py fetch               # everything: ~4–5 h at the default 25 rps, resumable
python release_events.py ledger              # raw -> events, prints the coverage ledger
python release_events.py registry --refresh  # Wikidata festival registry (~10–20 min, cached)
python release_events.py resolve             # festival resolution (+ edition windows from Wikidata)
python release_events.py windows             # per film x country windows and the film summary
python release_events.py export --compat     # CSVs to out/, incl. a 01_events-shaped file
python release_events.py report
```

Every stage is incremental and Ctrl-C safe. `fetch` retries `error` rows on the
next run and never re-asks for `ok` / `gone` rows unless `--refresh`, or unless
`changes --days N` (TMDb's `/movie/changes`) has marked them `stale`.

`--rps` and `--workers` tune the fetch (TMDb documents ~40–50 rps; 25 leaves
headroom and every worker pauses together on a 429). `--db` points at another
database; `ADP_PROJECT_DIR` overrides the project root, `RELEASE_EVENTS_DB` the
database path.

## Metadata and box office (added 2026-09-11)

```bash
python3 release_events.py credits            # /movie/{id}/credits for every film: ~4 h, resumable
python3 release_events.py metadata           # offline: TMDb payload + credits + imdb_match/imdb.db -> film_metadata
python3 release_events.py export             # now also writes out/films.csv

python3 box_office.py selftest               # 56 checks on real cached BOM pages + a fake BOM
python3 box_office.py scope                  # films with a TMDb theatrical entry (type 2/3) >= 2000; --us-only, --min-year
python3 box_office.py fetch --limit 200      # read it, then:
python3 box_office.py fetch                  # one request at a time, 1.5 s apart (--sleep); resumable
python3 box_office.py parse                  # offline, over the cached pages
python3 box_office.py match                  # BOM territory rows <-> TMDb theatrical events (ISO + +/-7 days)
python3 box_office.py reconcile              # events_bo: TMDb events + BOM opening/gross, BOM-only dates as source=bom; rebuilds windows
python3 box_office.py export                 # box_office_summary / territory / weekly_domestic / weekend_uk / event_matches
python3 box_office.py report
```

`box_office.py` tables: `bo_films` (scope + status `pending / ok / none / error`),
`bo_pages` (every fetched page, verbatim, compressed), `bo_summary` (headline
grosses, distributor, domestic opening + theaters, widest release, budget, MPAA,
UK block), `bo_territory` (per release group × market: release date, opening,
gross, BOM release id; plus `lifetime` rows), `bo_weekly_us`, `bo_weekend_uk`,
`bo_event_match`, and after `reconcile`, `events_bo` — every TMDb event with the
BOM opening/gross it matched (`match_status = matched_bom_gap<N>d`) plus one
`source='bom'` theatrical event per BOM market TMDb had no date for
(`bom_only`). Once `events_bo` exists, `windows` and the `01_events` compat
export read it instead of `events`; `windows.first_theatrical_source` says
whether a date came from TMDb or BOM. A film with no BOM page is `none` with
empty grosses — never zeros.
If BOM starts serving a robot check the run pauses five minutes, retries, and
stops itself after three in a row; nothing from a blocked response is cached.

## Enrichment: awards, ratings, distributors (added 2026-09-23)

```bash
python3 enrich.py selftest                  # 38 checks, offline
python3 enrich.py wikidata                  # P166 awards + P1411 nominations + P750 distributors + RT/Metacritic ids; ~1 h
export OMDB_API_KEY=...
python3 enrich.py omdb                      # 1,000 films per run (free tier); priority order; run daily
python3 enrich.py rt --dump tt6751668       # save ONE Rotten Tomatoes page to out/ and show what the parser reads -- do this first
nohup caffeinate -i python3 -u enrich.py rt > rt.log 2>&1 &                 # ~2.5 s per film with an RT id; days
pip3 install playwright curl_cffi && python3 -m playwright install chromium
python3 enrich.py tmdb_awards --probe tt6751668   # one film: shows what parsed, whether plain requests work once the browser primed the WAF token
nohup caffeinate -i python3 -u enrich.py tmdb_awards > awards.log 2>&1 &    # ~1.4 s per film when plain requests work; ~3 s browser-only
#   the awards page is server-rendered, but AWS WAF 403s a bare client (and python-requests even with the
#   token: the TLS handshake gives it away). The browser solves the challenge once, its cookies go into a
#   curl_cffi session that presents Chrome's TLS fingerprint, and plain requests run until the token expires
#   (then it primes again). If priming stops helping (5 in a row) the run switches itself to browser-only;
#   --browser forces that from the start.
python3 enrich.py export                    # awards.csv, ratings.csv, distributors.csv, enrichment_coverage.csv
python3 enrich.py report
```

Tables: `enrich_status` (per film: which sources have run and how they ended),
`wd_awards` (won/nominated, date at its true precision, edition, awarding body,
`fest_qid` when the body is a registry festival, `family`), `wd_film_ids`
(Wikidata / Rotten Tomatoes / Metacritic ids), `wd_distributors` (distributor
× place × format), `omdb_raw` + `omdb_ratings`, `rt_pages` + `rt_scores`
(critic + audience with counts and certified/verified flags),
`tmdb_awards_pages` + `tmdb_awards` (body, ceremony, category, Winner/Nominee).
`ratings.csv` prefers the RT page's critic score over OMDb's and says which
it used; a film with nothing has blanks plus the status columns, never zeros.

## What is stored

| table | grain | notes |
|---|---|---|
| `films` | one per `tmdb_id` | status `pending / ok / gone (404) / error / stale`, tconsts joined by `\|`, title, TMDb's own `imdb_id`, `wikidata_id`, entry counts |
| `raw_payloads` | one per fetched film | the **verbatim** response, zlib-compressed. Parsing is a pure function over this; a parser change re-runs `ledger`, never `fetch` |
| `events` | one per release entry | country, `type` 1–6, date (sliced, never timezone-converted), note raw + normalised + class (`festival / format / platform / version / admin / other / empty`), certification, language, descriptors |
| `festival_registry` | one per festival Q-id | label, aliases (all languages), country (P17→ISO), usual month (P2922), sections, editions with P580/P582 windows |
| `resolutions` | one per noted event | festival, score, band `accept / review / reject`, edition, section, **evidence string, reason** |
| `appearances` | one per (film, festival, edition) | the dedup of accepted rows across countries and languages; `min_type`, types, countries, notes |
| `windows` | one per (film, country) | premiere, festival, theatrical limited/wide/first, digital, physical, TV, days theatrical→digital / →physical |
| `film_summary` | one per film | earliest date anywhere, first festival, first theatrical (+ country), US premiere/theatrical/digital/physical |
| `runs` | one per stage invocation | counts and outcome, including interruptions |

## Principles

- **No data is never zero.** never fetched / TMDb 404 / fetched with no dates are three states.
- **Nothing is silently dropped.** Every noted event has a resolution row with a reason; the
  festival-shaped tail that matched nothing is exported by frequency as the promotion list.
- **Keys never touch disk.** Read from the environment only; `redact()` on every error path.
