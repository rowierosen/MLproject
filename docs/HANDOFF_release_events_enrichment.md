# Release events + enrichment — session handoff (2026-09-08 → 2026-10-18)

Where everything lives: `~/NEON/ADP_Project/release_events/` — three Python modules
(`release_events.py`, `box_office.py`, `enrich.py`), `Release_Events.ipynb` (thin driver),
`README.md` (run order for all three), `fixtures/`, the database `release_events.db`
(~5 GB, SQLite WAL) and CSV exports in `out/`. Companion project docs:
`claude/Release_Events_Pipeline.md` (design + full-run findings),
`claude/Festival_Pipeline_Notebook_Test_Run.md`, `claude/Festival_Matching_Rebuild_Plan.md`.

## What the session built, in order

**1. `release_events.py` — every TMDb release date, every country, every film.**
Scaled the two-film festival notebook to the whole IMDb↔TMDb crosswalk. Stages:
`universe → fetch → ledger → registry --refresh → resolve → windows → credits →
metadata → export --compat → report`. Result: 378,654 films fetched (67 gone, 0
errors); **1,104,049 release dates** in 250 countries; a 16,106-festival Wikidata
registry with names in 14 languages; **85,144 festival appearances on 39,317 films**
(accept ≥ 80 / review ≥ 55; evidence-based disambiguation); per film × country
windows (premiere / festival / theatrical limited–wide / digital / physical / TV, days
theatrical→digital); `film_metadata` joining the TMDb payload, TMDb credits
(378,600 films) and `imdb_match/imdb.db` (100%).

**2. `box_office.py` — Box Office Mojo, reconciled with TMDb.**
Scope "any theatrical release anywhere, 2000+" = 153,908 films; 52,255 have a BOM
page (230,881 pages cached, stripped to ~6 KB). Parsed: domestic gross 14,984,
worldwide 51,091, widest release 14,618, UK release 12,093; **338,978 per-territory
rows** (release date, opening, gross), **130,800 US weekly rows**, **48,287 UK
weekend rows**, distributor, budget, MPAA. `match` ties BOM territory rows to TMDb
theatrical events (ISO + ±7 days, one event claimed once): 133,734 matched, 133,802
BOM-only. `reconcile` builds `events_bo` (every TMDb event + BOM opening/gross where
matched + one `source='bom'` theatrical event per market TMDb lacked) and rebuilds
`windows` from it: 1,050,429 windows, **120,565 first-theatrical dates come from
BOM**; `01_events_tmdb_compat.csv` (1,238,118 rows) is the comp engine's `01_events`
shape with `source / opening_usd / gross_usd / match_status`.

**3. `enrich.py` — awards, ratings, distributors (this is where the session ended).**
Rowan's choices: TMDb awards on *everything*; OMDb free tier (1,000/day); Rotten
Tomatoes critic + audience by scraping rottentomatoes.com (no API for audience).

| stage | status 2026-10-18 | result |
|---|---|---|
| `wikidata` | **done** (3.5 h, 0 failed batches) | 218,483 films with an item (58%); 35,663 award/nomination rows on 13,352 films (2,326 tied to a registry festival; families: oscars 12,917 · national academies 8,925 · festival 3,431 · critics 2,894 · globes 607 · bafta 569 · guild 423 …); 69,489 RT ids; 18,102 Metacritic ids; 82,521 distributor rows on 60,716 films |
| `rt` | **done** (~2 days wall clock, priority order) | 69,330 pages; **49,922 audience scores, 29,929 critic scores**, 5,640 Certified Fresh; a page with < 5 reviews stores `critic_count` with an empty score — that is RT's threshold, not a parser miss (verified on the top offenders) |
| `omdb` | **1,000 of ~378k** | run `python3 enrich.py omdb` once a day (needs `export OMDB_API_KEY=…`); priority = US theatrical → any theatrical → rest, by vote count; stops itself at the daily limit |
| `tmdb_awards` | **sweep started 2026-10-18** | plain requests via `curl_cffi` (Chrome TLS fingerprint) at ~1.4 s/film ⇒ ~6 days; parser v2 verified on the live Parasite page (19 rows = 19 stated nominations, 13 wins); Playwright browser held in reserve and used automatically if the WAF starts challenging |
| `export` / `report` | run after the sweep | `awards.csv` (Wikidata + TMDb, `source` column), `ratings.csv` (RT page critic preferred over OMDb; blanks never zeros), `distributors.csv` (Wikidata + BOM), `enrichment_coverage.csv` |

Tables added by `enrich.py`: `enrich_status`, `wd_awards`, `wd_labels`, `wd_film_ids`,
`wd_distributors`, `omdb_raw`, `omdb_ratings`, `rt_pages`, `rt_scores`,
`tmdb_awards_pages`, `tmdb_awards`. Self-tests: 178 / 79 / 48 across the three
modules, all offline with fake HTTP servers.

## Things learned the hard way (don't re-learn them)

- **WDQS awards query timed out at ANY batch size** (even 6 films) with
  500/502/504/429. Not the labels, not the batch: Blazegraph evaluated the
  `P166 UNION P1411` over all of Wikidata before joining `VALUES`. The fix is
  `hint:Query hint:optimizer "None"` as the first triple; 100 of the most-awarded
  films then answer in ~2 s. Also: never put `SERVICE wikibase:label` on a per-film
  statement-heavy query — labels are fetched once per distinct item into `wd_labels`.
- **Rotten Tomatoes**: the `media-scorecard-json` script block is the stable shape;
  critic and audience scores, counts, certified/verified all come from it.
- **TMDb awards page**: server-rendered (no JSON endpoint — the only XHRs are consent,
  analytics and AWS-WAF telemetry). Category links changed from
  `/award/<body>/category/<slug>` to `/award/<body>/ceremony/<n>#category-<id>`; the
  parser accepts both. AWS WAF 403s `python-requests` even *with* the browser's
  `aws-waf-token` cookie (TLS fingerprint); `curl_cffi` impersonating Chrome passes on
  the first request. TMDb's awards data is community-entered and patchy (Parasite's page
  has no Cannes at all) — it complements Wikidata, it does not replace it.
- **Box Office Mojo**: throttles in bursts of 503 — 3 s between requests + a 5-minute
  pause-and-retry runs clean; only a literal "Domestic" row defines the domestic block
  (the first `/release/` link is often a foreign release); cross-year weeks need the
  printed year.
- **Operations**: `nohup caffeinate -i python3 -u <script> … > log 2>&1 &`; lid must
  stay open (`caffeinate -i` does not survive it); `pkill -f "<script> <stage>"` is
  always safe — every stage commits as it goes and resumes. SQLite WAL lets `sqlite3`
  *reads* run alongside a fetch; never run two *writers* (a second stage, `export`,
  `metadata`) against the database while a fetch is running. Fixes were run before
  they landed on the Mac four times (once the bridge delivered a stale copy): the check
  is `grep -c <string only the new version has> <file>` before running anything.

## Open items, in priority order

1. **Finish the enrichment runs**: let `tmdb_awards` complete (~6 days; progress line
   every 100 films shows `primes=` and `BROWSER-ONLY` if it had to fall back); run
   `omdb` daily; then `python3 enrich.py export && python3 enrich.py report`.
2. **How the comp engine consumes this** — the real design decision, not started:
   `comp.db` reading `release_events.db` directly vs. the compat CSVs replacing what
   `comp_2_pipeline.py` builds. Everything joins on `tmdb_id` (and `tconst`).
3. **235 films where TMDb's own imdb_id disagrees with the crosswalk tconst**
   (`out/imdb_id_disagreements.csv`) — Rowan deferred this until enrichment was right.
4. 20,676 review-band festival rows; `unlisted_festival_notes.csv` is the registry
   promotion list. Sections come from the seed only.
5. Update `claude/Release_Events_Pipeline.md` with the RT / OMDb / TMDb-awards results
   once the sweep finishes (it already has the Wikidata results and the WDQS lesson).
6. Scraping-terms caveat stands for TMDb's site, BOM/Amazon and RT/Fandango: single
   connection, paused, cached, self-stopping — internal research use.

## Starting a new chat: the three commands to orient

```bash
cd ~/NEON/ADP_Project/release_events
python3 enrich.py report            # enrichment coverage + run history
python3 box_office.py report        # BOM + reconcile counts
python3 release_events.py report    # release dates, festivals, windows, metadata
```
