#!/usr/bin/env python3
"""
ENRICH -- awards, ratings and distributors for every film in release_events.db.

Four sources, each its own stage, each resumable, all writing to the shared
database (same folder as release_events.py / box_office.py):

  wikidata     batched SPARQL by tconst (P345): awards received (P166) and
               nominations (P1411) with ceremony date / edition / rank,
               distributors (P750) with place, format and start date, and the
               Rotten Tomatoes (P1258) and Metacritic (P1712) ids the other
               stages need. ~1,500 queries for the catalogue; about an hour.
               Every award is classified against the festival registry
               (fest_qid) and a family (oscars, golden_globes, bafta, guild,
               critics, festival, national_academy, other).
  omdb         ratings by tconst: Rotten Tomatoes critic %, Metacritic,
               IMDb rating + votes, the free-text awards line. The free tier
               is 1,000 requests a day, so this stage does --limit films (1,000)
               in priority order and stops; run it daily. A "request limit
               reached" answer ends the run without marking the film.
  rt           rottentomatoes.com page per film that has a P1258 id: critic
               and audience scores + review counts + certified-fresh flags.
               One connection, a pause between pages, every page cached.
               The parser is written against RT's 2024-2026 page (a JSON
               scorecard block) and has NOT been run against a live page from
               here: `rt --dump <tconst>` saves one page for a first look.
  tmdb_awards  themoviedb.org/movie/{id}/awards, which has no API and is
               client-rendered: a headless Chromium (Playwright) per RUN, not
               per film, images/fonts/CSS blocked, ~2-3 s per film. Parser
               ported from comp_6_awards_tmdb.py (validated on real pages).
               `tmdb_awards --probe <tconst>` logs the network calls the page
               makes, so a JSON endpoint behind it can replace the browser.
  export       awards.csv (both sources, one row per nomination/win),
               ratings.csv (one row per film), distributors.csv (Wikidata +
               Box Office Mojo), enrichment_coverage.csv
  report / selftest

Keys from the environment only: OMDB_API_KEY. Nothing is printed or stored
with a key in it.

Terms: OMDb is an API with a key. TMDb's terms steer developers to the API
(which has no awards) and forbid load that degrades the site; Rotten Tomatoes
(Fandango) restricts automated access in its terms. Both scrapes here are
single-connection, paused, cached-forever and self-stopping on a block.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import gzip
import json
import os
import random
import re
import sqlite3
import sys
import time
import zlib
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_events as R   # noqa: E402

__version__ = "0.1.0"
OMDB_URL = os.environ.get("OMDB_URL", "https://www.omdbapi.com/")
RT_BASE = os.environ.get("RT_BASE", "https://www.rottentomatoes.com")
TMDB_SITE = os.environ.get("TMDB_SITE", "https://www.themoviedb.org")
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
WD_BATCH = 100
WD_MIN_BATCH = 10           # a batch this small that still times out stays pending
WD_LABEL_BATCH = 400
OMDB_DAILY_LIMIT = 1000
RT_SLEEP = 2.0
TMDB_AWARDS_SLEEP = 1.0         # plain requests; the page itself takes ~0.4 s
BACKOFF_SCALE = 1.0
AWARDS_CHALLENGE_LIMIT = 3      # plain requests not served an awards page this many times in a row -> stop
AWARDS_PARSER_VERSION = 2       # 2: category link is ceremony#category-N (live page, 2026-10-14)

SCHEMA = """
CREATE TABLE IF NOT EXISTS enrich_status (
  tmdb_id           INTEGER PRIMARY KEY,
  tconst            TEXT,
  wikidata_at       TEXT,
  wikidata_qid      TEXT,
  omdb_status       TEXT,        -- ok | notfound | error
  omdb_at           TEXT,
  rt_status         TEXT,        -- ok | no_id | notfound | blocked | error | noscore
  rt_at             TEXT,
  tmdb_awards_status TEXT,       -- ok | none | error
  tmdb_awards_at    TEXT
);

CREATE TABLE IF NOT EXISTS wd_awards (
  tmdb_id       INTEGER NOT NULL,
  tconst        TEXT,
  kind          TEXT NOT NULL,        -- won | nominated
  award_qid     TEXT NOT NULL,
  award_label   TEXT,
  when_date     TEXT,                 -- P585 as given (ISO date or year), precision-aware
  when_precision INTEGER,
  edition_qid   TEXT,
  edition_start TEXT,
  body_qid      TEXT,                 -- P179 (series) or P1027 (conferred by)
  body_label    TEXT,
  fest_qid      TEXT,                 -- when the body (or the label) is a registry festival
  family        TEXT,
  rank          TEXT,
  PRIMARY KEY (tmdb_id, kind, award_qid, when_date)
);
CREATE INDEX IF NOT EXISTS wd_awards_fest ON wd_awards(fest_qid);

CREATE TABLE IF NOT EXISTS wd_labels (
  qid   TEXT PRIMARY KEY,
  label TEXT
);

CREATE TABLE IF NOT EXISTS wd_film_ids (
  tmdb_id       INTEGER PRIMARY KEY,
  tconst        TEXT,
  wikidata_qid  TEXT,
  rt_id         TEXT,
  mc_id         TEXT
);

CREATE TABLE IF NOT EXISTS wd_distributors (
  tmdb_id       INTEGER NOT NULL,
  tconst        TEXT,
  dist_qid      TEXT NOT NULL,
  dist_label    TEXT,
  dist_country  TEXT,
  place_iso     TEXT,
  place_label   TEXT,
  format        TEXT,
  start_date    TEXT,
  PRIMARY KEY (tmdb_id, dist_qid, place_iso, format)
);

CREATE TABLE IF NOT EXISTS omdb_raw (
  tmdb_id     INTEGER PRIMARY KEY,
  tconst      TEXT,
  fetched_at  TEXT,
  http_status INTEGER,
  json        TEXT
);

CREATE TABLE IF NOT EXISTS omdb_ratings (
  tmdb_id            INTEGER PRIMARY KEY,
  tconst             TEXT,
  rt_critic          INTEGER,
  metacritic         INTEGER,
  imdb_rating        REAL,
  imdb_votes         INTEGER,
  rated              TEXT,
  awards_text        TEXT,
  oscar_wins         INTEGER,
  oscar_nominations  INTEGER,
  total_wins         INTEGER,
  total_nominations  INTEGER,
  box_office_text    TEXT,
  fetched_at         TEXT
);

CREATE TABLE IF NOT EXISTS rt_pages (
  rt_id       TEXT PRIMARY KEY,
  tmdb_id     INTEGER,
  fetched_at  TEXT,
  http_status INTEGER,
  html_z      BLOB
);

CREATE TABLE IF NOT EXISTS rt_scores (
  tmdb_id              INTEGER PRIMARY KEY,
  tconst               TEXT,
  rt_id                TEXT,
  critic_score         INTEGER,
  critic_count         INTEGER,
  critic_certified     INTEGER,
  critic_sentiment     TEXT,
  audience_score       INTEGER,
  audience_count       INTEGER,
  audience_verified    INTEGER,
  audience_sentiment   TEXT,
  parsed_via           TEXT,
  fetched_at           TEXT
);

CREATE TABLE IF NOT EXISTS tmdb_awards_pages (
  tmdb_id     INTEGER PRIMARY KEY,
  fetched_at  TEXT,
  stated_nominations INTEGER,
  stated_wins INTEGER,
  n_rows      INTEGER,
  parser_ver  INTEGER,
  html_z      BLOB
);

CREATE TABLE IF NOT EXISTS tmdb_awards (
  tmdb_id        INTEGER NOT NULL,
  tconst         TEXT,
  award_body     TEXT,
  award_body_id  TEXT NOT NULL,
  ceremony_label TEXT,
  ceremony_year  INTEGER,
  category       TEXT,
  category_id    TEXT NOT NULL,
  result         TEXT,
  PRIMARY KEY (tmdb_id, award_body_id, ceremony_label, category_id)
);
"""


def connect(cfg: R.Config) -> sqlite3.Connection:
    con = R.connect(cfg)
    con.executescript(SCHEMA)
    return con


def _ensure_status_rows(con):
    con.execute("INSERT OR IGNORE INTO enrich_status(tmdb_id, tconst) "
                "SELECT tmdb_id, substr(tconsts, 1, instr(tconsts || '|', '|') - 1) FROM films WHERE status='ok'")
    con.commit()


# =============================================================================
# 1. WIKIDATA
# =============================================================================
# Two things the live runs taught (2026-09-24):
#  1. `hint:Query hint:optimizer "None"` is mandatory. Without it Blazegraph
#     evaluates the P166 / P1411 UNION over ALL of Wikidata before joining
#     the VALUES list, and even 6 films time out; with it 100 of the most
#     awarded films in the catalogue answer in ~2 s.
#  2. No label service on the per-film queries: labelling every statement
#     of a famous film x 7 languages is what made the first version slow.
#     Labels are fetched once per DISTINCT item (SPARQL_LABELS) and cached
#     in wd_labels -- a few thousand items, not a few million rows.
SPARQL_AWARDS = """
SELECT ?imdb ?film ?kind ?award ?when ?prec ?edition ?edStart ?rank ?body ?conf WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { %s }
  ?film wdt:P345 ?imdb .
  { ?film p:P166 ?st . ?st ps:P166 ?award . BIND("won" AS ?kind) }
  UNION
  { ?film p:P1411 ?st . ?st ps:P1411 ?award . BIND("nominated" AS ?kind) }
  ?st wikibase:rank ?rank .
  FILTER(?rank != wikibase:DeprecatedRank)
  OPTIONAL { ?st pqv:P585 ?tv . ?tv wikibase:timeValue ?when ; wikibase:timePrecision ?prec . }
  OPTIONAL { ?st pq:P805 ?edition . OPTIONAL { ?edition wdt:P580 ?edStart } }
  OPTIONAL { ?award wdt:P179 ?body }
  OPTIONAL { ?award wdt:P1027 ?conf }
}
"""
SPARQL_LABELS = """
SELECT ?item ?itemLabel WHERE {
  VALUES ?item { %s }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,fr,de,es,it,ja,ko". }
}
"""
SPARQL_IDS_DIST = """
SELECT ?imdb ?film ?rt ?mc ?dist ?distCountry ?place ?placeIso ?fmt ?start WHERE {
  hint:Query hint:optimizer "None" .
  VALUES ?imdb { %s }
  ?film wdt:P345 ?imdb .
  OPTIONAL { ?film wdt:P1258 ?rt }
  OPTIONAL { ?film wdt:P1712 ?mc }
  OPTIONAL {
    ?film p:P750 ?ds . ?ds ps:P750 ?dist .
    FILTER NOT EXISTS { ?ds wikibase:rank wikibase:DeprecatedRank }
    OPTIONAL { ?ds pq:P291 ?place . OPTIONAL { ?place wdt:P297 ?placeIso } }
    OPTIONAL { ?ds pq:P437 ?fmt }
    OPTIONAL { ?ds pq:P580 ?start }
    OPTIONAL { ?dist wdt:P17 ?dc . ?dc wdt:P297 ?distCountry }
  }
}
"""

FAMILY_PATTERNS = [
    ("oscars", re.compile(r"\bacademy award|\boscar\b", re.I)),
    ("golden_globes", re.compile(r"golden globe", re.I)),
    ("bafta", re.compile(r"\bbafta\b|british academy film", re.I)),
    ("emmys", re.compile(r"\bemmy\b", re.I)),
    ("guild", re.compile(r"screen actors guild|\bsag\b|directors guild|writers guild|producers guild|"
                         r"\bdga\b|\bwga\b|\bpga\b|american society of cinematographers|\bace eddie\b|"
                         r"art directors guild|costume designers guild|casting society|cinema audio society|"
                         r"motion picture sound editors|visual effects society|annie award", re.I)),
    ("independent_spirit", re.compile(r"independent spirit", re.I)),
    ("critics", re.compile(r"critics?['’]? ?(choice|circle|association|society)|\bcritics? award|"
                           r"national society of film critics|national board of review|\bfipresci\b|"
                           r"golden raspberry|razzie", re.I)),
    ("national_academy", re.compile(r"\bc[ée]sar\b|\bgoya\b|\bdavid di donatello\b|japan academy|"
                                    r"european film award|\blola\b|deutscher filmpreis|\bmagritte\b|"
                                    r"\bariel\b|\bgenie\b|canadian screen|\baacta\b|\bafi award|australian film institute|"
                                    r"\bbodil\b|\brobert\b award|\bguldbagge\b|\bamanda\b award|\bjussi\b|"
                                    r"\bsophia\b award|\bgrand bell\b|\bbaeksang\b|blue dragon|\bfilmfare\b|"
                                    r"national film award|golden horse|hong kong film award|\bsatellite award", re.I)),
    ("saturn", re.compile(r"saturn award", re.I)),
    ("festival", re.compile(r"palme|cam[ée]ra d'or|golden bear|silver bear|golden lion|silver lion|"
                            r"volpi|golden leopard|golden shell|crystal globe|people'?s choice award|"
                            r"grand jury prize|jury prize|un certain regard|film festival|festival", re.I)),
]


def classify_award(label: str, body_qid: str | None, conf_qid: str | None, reg: R.Registry | None) -> tuple[str, str | None]:
    """(family, fest_qid). A festival is anything whose P179/P1027 body is in
    the festival registry, or whose label names one; families for the rest."""
    fest = None
    if reg is not None:
        for q in (body_qid, conf_qid):
            if q and q in reg.f:
                fest = q
                break
        if fest is None and label:
            hit = reg.name_score(R.norm(re.sub(r"^.*?\(([^)]+)\)\s*$", r"\1", label)))
            if hit.qid and hit.exact:
                fest = hit.qid
    if fest:
        return "festival", fest
    for fam, rx in FAMILY_PATTERNS:
        if rx.search(label or ""):
            return fam, None
    return "other", None


def _v(b, k):
    return (b.get(k) or {}).get("value")


def _qid(uri):
    return uri.rsplit("/", 1)[-1] if uri else None


def _when(when: str | None, prec) -> str | None:
    """A P585 at year precision must not become 1 January."""
    if not when:
        return None
    try:
        p = int(prec) if prec not in (None, "") else 11
    except (TypeError, ValueError):
        p = 11
    return when[:4] if p <= 9 else (when[:7] if p == 10 else when[:10])


def parse_awards_bindings(bindings: list[dict]) -> dict[str, list[dict]]:
    """tconst -> unlabelled award rows (labels and classification come after)."""
    out: dict[str, dict] = defaultdict(dict)
    for b in bindings:
        imdb, kind, aq = _v(b, "imdb"), _v(b, "kind"), _qid(_v(b, "award"))
        if not imdb or not aq or kind not in ("won", "nominated"):
            continue
        when = _when(_v(b, "when"), _v(b, "prec"))
        rank = (_v(b, "rank") or "").rsplit("#", 1)[-1]
        key = (kind, aq, when or "")
        row = {"kind": kind, "award_qid": aq, "when": when, "prec": int(_v(b, "prec")) if _v(b, "prec") else None,
               "edition": _qid(_v(b, "edition")), "ed_start": (_v(b, "edStart") or "")[:10] or None,
               "body": _qid(_v(b, "body")), "conf": _qid(_v(b, "conf")), "rank": rank}
        prev = out[imdb].get(key)
        if prev is None or (rank == "PreferredRank" and prev["rank"] != "PreferredRank"):
            out[imdb][key] = row
    return {t: list(d.values()) for t, d in out.items()}


def fetch_labels(con, wdqs: R.WdqsClient, qids: Iterable[str]) -> dict[str, str]:
    """Labels for items, from the wd_labels cache, fetching only the missing ones."""
    want = sorted({q for q in qids if q})
    have = {}
    for i in range(0, len(want), 500):
        part = want[i:i + 500]
        have.update(con.execute(f"SELECT qid, label FROM wd_labels WHERE qid IN ({','.join('?' * len(part))})", part))
    missing = [q for q in want if q not in have]
    for i in range(0, len(missing), WD_LABEL_BATCH):
        part = missing[i:i + WD_LABEL_BATCH]
        vals = " ".join(f"wd:{q}" for q in part)
        try:
            rows = wdqs.query(SPARQL_LABELS % vals, "labels:" + str(zlib.crc32(vals.encode())))
        except RuntimeError:
            continue
        got = {_qid(_v(b, "item")): _v(b, "itemLabel") for b in rows if _qid(_v(b, "item"))}
        with con:
            con.executemany("INSERT OR REPLACE INTO wd_labels VALUES (?,?)",
                            [(q, got.get(q) or q) for q in part])
        have.update({q: got.get(q) or q for q in part})
    return have


def finalize_awards(rows: list[dict], labels: dict[str, str], reg: R.Registry | None) -> list[tuple]:
    out = []
    for r in rows:
        label = labels.get(r["award_qid"]) or r["award_qid"]
        body = r["body"] or r["conf"]
        family, fest = classify_award(label, r["body"], r["conf"], reg)
        out.append((r["kind"], r["award_qid"], label, r["when"], r["prec"], r["edition"], r["ed_start"],
                    body, labels.get(body) if body else None, fest, family, r["rank"]))
    return out


def parse_ids_dist_bindings(bindings: list[dict]) -> tuple[dict, dict]:
    """tconst -> ids, tconst -> unlabelled distributor rows
    (dist_qid, dist_country, place_iso, place_qid, fmt_qid, start)."""
    ids: dict[str, dict] = {}
    dist: dict[str, dict] = defaultdict(dict)
    for b in bindings:
        imdb = _v(b, "imdb")
        if not imdb:
            continue
        d = ids.setdefault(imdb, {"qid": _qid(_v(b, "film")), "rt": None, "mc": None})
        rt, mc = _v(b, "rt"), _v(b, "mc")
        if rt and (d["rt"] is None or rt < d["rt"]):
            d["rt"] = rt
        if mc and (d["mc"] is None or mc < d["mc"]):
            d["mc"] = mc
        dq = _qid(_v(b, "dist"))
        if dq:
            place, fmt = _qid(_v(b, "place")), _qid(_v(b, "fmt"))
            key = (dq, _v(b, "placeIso") or place or "", fmt or "")
            dist[imdb][key] = (dq, _v(b, "distCountry"), _v(b, "placeIso"), place, fmt, (_v(b, "start") or "")[:10] or None)
    return ids, {t: list(d.values()) for t, d in dist.items()}


def finalize_dist(rows: list[tuple], labels: dict[str, str]) -> list[tuple]:
    """-> (dist_qid, dist_label, dist_country, place_iso, place_label, fmt_label, start)."""
    return [(dq, labels.get(dq) or dq, dc, iso, (labels.get(place) if place else None),
             (labels.get(fmt) if fmt else None), start) for dq, dc, iso, place, fmt, start in rows]


def wikidata_stage(cfg: R.Config, con, limit: int | None = None, wdqs: R.WdqsClient | None = None,
                   verbose=True) -> dict:
    _ensure_status_rows(con)
    wdqs = wdqs or R.WdqsClient(con)
    reg = R.Registry(R.registry_rows(con)) if con.execute("SELECT 1 FROM festival_registry LIMIT 1").fetchone() else None
    todo = con.execute("SELECT tmdb_id, tconst FROM enrich_status WHERE wikidata_at IS NULL AND tconst != '' "
                       "ORDER BY tmdb_id").fetchall()
    if limit:
        todo = todo[:limit]
    n_batches = (len(todo) + WD_BATCH - 1) // WD_BATCH
    if verbose:
        print(f"WIKIDATA  {len(todo)} films in {n_batches} batches of {WD_BATCH}; registry "
              f"{'loaded (' + str(reg.N) + ' festivals)' if reg else 'absent - festival classification by label only'}")
    stats = Counter()
    t0 = time.monotonic()

    def run_batch(by_t: dict) -> tuple[list, list, dict]:
        """Both queries for a set of tconsts -> (award bindings, id bindings,
        the films that succeeded). A timeout splits the batch in halves (down
        to WD_MIN_BATCH films) rather than losing it. Films in a sub-batch that still fails are NOT
        marked done, so the next run retries them."""
        vals = " ".join(f'"{t}"' for t in by_t)
        key = "enrich:v2:" + str(zlib.crc32(vals.encode()))     # v2: unlabelled queries; older cache rows are ignored
        try:
            return wdqs.query(SPARQL_AWARDS % vals, key + ":awards"), wdqs.query(SPARQL_IDS_DIST % vals, key + ":ids"), dict(by_t)
        except RuntimeError as e:
            if len(by_t) <= WD_MIN_BATCH:
                if verbose:
                    print(f"  a batch of {len(by_t)} failed ({e}); those films stay pending")
                stats["failed_batches"] += 1
                return [], [], {}
            items = list(by_t.items())
            half = len(items) // 2
            a = run_batch(dict(items[:half])); b = run_batch(dict(items[half:]))
            return a[0] + b[0], a[1] + b[1], {**a[2], **b[2]}

    with R.RunLog(con, "wd_enrich", f"n={len(todo)}") as rl:
        for bi in range(n_batches):
            batch = todo[bi * WD_BATCH:(bi + 1) * WD_BATCH]
            aw, idd, done = run_batch({t: tid for tid, t in batch})
            if not done:
                continue
            raw_awards = parse_awards_bindings(aw)
            ids, raw_dist = parse_ids_dist_bindings(idd)
            labels = fetch_labels(con, wdqs, [q for rows_ in raw_awards.values() for r in rows_
                                              for q in (r["award_qid"], r["body"], r["conf"])]
                                             + [q for rows_ in raw_dist.values() for r in rows_ for q in (r[0], r[3], r[4])])
            awards = {t: finalize_awards(rows_, labels, reg) for t, rows_ in raw_awards.items()}
            dist = {t: finalize_dist(rows_, labels) for t, rows_ in raw_dist.items()}
            now = R.utcnow()
            with con:
                for t, tid in done.items():
                    con.execute("DELETE FROM wd_awards WHERE tmdb_id=?", (tid,))
                    con.execute("DELETE FROM wd_distributors WHERE tmdb_id=?", (tid,))
                    for r in awards.get(t, []):
                        con.execute("INSERT OR REPLACE INTO wd_awards VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (tid, t, r[0], r[1], r[2], r[3] or "", *r[4:]))
                        stats["awards"] += 1
                    i = ids.get(t)
                    if i:
                        con.execute("INSERT OR REPLACE INTO wd_film_ids VALUES (?,?,?,?,?)", (tid, t, i["qid"], i["rt"], i["mc"]))
                        stats["ids"] += 1
                        stats["rt_ids"] += 1 if i["rt"] else 0
                    for d in dist.get(t, []):
                        con.execute("INSERT OR REPLACE INTO wd_distributors VALUES (?,?,?,?,?,?,?,?,?)",
                                    (tid, t, d[0], d[1], d[2], d[3] or "", d[4], d[5] or "", d[6]))
                        stats["distributors"] += 1
                    con.execute("UPDATE enrich_status SET wikidata_at=?, wikidata_qid=? WHERE tmdb_id=?",
                                (now, (i or {}).get("qid"), tid))
            stats["films"] += len(done)
            rl.n["ok"] = stats["films"]
            if verbose and (bi + 1) % 20 == 0:
                el = time.monotonic() - t0
                print(f"  batch {bi + 1}/{n_batches}: {stats['films']} films, {stats['awards']} award rows, "
                      f"{stats['rt_ids']} RT ids, {stats['distributors']} distributor rows  "
                      f"ETA {(n_batches - bi - 1) * el / (bi + 1) / 60:.0f} min")
    if verbose:
        print(f"  done: {dict(stats)}")
    return {"films": 0, "awards": 0, "ids": 0, "rt_ids": 0, "distributors": 0, "failed_batches": 0, **stats}


# =============================================================================
# 2. OMDb
# =============================================================================
class OmdbLimit(RuntimeError):
    pass


def omdb_parse(data: dict) -> dict:
    """Ratings[] and the free-text Awards line, the way comp_2 read them."""
    out = {"rt_critic": None, "metacritic": None, "imdb_rating": None, "imdb_votes": None, "rated": None,
           "awards_text": None, "oscar_wins": None, "oscar_nominations": None, "total_wins": None,
           "total_nominations": None, "box_office_text": None}
    if not isinstance(data, dict) or data.get("Response") != "True":
        return out
    for r in data.get("Ratings") or []:
        src, val = (r or {}).get("Source", ""), (r or {}).get("Value", "")
        m = re.search(r"(\d+)", val or "")
        if not m:
            continue
        if src == "Rotten Tomatoes":
            out["rt_critic"] = int(m.group(1))
        elif src == "Metacritic":
            out["metacritic"] = int(m.group(1))
    try:
        out["imdb_rating"] = float(data.get("imdbRating")) if data.get("imdbRating") not in (None, "N/A") else None
    except ValueError:
        pass
    v = (data.get("imdbVotes") or "").replace(",", "")
    out["imdb_votes"] = int(v) if v.isdigit() else None
    out["rated"] = data.get("Rated") if data.get("Rated") not in (None, "N/A") else None
    aw = data.get("Awards") if data.get("Awards") not in (None, "N/A") else None
    out["awards_text"] = aw
    if aw:
        m = re.search(r"Won (\d+) Oscar", aw)
        out["oscar_wins"] = int(m.group(1)) if m else 0
        m = re.search(r"Nominated for (\d+) Oscar", aw)
        out["oscar_nominations"] = int(m.group(1)) if m else 0
        m = re.search(r"(\d+) wins?", aw)
        out["total_wins"] = int(m.group(1)) if m else 0
        m = re.search(r"(\d+) nominations?", aw)
        out["total_nominations"] = int(m.group(1)) if m else 0
    bo = data.get("BoxOffice")
    out["box_office_text"] = bo if bo not in (None, "N/A") else None
    return out


def omdb_priority_ids(con, refresh=False) -> list[tuple[int, str]]:
    """US theatrical first, then any theatrical, then the rest; popular first within each."""
    where = "" if refresh else "AND (s.omdb_status IS NULL OR s.omdb_status='error')"
    return con.execute(f"""
        SELECT s.tmdb_id, s.tconst FROM enrich_status s
        LEFT JOIN film_summary fs ON fs.tmdb_id = s.tmdb_id
        LEFT JOIN film_metadata m ON m.tmdb_id = s.tmdb_id
        WHERE s.tconst != '' {where}
        ORDER BY CASE WHEN fs.us_theatrical IS NOT NULL THEN 0 WHEN fs.first_theatrical IS NOT NULL THEN 1 ELSE 2 END,
                 COALESCE(m.vote_count, 0) DESC, s.tmdb_id""").fetchall()


def omdb_stage(cfg: R.Config, con, limit: int = OMDB_DAILY_LIMIT, refresh=False, session=None, verbose=True) -> Counter:
    key = os.environ.get("OMDB_API_KEY", "")
    if not key:
        raise RuntimeError("OMDB_API_KEY is not set in the environment")
    _ensure_status_rows(con)
    todo = omdb_priority_ids(con, refresh)[:limit]
    s = session or requests.Session()
    counts = Counter()
    if verbose:
        print(f"OMDB  {len(todo)} films this run (daily budget {limit}); "
              f"{con.execute(chr(39).join(['SELECT COUNT(*) FROM enrich_status WHERE omdb_status=', 'ok', ''])).fetchone()[0]} already fetched")
    with R.RunLog(con, "omdb", f"limit={limit}") as rl:
        try:
            for i, (tid, tconst) in enumerate(todo, 1):
                try:
                    r = s.get(OMDB_URL, params={"i": tconst, "apikey": key, "tomatoes": "true"}, timeout=20)
                except requests.RequestException as e:
                    status, data, err = None, None, f"transport: {type(e).__name__}"
                else:
                    status = r.status_code
                    try:
                        data = r.json()
                    except ValueError:
                        data = None
                    err = ""
                    if isinstance(data, dict) and data.get("Response") == "False":
                        msg = data.get("Error") or ""
                        if "limit" in msg.lower():
                            raise OmdbLimit(msg)
                        err = msg
                now = R.utcnow()
                if isinstance(data, dict) and data.get("Response") == "True":
                    p = omdb_parse(data)
                    with con:
                        con.execute("INSERT OR REPLACE INTO omdb_raw VALUES (?,?,?,?,?)",
                                    (tid, tconst, now, status, json.dumps(data, ensure_ascii=False)))
                        con.execute("INSERT OR REPLACE INTO omdb_ratings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    (tid, tconst, p["rt_critic"], p["metacritic"], p["imdb_rating"], p["imdb_votes"],
                                     p["rated"], p["awards_text"], p["oscar_wins"], p["oscar_nominations"],
                                     p["total_wins"], p["total_nominations"], p["box_office_text"], now))
                        con.execute("UPDATE enrich_status SET omdb_status='ok', omdb_at=? WHERE tmdb_id=?", (now, tid))
                    counts["ok"] += 1
                elif isinstance(data, dict) and data.get("Response") == "False":
                    with con:
                        con.execute("INSERT OR REPLACE INTO omdb_raw VALUES (?,?,?,?,?)",
                                    (tid, tconst, now, status, json.dumps(data)))
                        con.execute("UPDATE enrich_status SET omdb_status='notfound', omdb_at=? WHERE tmdb_id=?", (now, tid))
                    counts["notfound"] += 1
                else:
                    with con:
                        con.execute("UPDATE enrich_status SET omdb_status='error', omdb_at=? WHERE tmdb_id=?", (now, tid))
                    counts["error"] += 1
                    if counts["error"] >= 25 and counts["ok"] == 0:
                        raise RuntimeError(f"25 errors and no successes -- stopping ({R.redact(err)})")
                rl.n["ok" if counts["ok"] else "error"] = counts["ok"] or counts["error"]
                time.sleep(0.25 * BACKOFF_SCALE)
                if verbose and i % 200 == 0:
                    print(f"  {i}/{len(todo)}  ok={counts['ok']} notfound={counts['notfound']} err={counts['error']}")
        except OmdbLimit as e:
            counts["limit"] += 1
            rl.note = f"daily limit reached: {e}"
            if verbose:
                print(f"  OMDb says: {e} -- stopping for today; run again tomorrow")
    if verbose:
        print(f"  done: ok={counts['ok']} notfound={counts['notfound']} err={counts['error']}")
    return counts


# =============================================================================
# 3. ROTTEN TOMATOES
# =============================================================================
def _strip_rt(html: str) -> str:
    """Keep the JSON script blocks and score elements; drop everything heavy."""
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["style", "svg", "noscript", "link", "meta", "iframe", "img", "picture", "video"]):
        t.decompose()
    for sc in soup.find_all("script"):
        if (sc.get("type") or "").lower() not in ("application/json", "application/ld+json"):
            sc.decompose()
    return re.sub(r"\s+", " ", str(soup))


def _walk_scores(obj, out: dict, path=""):
    """Find critic/audience score objects anywhere in a JSON blob."""
    if isinstance(obj, dict):
        keys = {k.lower(): k for k in obj}
        for side, names in (("critic", ("criticsscore", "tomatometerscore", "criticscore", "tomatometer")),
                            ("audience", ("audiencescore", "popcornmeterscore"))):
            for n in names:
                if n in keys and side not in out:
                    v = obj[keys[n]]
                    if isinstance(v, dict):
                        score = v.get("score", v.get("value", v.get("scorePercent")))
                        out[side] = {"score": _int(score), "count": _int(v.get("reviewCount", v.get("ratingCount", v.get("count")))),
                                     "certified": v.get("certified", v.get("certifiedFresh")),
                                     "sentiment": v.get("sentiment") or v.get("state"), "via": path + "/" + keys[n]}
                    elif isinstance(v, (int, float, str)) and _int(v) is not None:
                        out[side] = {"score": _int(v), "count": None, "certified": None, "sentiment": None, "via": path + "/" + keys[n]}
        for k, v in obj.items():
            _walk_scores(v, out, path + "/" + k)
    elif isinstance(obj, list):
        for x in obj:
            _walk_scores(x, out, path)


def _int(v):
    if v is None or isinstance(v, bool):
        return None
    m = re.search(r"-?\d+", str(v))
    return int(m.group(0)) if m else None


def parse_rt_page(html: str) -> dict:
    """Critic + audience scores from an RT film page.

    Three shapes, newest first: a JSON script block (media-scorecard-json /
    scoreDetails), a <score-board> / <media-scorecard> element with score
    attributes, and the visible rt-text slots. Returns {} if none is present."""
    soup = BeautifulSoup(html or "", "html.parser")
    found: dict = {}
    for sc in soup.find_all("script", attrs={"type": re.compile("json", re.I)}):
        txt = sc.string or sc.get_text() or ""
        if not any(k in txt for k in ("criticsScore", "tomatometerScore", "audienceScore", "popcornmeter")):
            continue
        try:
            data = json.loads(txt)
        except ValueError:
            continue
        _walk_scores(data, found, f"script#{sc.get('id') or ''}")
        if "critic" in found or "audience" in found:
            break
    if not found:
        el = soup.find(["score-board", "media-scorecard", "score-board-deprecated"])
        if el is not None:
            c = el.get("tomatometerscore") or el.get("criticsscore")
            a = el.get("audiencescore")
            if c:
                found["critic"] = {"score": _int(c), "count": _int(el.get("tomatometercount") or el.get("criticscount")),
                                   "certified": (el.get("tomatometerstate") or el.get("criticscertified") or "").lower() in ("certified-fresh", "true", "certified"),
                                   "sentiment": el.get("tomatometerstate"), "via": "element/" + el.name}
            if a:
                found["audience"] = {"score": _int(a), "count": _int(el.get("audiencecount")),
                                     "certified": (el.get("audiencestate") or "").lower() in ("verified-hot", "true"),
                                     "sentiment": el.get("audiencestate"), "via": "element/" + el.name}
    if not found:
        for slot, side in (("criticsScore", "critic"), ("audienceScore", "audience")):
            el = soup.find(attrs={"slot": slot})
            if el is not None and _int(el.get_text()) is not None:
                found[side] = {"score": _int(el.get_text()), "count": None, "certified": None, "sentiment": None, "via": "slot"}
    return found


def _rt_get(session: requests.Session, url: str, sleep: float) -> tuple[int, str]:
    time.sleep(sleep * random.uniform(0.8, 1.3))
    r = session.get(url, timeout=30, allow_redirects=True)
    return r.status_code, r.text


def rt_stage(cfg: R.Config, con, limit: int | None = None, sleep: float = RT_SLEEP, session=None, base: str | None = None,
             verbose=True, dump: str | None = None) -> Counter:
    _ensure_status_rows(con)
    base = (base or RT_BASE).rstrip("/")
    s = session or requests.Session()
    s.headers.update({"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9", "Accept": "text/html"})
    if dump:
        rid = con.execute("SELECT rt_id FROM wd_film_ids WHERE tconst=?", (dump,)).fetchone()
        rid = rid[0] if rid and rid[0] else (dump if dump.startswith("m/") else None)
        if not rid:
            print(f"no RT id known for {dump}; pass an RT path like m/parasite_2019")
            return Counter()
        st, html = _rt_get(s, f"{base}/{rid}", 0)
        p = cfg.out_dir / f"rt_page_{rid.replace('/', '_')}.html"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(html, encoding="utf-8")
        print(f"HTTP {st}; {len(html)} bytes -> {p}\nparsed: {parse_rt_page(html)}")
        return Counter(dumped=1)
    # Same priority as OMDb (US theatrical -> any theatrical -> rest, popular
    # first): a run that stops early has still covered the films that matter.
    todo = con.execute("""
        SELECT s.tmdb_id, s.tconst, i.rt_id FROM enrich_status s JOIN wd_film_ids i USING(tmdb_id)
        LEFT JOIN film_summary fs ON fs.tmdb_id = s.tmdb_id
        LEFT JOIN film_metadata m ON m.tmdb_id = s.tmdb_id
        WHERE i.rt_id IS NOT NULL AND (s.rt_status IS NULL OR s.rt_status='error')
        ORDER BY CASE WHEN fs.us_theatrical IS NOT NULL THEN 0 WHEN fs.first_theatrical IS NOT NULL THEN 1 ELSE 2 END,
                 COALESCE(m.vote_count, 0) DESC, s.tmdb_id""").fetchall()
    n_noid = con.execute("UPDATE enrich_status SET rt_status='no_id', rt_at=? WHERE rt_status IS NULL AND wikidata_at IS NOT NULL "
                         "AND tmdb_id NOT IN (SELECT tmdb_id FROM wd_film_ids WHERE rt_id IS NOT NULL)", (R.utcnow(),)).rowcount
    con.commit()
    if limit:
        todo = todo[:limit]
    counts = Counter()
    blocks = 0
    t0 = time.monotonic()
    if verbose:
        print(f"RT  {len(todo)} films with a Rotten Tomatoes id ({n_noid} newly marked no_id); {sleep:.1f}s between pages")
    with R.RunLog(con, "rt", f"n={len(todo)}") as rl:
        try:
            for i, (tid, tconst, rid) in enumerate(todo, 1):
                row = con.execute("SELECT http_status, html_z FROM rt_pages WHERE rt_id=?", (rid,)).fetchone()
                if row:
                    st, html = row[0], (zlib.decompress(row[1]).decode() if row[1] else "")
                else:
                    try:
                        st, html = _rt_get(s, f"{base}/{rid}", sleep)
                    except requests.RequestException as e:
                        with con:
                            con.execute("UPDATE enrich_status SET rt_status='error', rt_at=? WHERE tmdb_id=?", (R.utcnow(), tid))
                        counts["error"] += 1
                        continue
                    if st in (403, 429) or (st == 200 and ("captcha" in html[:3000].lower() or "access denied" in html[:3000].lower())):
                        blocks += 1
                        counts["blocked"] += 1
                        if blocks >= 3:
                            raise RuntimeError("Rotten Tomatoes is blocking (3 in a row) -- stop; try later with a longer --sleep")
                        if verbose:
                            print(f"  blocked (HTTP {st}); pausing 300s")
                        time.sleep(300 * BACKOFF_SCALE)
                        continue
                    blocks = 0
                    stripped = _strip_rt(html) if st == 200 else ""
                    with con:
                        con.execute("INSERT OR REPLACE INTO rt_pages VALUES (?,?,?,?,?)",
                                    (rid, tid, R.utcnow(), st, zlib.compress(stripped.encode(), 6) if stripped else None))
                    html = stripped
                now = R.utcnow()
                if st == 404 or not html:
                    status = "notfound"
                else:
                    sc = parse_rt_page(html)
                    if sc:
                        c, a = sc.get("critic", {}), sc.get("audience", {})
                        with con:
                            con.execute("INSERT OR REPLACE INTO rt_scores VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                        (tid, tconst, rid, c.get("score"), c.get("count"),
                                         int(bool(c.get("certified"))) if c.get("certified") is not None else None, c.get("sentiment"),
                                         a.get("score"), a.get("count"),
                                         int(bool(a.get("certified"))) if a.get("certified") is not None else None, a.get("sentiment"),
                                         (c.get("via") or a.get("via")), now))
                        status = "ok"
                    else:
                        status = "noscore"
                with con:
                    con.execute("UPDATE enrich_status SET rt_status=?, rt_at=? WHERE tmdb_id=?", (status, now, tid))
                counts[status] += 1
                rl.n["ok" if status == "ok" else "gone"] += 1
                if verbose and i % 200 == 0:
                    el = time.monotonic() - t0
                    print(f"  {i}/{len(todo)}  ok={counts['ok']} noscore={counts['noscore']} notfound={counts['notfound']} "
                          f"err={counts['error']}  {el/i:.1f}s/film  ETA {(len(todo)-i)*el/i/3600:.1f} h")
        except KeyboardInterrupt:
            if verbose:
                print("\n  interrupted -- everything fetched so far is committed")
            raise
    if verbose:
        print(f"  done: {dict(counts)}")
        if counts["noscore"] and counts["noscore"] >= max(5, counts["ok"]):
            print("  !! most pages parsed with NO score: RT's markup probably changed. Run `rt --dump <tconst>` and send the file.")
    return counts


# =============================================================================
# 4. TMDb AWARDS PAGE  (parser ported from comp_6_awards_tmdb.py)
# =============================================================================
NOM_RE = re.compile(r"(\d+)\s+Nominations?\b", re.IGNORECASE)
WIN_RE = re.compile(r"(\d+)\s+Wins?\b", re.IGNORECASE)
CEREMONY_RE = re.compile(r"^(?P<label>.+\((?P<year>\d{4})\))\s*$")
RESULT_RE = re.compile(r"^(Winner|Nominee|Nominated)$", re.IGNORECASE)
AWARD_SLUG_RE = re.compile(r"^/award/([^/?]+)")
CATEGORY_SLUG_RE = re.compile(r"/category/([^/?#]+)|#category-([^/?#&]+)")   # 2024 markup | Oct-2026 markup


def parse_awards_html(html: str) -> tuple[list[dict], int | None, int | None]:
    """(rows, stated_nominations, stated_wins). Selectors from comp_6_awards_tmdb.py;
    one change for the live Oct-2026 page: the category link became
    `/award/<body>/ceremony/<n>?...#category-<id>` (text = category name)."""
    soup = BeautifulSoup(html or "", "html.parser")
    page_text = soup.get_text(" ", strip=True)
    stated_nom = stated_win = None
    nom_m, win_m = NOM_RE.search(page_text), WIN_RE.search(page_text)
    if nom_m:
        stated_nom = int(nom_m.group(1))
        stated_win = int(win_m.group(1)) if win_m else 0
    elif win_m:
        stated_win = int(win_m.group(1))
    rows = []
    for block in soup.select("div.space-y-12 > div"):
        header_a = block.select_one("div.font-semibold.leading-9.text-xl a")
        award_body = header_a.get_text(strip=True) if header_a else None
        for entry in block.select("div[id]"):
            ceremony_a = entry.select_one('a[href*="/ceremony/"]')
            category_a = entry.select_one('a[href*="/category/"], a[href*="#category-"]')
            if ceremony_a is None or category_a is None:
                continue
            body_m = AWARD_SLUG_RE.match(ceremony_a.get("href") or "")
            cat_m = CATEGORY_SLUG_RE.search(category_a.get("href") or "")
            if body_m is None or cat_m is None:
                continue
            ceremony_label = ceremony_a.get_text(strip=True)
            ym = CEREMONY_RE.match(ceremony_label)
            result = None
            for sp in entry.find_all("span"):
                t = sp.get_text(strip=True)
                if RESULT_RE.match(t):
                    result = t.title()
                    break
            if result is None:
                continue
            rows.append({"award_body": award_body or body_m.group(1), "award_body_id": body_m.group(1),
                         "ceremony_label": ceremony_label, "ceremony_year": int(ym.group("year")) if ym else None,
                         "category": category_a.get_text(strip=True), "category_id": cat_m.group(1) or cat_m.group(2),
                         "result": result})
    return rows, stated_nom, stated_win


def _strip_awards(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "svg", "noscript", "link", "meta", "iframe", "img", "picture"]):
        t.decompose()
    return re.sub(r"\s+", " ", str(soup))


AWARDS_PAGE_MARKERS = ("bg-awards-header", "No awards", "- Awards")   # any of these = a real awards page


def _cookie_names(session) -> list[str]:
    """requests iterates Cookie objects, curl_cffi iterates names."""
    return sorted(getattr(c, "name", c) for c in session.cookies)


class AwardsChallenged(RuntimeError):
    """TMDb answered with something that is not an awards page (WAF challenge, block)."""


def _require_awards_page(html: str, where: str) -> str:
    """Pass a real awards page, or "" (film gone); raise AwardsChallenged for anything else.
    Without this the browser path recorded firewall challenge pages as 'none' (2026-10-18)."""
    if html and not any(m in html for m in AWARDS_PAGE_MARKERS):
        raise AwardsChallenged(f"{where}: {len(html)} bytes, no awards page markers")
    return html


class PlainAwardsFetcher:
    """requests, no browser per page. The live probe (2026-10-14) showed the
    awards page is server-rendered -- every entry is in the HTML -- but AWS WAF
    answers a bare client with HTTP 403 (a JS challenge). So: a headless
    browser solves the challenge ONCE, its cookies (the aws-waf-token) are
    copied into the requests session, and plain requests carry on until the
    token expires, when the browser primes it again. If priming stops helping
    (every request challenged) the fetcher switches itself to browser-only."""

    def __init__(self, site: str | None = None, session=None, timeout=25, browser=None):
        self.site = (site or TMDB_SITE).rstrip("/")
        self.client = "requests"
        if session is None:
            # The live probe (2026-10-14) showed the WAF token alone is not enough for a
            # python-requests client: the TLS handshake gives it away. curl_cffi presents
            # Chrome's TLS fingerprint and, with the browser's cookies, usually passes.
            try:
                from curl_cffi import requests as curl_requests
                session = curl_requests.Session(impersonate="chrome")
                self.client = "curl_cffi(chrome)"
            except ImportError:
                session = requests.Session()
                session.headers.update({"User-Agent": BROWSER_UA, "Accept-Language": "en-US,en;q=0.9",
                                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                                        "Upgrade-Insecure-Requests": "1"})
        self.s = session
        self.timeout = timeout
        self.browser = browser                  # an AwardsBrowser, or None
        self.seen_xhr: list[str] = []
        self.last_status: int | None = None
        self.last_mode = "plain"
        self.primes = 0                         # browser-assisted fetches
        self.consecutive_primes = 0
        self.browser_only = False
        self.last_block_body = ""

    def _url(self, tmdb_id):
        return f"{self.site}/movie/{tmdb_id}/awards?language=en-US"

    def _prime(self, tmdb_id: int) -> str:
        """Fetch this film in the browser (solves the challenge) and copy its cookies.
        A browser that is not served an awards page either raises AwardsChallenged."""
        html = _require_awards_page(self.browser.fetch(tmdb_id), f"browser, tmdb {tmdb_id}")
        for c in self.browser.ctx.cookies():
            self.s.cookies.set(c["name"], c["value"], domain=c.get("domain") or None, path=c.get("path") or "/")
        self.primes += 1
        self.consecutive_primes += 1
        self.last_mode = "browser"
        if self.consecutive_primes >= 5 and not self.browser_only:
            self.browser_only = True                    # the token is not helping; stop paying for 403s
        return html

    def fetch(self, tmdb_id: int) -> str:
        if self.browser_only:
            return self._prime(tmdb_id)
        r = self.s.get(self._url(tmdb_id), timeout=self.timeout)
        self.last_status, self.last_mode = r.status_code, "plain"
        if r.status_code == 404:
            return ""                                   # film gone: parses to no rows -> 'none'
        if r.status_code == 200 and any(m in r.text for m in AWARDS_PAGE_MARKERS):
            self.consecutive_primes = 0
            return r.text
        self.last_block_body = r.text[:4000]
        if self.browser is not None:
            return self._prime(tmdb_id)
        raise AwardsChallenged(f"HTTP {r.status_code}, {len(r.text)} bytes, no awards page markers")

    def close(self):
        self.s.close()
        if self.browser is not None:
            self.browser.close()


class AwardsBrowser:
    """One headless Chromium for the whole run; a fresh page per film.
    Fallback only (--browser): ~3 s a film vs ~0.5 s for PlainAwardsFetcher."""

    def __init__(self, site: str | None = None, timeout_ms=25000, probe=False):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self.browser = self._pw.chromium.launch()
        self.ctx = self.browser.new_context(user_agent=BROWSER_UA, viewport={"width": 1280, "height": 900})
        self.ctx.route("**/*", self._route)
        self.site = (site or TMDB_SITE).rstrip("/")
        self.timeout_ms = timeout_ms
        self.probe = probe
        self.seen_xhr: list[str] = []

    def _route(self, route, request):
        if request.resource_type in ("image", "media", "font", "stylesheet"):
            return route.abort()
        if self.probe and request.resource_type in ("xhr", "fetch"):
            self.seen_xhr.append(f"{request.method} {request.url}")
        return route.continue_()

    def fetch(self, tmdb_id: int) -> str:
        """The awards page, or "" if TMDb says the film is gone (404). Anything else (a firewall
        challenge that did not clear, an error page, a timeout) raises AwardsChallenged, so the
        film is retried later instead of being recorded as 'none'."""
        page = self.ctx.new_page()
        try:
            resp = page.goto(f"{self.site}/movie/{tmdb_id}/awards?language=en-US", timeout=self.timeout_ms, wait_until="domcontentloaded")
            if resp is not None and resp.status == 404:
                return ""
            try:    # a firewall challenge reloads itself into the real page once solved: wait for that
                page.wait_for_function("ms => ms.some(m => document.documentElement.innerHTML.includes(m))",
                                       arg=list(AWARDS_PAGE_MARKERS), polling=500, timeout=self.timeout_ms)
            except Exception:
                pass
            page.wait_for_timeout(800)
            html = page.content()
        finally:
            page.close()
        return _require_awards_page(html, f"browser, tmdb {tmdb_id}")

    def close(self):
        with contextlib.suppress(Exception):
            self.browser.close()
        with contextlib.suppress(Exception):
            self._pw.stop()


def tmdb_awards_stage(cfg: R.Config, con, limit: int | None = None, sleep: float = TMDB_AWARDS_SLEEP, fetch_fn=None,
                      verbose=True, probe: str | None = None, use_browser: bool = False) -> Counter:
    _ensure_status_rows(con)
    browser = None
    if fetch_fn is None:
        try:
            ab = AwardsBrowser(probe=bool(probe))
        except ImportError:
            ab = None
        if use_browser:
            if ab is None:
                raise RuntimeError("playwright is not installed: pip3 install playwright && python3 -m playwright install chromium")
            browser = ab
        else:
            browser = PlainAwardsFetcher(browser=ab)
            if ab is None and verbose:
                print("  (playwright not installed: plain requests only; if TMDb challenges them the run stops)")
        fetch_fn = browser.fetch
    challenged = 0
    try:
        if probe:
            tid = con.execute("SELECT tmdb_id FROM films WHERE '|'||tconsts||'|' LIKE ?", (f"%|{probe}|%",)).fetchone()
            if not tid:
                print(f"{probe} not in the universe"); return Counter()
            try:
                html = fetch_fn(tid[0])
            except AwardsChallenged as e:
                print(f"tmdb {tid[0]}: plain request was NOT served an awards page ({e}) and playwright is not installed")
                if isinstance(browser, PlainAwardsFetcher) and browser.last_block_body:
                    p = cfg.out_dir / f"tmdb_block_{tid[0]}.html"; p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_text(browser.last_block_body, encoding="utf-8"); print(f"blocked response saved -> {p}")
                return Counter(challenged=1)
            rows, sn, sw = parse_awards_html(html)
            how = "browser"
            if isinstance(browser, PlainAwardsFetcher):
                how = f"{browser.last_mode}; plain client {browser.client} HTTP {browser.last_status}; browser primes so far {browser.primes}"
            print(f"tmdb {tid[0]}: {len(rows)} award rows parsed; page states nominations={sn} wins={sw}  ({how})")
            for r in rows[:5]:
                print(f"    {r['result']:8} {r['award_body']} / {r['ceremony_label']} / {r['category']} [{r['category_id']}]")
            if isinstance(browser, PlainAwardsFetcher) and browser.primes:
                # does the browser's token make the NEXT plain request work?
                html2 = browser.fetch(tid[0])
                r2 = parse_awards_html(html2)[0]
                print(f"second fetch with the browser's cookies: mode={browser.last_mode}, HTTP {browser.last_status}, {len(r2)} rows -> "
                      f"{'plain requests work once primed; the sweep will run mostly without the browser' if browser.last_mode == 'plain' else 'still challenged; the sweep will fall back to browser-only after 5 in a row'}")
                print(f"cookies now held: {_cookie_names(browser.s)}")
            if isinstance(browser, AwardsBrowser):
                print("network calls the page made (candidates for a JSON endpoint):")
                for u in browser.seen_xhr:
                    print("   ", u)
            p = cfg.out_dir / f"tmdb_awards_{tid[0]}.html"; p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(html, encoding="utf-8"); print(f"page saved -> {p}")
            return Counter(probed=1)
        todo = con.execute("""
            SELECT s.tmdb_id, s.tconst FROM enrich_status s LEFT JOIN film_summary fs USING(tmdb_id)
            LEFT JOIN film_metadata m USING(tmdb_id)
            WHERE s.tmdb_awards_status IS NULL OR s.tmdb_awards_status='error'
            ORDER BY CASE WHEN fs.us_theatrical IS NOT NULL THEN 0 WHEN fs.first_theatrical IS NOT NULL THEN 1 ELSE 2 END,
                     COALESCE(m.vote_count, 0) DESC, s.tmdb_id""").fetchall()
        if limit:
            todo = todo[:limit]
        counts = Counter()
        t0 = time.monotonic()
        consecutive_errors = 0
        if verbose:
            print(f"TMDB AWARDS  {len(todo)} films; {sleep:.1f}s between pages; "
                  f"{'browser only' if isinstance(browser, AwardsBrowser) else 'plain requests, browser primes the WAF token' if getattr(browser, 'browser', None) else 'plain requests'}")
        with R.RunLog(con, "tmdb_awards", f"n={len(todo)}") as rl:
            try:
                for i, (tid, tconst) in enumerate(todo, 1):
                    try:
                        html = fetch_fn(tid)
                        consecutive_errors = challenged = 0
                    except AwardsChallenged as e:
                        challenged += 1
                        counts["challenged"] += 1
                        if challenged >= AWARDS_CHALLENGE_LIMIT:
                            raise RuntimeError(f"TMDb is not serving awards pages to plain requests ({challenged} in a row: {e}) -- "
                                               f"wait, or re-run with --browser")
                        if verbose:
                            print(f"  not served an awards page ({e}); pausing 300s")
                        time.sleep(300 * BACKOFF_SCALE)
                        continue
                    except Exception as e:
                        consecutive_errors += 1
                        with con:
                            con.execute("UPDATE enrich_status SET tmdb_awards_status='error', tmdb_awards_at=? WHERE tmdb_id=?", (R.utcnow(), tid))
                        counts["error"] += 1
                        if consecutive_errors >= 10:
                            raise RuntimeError(f"10 page loads failed in a row -- stopping ({R.redact(e)})")
                        continue
                    rows, sn, sw = parse_awards_html(html)
                    now = R.utcnow()
                    status = "ok" if rows or sn else "none"
                    with con:
                        con.execute("INSERT OR REPLACE INTO tmdb_awards_pages VALUES (?,?,?,?,?,?,?)",
                                    (tid, now, sn, sw, len(rows), AWARDS_PARSER_VERSION,
                                     zlib.compress(_strip_awards(html).encode(), 6) if rows or sn else None))
                        con.execute("DELETE FROM tmdb_awards WHERE tmdb_id=?", (tid,))
                        con.executemany("INSERT OR REPLACE INTO tmdb_awards VALUES (?,?,?,?,?,?,?,?,?)",
                                        [(tid, tconst, r["award_body"], r["award_body_id"], r["ceremony_label"], r["ceremony_year"],
                                          r["category"], r["category_id"], r["result"]) for r in rows])
                        con.execute("UPDATE enrich_status SET tmdb_awards_status=?, tmdb_awards_at=? WHERE tmdb_id=?", (status, now, tid))
                    counts[status] += 1
                    if sn is not None and len(rows) != sn:
                        counts["count_mismatch"] += 1
                    rl.n["ok" if status == "ok" else "gone"] += 1
                    time.sleep(sleep * random.uniform(0.8, 1.3) * BACKOFF_SCALE)
                    if verbose and i % 100 == 0:
                        el = time.monotonic() - t0
                        primes = f" primes={browser.primes}{' BROWSER-ONLY' if browser.browser_only else ''}" if isinstance(browser, PlainAwardsFetcher) else ""
                        print(f"  {i}/{len(todo)}  ok={counts['ok']} none={counts['none']} err={counts['error']} "
                              f"mismatch={counts['count_mismatch']}{primes}  {el/i:.1f}s/film  ETA {(len(todo)-i)*el/i/3600:.1f} h")
            except KeyboardInterrupt:
                if verbose:
                    print("\n  interrupted -- everything fetched so far is committed")
                raise
        if verbose:
            print(f"  done: {dict(counts)}")
        return counts
    finally:
        if browser:
            browser.close()


# =============================================================================
# 5. EXPORT + REPORT
# =============================================================================
def export_stage(cfg: R.Config, con, verbose=True) -> dict:
    out = cfg.out_dir
    w = {}
    meta = {t: (tc or "", ti or "") for t, tc, ti in con.execute("SELECT tmdb_id, tconsts, title FROM films")}

    def awards_rows():
        for r in con.execute("SELECT tmdb_id, tconst, award_body, award_body_id, ceremony_label, ceremony_year, category, "
                             "category_id, result FROM tmdb_awards ORDER BY tmdb_id, ceremony_year"):
            yield (r[0], r[1], meta.get(r[0], ("", ""))[1], "tmdb", r[2], r[3], r[6], r[7], r[5], r[8], "", "", "", r[4])
        fest_labels = dict(con.execute("SELECT qid, label FROM festival_registry"))
        for r in con.execute("SELECT tmdb_id, tconst, kind, award_qid, award_label, when_date, body_qid, body_label, fest_qid, family "
                             "FROM wd_awards ORDER BY tmdb_id, when_date"):
            yield (r[0], r[1], meta.get(r[0], ("", ""))[1], "wikidata", r[7] or "", r[6] or "", r[4], r[3],
                   (r[5] or "")[:4] or "", "Winner" if r[2] == "won" else "Nominee", r[9], r[8] or "",
                   fest_labels.get(r[8], "") if r[8] else "", r[5] or "")
    w["awards.csv"] = R._atomic_csv(out / "awards.csv",
        ["tmdb_id", "tconst", "title", "source", "award_body", "award_body_id", "category", "category_id", "ceremony_year",
         "result", "family", "fest_qid", "fest_label", "ceremony_or_date"], awards_rows())

    def ratings_rows():
        q = """SELECT s.tmdb_id, s.tconst, i.rt_id, i.mc_id, r.critic_score, r.critic_count, r.critic_certified, r.audience_score,
                      r.audience_count, r.audience_verified, r.fetched_at, o.rt_critic, o.metacritic, o.imdb_rating, o.imdb_votes,
                      o.rated, o.awards_text, o.oscar_wins, o.oscar_nominations, o.total_wins, o.total_nominations, o.fetched_at,
                      m.imdb_rating, m.imdb_votes, m.vote_average, m.vote_count, s.rt_status, s.omdb_status
               FROM enrich_status s LEFT JOIN wd_film_ids i USING(tmdb_id) LEFT JOIN rt_scores r USING(tmdb_id)
               LEFT JOIN omdb_ratings o USING(tmdb_id) LEFT JOIN film_metadata m USING(tmdb_id) ORDER BY s.tmdb_id"""
        for (tid, tc, rt_id, mc_id, cs, cc, ccert, as_, ac, aver, rt_at, o_rt, o_mc, o_ir, o_iv, rated, aw, ow, on_, tw, tn, o_at,
             m_ir, m_iv, tva, tvc, rts, oms) in con.execute(q):
            critic = cs if cs is not None else o_rt
            critic_src = "rt_page" if cs is not None else ("omdb" if o_rt is not None else "")
            imdb_r = o_ir if o_ir is not None else m_ir
            imdb_src = "omdb" if o_ir is not None else ("imdb.db" if m_ir is not None else "")
            yield (tid, tc, meta.get(tid, ("", ""))[1], critic, critic_src, cc, ccert, as_, ac, aver, o_mc, imdb_r, imdb_src,
                   o_iv if o_iv is not None else m_iv, tva, tvc, rated, aw, ow, on_, tw, tn, rt_id or "", mc_id or "",
                   rts or "", oms or "", rt_at or o_at or "")
    w["ratings.csv"] = R._atomic_csv(out / "ratings.csv",
        ["tmdb_id", "tconst", "title", "rt_critic", "rt_critic_source", "rt_critic_count", "rt_certified_fresh", "rt_audience",
         "rt_audience_count", "rt_audience_verified", "metacritic", "imdb_rating", "imdb_rating_source", "imdb_votes",
         "tmdb_vote_average", "tmdb_vote_count", "omdb_rated", "omdb_awards_text", "oscar_wins", "oscar_nominations",
         "total_wins", "total_nominations", "rt_id", "metacritic_id", "rt_status", "omdb_status", "as_of"], ratings_rows())

    def dist_rows():
        for r in con.execute("SELECT tmdb_id, tconst, dist_label, dist_qid, dist_country, place_iso, place_label, format, start_date "
                             "FROM wd_distributors ORDER BY tmdb_id"):
            yield (r[0], r[1], meta.get(r[0], ("", ""))[1], "wikidata", r[2], r[3], r[4] or "", r[5] or "", r[6] or "", r[7] or "", r[8] or "")
        if R.has_table(con, "bo_summary"):
            for tid, tc, d, ud, dd in con.execute("SELECT tmdb_id, tconst, distributor, uk_distributor, domestic_release_date FROM bo_summary"):
                if d:
                    yield (tid, tc, meta.get(tid, ("", ""))[1], "bom", d, "", "", "US", "Domestic (US/CA)", "theatrical", dd or "")
                if ud:
                    yield (tid, tc, meta.get(tid, ("", ""))[1], "bom", ud, "", "", "GB", "United Kingdom", "theatrical", "")
    w["distributors.csv"] = R._atomic_csv(out / "distributors.csv",
        ["tmdb_id", "tconst", "title", "source", "distributor", "distributor_qid", "distributor_country", "territory_iso",
         "territory", "format", "start_date"], dist_rows())

    cov = coverage(con)
    w["enrichment_coverage.csv"] = R._atomic_csv(out / "enrichment_coverage.csv", ["measure", "films"],
                                                 [(k, v) for k, v in cov.items()])
    if verbose:
        print("EXPORT")
        for k, v in w.items():
            print(f"  {k:<28}{v:>9} rows")
        print(f"  -> {out}")
    return w


def coverage(con) -> dict:
    c = {}
    c["films_ok"] = con.execute("SELECT COUNT(*) FROM films WHERE status='ok'").fetchone()[0]
    c["wikidata_done"] = con.execute("SELECT COUNT(*) FROM enrich_status WHERE wikidata_at IS NOT NULL").fetchone()[0]
    c["wikidata_item_found"] = con.execute("SELECT COUNT(*) FROM wd_film_ids").fetchone()[0]
    c["films_with_wd_awards"] = con.execute("SELECT COUNT(DISTINCT tmdb_id) FROM wd_awards").fetchone()[0]
    c["wd_award_rows"] = con.execute("SELECT COUNT(*) FROM wd_awards").fetchone()[0]
    c["wd_festival_award_rows"] = con.execute("SELECT COUNT(*) FROM wd_awards WHERE fest_qid IS NOT NULL").fetchone()[0]
    c["films_with_rt_id"] = con.execute("SELECT COUNT(*) FROM wd_film_ids WHERE rt_id IS NOT NULL").fetchone()[0]
    c["films_with_mc_id"] = con.execute("SELECT COUNT(*) FROM wd_film_ids WHERE mc_id IS NOT NULL").fetchone()[0]
    c["films_with_wd_distributor"] = con.execute("SELECT COUNT(DISTINCT tmdb_id) FROM wd_distributors").fetchone()[0]
    c["omdb_ok"] = con.execute("SELECT COUNT(*) FROM enrich_status WHERE omdb_status='ok'").fetchone()[0]
    c["omdb_rt_critic"] = con.execute("SELECT COUNT(*) FROM omdb_ratings WHERE rt_critic IS NOT NULL").fetchone()[0]
    c["omdb_metacritic"] = con.execute("SELECT COUNT(*) FROM omdb_ratings WHERE metacritic IS NOT NULL").fetchone()[0]
    c["rt_ok"] = con.execute("SELECT COUNT(*) FROM enrich_status WHERE rt_status='ok'").fetchone()[0]
    c["rt_audience_scores"] = con.execute("SELECT COUNT(*) FROM rt_scores WHERE audience_score IS NOT NULL").fetchone()[0]
    c["tmdb_awards_done"] = con.execute("SELECT COUNT(*) FROM enrich_status WHERE tmdb_awards_status IN ('ok','none')").fetchone()[0]
    c["films_with_tmdb_awards"] = con.execute("SELECT COUNT(DISTINCT tmdb_id) FROM tmdb_awards").fetchone()[0]
    c["tmdb_award_rows"] = con.execute("SELECT COUNT(*) FROM tmdb_awards").fetchone()[0]
    return c


def report(cfg: R.Config, con):
    print("ENRICHMENT")
    for k, v in coverage(con).items():
        print(f"  {k:<28}{v:>9}")
    for k, n in con.execute("SELECT family, COUNT(*) FROM wd_awards GROUP BY family ORDER BY 2 DESC"):
        print(f"    wd award family {k:<18}{n:>8}")
    for r in con.execute("SELECT stage, started, finished, n_done, n_ok, n_gone, n_error, note FROM runs "
                         "WHERE stage IN ('wd_enrich','omdb','rt','tmdb_awards') ORDER BY run_id DESC LIMIT 6"):
        print(f"  {r[0]:<12} {r[1]} -> {r[2] or '...':<20} done={r[3]} ok={r[4]} other={r[5]} err={r[6]} {r[7] or ''}")


# =============================================================================
# 6. SELF-TEST
# =============================================================================
def _award_entry_html(entry_id, ceremony_href, ceremony_label, result, category_href, category):
    return (f'<div id="{entry_id}" class="flex"><div><p><a href="{ceremony_href}">{ceremony_label}</a></p>'
            f'<p><span class="rounded-md">{result}</span> <a href="{category_href}">{category}</a></p></div></div>')


def _award_body_html(award_href, award_name, entries_html):
    return (f'<div><div class="flex"><div class="font-semibold leading-9 text-xl"><a href="{award_href}">{award_name}</a></div></div>'
            f'<div class="w-full">{entries_html}</div></div>')


def _awards_page_html(summary_line, body_blocks_html):
    return f'<!DOCTYPE html><html><body><section><p class="text-lg">{summary_line}</p><div class="space-y-12">{body_blocks_html}</div></section></body></html>'


RT_FIXTURE_JSON = """<!DOCTYPE html><html><head><title>Parasite | Rotten Tomatoes</title>
<script src="/x.js"></script><style>.a{}</style></head><body>
<script id="media-scorecard-json" type="application/json">{"criticsScore":{"score":"99","reviewCount":466,"certified":true,"sentiment":"POSITIVE"},
"audienceScore":{"score":"90","ratingCount":25000,"certified":false,"sentiment":"POSITIVE"},"title":"Parasite"}</script>
<media-scorecard></media-scorecard></body></html>"""
RT_FIXTURE_ELEMENT = """<html><body><score-board audiencescore="72" audiencestate="upright" rating="R" tomatometerscore="58"
tomatometerstate="rotten" tomatometercount="120" audiencecount="5000"></score-board></body></html>"""
RT_FIXTURE_NOSCORE = "<html><body><h1>Some Film</h1><p>No score yet</p></body></html>"


def selftest(cfg: R.Config | None = None, verbose=True) -> tuple[int, int]:
    global BACKOFF_SCALE, WD_MIN_BATCH
    import tempfile
    cfg = cfg or R.Config.from_env()
    checks = []

    def ck(name, cond, detail=""):
        checks.append((name, bool(cond), str(detail)[:220]))

    # ---- award classification --------------------------------------------
    seed = R.load_seed(cfg)
    reg = R.Registry({f["qid"]: dict(f, source="seed") for f in seed})
    ck("Palme d'Or with P179 -> Cannes is a festival award", classify_award("Palme d'Or", "Q42369", None, reg) == ("festival", "Q42369"))
    ck("award labelled with a festival in brackets resolves by name", classify_award("Best Actor Award (Cannes Film Festival)", None, None, reg) == ("festival", "Q42369"))
    ck("Academy Award -> oscars", classify_award("Academy Award for Best Picture", "Q19020", None, reg) == ("oscars", None))
    ck("Golden Globe / BAFTA / César / SAG families",
       classify_award("Golden Globe Award for Best Director", None, None, reg)[0] == "golden_globes"
       and classify_award("BAFTA Award for Best Film", None, None, reg)[0] == "bafta"
       and classify_award("César Award for Best Film", None, None, reg)[0] == "national_academy"
       and classify_award("Screen Actors Guild Award for Outstanding Cast", None, None, reg)[0] == "guild")
    ck("unknown award -> other, no festival", classify_award("Golden Duck of Cracow", None, None, reg) == ("other", None))
    ck("year-precision P585 stays a year", _when("2020-01-01T00:00:00Z", "9") == "2020" and _when("2020-05-25T00:00:00Z", "11") == "2020-05-25")
    ent = "http://www.wikidata.org/entity/"
    bind = [
        {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "kind": {"value": "won"}, "award": {"value": ent + "Q179808"},
         "awardLabel": {"value": "Palme d'Or"}, "when": {"value": "2019-05-25T00:00:00Z"}, "prec": {"value": "11"},
         "rank": {"value": "http://wikiba.se/ontology#NormalRank"}, "body": {"value": ent + "Q42369"}, "bodyLabel": {"value": "Cannes Film Festival"}},
        {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "kind": {"value": "nominated"}, "award": {"value": ent + "Q102427"},
         "awardLabel": {"value": "Academy Award for Best Picture"}, "when": {"value": "2020-01-01T00:00:00Z"}, "prec": {"value": "9"},
         "rank": {"value": "http://wikiba.se/ontology#NormalRank"}},
        {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "kind": {"value": "won"}, "award": {"value": ent + "Q102427"},
         "awardLabel": {"value": "Academy Award for Best Picture"}, "when": {"value": "2020-02-09T00:00:00Z"}, "prec": {"value": "11"},
         "rank": {"value": "http://wikiba.se/ontology#PreferredRank"}},
        {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "kind": {"value": "won"}, "award": {"value": ent + "Q102427"},
         "awardLabel": {"value": "Academy Award for Best Picture"}, "when": {"value": "2020-02-09T00:00:00Z"}, "prec": {"value": "11"},
         "rank": {"value": "http://wikiba.se/ontology#NormalRank"}},
    ]
    lab = {"Q179808": "Palme d'Or", "Q102427": "Academy Award for Best Picture", "Q42369": "Cannes Film Festival"}
    rows = finalize_awards(parse_awards_bindings(bind)["tt6751668"], lab, reg)
    ck("awards parsed: 3 distinct (kind, award, date), duplicate statement collapsed to the preferred rank",
       len(rows) == 3 and any(r[0] == "won" and r[1] == "Q102427" and r[-1] == "PreferredRank" for r in rows), rows)
    ck("nomination at year precision keeps the year; festival row carries fest_qid",
       any(r[0] == "nominated" and r[3] == "2020" for r in rows) and any(r[9] == "Q42369" and r[10] == "festival" for r in rows))
    idb = [{"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "rt": {"value": "m/parasite_2019"}, "mc": {"value": "movie/parasite"},
            "dist": {"value": ent + "Q1"}, "distCountry": {"value": "US"}, "place": {"value": ent + "Q30"}, "placeIso": {"value": "US"},
            "fmt": {"value": ent + "Q1000"}, "start": {"value": "2019-10-11T00:00:00Z"}},
           {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "rt": {"value": "m/parasite_2019"},
            "dist": {"value": ent + "Q2"}, "distCountry": {"value": "KR"}},
           {"imdb": {"value": "tt6751668"}, "film": {"value": ent + "Q61448040"}, "rt": {"value": "m/parasite_2019"},
            "dist": {"value": ent + "Q2"}, "distCountry": {"value": "KR"}}]
    lab.update({"Q1": "Neon", "Q2": "CJ Entertainment", "Q30": "United States", "Q1000": "theatrical"})
    ids, dist = parse_ids_dist_bindings(idb)
    ck("ids: RT and Metacritic ids, one row per distributor x place x format",
       ids["tt6751668"] == {"qid": "Q61448040", "rt": "m/parasite_2019", "mc": "movie/parasite"} and len(dist["tt6751668"]) == 2, (ids, dist))
    fd = finalize_dist(dist["tt6751668"], lab)
    ck("distributors labelled from the cache: Neon / United States / theatrical; unlabelled place stays None",
       ("Q1", "Neon", "US", "US", "United States", "theatrical", "2019-10-11") in fd and any(r[0] == "Q2" and r[1] == "CJ Entertainment" and r[4] is None for r in fd), fd)
    ck("both live queries carry the optimizer hint", 'hint:optimizer "None"' in SPARQL_AWARDS and 'hint:optimizer "None"' in SPARQL_IDS_DIST
       and "wikibase:label" not in SPARQL_AWARDS and "wikibase:label" not in SPARQL_IDS_DIST)

    # ---- OMDb parsing --------------------------------------------------------
    od = {"Response": "True", "Ratings": [{"Source": "Internet Movie Database", "Value": "8.5/10"}, {"Source": "Rotten Tomatoes", "Value": "99%"},
                                          {"Source": "Metacritic", "Value": "97/100"}], "imdbRating": "8.5", "imdbVotes": "1,000,123", "Rated": "R",
          "Awards": "Won 4 Oscars. 314 wins & 271 nominations total", "BoxOffice": "$53,369,749"}
    p = omdb_parse(od)
    ck("omdb: RT 99, MC 97, IMDb 8.5 / 1,000,123 votes, 4 Oscars, 314 wins, 271 nominations",
       (p["rt_critic"], p["metacritic"], p["imdb_rating"], p["imdb_votes"], p["oscar_wins"], p["oscar_nominations"], p["total_wins"], p["total_nominations"])
       == (99, 97, 8.5, 1000123, 4, 0, 314, 271), p)
    ck("omdb: N/A everywhere -> None, not 0", omdb_parse({"Response": "True", "Ratings": [], "imdbRating": "N/A", "imdbVotes": "N/A", "Awards": "N/A"})["rt_critic"] is None
       and omdb_parse({"Response": "True", "Ratings": [], "imdbRating": "N/A", "imdbVotes": "N/A", "Awards": "N/A"})["total_wins"] is None)
    ck("omdb: nominated-only awards line", omdb_parse({"Response": "True", "Awards": "Nominated for 2 Oscars. 5 wins & 20 nominations total"})["oscar_nominations"] == 2)

    # ---- RT parsing ------------------------------------------------------------
    r1 = parse_rt_page(RT_FIXTURE_JSON)
    ck("rt: JSON scorecard -> critic 99 (466, certified), audience 90 (25000)",
       r1.get("critic", {}).get("score") == 99 and r1["critic"]["count"] == 466 and r1["critic"]["certified"] is True
       and r1.get("audience", {}).get("score") == 90 and r1["audience"]["count"] == 25000, r1)
    r2 = parse_rt_page(RT_FIXTURE_ELEMENT)
    ck("rt: score-board element -> critic 58 (120), audience 72 (5000)",
       r2.get("critic", {}).get("score") == 58 and r2["critic"]["count"] == 120 and r2.get("audience", {}).get("score") == 72, r2)
    ck("rt: no score -> empty", parse_rt_page(RT_FIXTURE_NOSCORE) == {})
    st = _strip_rt(RT_FIXTURE_JSON)
    ck("rt: stripping keeps the JSON block, drops js/css, parses the same", "media-scorecard-json" in st and "x.js" not in st
       and parse_rt_page(st) == r1)

    # ---- TMDb awards parser (comp_6 fixtures) ----------------------------------
    lev = _awards_page_html("1 Nomination, 1 Win", _award_body_html("/award/1-astra?language=en-US", "Astra Midseason Movie Awards",
          _award_entry_html("aaa111", "/award/1-astra/ceremony/9?language=en-US", "9th Astra Midseason Movie Awards (2026)", "Winner",
                            "/award/1-astra/category/3-best-independent-film?language=en-US", "Best Independent Film")))
    rows, sn, sw = parse_awards_html(lev)
    ck("tmdb awards: one winner row with slugs and year", len(rows) == 1 and (sn, sw) == (1, 1) and rows[0]["award_body_id"] == "1-astra"
       and rows[0]["category_id"] == "3-best-independent-film" and rows[0]["ceremony_year"] == 2026 and rows[0]["result"] == "Winner", rows)
    zero = _awards_page_html("2 Nominations", _award_body_html("/award/7-guild", "Some Guild Awards",
           _award_entry_html("d1", "/award/7-guild/ceremony/2", "2nd Some Guild Awards (2023)", "Nominee", "/award/7-guild/category/1-best-ensemble", "Best Ensemble")
           + _award_entry_html("d2", "/award/7-guild/ceremony/2", "2nd Some Guild Awards (2023)", "Nominee", "/award/7-guild/category/2-best-score", "Best Score")))
    rows, sn, sw = parse_awards_html(zero)
    ck("tmdb awards: nominations with no Wins clause -> wins 0, not None", len(rows) == 2 and (sn, sw) == (2, 0))
    ck("tmdb awards: a page with nothing -> no rows, no counts", parse_awards_html("<html><body><p>No awards information available.</p></body></html>") == ([], None, None))
    ck("tmdb awards: stripping keeps the entries", parse_awards_html(_strip_awards(zero))[0] == rows)
    live = gzip.open(cfg.fixtures_dir / "tmdb_awards_496243.html.gz", "rt", encoding="utf-8").read()
    lrows, lsn, lsw = parse_awards_html(live)
    ck("tmdb awards LIVE page (Parasite, 2026-10-14): 19 rows = 19 stated nominations, 13 winners, category ids from #category-N",
       len(lrows) == 19 and (lsn, lsw) == (19, 13) and sum(r["result"] == "Winner" for r in lrows) == 13
       and any(r["award_body_id"] == "1-academy-awards" and r["category"] == "Best Picture" and r["category_id"] == "1"
               and r["ceremony_year"] == 2020 and r["result"] == "Winner" for r in lrows), (len(lrows), lsn, lsw))

    # ---- end to end on a temp database ----------------------------------------
    with tempfile.TemporaryDirectory() as td:
        tcfg = R.Config(root=Path(td), db_path=Path(td) / "t.db", out_dir=Path(td) / "out", crosswalk_csv=Path(td) / "cw.csv", fixtures_dir=cfg.fixtures_dir)
        con = connect(tcfg)
        fx = {fid: json.loads((cfg.fixtures_dir / f"tmdb_{fid}.json").read_text(encoding="utf-8")) for fid in (496243, 1050035)}
        R.load_universe(tcfg, con, ids=[496243, 1050035], verbose=False)
        con.execute("UPDATE films SET tconsts='tt6751668' WHERE tmdb_id=496243")
        con.execute("UPDATE films SET tconsts='tt23736044' WHERE tmdb_id=1050035"); con.commit()
        R.write_results(con, [R.FetchResult(f, "ok", 200, fx[f]) for f in fx], tcfg)
        R.ledger_stage(tcfg, con, verbose=False); R.seed_registry(tcfg, con, verbose=False)
        R.windows_stage(tcfg, con, verbose=False)
        old, BACKOFF_SCALE = BACKOFF_SCALE, 0.001
        sc = R._Scripted(); srv, base = R._fake_server(sc)
        try:
            # fake WDQS: awards query then ids query (dispatch by body content)
            label_bind = [{"item": {"value": ent + q}, "itemLabel": {"value": l}} for q, l in lab.items()]
            def sparql_body(body_in):
                body = body_in.decode("utf-8", "replace")
                if "P1411" in body:
                    return json.dumps({"results": {"bindings": bind}}).encode()
                if "itemLabel" in body:
                    return json.dumps({"results": {"bindings": label_bind}}).encode()
                return json.dumps({"results": {"bindings": idb}}).encode()
            sc.add("/sparql", (200, {}, sparql_body))
            w = R.WdqsClient(con, url=base + "/sparql", min_gap=0)
            st = wikidata_stage(tcfg, con, wdqs=w, verbose=False)
            ck("wikidata stage: both films processed; awards, ids, distributors stored",
               st["films"] == 2 and st["awards"] == 3 and st["ids"] == 1 and st["distributors"] == 2 and st["rt_ids"] == 1, st)
            ck("wikidata stage: re-run does nothing", wikidata_stage(tcfg, con, wdqs=w, verbose=False)["films"] == 0)
            ck("wikidata stage: labels resolved through the cache and stored on the rows",
               con.execute("SELECT award_label, body_label FROM wd_awards WHERE tmdb_id=496243 AND award_qid='Q179808'").fetchone()
               == ("Palme d'Or", "Cannes Film Festival") and con.execute("SELECT COUNT(*) FROM wd_labels").fetchone()[0] == 7
               and con.execute("SELECT dist_label, place_label, format FROM wd_distributors WHERE tmdb_id=496243 AND dist_qid='Q1'").fetchone()
               == ("Neon", "United States", "theatrical"))
            n_label_calls = sum(1 for p_, h in sc.seen if "itemLabel" in h.get("_body", ""))
            ck("wikidata stage: a second film with the same awards costs no new label query",
               (wikidata_stage(tcfg, con, wdqs=w, verbose=False), sum(1 for p_, h in sc.seen if "itemLabel" in h.get("_body", "")))[1] == n_label_calls)
            # a batch whose full query times out is split, and a half that keeps failing stays pending
            con.execute("UPDATE enrich_status SET wikidata_at=NULL"); con.execute("DELETE FROM wdqs_cache WHERE key LIKE 'enrich:%'"); con.commit()
            calls = {"n": 0}
            def flaky(body_in):
                body = body_in.decode("utf-8", "replace")
                calls["n"] += 1
                if "itemLabel" in body:
                    return json.dumps({"results": {"bindings": label_bind}}).encode()
                if body.count('"tt') >= 2 or "tt23736044" in body:      # whole batch, and Monster's half, time out
                    return b"timeout"
                return json.dumps({"results": {"bindings": bind if "P1411" in body else idb}}).encode()
            sc.add("/sparql", (504, {}, b"x"))
            sc.script["/sparql"] = [(200, {}, flaky)]
            # the fake answers 200 with a non-JSON body for the failing cases -> WdqsClient retries then raises
            w2 = R.WdqsClient(con, url=base + "/sparql", min_gap=0)
            old_min, WD_MIN_BATCH = WD_MIN_BATCH, 1
            st2 = wikidata_stage(tcfg, con, wdqs=w2, verbose=False)
            WD_MIN_BATCH = old_min
            ck("wikidata stage: timed-out batch is split; the good half completes, the bad half stays pending",
               st2["films"] == 1 and st2["failed_batches"] >= 1 and
               con.execute("SELECT wikidata_at IS NULL FROM enrich_status WHERE tmdb_id=1050035").fetchone()[0] == 1 and
               con.execute("SELECT wikidata_at IS NOT NULL FROM enrich_status WHERE tmdb_id=496243").fetchone()[0] == 1, st2)
            sc.script["/sparql"] = [(200, {}, sparql_body)]
            wikidata_stage(tcfg, con, wdqs=R.WdqsClient(con, url=base + "/sparql", min_gap=0), verbose=False)
            ck("wikidata stage: film without an item still marked done, no ids row",
               con.execute("SELECT wikidata_at IS NOT NULL, wikidata_qid FROM enrich_status WHERE tmdb_id=1050035").fetchone() == (1, None))
            # fake OMDb
            sc.add("/omdb", (200, {}, od), (200, {}, {"Response": "False", "Error": "Request limit reached!"}))
            global OMDB_URL
            old_url, OMDB_URL = OMDB_URL, base + "/omdb"
            os.environ["OMDB_API_KEY"] = "SECRETOMDB"
            oc = omdb_stage(tcfg, con, limit=5, verbose=False)
            ck("omdb: first film ok, the limit answer stops the run without marking the second film",
               oc["ok"] == 1 and oc["limit"] == 1 and dict(con.execute("SELECT tmdb_id, omdb_status FROM enrich_status WHERE omdb_status IS NOT NULL")) == {496243: "ok"}, oc)
            ck("omdb: ratings row stored", con.execute("SELECT rt_critic, metacritic FROM omdb_ratings WHERE tmdb_id=496243").fetchone() == (99, 97))
            ck("omdb: the key never reaches the run log", "SECRETOMDB" not in " ".join(str(r) for r in con.execute("SELECT note, args FROM runs").fetchall()))
            ck("omdb: priority puts the film with a US theatrical date first", omdb_priority_ids(con, refresh=True)[0][0] == 496243)
            OMDB_URL = old_url
            # fake RT
            sc.add("/m/parasite_2019", (200, {}, RT_FIXTURE_JSON.encode()))
            rc = rt_stage(tcfg, con, base=base, sleep=0, verbose=False)
            ck("rt: the film with an RT id is scraped; the one without is no_id",
               rc["ok"] == 1 and dict(con.execute("SELECT tmdb_id, rt_status FROM enrich_status")) == {496243: "ok", 1050035: "no_id"}, (rc, dict(con.execute("SELECT tmdb_id, rt_status FROM enrich_status"))))
            ck("rt: scores stored", con.execute("SELECT critic_score, audience_score, critic_certified FROM rt_scores WHERE tmdb_id=496243").fetchone() == (99, 90, 1))
            ck("rt: page cached, re-run is free", rt_stage(tcfg, con, base=base, sleep=0, verbose=False) == Counter()
               and con.execute("SELECT COUNT(*) FROM rt_pages").fetchone()[0] == 1)
            # TMDb awards via an injected fetch function (no browser)
            pages = {496243: lev, 1050035: "<html><body><p>No awards information available.</p></body></html>"}
            ac = tmdb_awards_stage(tcfg, con, fetch_fn=lambda tid: pages[tid], sleep=0, verbose=False)
            ck("tmdb awards: ok + none, rows stored, statuses set", ac["ok"] == 1 and ac["none"] == 1
               and con.execute("SELECT COUNT(*) FROM tmdb_awards WHERE tmdb_id=496243").fetchone()[0] == 1
               and dict(con.execute("SELECT tmdb_id, tmdb_awards_status FROM enrich_status")) == {496243: "ok", 1050035: "none"}, ac)
            ck("tmdb awards: re-run does nothing", tmdb_awards_stage(tcfg, con, fetch_fn=lambda tid: pages[tid], sleep=0, verbose=False) == Counter())
            def failing(tid):
                raise RuntimeError("boom")
            con.execute("UPDATE enrich_status SET tmdb_awards_status=NULL WHERE tmdb_id=1050035"); con.commit()
            fc = tmdb_awards_stage(tcfg, con, fetch_fn=failing, sleep=0, verbose=False)
            ck("tmdb awards: a failed page load is an error to retry, not a 'none'", fc["error"] == 1
               and con.execute("SELECT tmdb_awards_status FROM enrich_status WHERE tmdb_id=1050035").fetchone()[0] == "error")
            # plain-requests fetcher against the fake site: a real page, a 404, then a WAF challenge x3
            sc.add("/movie/496243/awards", (200, {}, live.encode()))
            sc.add("/movie/1050035/awards", (404, {}, b"gone"))
            pf = PlainAwardsFetcher(site=base)
            ck("plain fetcher: awards page returned, 404 -> empty string",
               parse_awards_html(pf.fetch(496243))[1] == 19 and pf.fetch(1050035) == "")
            sc.add("/movie/496243/awards", (202, {}, b"<html><script src='challenge.js'></script></html>"))
            try:
                pf.fetch(496243); chal = False
            except AwardsChallenged:
                chal = True
            ck("plain fetcher: a WAF challenge page raises AwardsChallenged instead of parsing as 'none'", chal)
            # with a browser attached: a 403 is answered by one browser fetch that primes the cookies
            class _StubBrowser:
                def __init__(self): self.calls = 0; self.ctx = self
                def fetch(self, tid): self.calls += 1; return live
                def cookies(self): return [{"name": "aws-waf-token", "value": "tok", "domain": "127.0.0.1", "path": "/"}]
                def close(self): pass
            sb = _StubBrowser(); pfb = PlainAwardsFetcher(site=base, browser=sb)
            sc.add("/movie/496243/awards", (403, {}, b"<html>challenge</html>"), (200, {}, live.encode()))
            h1 = pfb.fetch(496243); h2 = pfb.fetch(496243)
            ck("plain fetcher + browser: 403 -> browser primes (1 call), next plain request succeeds with the token",
               parse_awards_html(h1)[1] == 19 and sb.calls == 1 and pfb.primes == 1 and pfb.last_mode == "plain"
               and parse_awards_html(h2)[1] == 19 and "aws-waf-token" in _cookie_names(pfb.s), (sb.calls, pfb.primes, pfb.last_mode))
            sc.add("/movie/496243/awards", (403, {}, b"<html>challenge</html>"))
            n_before = len(sc.seen)
            for _ in range(6):
                pfb.fetch(496243)
            ck("plain fetcher + browser: five primes in a row -> browser-only, no more plain requests",
               pfb.browser_only and sb.calls == 7 and len(sc.seen) - n_before == 5, (pfb.browser_only, sb.calls, len(sc.seen) - n_before))
            sb.fetch = lambda tid: "<html><script src='challenge.js'></script></html>"
            try:
                pfb.fetch(496243); bchal = False
            except AwardsChallenged:
                bchal = True
            ck("browser-only: a challenge page served to the browser raises AwardsChallenged instead of parsing as 'none'", bchal)
            con.execute("UPDATE enrich_status SET tmdb_awards_status=NULL"); con.commit()
            sc.script["/movie/496243/awards"] = [(202, {}, b"<html>challenge</html>")] * 3
            sc.script["/movie/1050035/awards"] = [(202, {}, b"<html>challenge</html>")] * 3
            global AWARDS_CHALLENGE_LIMIT
            old_lim, AWARDS_CHALLENGE_LIMIT = AWARDS_CHALLENGE_LIMIT, 2      # the test universe has two films
            try:
                tmdb_awards_stage(tcfg, con, fetch_fn=pf.fetch, sleep=0, verbose=False); stopped = False
            except RuntimeError as e:
                stopped = "browser" in str(e)
            AWARDS_CHALLENGE_LIMIT = old_lim
            ck("plain fetcher: three challenges in a row stop the run and no film is marked",
               stopped and con.execute("SELECT COUNT(*) FROM enrich_status WHERE tmdb_awards_status IS NOT NULL").fetchone()[0] == 0)
            con.execute("UPDATE enrich_status SET tmdb_awards_status='ok' WHERE tmdb_id=496243"); con.execute("UPDATE enrich_status SET tmdb_awards_status='error' WHERE tmdb_id=1050035"); con.commit()
            w_ = export_stage(tcfg, con, verbose=False)
            ck("export: awards from both sources, ratings for every film, distributors", w_["awards.csv"] == 4 and w_["ratings.csv"] == 2 and w_["distributors.csv"] == 2, w_)
            with open(tcfg.out_dir / "ratings.csv", newline="", encoding="utf-8") as fh:
                rr = {r["tmdb_id"]: r for r in csv.DictReader(fh)}
            ck("export: RT page score preferred over OMDb's for the critic column, source labelled",
               rr["496243"]["rt_critic"] == "99" and rr["496243"]["rt_critic_source"] == "rt_page" and rr["496243"]["rt_audience"] == "90"
               and rr["496243"]["metacritic"] == "97", rr["496243"])
            ck("export: a film with no ratings at all has blanks and status columns, not zeros",
               rr["1050035"]["rt_critic"] == "" and rr["1050035"]["rt_status"] == "no_id", rr["1050035"])
            with open(tcfg.out_dir / "awards.csv", newline="", encoding="utf-8") as fh:
                ar = list(csv.DictReader(fh))
            ck("export: Palme d'Or row carries fest_qid and label; TMDb row carries slugs",
               any(r["source"] == "wikidata" and r["fest_qid"] == "Q42369" and r["fest_label"].startswith("Cannes") and r["result"] == "Winner" for r in ar)
               and any(r["source"] == "tmdb" and r["award_body_id"] == "1-astra" for r in ar), ar[:2])
        finally:
            BACKOFF_SCALE = old
            with contextlib.suppress(Exception):
                srv.shutdown(); srv.server_close()

    passed = sum(1 for _, ok, _ in checks if ok)
    if verbose:
        print("=" * 88)
        for name, ok, detail in checks:
            if not ok:
                print(f"  FAIL  {name}   [{detail}]")
        print(f"{passed}/{len(checks)} checks passed")
        print("=" * 88)
    return passed, len(checks)


# =============================================================================
# CLI
# =============================================================================
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db")
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("wikidata"); w.add_argument("--limit", type=int)
    o = sub.add_parser("omdb"); o.add_argument("--limit", type=int, default=OMDB_DAILY_LIMIT); o.add_argument("--refresh", action="store_true")
    r = sub.add_parser("rt"); r.add_argument("--limit", type=int); r.add_argument("--sleep", type=float, default=RT_SLEEP)
    r.add_argument("--dump", help="tconst (or RT path m/...) whose page to save to out/ for inspection")
    t = sub.add_parser("tmdb_awards"); t.add_argument("--limit", type=int); t.add_argument("--sleep", type=float, default=TMDB_AWARDS_SLEEP)
    t.add_argument("--probe", help="tconst: fetch one page, show what parsed, save the HTML")
    t.add_argument("--browser", action="store_true", help="use headless Chromium (Playwright) instead of plain requests")
    sub.add_parser("export"); sub.add_parser("report"); sub.add_parser("selftest")
    a = ap.parse_args(argv)
    cfg = R.Config.from_env()
    if a.db:
        cfg.db_path = Path(a.db); cfg.out_dir = cfg.db_path.parent / "out"
    if a.cmd == "selftest":
        ok, total = selftest(cfg)
        return 0 if ok == total else 1
    con = connect(cfg)
    print(f"[{cfg.db_path}]")
    if a.cmd == "wikidata":
        wikidata_stage(cfg, con, limit=a.limit)
    elif a.cmd == "omdb":
        omdb_stage(cfg, con, limit=a.limit, refresh=a.refresh)
    elif a.cmd == "rt":
        rt_stage(cfg, con, limit=a.limit, sleep=a.sleep, dump=a.dump)
    elif a.cmd == "tmdb_awards":
        tmdb_awards_stage(cfg, con, limit=a.limit, sleep=a.sleep, probe=a.probe, use_browser=a.browser)
    elif a.cmd == "export":
        export_stage(cfg, con)
    elif a.cmd == "report":
        report(cfg, con)
    return 0


if __name__ == "__main__":
    sys.exit(main())
