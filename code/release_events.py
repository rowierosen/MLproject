#!/usr/bin/env python3
"""
RELEASE EVENTS -- every TMDb release date, every country, every film.

Scales the Festival_Pipeline_Test notebook (Parasite / Monster) to the whole
IMDb<->TMDb crosswalk: one `append_to_response` call per TMDb id, the verbatim
payload persisted, and everything downstream a pure function over that store.

Stages (each is a CLI sub-command and a Python function):

  universe   crosswalk -> films table (one row per tmdb_id; tconsts joined)
  fetch      concurrent, rate-limited, resumable capture of raw payloads
  changes    mark ids TMDb reports as changed (/movie/changes) for re-fetch
  credits    /movie/{id}/credits for every fetched film (cast + crew by TMDb id)
  metadata   film_metadata: TMDb payload + credits + imdb.db (imdb_match/imdb.db)
  ledger     raw -> events: all six release types, every country, note parsed
  registry   festival registry: verified seed + Wikidata (labels, aliases,
             country, usual month, edition windows)
  resolve    evidence-scored festival resolution of every note -> appearances
  windows    per film x country release windows and per-film summary
  export     CSVs (events, appearances, windows, summary, review queue,
             coverage ledger); --compat writes a 01_events-shaped file
  report     the coverage ledger on screen
  selftest   offline checks + fault-injecting fake TMDb / WDQS over real HTTP

Run order for a full build:

  export TMDB_API_KEY=...            # v3 key   (or TMDB_READ_TOKEN, v4 bearer)
  python release_events.py universe
  python release_events.py fetch --limit 500      # read the output, then:
  python release_events.py fetch                  # ~4-5 h for 380k at 25 rps
  python release_events.py ledger
  python release_events.py registry --refresh     # Wikidata; ~10-20 min
  python release_events.py resolve
  python release_events.py windows
  python release_events.py export

Every stage is incremental and Ctrl-C safe: re-running does only what is not
done yet. Keys are read from the environment and never written to disk or
printed; every error string passes through redact().

Principles carried over from the rest of the project:
  * "No data" is never rendered as zero. A film that was never fetched, a
    film TMDb no longer has (404), and a film with no release dates are three
    different states and are stored as three different states.
  * Every number is traceable: each resolution carries its evidence string,
    each rejection its reason, and nothing is silently dropped.
  * The raw payload is the source of truth. A parser change costs a re-run of
    `ledger`, never a re-fetch.
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import email.utils
import io
import json
import logging
import math
import os
import re
import sqlite3
import sys
import threading
import time
import unicodedata
import urllib.parse
import zlib
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable, Iterator

import requests

__version__ = "0.3.0"
LEDGER_VERSION = 3        # bump when flatten()/parse_note()/classify_note() change
RESOLVER_VERSION = 4      # bump when resolve()/scoring change

log = logging.getLogger("release_events")

# =============================================================================
# 0. CONFIGURATION
# =============================================================================
TMDB_BASE = os.environ.get("TMDB_BASE", "https://api.themoviedb.org/3")
WDQS_URL = os.environ.get("WDQS_URL", "https://query.wikidata.org/sparql")
USER_AGENT = f"CompEngine-ReleaseEvents/{__version__} (rowanrosenblum@gmail.com)"

# release_dates.type codes. Verified against the TMDb API reference; nothing
# above 6 exists. Unknown codes are kept verbatim and labelled unknown(<n>).
TYPE_NAMES = {1: "Premiere", 2: "Theatrical (limited)", 3: "Theatrical",
              4: "Digital", 5: "Physical", 6: "TV"}
TYPE_CHANNEL = {1: "premiere", 2: "theatrical", 3: "theatrical",
                4: "digital", 5: "physical", 6: "tv"}

DEFAULT_APPEND = "release_dates,external_ids"
DEFAULT_RPS = 25.0          # TMDb documents "~40-50/s"; leave headroom
DEFAULT_WORKERS = 8
HTTP_TIMEOUT = 30
MAX_TRIES = 5
RETRY_AFTER_CAP = 120       # seconds; a far-future Retry-After is not obeyed literally
BREAKER_WINDOW = 60         # consecutive failures that abort a run
COMMIT_EVERY = 200          # films per transaction
BACKOFF_SCALE = 1.0         # multiplies every internal back-off sleep; the self-test sets it to ~0


def find_project_root(start: Path | None = None) -> Path:
    """Walk up from the working directory (then from this file) for comp_dataset/.

    The IMDb<->TMDb matching notebook learned this the hard way: a kernel that
    starts one level too deep makes every input vanish at once. ADP_PROJECT_DIR
    overrides.
    """
    env = os.environ.get("ADP_PROJECT_DIR")
    if env:
        return Path(env).expanduser().resolve()
    for base in (start or Path.cwd(), Path(__file__).resolve().parent):
        p = Path(base).resolve()
        for cand in (p, *p.parents):
            if (cand / "comp_dataset").is_dir():
                return cand
    return (start or Path.cwd()).resolve()


@dataclass
class Config:
    root: Path
    db_path: Path
    out_dir: Path
    crosswalk_csv: Path
    fixtures_dir: Path
    api_key: str = ""
    read_token: str = ""
    rps: float = DEFAULT_RPS
    workers: int = DEFAULT_WORKERS
    append: str = DEFAULT_APPEND

    @classmethod
    def from_env(cls, root: Path | None = None) -> "Config":
        root = root or find_project_root()
        here = Path(__file__).resolve().parent
        db = Path(os.environ.get("RELEASE_EVENTS_DB") or (root / "release_events" / "release_events.db"))
        return cls(root=root, db_path=db, out_dir=db.parent / "out",
                   crosswalk_csv=root / "comp_dataset" / "28_imdb_tmdb_crosswalk.csv",
                   fixtures_dir=here / "fixtures",
                   api_key=os.environ.get("TMDB_API_KEY", ""),
                   read_token=os.environ.get("TMDB_READ_TOKEN", ""))

    @property
    def live(self) -> bool:
        return bool(self.api_key or self.read_token)


def redact(text: str, cfg: Config | None = None) -> str:
    """Strip any secret from a string before it is logged or stored."""
    s = str(text)
    for secret in filter(None, [getattr(cfg, "api_key", ""), getattr(cfg, "read_token", ""),
                                os.environ.get("TMDB_API_KEY", ""),
                                os.environ.get("TMDB_READ_TOKEN", "")]):
        s = s.replace(secret, "***")
    s = re.sub(r"api_key=[^&\s]+", "api_key=***", s)
    s = re.sub(r"Bearer\s+\S+", "Bearer ***", s)
    return s


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# 1. STORAGE
# =============================================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS films (
  tmdb_id        INTEGER PRIMARY KEY,
  tconsts        TEXT,                -- pipe-joined; the crosswalk can map several tconsts to one tmdb_id
  n_tconsts      INTEGER,
  tier_best      TEXT,
  status         TEXT NOT NULL DEFAULT 'pending',   -- pending | ok | gone | error | stale
  fetched_at     TEXT,
  http_status    INTEGER,
  error          TEXT,
  title          TEXT,
  imdb_id_tmdb   TEXT,                -- what TMDb itself says the imdb_id is
  wikidata_id    TEXT,
  tmdb_release   TEXT,
  n_countries    INTEGER,
  n_entries      INTEGER,
  ledger_ver     INTEGER,
  ledger_at      TEXT,
  resolved_ver   INTEGER,
  resolved_at    TEXT
);
CREATE INDEX IF NOT EXISTS films_status ON films(status);

CREATE TABLE IF NOT EXISTS raw_payloads (
  tmdb_id     INTEGER PRIMARY KEY,
  fetched_at  TEXT NOT NULL,
  http_status INTEGER,
  nbytes      INTEGER,
  json_z      BLOB NOT NULL          -- zlib(compact JSON of the verbatim response)
);

CREATE TABLE IF NOT EXISTS events (
  tmdb_id     INTEGER NOT NULL,
  country     TEXT NOT NULL,
  src_idx     INTEGER NOT NULL,      -- position inside that country's array; the stable key
  type        INTEGER,
  type_name   TEXT,
  date        TEXT,                  -- YYYY-MM-DD sliced, never timezone-converted
  note_raw    TEXT,
  note_norm   TEXT,
  note_class  TEXT,                  -- empty|festival|format|platform|version|admin|other
  cert        TEXT,
  lang        TEXT,
  descriptors TEXT,
  PRIMARY KEY (tmdb_id, country, src_idx)
);
CREATE INDEX IF NOT EXISTS events_note ON events(note_norm) WHERE note_norm != '';
CREATE INDEX IF NOT EXISTS events_type ON events(type);

CREATE TABLE IF NOT EXISTS festival_registry (
  qid          TEXT PRIMARY KEY,
  label        TEXT,
  country      TEXT,
  month        INTEGER,
  aliases      TEXT,   -- JSON list
  sections     TEXT,   -- JSON list
  editions     TEXT,   -- JSON list of {num, year, start, end, precision}
  source       TEXT,   -- seed | wikidata | seed+wikidata
  refreshed_at TEXT
);

CREATE TABLE IF NOT EXISTS wdqs_cache (
  key        TEXT PRIMARY KEY,
  fetched_at TEXT,
  json       TEXT
);

CREATE TABLE IF NOT EXISTS resolutions (
  tmdb_id      INTEGER NOT NULL,
  country      TEXT NOT NULL,
  src_idx      INTEGER NOT NULL,
  fest_qid     TEXT,
  fest_label   TEXT,
  score        REAL,
  band         TEXT,                 -- accept | review | reject
  edition_num  INTEGER,
  edition_year INTEGER,
  edition_how  TEXT,
  section      TEXT,
  evidence     TEXT,
  reason       TEXT,
  resolver_ver INTEGER,
  PRIMARY KEY (tmdb_id, country, src_idx)
);
CREATE INDEX IF NOT EXISTS resolutions_band ON resolutions(band);

CREATE TABLE IF NOT EXISTS appearances (
  tmdb_id      INTEGER NOT NULL,
  fest_qid     TEXT NOT NULL,
  ed_key       TEXT NOT NULL,
  fest_label   TEXT,
  date         TEXT,
  best_score   REAL,
  min_type     INTEGER,
  types        TEXT,
  countries    TEXT,
  notes        TEXT,
  section      TEXT,
  edition_num  INTEGER,
  edition_year INTEGER,
  PRIMARY KEY (tmdb_id, fest_qid, ed_key)
);

CREATE TABLE IF NOT EXISTS windows (
  tmdb_id            INTEGER NOT NULL,
  country            TEXT NOT NULL,
  premiere_date      TEXT,
  festival_date      TEXT,
  theatrical_ltd     TEXT,
  theatrical_wide    TEXT,
  first_theatrical   TEXT,
  digital_date       TEXT,
  physical_date      TEXT,
  tv_date            TEXT,
  days_thea_to_dig   INTEGER,
  days_thea_to_phys  INTEGER,
  n_entries          INTEGER,
  PRIMARY KEY (tmdb_id, country)
);

CREATE TABLE IF NOT EXISTS film_summary (
  tmdb_id               INTEGER PRIMARY KEY,
  earliest_any          TEXT,
  first_festival_date   TEXT,
  first_festival_qid    TEXT,
  first_festival_label  TEXT,
  n_festivals           INTEGER,
  first_theatrical      TEXT,
  first_theatrical_cc   TEXT,
  us_premiere           TEXT,
  us_theatrical         TEXT,
  us_digital            TEXT,
  us_physical           TEXT,
  n_countries           INTEGER,
  n_entries             INTEGER
);

CREATE TABLE IF NOT EXISTS tmdb_credits (
  tmdb_id     INTEGER PRIMARY KEY,
  fetched_at  TEXT NOT NULL,
  n_cast      INTEGER,
  n_crew      INTEGER,
  json_z      BLOB NOT NULL          -- zlib(compact JSON of the verbatim /movie/{id}/credits response)
);

CREATE TABLE IF NOT EXISTS film_metadata (
  tmdb_id              INTEGER PRIMARY KEY,
  tconst               TEXT,               -- primary tconst (first in the crosswalk list)
  tconsts              TEXT,
  title                TEXT,
  original_title       TEXT,
  release_date_tmdb    TEXT,
  year_imdb            INTEGER,
  title_type_imdb      TEXT,
  runtime_tmdb         INTEGER,
  runtime_imdb         INTEGER,
  genres_tmdb          TEXT,
  genres_imdb          TEXT,
  original_language    TEXT,
  spoken_languages     TEXT,
  origin_country       TEXT,
  production_countries TEXT,
  production_companies TEXT,
  company_countries    TEXT,
  budget_tmdb          INTEGER,
  revenue_tmdb         INTEGER,
  popularity           REAL,
  vote_average         REAL,
  vote_count           INTEGER,
  imdb_rating          REAL,
  imdb_votes           INTEGER,
  collection           TEXT,
  status               TEXT,
  adult                INTEGER,
  tagline              TEXT,
  overview             TEXT,
  directors_imdb       TEXT,
  director_nconsts     TEXT,
  writers_imdb         TEXT,
  cast_imdb            TEXT,
  cast_nconsts         TEXT,
  directors_tmdb       TEXT,
  director_tmdb_ids    TEXT,
  writers_tmdb         TEXT,
  producers_tmdb       TEXT,
  dop_tmdb             TEXT,
  editor_tmdb          TEXT,
  composer_tmdb        TEXT,
  cast_tmdb            TEXT,
  cast_tmdb_ids        TEXT,
  imdb_id_tmdb         TEXT,
  imdb_id_agrees       INTEGER,            -- 1 if TMDb's own imdb_id equals the crosswalk tconst
  wikidata_id          TEXT,
  imdb_source          TEXT,               -- imdb.db | missing
  credits_source       TEXT,               -- tmdb | missing
  built_at             TEXT
);

CREATE TABLE IF NOT EXISTS runs (
  run_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  stage      TEXT,
  started    TEXT,
  finished   TEXT,
  args       TEXT,
  n_done     INTEGER,
  n_ok       INTEGER,
  n_gone     INTEGER,
  n_error    INTEGER,
  note       TEXT
);
"""


def connect(cfg: Config) -> sqlite3.Connection:
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(cfg.db_path), timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("PRAGMA temp_store=MEMORY")
    con.executescript(SCHEMA)
    _migrate(con)
    return con


def _migrate(con):
    """Add columns introduced after a database was first created. Additive only."""
    have = {r[1] for r in con.execute("PRAGMA table_info(films)")}
    for col, decl in (("credits_status", "TEXT"), ("credits_at", "TEXT"), ("credits_error", "TEXT")):
        if col not in have:
            con.execute(f"ALTER TABLE films ADD COLUMN {col} {decl}")
    have = {r[1] for r in con.execute("PRAGMA table_info(windows)")}
    for col, decl in (("theatrical_bom", "TEXT"), ("first_theatrical_source", "TEXT")):
        if col not in have:
            con.execute(f"ALTER TABLE windows ADD COLUMN {col} {decl}")
    con.commit()


def has_table(con, name: str) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() is not None


def events_source(con) -> str:
    """`events_bo` (TMDb events reconciled with Box Office Mojo, see
    box_office.reconcile_stage) when it exists and is populated, else `events`."""
    if has_table(con, "events_bo") and con.execute("SELECT 1 FROM events_bo LIMIT 1").fetchone():
        return "events_bo"
    return "events"


WINDOW_COLS = ["tmdb_id", "country", "premiere_date", "festival_date", "theatrical_ltd", "theatrical_wide",
               "first_theatrical", "digital_date", "physical_date", "tv_date", "days_thea_to_dig",
               "days_thea_to_phys", "n_entries", "theatrical_bom", "first_theatrical_source"]


class RunLog:
    """One row in `runs` per stage invocation, finished in __exit__ even on Ctrl-C."""

    def __init__(self, con, stage, args=""):
        self.con, self.stage, self.args = con, stage, args
        self.n = Counter()
        self.note = ""

    def __enter__(self):
        cur = self.con.execute("INSERT INTO runs(stage, started, args) VALUES (?,?,?)",
                               (self.stage, utcnow(), self.args))
        self.run_id = cur.lastrowid
        self.con.commit()
        return self

    def __exit__(self, et, ev, tb):
        if et is KeyboardInterrupt:
            self.note = (self.note + " | interrupted").strip(" |")
        elif et is not None:
            self.note = (self.note + f" | failed: {redact(ev)}").strip(" |")
        self.con.execute(
            "UPDATE runs SET finished=?, n_done=?, n_ok=?, n_gone=?, n_error=?, note=? WHERE run_id=?",
            (utcnow(), sum(self.n.values()), self.n["ok"], self.n["gone"], self.n["error"],
             self.note, self.run_id))
        self.con.commit()
        return False


# =============================================================================
# 2. UNIVERSE  -- the crosswalk becomes the films table
# =============================================================================
TIER_RANK = ["human_confirmed", "confirmed", "confident", "enriched_verified", "auto_confirmed",
             "enriched_weak", "uncorroborated", "enriched_conflict", "no_tmdb_match"]


def _tier_rank(tier: str) -> int:
    return TIER_RANK.index(tier) if tier in TIER_RANK else len(TIER_RANK)


def load_universe(cfg: Config, con, source: str | None = None, ids: Iterable[int] | None = None,
                  verbose=True) -> dict:
    """Populate `films` from the crosswalk CSV (default), 02_features.csv, or an id list.

    Rows without a tmdb_id (tier no_tmdb_match) are counted and skipped, never
    stored as tmdb_id 0. Several tconsts mapping to one tmdb_id become one film
    with a pipe-joined tconst list -- TMDb is fetched once per TMDb id.
    """
    groups: dict[int, dict] = {}
    n_rows = n_no_id = 0
    if ids is not None:
        for i in ids:
            groups[int(i)] = {"tconsts": [], "tier": ""}
        src_name = "id list"
    else:
        path = Path(source) if source else cfg.crosswalk_csv
        if not path.exists():
            raise FileNotFoundError(f"crosswalk not found: {path}")
        src_name = str(path)
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                n_rows += 1
                raw = (row.get("tmdb_id") or "").strip()
                if not raw or raw.lower() in ("nan", "none"):
                    n_no_id += 1
                    continue
                try:
                    tid = int(float(raw))
                except ValueError:
                    n_no_id += 1
                    continue
                if tid <= 0:
                    n_no_id += 1
                    continue
                g = groups.setdefault(tid, {"tconsts": [], "tier": ""})
                tc = (row.get("tconst") or "").strip()
                if tc and tc not in g["tconsts"]:
                    g["tconsts"].append(tc)
                tier = (row.get("tier") or "").strip()
                if tier and (not g["tier"] or _tier_rank(tier) < _tier_rank(g["tier"])):
                    g["tier"] = tier
    n_new = 0
    with con:
        for tid, g in groups.items():
            cur = con.execute(
                "INSERT INTO films(tmdb_id, tconsts, n_tconsts, tier_best) VALUES (?,?,?,?) "
                "ON CONFLICT(tmdb_id) DO UPDATE SET tconsts=excluded.tconsts, "
                "n_tconsts=excluded.n_tconsts, tier_best=excluded.tier_best",
                (tid, "|".join(g["tconsts"]), len(g["tconsts"]), g["tier"]))
            n_new += cur.rowcount
    total = con.execute("SELECT COUNT(*) FROM films").fetchone()[0]
    multi = sum(1 for g in groups.values() if len(g["tconsts"]) > 1)
    stats = {"source": src_name, "rows_read": n_rows, "rows_without_tmdb_id": n_no_id,
             "distinct_tmdb_ids": len(groups), "tmdb_ids_with_several_tconsts": multi,
             "films_in_table": total}
    if verbose:
        print("UNIVERSE")
        for k, v in stats.items():
            print(f"  {k:<32}{v}")
    return stats


# =============================================================================
# 3. TMDb CLIENT + FETCH STAGE
# =============================================================================
class TokenBucket:
    """Process-wide rate limiter shared by every worker thread."""

    def __init__(self, rate: float, burst: int | None = None):
        self.rate = max(rate, 0.1)
        self.capacity = burst or max(1, int(self.rate))
        self.tokens = float(self.capacity)
        self.ts = time.monotonic()
        self.lock = threading.Lock()
        self.pause_until = 0.0        # set on 429: every thread waits, not just the one hit

    def pause(self, seconds: float):
        with self.lock:
            self.pause_until = max(self.pause_until, time.monotonic() + seconds)

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                if now < self.pause_until:
                    sleep_for = self.pause_until - now
                else:
                    self.tokens = min(self.capacity, self.tokens + (now - self.ts) * self.rate)
                    self.ts = now
                    if self.tokens >= 1:
                        self.tokens -= 1
                        return
                    sleep_for = (1 - self.tokens) / self.rate
            time.sleep(min(sleep_for, 5.0))


class FatalApiError(RuntimeError):
    """401/403: the key is wrong. Every further request would fail identically."""


def parse_retry_after(value: str | None, default: float) -> float:
    """Seconds to wait. Accepts an integer or an HTTP-date (RFC 7231); capped."""
    if not value:
        return default
    v = value.strip()
    try:
        secs = float(v)
    except ValueError:
        try:
            dt = email.utils.parsedate_to_datetime(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            secs = (dt - datetime.now(timezone.utc)).total_seconds()
        except (TypeError, ValueError, IndexError):
            return default
    return max(0.0, min(secs, RETRY_AFTER_CAP))


@dataclass
class FetchResult:
    tmdb_id: int
    outcome: str                 # ok | gone | error
    http_status: int | None = None
    payload: dict | None = None
    error: str = ""
    tries: int = 0
    ms: int = 0


class TmdbClient:
    def __init__(self, cfg: Config, bucket: TokenBucket | None = None, base: str | None = None):
        if not cfg.live:
            raise FatalApiError("no TMDB_API_KEY / TMDB_READ_TOKEN in the environment")
        self._local = threading.local()          # one requests.Session per worker thread
        self.cfg = cfg
        self.base = (base or TMDB_BASE).rstrip("/")
        self.bucket = bucket or TokenBucket(cfg.rps)

    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = requests.Session()
            s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
            if self.cfg.read_token:
                s.headers["Authorization"] = f"Bearer {self.cfg.read_token}"
            self._local.session = s
        return s

    def get_json(self, path: str, params: dict | None = None) -> tuple[int, dict | None, str]:
        """(status, json-or-None, error). Retries 429/5xx/transport; never raises
        except FatalApiError. 404 is returned, not retried."""
        params = dict(params or {})
        if not self.cfg.read_token:
            params["api_key"] = self.cfg.api_key
        url = f"{self.base}{path}"
        last_err, status = "", None
        for attempt in range(1, MAX_TRIES + 1):
            self.bucket.acquire()
            try:
                r = self._session().get(url, params=params, timeout=HTTP_TIMEOUT)
            except requests.RequestException as e:
                last_err = f"transport: {type(e).__name__}: {redact(e, self.cfg)}"
                status = None
                time.sleep(min(2 ** attempt, 30) * BACKOFF_SCALE)
                continue
            status = r.status_code
            if status == 200:
                try:
                    return status, r.json(), ""
                except ValueError as e:
                    last_err = f"non-JSON 200: {redact(e, self.cfg)}"
                    time.sleep(min(2 ** attempt, 30) * BACKOFF_SCALE)
                    continue
            if status == 404:
                return status, None, ""
            if status in (401, 403):
                raise FatalApiError(f"HTTP {status} from TMDb: the key/token was rejected")
            if status == 429 or status >= 500:
                wait_s = parse_retry_after(r.headers.get("Retry-After"), 2.0 * attempt * BACKOFF_SCALE)
                if status == 429:
                    self.bucket.pause(wait_s)      # everyone slows down, not just this thread
                else:
                    time.sleep(wait_s)
                last_err = f"HTTP {status}"
                continue
            return status, None, f"HTTP {status}"
        return status, None, last_err or "exhausted retries"

    def movie(self, tmdb_id: int, append: str) -> FetchResult:
        t0 = time.monotonic()
        params = {"append_to_response": append} if append else {}
        status, data, err = self.get_json(f"/movie/{tmdb_id}", params)
        ms = int((time.monotonic() - t0) * 1000)
        if status == 200 and isinstance(data, dict):
            if data.get("id") != tmdb_id:
                return FetchResult(tmdb_id, "error", status, None,
                                   f"200 but id={data.get('id')!r} != {tmdb_id}", ms=ms)
            return FetchResult(tmdb_id, "ok", status, data, ms=ms)
        if status == 404:
            return FetchResult(tmdb_id, "gone", status, None, "", ms=ms)
        return FetchResult(tmdb_id, "error", status, None, err or "unknown", ms=ms)

    def credits(self, tmdb_id: int) -> FetchResult:
        t0 = time.monotonic()
        status, data, err = self.get_json(f"/movie/{tmdb_id}/credits")
        ms = int((time.monotonic() - t0) * 1000)
        if status == 200 and isinstance(data, dict):
            if data.get("id") != tmdb_id:
                return FetchResult(tmdb_id, "error", status, None, f"200 but id={data.get('id')!r} != {tmdb_id}", ms=ms)
            return FetchResult(tmdb_id, "ok", status, data, ms=ms)
        if status == 404:
            return FetchResult(tmdb_id, "gone", status, None, "", ms=ms)
        return FetchResult(tmdb_id, "error", status, None, err or "unknown", ms=ms)

    def changed_ids(self, start: str, end: str) -> list[int]:
        """/movie/changes: ids changed between two dates (window <= 14 days). Paged."""
        ids, page = [], 1
        while True:
            status, data, err = self.get_json("/movie/changes",
                                              {"start_date": start, "end_date": end, "page": page})
            if status != 200 or not data:
                raise RuntimeError(f"/movie/changes failed: HTTP {status} {err}")
            for r in data.get("results", []) or []:
                if isinstance(r, dict) and isinstance(r.get("id"), int):
                    ids.append(r["id"])
            if page >= int(data.get("total_pages") or 1):
                break
            page += 1
        return ids


def compress_payload(data: dict) -> bytes:
    return zlib.compress(json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), 6)


def decompress_payload(blob: bytes) -> dict:
    return json.loads(zlib.decompress(blob).decode("utf-8"))


def _summarise_payload(data: dict) -> dict:
    """The film-level columns worth keeping next to the blob."""
    rd = (data.get("release_dates") or {}).get("results") or []
    ext = data.get("external_ids") or {}
    n_entries = sum(len(c.get("release_dates") or []) for c in rd if isinstance(c, dict))
    return {"title": data.get("title"), "imdb_id_tmdb": ext.get("imdb_id") or data.get("imdb_id"),
            "wikidata_id": ext.get("wikidata_id"), "tmdb_release": data.get("release_date"),
            "n_countries": len(rd), "n_entries": n_entries}


def pending_ids(con, refresh: bool, statuses=("pending", "stale", "error")) -> list[int]:
    if refresh:
        return [r[0] for r in con.execute("SELECT tmdb_id FROM films ORDER BY tmdb_id")]
    q = ",".join("?" * len(statuses))
    return [r[0] for r in con.execute(
        f"SELECT tmdb_id FROM films WHERE status IN ({q}) ORDER BY tmdb_id", statuses)]


def pending_credit_ids(con, refresh: bool) -> list[int]:
    if refresh:
        return [r[0] for r in con.execute("SELECT tmdb_id FROM films WHERE status='ok' ORDER BY tmdb_id")]
    return [r[0] for r in con.execute(
        "SELECT tmdb_id FROM films WHERE status='ok' AND (credits_status IS NULL OR credits_status IN ('error','stale')) "
        "ORDER BY tmdb_id")]


def write_credit_results(con, results: list[FetchResult], cfg: Config):
    now = utcnow()
    with con:
        for fr in results:
            if fr.outcome == "ok":
                blob = compress_payload(fr.payload)
                con.execute("INSERT OR REPLACE INTO tmdb_credits(tmdb_id, fetched_at, n_cast, n_crew, json_z) VALUES (?,?,?,?,?)",
                            (fr.tmdb_id, now, len(fr.payload.get("cast") or []), len(fr.payload.get("crew") or []), blob))
                con.execute("UPDATE films SET credits_status='ok', credits_at=?, credits_error=NULL WHERE tmdb_id=?",
                            (now, fr.tmdb_id))
            elif fr.outcome == "gone":
                con.execute("UPDATE films SET credits_status='gone', credits_at=?, credits_error=NULL WHERE tmdb_id=?",
                            (now, fr.tmdb_id))
            else:
                con.execute("UPDATE films SET credits_status='error', credits_at=?, credits_error=? WHERE tmdb_id=?",
                            (now, redact(fr.error, cfg)[:300], fr.tmdb_id))


def write_results(con, results: list[FetchResult], cfg: Config):
    now = utcnow()
    with con:
        for fr in results:
            if fr.outcome == "ok":
                blob = compress_payload(fr.payload)
                s = _summarise_payload(fr.payload)
                con.execute("INSERT OR REPLACE INTO raw_payloads(tmdb_id, fetched_at, http_status, nbytes, json_z) "
                            "VALUES (?,?,?,?,?)", (fr.tmdb_id, now, fr.http_status, len(blob), blob))
                con.execute("UPDATE films SET status='ok', fetched_at=?, http_status=?, error=NULL, title=?, "
                            "imdb_id_tmdb=?, wikidata_id=?, tmdb_release=?, n_countries=?, n_entries=?, "
                            "ledger_ver=NULL, resolved_ver=NULL WHERE tmdb_id=?",
                            (now, fr.http_status, s["title"], s["imdb_id_tmdb"], s["wikidata_id"],
                             s["tmdb_release"], s["n_countries"], s["n_entries"], fr.tmdb_id))
            elif fr.outcome == "gone":
                con.execute("UPDATE films SET status='gone', fetched_at=?, http_status=404, error=NULL "
                            "WHERE tmdb_id=?", (now, fr.tmdb_id))
            else:
                con.execute("UPDATE films SET status='error', fetched_at=?, http_status=?, error=? "
                            "WHERE tmdb_id=?", (now, fr.http_status, redact(fr.error, cfg)[:300], fr.tmdb_id))


def fetch_stage(cfg: Config, con, limit: int | None = None, refresh=False, ids: list[int] | None = None,
                client: TmdbClient | None = None, verbose=True, progress_every=500, kind="movie") -> Counter:
    """Concurrent, resumable capture. Only the main thread touches SQLite.

    kind="movie"   -> /movie/{id}?append_to_response=...   (release dates + external ids)
    kind="credits" -> /movie/{id}/credits                 (cast + crew by TMDb person id)

    In flight is bounded to workers*4 so Ctrl-C drops at most that many
    requests, and the executor is shut down with cancel_futures=True.
    """
    client = client or TmdbClient(cfg)
    if kind == "credits":
        todo = ids if ids is not None else pending_credit_ids(con, refresh)
        submit = lambda tid: ex.submit(client.credits, tid)
        writer, stage, what = write_credit_results, "credits", "credits"
    else:
        todo = ids if ids is not None else pending_ids(con, refresh)
        submit = lambda tid: ex.submit(client.movie, tid, cfg.append)
        writer, stage, what = write_results, "fetch", f"append={cfg.append}"
    if limit:
        todo = todo[:limit]
    n_total = len(todo)
    counts = Counter()
    consecutive_failures = 0
    buf: list[FetchResult] = []
    t0 = time.monotonic()
    next_report = progress_every
    if verbose:
        print(f"{stage.upper()}  {n_total} films to fetch  ({cfg.workers} workers, {cfg.rps:.0f} rps, {what})")
    if not n_total:
        return counts
    with RunLog(con, stage, f"limit={limit} refresh={refresh} n={n_total}") as rl:
        ex = ThreadPoolExecutor(max_workers=cfg.workers)
        it = iter(todo)
        in_flight = set()
        window = cfg.workers * 4
        try:
            while True:
                while len(in_flight) < window:
                    try:
                        tid = next(it)
                    except StopIteration:
                        break
                    in_flight.add(submit(tid))
                if not in_flight:
                    break
                done, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    fr = fut.result()          # FatalApiError propagates here and aborts
                    buf.append(fr)
                    counts[fr.outcome] += 1
                    rl.n[fr.outcome] += 1
                    consecutive_failures = consecutive_failures + 1 if fr.outcome == "error" else 0
                    if consecutive_failures >= BREAKER_WINDOW:
                        raise RuntimeError(f"{BREAKER_WINDOW} consecutive failures; last: {redact(fr.error, cfg)}")
                if len(buf) >= COMMIT_EVERY:
                    writer(con, buf, cfg)
                    buf.clear()
                done_n = sum(counts.values())
                if verbose and done_n >= next_report:
                    next_report += progress_every
                    el = time.monotonic() - t0
                    rate = done_n / el if el else 0
                    eta = (n_total - done_n) / rate if rate else float("inf")
                    print(f"  {done_n:>7}/{n_total}  ok={counts['ok']} gone={counts['gone']} "
                          f"err={counts['error']}  {rate:.1f}/s  ETA {eta/60:.0f} min")
        except KeyboardInterrupt:
            if verbose:
                print("\n  interrupted -- committing what finished; the rest stays pending")
            raise
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
            if buf:
                writer(con, buf, cfg)
                buf.clear()
    if verbose:
        el = time.monotonic() - t0
        print(f"  done: ok={counts['ok']} gone={counts['gone']} err={counts['error']} "
              f"in {el/60:.1f} min")
    return counts


def changes_stage(cfg: Config, con, days: int = 1, client: TmdbClient | None = None, verbose=True) -> int:
    """Mark films TMDb reports as changed in the last `days` (<=14) as stale."""
    client = client or TmdbClient(cfg)
    end = date.today()
    start = end - timedelta(days=min(max(days, 1), 14))
    changed = set(client.changed_ids(start.isoformat(), end.isoformat()))
    have = {r[0] for r in con.execute("SELECT tmdb_id FROM films")}
    hit = sorted(changed & have)
    with con:
        con.executemany("UPDATE films SET status='stale' WHERE tmdb_id=? AND status IN ('ok','gone')",
                        [(i,) for i in hit])
    if verbose:
        print(f"CHANGES  {len(changed)} ids changed on TMDb {start}..{end}; {len(hit)} are in the universe "
              f"-> marked stale (run `fetch` to refresh)")
    return len(hit)


# =============================================================================
# 4. LEDGER  -- raw payload -> events (pure, offline, re-runnable)
# =============================================================================
def strip_accents(s: str) -> str:
    """Fold diacritics only where the base codepoint is Latin/Greek/Cyrillic.

    A naive NFKD pass corrupts Japanese (ベ -> ヘ) and deletes Devanagari and
    Thai vowel signs. Fold below U+0500; leave every other script byte-identical.
    """
    out = []
    for ch in unicodedata.normalize("NFC", s):
        dec = unicodedata.normalize("NFKD", ch)
        if dec and ord(dec[0]) < 0x0500:
            out.append("".join(c for c in dec if not unicodedata.combining(c)))
        else:
            out.append(ch)
    return "".join(out)


def _is_punct(ch: str) -> bool:
    return unicodedata.category(ch)[0] in ("P", "S", "Z", "C")


def norm(s) -> str:
    """Aggressive normalisation for index lookup and comparison.

    Punctuation, symbols and separators become spaces by Unicode CATEGORY, not
    by `\\w`, which does not match combining marks (Mn/Mc) and so deleted every
    Devanagari vowel sign and Thai tone mark in an earlier matcher.
    """
    s = strip_accents(str(s or "")).lower()
    s = s.replace("&", " and ").replace("'", "").replace("’", "")
    s = "".join(" " if _is_punct(ch) else ch for ch in s)
    return re.sub(r"\s+", " ", s).strip()


# ---- note parsing ------------------------------------------------------------
_ORD_HEAD = re.compile(r"^\s*(\d{1,3})\s*(?:st|nd|rd|th|e|ème|eme|º|ª|\.)(?:\s+|$)", re.I)
_JP_ORD = re.compile(r"^\s*第\s*(\d{1,3})\s*回\s*")
_KO_ORD = re.compile(r"^\s*제\s*(\d{1,3})\s*회\s*")
_YEAR_ANY = re.compile(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)")
_SECTION = re.compile(r"\s*[\(\[]([^\)\]]{2,60})[\)\]]\s*$")
_DASH_SEC = re.compile(r"\s+[-–—:|]\s+(.{2,60})$")
_PREMIERE = re.compile(
    r"\b(world|international|north american|european|asian|us|u\.s\.|uk|national|canadian|"
    r"domestic|texas|hawai['i]?i|regional|city|american|australian|latin american)\s+premiere\b", re.I)
_WORD_ORD = {"first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6,
             "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10}


@dataclass
class ParsedNote:
    raw: str
    residual: str = ""            # the festival-name candidate, cleaned
    year: int | None = None
    ordinal: int | None = None
    section: str | None = None
    premiere: str | None = None

    @property
    def key(self) -> str:
        return norm(self.residual)


def parse_note(note: str | None) -> ParsedNote:
    raw = (note or "").strip()
    p = ParsedNote(raw=raw)
    if not raw:
        return p
    s = raw
    m = _PREMIERE.search(s)
    if m:
        p.premiere = m.group(0).lower()
        s = (s[:m.start()] + " " + s[m.end():]).strip(" -–—:,|")
    m = _SECTION.search(s)
    if m:
        p.section = m.group(1).strip()
        s = s[:m.start()].strip()
    else:
        m = _DASH_SEC.search(s)
        if m and not _YEAR_ANY.fullmatch(m.group(1).strip()):
            p.section = m.group(1).strip()
            s = s[:m.start()].strip()
    for rx in (_JP_ORD, _KO_ORD, _ORD_HEAD):
        m = rx.match(s)
        if m:
            p.ordinal = int(m.group(1))
            s = s[m.end():].strip()
            break
    m = _YEAR_ANY.search(s)
    if m:
        p.year = int(m.group(1))
        s = (s[:m.start()] + " " + s[m.end():]).strip(" -–—:,|")
    p.residual = re.sub(r"\s+", " ", s).strip(" -–—:,|")
    if not p.residual and p.section:
        # "(Hong Kong International Film Festival)" -- the whole note was in
        # brackets; that is the name, not a section of nothing
        p.residual, p.section = p.section, None
    return p


# ---- note classification -----------------------------------------------------
# Uses of `note` that TMDb's Contribution Bible sanctions and that are NOT
# festivals. Matching is on normalised tokens/phrases, so "4K Ultra HD Blu-ray
# Disc" and "Blu-ray" both classify as format.
FORMAT_TERMS = ("dvd", "blu ray", "bluray", "blu-ray", "4k", "uhd", "ultra hd", "vhs", "laserdisc",
                "disc", "steelbook", "box set", "boxset", "criterion collection", "arrow video",
                "digital hd", "collection", "home video", "home media")
PLATFORM_TERMS = ("netflix", "amazon", "prime video", "hulu", "mubi", "apple tv", "disney", "hbo",
                  "max", "paramount", "peacock", "shudder", "vod", "svod", "tvod", "avod", "arte",
                  "canal", "sky", "bbc", "itv", "channel 4", "film4", "telecine", "tubi", "kanopy",
                  "criterion channel", "youtube", "itunes", "google play", "stan", "binge", "crave",
                  "viaplay", "wowow", "u next", "unext", "tving", "wavve", "watcha", "coupang play",
                  "hotstar", "zee5", "sonyliv", "jiocinema", "globoplay", "starz", "showtime",
                  "amc", "ifc", "fx", "tnt", "tbs", "pbs", "pluto", "roku", "vudu", "fandango",
                  "klikfilm", "vidio", "iqiyi", "youku", "tencent", "bilibili", "streaming",
                  "digital release", "digital", "ott", "tv premiere", "television", "rtve",
                  "zdf", "ard", "rai", "france tv", "france 2", "france 3", "nhk", "kbs", "mbc", "sbs")
VERSION_TERMS = ("re release", "rerelease", "re-release", "director s cut", "directors cut",
                 "extended", "remaster", "restor", "anniversary", "imax", "3d", "black and white",
                 "black white", "b w", "colour", "color", "version", "edition", "cut", "dubbed",
                 "subtitled", "4dx", "screenx", "dolby", "70mm", "35mm", "roadshow", "sneak",
                 "preview", "early access", "encore", "limited", "wide", "expansion", "expands")
ADMIN_TERMS = ("visa cnc", "visa n", "cnc visa", "certificate", "rating")
# Generic markers: a fuzzy name match must carry one of these to count as a
# festival at all ("tokyo drift" must not reach Tokyo IFF).
FEST_WORDS = ("festival", "fest", "filmfest", "biennale", "berlinale", "zinemaldia", "mostra",
              "quinzaine", "semaine de la critique", "giornate", "muestra", "encuentro",
              "festiwal", "festivalen", "filmfestival", "cinefest", "映画祭", "영화제", "電影節", "电影节",
              "kinofest", "docfest", "filmtage", "rencontres", "showcase", "film week", "cinema week",
              "film days", "cinema days", "filmdagen", "frightfest", "viennale", "cinéfest",
              "festival internacional")
# Brand words: enough for the pre-resolution CLASSIFIER to call a note
# festival-shaped (so an unlisted "Sundance" survives into the review tail),
# but never enough on their own to license a fuzzy match.
FEST_BRANDS = ("sitges", "cannes", "sundance", "tribeca", "newfest", "outfest", "frameline",
               "slamdance", "cinequest", "cinemania", "fantastic", "telluride", "locarno")

_FMT = tuple(norm(t) for t in FORMAT_TERMS)
_PLAT = tuple(norm(t) for t in PLATFORM_TERMS)
_VER = tuple(norm(t) for t in VERSION_TERMS)
_ADM = tuple(norm(t) for t in ADMIN_TERMS)
_FEST = tuple(norm(t) for t in FEST_WORDS)
_BRAND = tuple(norm(t) for t in FEST_BRANDS)


def _has_phrase(key: str, terms) -> bool:
    padded = f" {key} "
    return any(f" {t} " in padded for t in terms)


def looks_like_festival(key: str) -> bool:
    return any(w in key for w in _FEST)


def classify_note(note: str | None) -> str:
    """empty | festival | format | platform | version | admin | other.

    A pre-resolution heuristic. The resolver's verdict overrides it: an
    accepted resolution makes the event a festival whatever this said, and a
    'festival' here that resolves to nothing stays 'festival' (unlisted) --
    it is kept, never dropped, so the tail can be reviewed.
    """
    key = norm(note)
    if not key:
        return "empty"
    if looks_like_festival(key) or _has_phrase(key, _BRAND):
        return "festival"
    if _has_phrase(key, _ADM):
        return "admin"
    if _has_phrase(key, _FMT):
        return "format"
    if _has_phrase(key, _PLAT) or key.endswith(" tv") or key.endswith(" plus") or re.search(r"\w\+(\s|$)", str(note or "")):
        return "platform"
    if _has_phrase(key, _VER):
        return "version"
    return "other"


def flatten(tmdb_id: int, payload: dict) -> list[dict]:
    """One row per release entry. Defensive: a null date or a missing type
    is kept as a row with an empty field, never a crash, never a dropped row."""
    rows = []
    results = (payload.get("release_dates") or {}).get("results") or []
    for country in results:
        if not isinstance(country, dict):
            continue
        cc = str(country.get("iso_3166_1") or "??")
        for i, e in enumerate(country.get("release_dates") or []):
            if not isinstance(e, dict):
                continue
            t = e.get("type")
            t = int(t) if isinstance(t, (int, float)) and not isinstance(t, bool) else None
            note = (e.get("note") or "").strip()
            rd = e.get("release_date") or ""
            rows.append({
                "tmdb_id": tmdb_id, "country": cc, "src_idx": i, "type": t,
                "type_name": TYPE_NAMES.get(t, f"unknown({t})" if t is not None else "unknown"),
                # TMDb returns a zeroed time component; slicing is the only safe move
                "date": rd[:10] if isinstance(rd, str) and len(rd) >= 10 else "",
                "note_raw": note, "note_norm": norm(note), "note_class": classify_note(note),
                "cert": (e.get("certification") or "").strip(),
                "lang": (e.get("iso_639_1") or "").strip(),
                "descriptors": "|".join(str(d) for d in (e.get("descriptors") or []) if d),
            })
    return rows


def iter_payloads(con, ids: Iterable[int] | None = None, chunk=500) -> Iterator[tuple[int, dict]]:
    """Stream (tmdb_id, payload) without holding every blob in memory."""
    if ids is None:
        ids = [r[0] for r in con.execute("SELECT tmdb_id FROM raw_payloads ORDER BY tmdb_id")]
    ids = list(ids)
    for i in range(0, len(ids), chunk):
        part = ids[i:i + chunk]
        q = ",".join("?" * len(part))
        for tid, blob in con.execute(f"SELECT tmdb_id, json_z FROM raw_payloads WHERE tmdb_id IN ({q})", part):
            yield tid, decompress_payload(blob)


def ledger_stage(cfg: Config, con, rebuild=False, verbose=True) -> dict:
    """Flatten every fetched payload whose ledger is missing or from an older parser."""
    if rebuild:
        ids = [r[0] for r in con.execute("SELECT tmdb_id FROM films WHERE status='ok'")]
    else:
        ids = [r[0] for r in con.execute(
            "SELECT tmdb_id FROM films WHERE status='ok' AND (ledger_ver IS NULL OR ledger_ver < ?)",
            (LEDGER_VERSION,))]
    n_films = n_rows = 0
    if verbose:
        print(f"LEDGER  {len(ids)} films to flatten (parser v{LEDGER_VERSION})")
    with RunLog(con, "ledger", f"rebuild={rebuild} n={len(ids)}") as rl:
        batch_ids, batch_rows = [], []
        for tid, payload in iter_payloads(con, ids):
            rows = flatten(tid, payload)
            batch_ids.append(tid)
            batch_rows.extend(rows)
            n_films += 1
            n_rows += len(rows)
            if len(batch_ids) >= COMMIT_EVERY:
                _write_ledger(con, batch_ids, batch_rows)
                batch_ids, batch_rows = [], []
                rl.n["ok"] = n_films
                if verbose and n_films % 20000 < COMMIT_EVERY:
                    print(f"  {n_films:>7} films, {n_rows} events")
        if batch_ids:
            _write_ledger(con, batch_ids, batch_rows)
        rl.n["ok"] = n_films
    stats = {"films": n_films, "events": n_rows}
    if verbose:
        print(f"  done: {n_films} films -> {n_rows} events")
    return stats


def _write_ledger(con, ids, rows):
    now = utcnow()
    with con:
        con.executemany("DELETE FROM events WHERE tmdb_id=?", [(i,) for i in ids])
        con.executemany(
            "INSERT INTO events(tmdb_id,country,src_idx,type,type_name,date,note_raw,note_norm,note_class,"
            "cert,lang,descriptors) VALUES (:tmdb_id,:country,:src_idx,:type,:type_name,:date,:note_raw,"
            ":note_norm,:note_class,:cert,:lang,:descriptors)", rows)
        con.executemany("UPDATE films SET ledger_ver=?, ledger_at=? WHERE tmdb_id=?",
                        [(LEDGER_VERSION, now, i) for i in ids])


def coverage_ledger(con) -> dict:
    """Entries by type and note fill rate -- the number that bounds everything else."""
    out = {"films": {}, "types": {}, "classes": {}, "countries": 0}
    for status, n in con.execute("SELECT status, COUNT(*) FROM films GROUP BY status"):
        out["films"][status] = n
    for t, n, with_note in con.execute(
            "SELECT type, COUNT(*), SUM(note_raw != '') FROM events GROUP BY type ORDER BY type"):
        out["types"][t] = {"name": TYPE_NAMES.get(t, f"unknown({t})"), "n": n, "with_note": with_note or 0}
    for c, n in con.execute("SELECT note_class, COUNT(*) FROM events WHERE note_raw != '' GROUP BY note_class"):
        out["classes"][c] = n
    out["countries"] = con.execute("SELECT COUNT(DISTINCT country) FROM events").fetchone()[0]
    out["events"] = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    out["films_with_events"] = con.execute("SELECT COUNT(DISTINCT tmdb_id) FROM events").fetchone()[0]
    out["films_ok_no_events"] = con.execute(
        "SELECT COUNT(*) FROM films WHERE status='ok' AND n_entries=0").fetchone()[0]
    return out


def print_coverage(con):
    c = coverage_ledger(con)
    print("=" * 72)
    print("COVERAGE LEDGER")
    print("=" * 72)
    print("films by status : " + ", ".join(f"{k}={v}" for k, v in sorted(c["films"].items())))
    print(f"films with >=1 release entry : {c['films_with_events']}   "
          f"(fetched OK but TMDb holds no dates: {c['films_ok_no_events']})")
    print(f"events : {c['events']}  across {c['countries']} countries\n")
    print(f"  {'type':<24}{'n':>9}{'with note':>11}{'fill %':>8}")
    for t, d in c["types"].items():
        pct = 100 * d["with_note"] / d["n"] if d["n"] else 0
        print(f"  {d['name']:<24}{d['n']:>9}{d['with_note']:>11}{pct:>7.0f}%")
    if c["classes"]:
        print("\n  non-empty notes by class (pre-resolution heuristic):")
        for k, v in sorted(c["classes"].items(), key=lambda kv: -kv[1]):
            print(f"    {k:<12}{v:>9}")
    print("\nAn empty note on a type-1 entry is indistinguishable from a city premiere.")
    print("That is the recall ceiling on any TMDb-only festival approach.")


# =============================================================================
# 5. WINDOWS  -- per film x country dates, and a per-film summary
# =============================================================================
def _days(a: str | None, b: str | None) -> int | None:
    if not a or not b or len(a) < 10 or len(b) < 10:
        return None
    try:
        return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days
    except ValueError:
        return None


def _min_date(vals) -> str | None:
    vals = [v for v in vals if v and len(v) >= 10]
    return min(vals) if vals else None


def windows_stage(cfg: Config, con, verbose=True, source: str | None = None) -> dict:
    """Rebuilt in full from events (or events_bo, once Box Office Mojo has been
    reconciled) + appearances; both are cheap SQL scans.

    A BOM-only theatrical date fills `first_theatrical` when TMDb has none for
    that country; `first_theatrical_source` says which source won."""
    source = source or events_source(con)
    if verbose:
        print(f"WINDOWS  rebuilding per film x country windows and the film summary from `{source}`")
    fest_by_film_cc: dict[tuple[int, str], str] = {}
    fest_first: dict[int, tuple[str, str, str]] = {}
    n_fest: Counter = Counter()
    for tid, qid, label, d, countries in con.execute(
            "SELECT tmdb_id, fest_qid, fest_label, date, countries FROM appearances"):
        n_fest[tid] += 1
        for cc in (countries or "").split("|"):
            if cc:
                k = (tid, cc)
                if d and (k not in fest_by_film_cc or d < fest_by_film_cc[k]):
                    fest_by_film_cc[k] = d
        if d and (tid not in fest_first or d < fest_first[tid][0]):
            fest_first[tid] = (d, qid, label)

    rows, summary = [], {}
    n_windows = 0
    with con:
        con.execute("DELETE FROM windows")
    # a separate read connection: the write below must not disturb the scan
    rcon = sqlite3.connect(str(cfg.db_path), timeout=60)
    src_col = "source" if source == "events_bo" else "'tmdb'"
    cur = rcon.execute(f"SELECT tmdb_id, country, type, date, {src_col} FROM {source} ORDER BY tmdb_id, country")
    current = None
    bucket: dict[int, list] = defaultdict(list)
    n_entries = 0

    def write_rows():
        nonlocal rows, n_windows
        if rows:
            with con:
                con.executemany(f"INSERT INTO windows({','.join(WINDOW_COLS)}) VALUES ({','.join('?' * len(WINDOW_COLS))})", rows)
            n_windows += len(rows)
            rows = []

    def flush(key):
        if key is None:
            return
        tid, cc = key
        prem = _min_date(bucket[1]); ltd = _min_date(bucket[2]); wide = _min_date(bucket[3])
        dig = _min_date(bucket[4]); phys = _min_date(bucket[5]); tv = _min_date(bucket[6])
        bom = _min_date(bucket["bom"])
        thea_tmdb = _min_date([ltd, wide])
        thea = _min_date([thea_tmdb, bom])
        thea_src = None if thea is None else ("tmdb" if thea == thea_tmdb else "bom")
        rows.append((tid, cc, prem, fest_by_film_cc.get((tid, cc)), ltd, wide, thea, dig, phys, tv,
                     _days(thea, dig), _days(thea, phys), n_entries, bom, thea_src))
        if len(rows) >= 20000:
            write_rows()
        s = summary.setdefault(tid, {"earliest": None, "thea": None, "thea_cc": None,
                                     "us": {}, "n_cc": 0, "n_entries": 0})
        s["n_cc"] += 1
        s["n_entries"] += n_entries
        e = _min_date([prem, ltd, wide, dig, phys, tv])
        if e and (s["earliest"] is None or e < s["earliest"]):
            s["earliest"] = e
        if thea and (s["thea"] is None or thea < s["thea"]):
            s["thea"], s["thea_cc"] = thea, cc
        if cc == "US":
            s["us"] = {"premiere": prem, "theatrical": thea, "digital": dig, "physical": phys}

    for tid, cc, t, d, src in cur:
        key = (tid, cc)
        if key != current:
            flush(current)
            current, bucket, n_entries = key, defaultdict(list), 0
        n_entries += 1
        if src == "bom":
            bucket["bom"].append(d)
        elif t in TYPE_NAMES:
            bucket[t].append(d)
    flush(current)
    write_rows()
    rcon.close()

    with con:
        con.execute("DELETE FROM film_summary")
        con.executemany(
            "INSERT INTO film_summary VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(tid, s["earliest"], fest_first.get(tid, (None,))[0],
              fest_first.get(tid, (None, None))[1], fest_first.get(tid, (None, None, None))[2],
              n_fest.get(tid, 0), s["thea"], s["thea_cc"], s["us"].get("premiere"),
              s["us"].get("theatrical"), s["us"].get("digital"), s["us"].get("physical"),
              s["n_cc"], s["n_entries"]) for tid, s in summary.items()])
    stats = {"windows": n_windows, "films": len(summary)}
    if verbose:
        print(f"  {n_windows} film x country windows, {len(summary)} film summaries")
    return stats


# =============================================================================
# 6. FESTIVAL REGISTRY  -- verified seed + Wikidata
# =============================================================================
# Three levels, per the rebuild plan: FESTIVAL (Q42369 Cannes) / EDITION (P393
# number with P580-P582 real dates) / SECTION. Materialised locally, refreshed
# on demand -- never queried per film.
#
# The seed (fixtures/festival_seed.json) is the notebook's 40 festivals with
# verified Q-ids and edition windows. Wikidata only ever ADDS to it: a sparser
# live result never overwrites a verified seed window.

SPARQL_FESTIVALS = """
SELECT DISTINCT ?f ?iso ?monthLabel WHERE {
  ?f wdt:P31/wdt:P279* wd:Q220505 .
  MINUS { ?f wdt:P31 wd:Q27787439 }
  MINUS { ?f wdt:P393 [] }
  OPTIONAL { ?f wdt:P17 ?c . ?c wdt:P297 ?iso }
  OPTIONAL { ?f wdt:P2922 ?m . ?m rdfs:label ?monthLabel FILTER(LANG(?monthLabel) = "en") }
}
"""
# Names are fetched per VALUES batch of festival ids, all languages at once.
# The first live run proved the alternative -- the class path repeated for
# every language -- times out at the 60 s WDQS deadline (HTTP 504).
SPARQL_NAMES = """
SELECT ?f ?name ?lang ?kind WHERE {
  VALUES ?f { %s }
  { ?f rdfs:label ?name . BIND("label" AS ?kind) }
  UNION
  { ?f skos:altLabel ?name . BIND("alias" AS ?kind) }
  FILTER(LANG(?name) IN (%s))
  BIND(LANG(?name) AS ?lang)
}
"""
NAME_BATCH = 250
SPARQL_EDITIONS = """
SELECT ?parent ?edition ?editionLabel ?num ?start ?end ?pit WHERE {
  VALUES ?parent { %s }
  ?edition wdt:P179|wdt:P361 ?parent .
  OPTIONAL { ?edition wdt:P393 ?num }
  OPTIONAL { ?edition wdt:P580 ?start }
  OPTIONAL { ?edition wdt:P582 ?end }
  OPTIONAL { ?edition wdt:P585 ?pit }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
"""
NAME_LANGS = ("en", "fr", "de", "es", "it", "pt", "ja", "ko", "zh", "nl", "pl", "sv", "cs", "ru")
MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July", "August", "September",
     "October", "November", "December"], 1)}
EDITION_BATCH = 150


def E(num, y, start=None, end=None) -> dict:
    return {"num": num, "year": y, "start": start, "end": end,
            "precision": "exact" if start and end else "year-only"}


class WdqsClient:
    """POST to the Wikidata Query Service, politely, with a disk cache.

    WDQS limits: 60 s query deadline, 30 error-queries a minute, all keyed on
    user-agent + IP. Requests are serialised and backed off; a response is
    cached by query text so a re-run is free.
    """

    def __init__(self, con, url: str | None = None, min_gap=1.0):
        self.con, self.url, self.min_gap = con, (url or WDQS_URL), min_gap
        self._last = 0.0
        self.calls = 0

    def query(self, sparql: str, cache_key: str | None = None, tries=4) -> list[dict]:
        key = cache_key or ("q:" + str(zlib.crc32(sparql.encode("utf-8"))))
        row = self.con.execute("SELECT json FROM wdqs_cache WHERE key=?", (key,)).fetchone()
        if row:
            return json.loads(row[0])["results"]["bindings"]
        last_err = ""
        for attempt in range(1, tries + 1):
            gap = self.min_gap - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()
            try:
                r = requests.post(self.url, data={"query": sparql}, timeout=90,
                                  headers={"User-Agent": USER_AGENT,
                                           "Accept": "application/sparql-results+json"})
            except requests.RequestException as e:
                last_err = f"transport: {type(e).__name__}"
                time.sleep(min(5 * attempt, 30) * BACKOFF_SCALE)
                continue
            self.calls += 1
            if r.status_code == 200:
                try:
                    data = r.json()
                    bindings = data["results"]["bindings"]
                except (ValueError, KeyError, TypeError) as e:
                    last_err = f"malformed SPARQL JSON: {type(e).__name__}"
                    time.sleep(min(5 * attempt, 30) * BACKOFF_SCALE)
                    continue
                with self.con:
                    self.con.execute("INSERT OR REPLACE INTO wdqs_cache(key, fetched_at, json) VALUES (?,?,?)",
                                     (key, utcnow(), json.dumps(data)))
                return bindings
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(parse_retry_after(r.headers.get("Retry-After"), 10.0 * attempt * BACKOFF_SCALE))
                last_err = f"HTTP {r.status_code}"
                continue
            raise RuntimeError(f"WDQS HTTP {r.status_code}: {r.text[:200]}")
        raise RuntimeError(f"WDQS failed after {tries} tries: {last_err}")


def _v(b: dict, k: str):
    return (b.get(k) or {}).get("value")


def _qid(uri: str | None) -> str | None:
    return uri.rsplit("/", 1)[-1] if uri else None


def load_seed(cfg: Config) -> list[dict]:
    p = cfg.fixtures_dir / "festival_seed.json"
    if not p.exists():
        log.warning("festival seed missing at %s -- registry will be Wikidata-only", p)
        return []
    return json.loads(p.read_text(encoding="utf-8"))


def registry_rows(con) -> dict[str, dict]:
    out = {}
    for qid, label, country, month, aliases, sections, editions, source, ts in con.execute(
            "SELECT qid,label,country,month,aliases,sections,editions,source,refreshed_at FROM festival_registry"):
        out[qid] = dict(qid=qid, label=label, country=country, month=month,
                        aliases=json.loads(aliases or "[]"), sections=json.loads(sections or "[]"),
                        editions=json.loads(editions or "[]"), source=source, refreshed_at=ts)
    return out


def save_registry(con, reg: dict[str, dict]):
    now = utcnow()
    with con:
        con.executemany(
            "INSERT OR REPLACE INTO festival_registry(qid,label,country,month,aliases,sections,editions,source,refreshed_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            [(q, f.get("label"), f.get("country"), f.get("month"),
              json.dumps(f.get("aliases") or [], ensure_ascii=False),
              json.dumps(f.get("sections") or [], ensure_ascii=False),
              json.dumps(f.get("editions") or [], ensure_ascii=False),
              f.get("source") or "seed", now) for q, f in reg.items()])


def seed_registry(cfg: Config, con, verbose=True) -> dict[str, dict]:
    """Ensure the seed is in the table; existing rows keep whatever Wikidata added."""
    reg = registry_rows(con)
    added = 0
    for f in load_seed(cfg):
        q = f["qid"]
        if q in reg:
            r = reg[q]
            for a in [f["label"], *f.get("aliases", [])]:
                if a not in r["aliases"] and a != r["label"]:
                    r["aliases"].append(a)
            for s in f.get("sections", []):
                if s not in r["sections"]:
                    r["sections"].append(s)
            have = {(e["num"], e["year"]) for e in r["editions"]}
            for e in f.get("editions", []):
                if (e["num"], e["year"]) not in have:
                    r["editions"].append(e)
            r["country"] = r["country"] or f.get("country")
            r["month"] = r["month"] or f.get("month")
        else:
            reg[q] = dict(qid=q, label=f["label"], country=f.get("country"), month=f.get("month"),
                          aliases=list(f.get("aliases", [])), sections=list(f.get("sections", [])),
                          editions=list(f.get("editions", [])), source="seed")
            added += 1
    save_registry(con, reg)
    if verbose:
        print(f"REGISTRY  seed: {added} festivals added, {len(reg)} in table")
    return reg


def refresh_registry(cfg: Config, con, wdqs: WdqsClient | None = None, langs=NAME_LANGS, verbose=True) -> dict:
    """Pull every film festival from Wikidata: identity, country, usual month, names."""
    wdqs = wdqs or WdqsClient(con)
    reg = seed_registry(cfg, con, verbose=False)
    t0 = time.monotonic()
    rows = wdqs.query(SPARQL_FESTIVALS, "festivals")
    n_new = 0
    for b in rows:
        q = _qid(_v(b, "f"))
        if not q:
            continue
        iso = _v(b, "iso")
        month = MONTHS.get((_v(b, "monthLabel") or "").lower())
        if q not in reg:
            reg[q] = dict(qid=q, label=None, country=iso, month=month, aliases=[], sections=[],
                          editions=[], source="wikidata")
            n_new += 1
        else:
            r = reg[q]
            r["country"] = r["country"] or iso
            r["month"] = r["month"] or month
            if r["source"] == "seed":
                r["source"] = "seed+wikidata"
    save_registry(con, reg)              # progress persists even if the names pass fails midway
    if verbose:
        print(f"  festivals: {len(rows)} bindings, {n_new} new items ({time.monotonic()-t0:.0f}s)")
    n_names = 0
    lang_list = ", ".join(f'"{l}"' for l in langs)
    qids = sorted(reg)
    labels: dict[str, dict[str, str]] = defaultdict(dict)      # qid -> lang -> label
    n_batches = (len(qids) + NAME_BATCH - 1) // NAME_BATCH
    for bi in range(n_batches):
        batch = qids[bi * NAME_BATCH:(bi + 1) * NAME_BATCH]
        vals = " ".join(f"wd:{q}" for q in batch)
        key = "names:" + str(zlib.crc32((",".join(batch) + lang_list).encode()))
        try:
            rows = wdqs.query(SPARQL_NAMES % (vals, lang_list), key)
        except RuntimeError as e:
            if verbose:
                print(f"  names batch {bi + 1}/{n_batches} failed ({e}); re-run `registry --refresh` to fill it")
            continue
        for b in rows:
            q, name, kind, lang = _qid(_v(b, "f")), (_v(b, "name") or "").strip(), _v(b, "kind"), _v(b, "lang")
            if not q or not name or q not in reg:
                continue
            r = reg[q]
            if kind == "label" and lang:
                labels[q][lang] = name
            if name != r["label"] and name not in r["aliases"]:
                r["aliases"].append(name)
                n_names += 1
        if verbose and (bi + 1) % 10 == 0:
            print(f"  names: batch {bi + 1}/{n_batches}, {n_names} names so far")
        if (bi + 1) % 20 == 0:
            save_registry(con, reg)
    for q, r in reg.items():
        if not r["label"]:
            lab = labels.get(q, {})
            r["label"] = next((lab[l] for l in langs if l in lab), None) or (r["aliases"][0] if r["aliases"] else q)
            if r["label"] in r["aliases"]:
                r["aliases"].remove(r["label"])
    save_registry(con, reg)
    if verbose:
        print(f"  registry: {len(reg)} festivals, {n_names} names added, {wdqs.calls} WDQS calls, "
              f"{(time.monotonic()-t0)/60:.1f} min")
    return {"festivals": len(reg), "new": n_new, "names_added": n_names}


def merge_editions(reg: dict[str, dict], bindings: list[dict]) -> int:
    """Add editions from a WDQS response. Only ADDS; a blank-node date on one
    binding is skipped on its own, not the whole batch."""
    added = 0
    for b in bindings:
        try:
            parent = _qid(_v(b, "parent"))
            if parent not in reg:
                continue
            num = _v(b, "num")
            start = (_v(b, "start") or "")[:10]
            end = (_v(b, "end") or "")[:10]
            pit = (_v(b, "pit") or "")[:10]
            if start and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", start):
                start = ""
            if end and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", end):
                end = ""
            yr_src = start or pit or end
            yr = int(yr_src[:4]) if yr_src and yr_src[:4].isdigit() else None
            if not yr:
                continue
            k = (int(num) if num and num.isdigit() else None, yr)
            have = {(e["num"], e["year"]) for e in reg[parent]["editions"]}
            if k in have:
                continue
            reg[parent]["editions"].append(E(k[0], yr, start or None, end or None))
            added += 1
        except (ValueError, TypeError, AttributeError):
            continue
    return added


def fetch_editions(cfg: Config, con, qids: Iterable[str], wdqs: WdqsClient | None = None, verbose=True) -> int:
    """Edition windows for a set of festivals, in VALUES batches. Cached per batch."""
    wdqs = wdqs or WdqsClient(con)
    reg = registry_rows(con)
    todo = sorted(q for q in set(qids) if q in reg)
    added = 0
    for i in range(0, len(todo), EDITION_BATCH):
        batch = todo[i:i + EDITION_BATCH]
        vals = " ".join(f"wd:{q}" for q in batch)
        try:
            rows = wdqs.query(SPARQL_EDITIONS % vals, "editions:" + ",".join(batch))
        except RuntimeError as e:
            if verbose:
                print(f"  editions batch {i//EDITION_BATCH} failed ({e}); continuing -- absent editions score zero")
            continue
        added += merge_editions(reg, rows)
    save_registry(con, reg)
    if verbose:
        print(f"  editions: {added} added for {len(todo)} festivals")
    return added


# ---- the in-memory index ---------------------------------------------------
NAME_FLOOR = 0.62
TYPO_MIN_LEN = 5
TYPO_RATIO = 0.86
_CJK = re.compile(r"[぀-ヿ㐀-鿿가-힯]")


class Registry:
    """Alias index + IDF over festival names, and the name scorer.

    Scoring is IDF-weighted token F2: tokens are weighted by how rare they are
    ACROSS festivals, so `sitges` and `cannes` decide a match and the shared
    "international film festival" boilerplate barely counts. Typo tolerance is
    at the token level, where it cannot outrank token evidence.
    """

    def __init__(self, festivals: dict[str, dict]):
        self.f = festivals
        self.exact: dict[str, set[str]] = defaultdict(set)      # norm(name) -> qids
        self.alias_tokens: dict[str, list[tuple[str, frozenset]]] = {}   # qid -> [(alias, tokens)]
        self.union_tokens: dict[str, frozenset] = {}
        self.postings: dict[str, set[str]] = defaultdict(set)   # token -> qids
        df: Counter = Counter()
        for qid, fest in festivals.items():
            names = [n for n in [fest.get("label"), *(fest.get("aliases") or [])] if n]
            al, union = [], set()
            for n in names:
                k = norm(n)
                if not k:
                    continue
                self.exact[k].add(qid)
                toks = frozenset(k.split())
                al.append((k, toks))
                union |= toks
            self.alias_tokens[qid] = al
            self.union_tokens[qid] = frozenset(union)
            for t in union:
                self.postings[t].add(qid)
                df[t] += 1
        self.N = max(len(festivals), 1)
        self.df = df
        self.idf = {t: math.log((self.N + 1) / (c + 1)) + 1.0 for t, c in df.items()}
        self.idf_unknown = math.log(self.N + 1) + 1.0
        self.cand_df_max = max(3, int(0.05 * self.N))          # boilerplate tokens never seed candidates
        self.stop = {"the", "of", "de", "du", "la", "le", "el", "and", "di", "der", "des", "del", "das",
                     "das", "les", "los", "las", "den", "het", "van", "von", "a", "o", "e", "y", "et", "und"}
        self.distinct_df_max = max(2, int(0.002 * self.N))     # a token this rare identifies a festival
        self.vocab_by_shape: dict[tuple, list[str]] = defaultdict(list)
        for t in df:
            if len(t) >= TYPO_MIN_LEN - 1:
                self.vocab_by_shape[(t[0], len(t) // 2)].append(t)
        self._typo_cache: dict[str, str | None] = {}
        self._score_cache: dict[str, "NameHit"] = {}

    def w(self, t: str) -> float:
        """A token no festival name contains is WEAK evidence against a match
        (a sponsor, a city descriptor); a known rare token belongs to some other
        festival and is strong evidence. So unknown tokens weigh the minimum."""
        return self.idf.get(t, 1.0)

    def typo_equiv(self, t: str) -> str | None:
        """A vocabulary token within edit-similarity TYPO_RATIO of t, or None."""
        if t in self.df:
            return t
        if len(t) < TYPO_MIN_LEN or t.isdigit():
            return None
        if t in self._typo_cache:
            return self._typo_cache[t]
        best, best_r = None, 0.0
        for half in (len(t) // 2 - 1, len(t) // 2, len(t) // 2 + 1):
            for cand in self.vocab_by_shape.get((t[0], half), ()):
                r = SequenceMatcher(None, t, cand).ratio()
                if r > best_r:
                    best, best_r = cand, r
        out = best if best_r >= TYPO_RATIO else None
        self._typo_cache[t] = out
        return out

    def name_score(self, key: str) -> "NameHit":
        if key in self._score_cache:
            return self._score_cache[key]
        hit = self._name_score(key)
        self._score_cache[key] = hit
        return hit

    def _name_score(self, key: str) -> "NameHit":
        if not key:
            return NameHit()
        if key in self.exact:
            cands = sorted(self.exact[key])
            return NameHit(1.0, cands[0], key, exact=True, ambiguous=len(cands) > 1, candidates=cands)
        toks = key.split()
        if len(toks) == 1 and _CJK.search(key):
            return self._cjk_score(key)
        mapped = {t: self.typo_equiv(t) for t in toks}          # note token -> vocab token
        cands: set[str] = set()
        for t, v in mapped.items():
            if v and self.df.get(v, 0) <= self.cand_df_max:
                cands |= self.postings[v]
        if not cands:
            return NameHit()
        total = sum(self.w(mapped[t] or t) for t in toks)
        best = NameHit()
        for q in cands:
            union = self.union_tokens[q]
            matched = {t for t, v in mapped.items() if v and v in union}
            if not matched:
                continue
            prec = sum(self.w(mapped[t]) for t in matched) / total if total else 0.0
            rec, best_alias = 0.0, ""
            mv = {mapped[t] for t in matched}
            # tokens in the note that are neither boilerplate nor stopwords
            substantive = [t for t in toks if t not in self.stop and self.df.get(mapped[t] or t, 0) <= self.cand_df_max]
            for alias, atoks in self.alias_tokens[q]:
                aw = sum(self.w(t) for t in atoks)
                r = sum(self.w(t) for t in atoks if t in mv) / aw if aw else 0.0
                # A bare one-word alias ("Cannes", "Tokyo") is fully recalled by
                # ANY note containing that word. If the note carries other
                # substantive words ("... Film Week"), that alias is weak evidence.
                if len(atoks) == 1 and any(t not in matched for t in substantive):
                    r *= 0.75
                if r > rec:
                    rec, best_alias = r, alias
            # F2: every token of the ALIAS must be in the note (recall), while
            # extra tokens in the note count less -- notes carry sponsors,
            # cities and descriptors that no alias will ever list.
            f2 = 5 * prec * rec / (4 * prec + rec) if prec + rec else 0.0
            if f2 > best.score:
                extra = [t for t in toks if t not in matched and not t.isdigit() and len(t) >= 3
                         and 1 <= self.df.get(mapped[t] or t, 0) <= self.distinct_df_max]
                best = NameHit(round(f2, 4), q, best_alias, extra_tokens=extra)
        return best

    def _cjk_score(self, key: str) -> "NameHit":
        best = NameHit()
        for alias, qids in self.exact.items():
            if not _CJK.search(alias):
                continue
            if alias in key or key in alias:
                r = min(len(alias), len(key)) / max(len(alias), len(key))
                r = max(r, 0.9) if len(min(alias, key, key=len)) >= 4 else r
            else:
                r = SequenceMatcher(None, key, alias).ratio()
            if r > best.score:
                best = NameHit(round(r, 4), sorted(qids)[0], alias)
        return best


@dataclass
class NameHit:
    score: float = 0.0
    qid: str | None = None
    alias: str = ""
    exact: bool = False
    ambiguous: bool = False
    extra_tokens: list = field(default_factory=list)
    candidates: list = field(default_factory=list)      # every festival an exact name hit names


# =============================================================================
# 7. RESOLVER  -- five signals, weighted; absent evidence scores zero
# =============================================================================
W_NAME, W_COUNTRY, W_COUNTRY_MISS = 60.0, 20.0, -10.0
W_DATE_IN, W_DATE_OUT = 25.0, -30.0
W_MONTH, W_MONTH_MISS = 8.0, -5.0
W_SECTION = 12.0
W_ORDINAL, W_ORDINAL_MISS = 15.0, -30.0
ACCEPT, REVIEW = 80.0, 55.0
GRACE_DAYS = 2


def as_date(v) -> date | None:
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError):
        return None


def _in_window(e: dict, d: date) -> bool:
    try:
        s = date.fromisoformat(e["start"]) - timedelta(days=GRACE_DAYS)
        t = date.fromisoformat(e["end"]) + timedelta(days=GRACE_DAYS)
    except (TypeError, ValueError):
        return False
    return s <= d <= t


def pick_edition(fest: dict, ev_date, ordinal, year):
    """(edition, how) with how in {ordinal, window, year, None}."""
    eds = fest.get("editions") or []
    if not eds:
        return None, None
    if ordinal is not None:
        for e in eds:
            if e.get("num") == ordinal:
                return e, "ordinal"
    d = as_date(ev_date)
    if d is None:
        for e in eds:
            if year and e.get("year") == year:
                return e, "year"
        return None, None
    for e in eds:
        if e.get("precision") == "exact" and _in_window(e, d):
            return e, "window"
    for e in eds:
        if e.get("year") == (year or d.year):
            return e, "year"
    return None, None


@dataclass
class Resolution:
    qid: str | None = None
    label: str = ""
    score: float = 0.0
    band: str = "reject"
    edition: dict | None = None
    edition_how: str | None = None
    section: str | None = None
    evidence: list = field(default_factory=list)
    reason: str = ""


def _tiebreak(fest: dict) -> tuple:
    """Deterministic preference among festivals that share a name and tie on evidence."""
    q = fest.get("qid") or ""
    return (1 if (fest.get("source") or "").startswith("seed") else 0, 1 if fest.get("editions") else 0,
            len(fest.get("aliases") or []), -int(q[1:]) if q[1:].isdigit() else 0)


def _score_fest(fest: dict, hit: "NameHit", p: ParsedNote, country: str, ev_date: str):
    """Evidence score of one candidate festival. Returns (score, evidence, edition, how, section)."""
    evidence: list[str] = []
    section = None
    score = W_NAME * hit.score
    evidence.append(f"name {hit.score:.2f} " + ("exact" if hit.exact else f"fuzzy vs '{hit.alias}'"))

    fc, cc = fest.get("country"), country
    if fc and cc and cc != "??":
        if fc == cc:
            score += W_COUNTRY
            evidence.append(f"country {cc} matches P17")
        else:
            score += W_COUNTRY_MISS
            evidence.append(f"country {cc} != P17 {fc}")

    ed, how = pick_edition(fest, ev_date, p.ordinal, p.year)
    d = as_date(ev_date)
    if d is None:
        evidence.append("unparseable date - date and month signals unavailable")
    if how == "window":
        score += W_DATE_IN
        evidence.append(f"date inside edition {ed.get('num')} window {ed.get('start')}..{ed.get('end')}")
    elif how == "ordinal":
        score += W_ORDINAL
        evidence.append(f"ordinal {p.ordinal} matches edition {ed.get('num')}")
        if ed.get("precision") == "exact" and d is not None:
            if _in_window(ed, d):
                score += W_DATE_IN
                evidence.append("date also inside that edition's window")
            else:
                score += W_DATE_OUT
                evidence.append("date OUTSIDE that edition's window")
    elif fest.get("editions") and d is not None:
        if any(e.get("precision") == "exact" and e.get("year") == d.year for e in fest["editions"]):
            score += W_DATE_OUT
            evidence.append(f"date {ev_date} outside every known {d.year} window")
    else:
        evidence.append("no edition data (no penalty)")

    if how not in ("window", "ordinal") and d is not None and fest.get("month"):
        diff = abs(int(fest["month"]) - d.month)
        diff = min(diff, 12 - diff)
        if diff == 0:
            score += W_MONTH
            evidence.append(f"month {d.month} matches usual month")
        elif diff > 1:
            score += W_MONTH_MISS
            evidence.append(f"month {d.month} vs usual {fest['month']}")

    if p.section:
        secs = {norm(s): s for s in fest.get("sections") or []}
        if norm(p.section) in secs:
            score += W_SECTION
            section = secs[norm(p.section)]
            evidence.append(f"section '{section}' belongs to this festival")

    if p.ordinal is not None and how != "ordinal" and fest.get("editions"):
        # Only a REAL contradiction is penalised: an edition of that year with
        # a different known number. Wikidata's edition numbering is sparse, and
        # "number simply unknown" is absent evidence, not negative evidence.
        yr = d.year if d else p.year
        same_year = [e for e in fest["editions"] if e.get("year") == yr and e.get("num") is not None]
        if same_year:
            score += W_ORDINAL_MISS
            evidence.append(f"ordinal {p.ordinal} contradicts edition {same_year[0]['num']} of {yr}")
        else:
            evidence.append(f"ordinal {p.ordinal}: no numbered edition for {yr} to check")
    return score, evidence, ed, how, section


def resolve(reg: Registry, note_raw: str, country: str, ev_date: str, note_class: str | None = None):
    p = parse_note(note_raw)
    r = Resolution()
    if not p.raw:
        r.reason = "empty note"
        return r, p
    cls = note_class or classify_note(note_raw)
    if cls in ("format", "platform", "admin", "version"):
        r.reason = f"note is a non-festival use ({cls}: {p.residual[:40]})"
        return r, p
    hit = reg.name_score(p.key)
    if hit.qid is None or hit.score < NAME_FLOOR:
        r.reason = f"no festival name candidate (best {hit.score:.2f})"
        return r, p
    if not hit.exact and hit.score < 0.97 and not looks_like_festival(p.key):
        r.reason = f"fuzzy match ({hit.score:.2f}) on a name with no festival marker - not treated as a festival"
        return r, p
    cands = hit.candidates if hit.candidates else [hit.qid]
    scored = []
    for q in cands:
        fest = reg.f[q]
        sc, ev, ed, how, sec = _score_fest(fest, hit, p, country, ev_date)
        scored.append((sc, _tiebreak(fest), q, ev, ed, how, sec))
    scored.sort(key=lambda t: (t[0], t[1]), reverse=True)
    score, _, qid, evidence, ed, how, sec = scored[0]
    fest = reg.f[qid]
    r.qid, r.label = qid, fest.get("label") or qid
    r.evidence = evidence
    r.edition, r.edition_how, r.section = ed, how, sec
    # A name shared by several registry items is nearly always a Wikidata
    # duplicate or a parent/sub-event in the same country. Country, date and
    # month decide where they can; otherwise a deterministic preference does
    # (seed > has editions > richer item > older item) and the choice is
    # written into the evidence rather than parked in the review queue.
    tied = len(scored) > 1 and abs(scored[0][0] - scored[1][0]) < 1.0
    if len(scored) > 1:
        r.evidence.append(f"'{hit.alias}' names {len(scored)} festivals ({', '.join(t[2] for t in scored)}); chose {qid} by "
                          + ("tie-break (seed > editions > richer item > older item)" if tied else "evidence"))
    r.score = max(0.0, score)
    r.band = "accept" if r.score >= ACCEPT else ("review" if r.score >= REVIEW else "reject")
    if hit.extra_tokens and r.band == "accept":
        r.band = "review"
        r.reason = f"distinguishing word(s) {hit.extra_tokens} not in any name of {r.label}"
        r.evidence.append("capped at review: " + r.reason)
    elif r.band == "reject":
        r.reason = "score below review threshold"
    elif r.band == "review":
        r.reason = f"score {r.score:.0f} is between REVIEW ({REVIEW:.0f}) and ACCEPT ({ACCEPT:.0f})"
    return r, p


def ed_key(edition_num, edition_year, ev_date: str) -> str:
    """Dedup key. Falls back to the RELEASE year, never 0, when no edition was pinned."""
    num = int(edition_num) if edition_num is not None else -1
    yr = int(edition_year) if edition_year is not None else (int(ev_date[:4]) if ev_date[:4].isdigit() else 0)
    return f"{num}|{yr}"


def load_registry(con) -> Registry:
    reg = Registry(registry_rows(con))
    return reg


def resolve_stage(cfg: Config, con, rebuild=False, fetch_editions_live=True, wdqs: WdqsClient | None = None,
                  verbose=True) -> dict:
    """Resolve every non-empty note; then dedup accepted rows into appearances.

    Edition windows are fetched from Wikidata only for festivals that received
    a name candidate (scoped, cached), and only when a network is available.
    """
    reg = load_registry(con)
    if not reg.f:
        raise RuntimeError("festival registry is empty -- run `registry` first")
    if rebuild:
        film_ids = [r[0] for r in con.execute("SELECT tmdb_id FROM films WHERE ledger_ver IS NOT NULL")]
    else:
        film_ids = [r[0] for r in con.execute(
            "SELECT tmdb_id FROM films WHERE ledger_ver IS NOT NULL AND "
            "(resolved_ver IS NULL OR resolved_ver < ?)", (RESOLVER_VERSION,))]
    if verbose:
        print(f"RESOLVE  {len(film_ids)} films, registry of {reg.N} festivals (resolver v{RESOLVER_VERSION})")
    if not film_ids:
        return {"films": 0}
    id_set = set(film_ids)

    # pass 1: which festivals do the notes name at all? (edition scoping)
    keys: Counter = Counter()
    for (nn,) in con.execute("SELECT DISTINCT note_norm FROM events WHERE note_raw != '' "
                             "AND note_class NOT IN ('format','platform','admin','version')"):
        keys[nn] += 1
    named: set[str] = set()
    for nn in keys:
        hit = reg.name_score(parse_note(nn).key)
        if hit.qid and hit.score >= NAME_FLOOR:
            named.add(hit.qid)
    if verbose:
        print(f"  {len(keys)} distinct festival-shaped notes name {len(named)} festivals")
    if fetch_editions_live and named:
        lacking = [q for q in named if not reg.f[q].get("editions")]
        if lacking:
            try:
                fetch_editions(cfg, con, lacking, wdqs=wdqs, verbose=verbose)
                reg = load_registry(con)
            except Exception as e:      # network down: the registry is still verified data
                if verbose:
                    print(f"  edition fetch unavailable ({redact(e, cfg)}); absent editions score zero")

    # pass 2: score every event with a note
    n_rows = 0
    counts: Counter = Counter()
    with RunLog(con, "resolve", f"rebuild={rebuild} n={len(film_ids)}") as rl:
        with con:
            for i in range(0, len(film_ids), 500):
                part = film_ids[i:i + 500]
                con.execute(f"DELETE FROM resolutions WHERE tmdb_id IN ({','.join('?' * len(part))})", part)
        batch_rows = []
        cur = con.execute("SELECT tmdb_id, country, src_idx, date, note_raw, note_class FROM events "
                          "WHERE note_raw != '' ORDER BY tmdb_id")
        for tid, cc, idx, d, note, cls in cur:
            if tid not in id_set:
                continue
            r, p = resolve(reg, note, cc, d, cls)
            counts[r.band] += 1
            batch_rows.append((tid, cc, idx, r.qid, r.label if r.qid else None, round(r.score, 1), r.band,
                               (r.edition or {}).get("num"), (r.edition or {}).get("year"), r.edition_how,
                               r.section, " | ".join(r.evidence), r.reason, RESOLVER_VERSION))
            n_rows += 1
            if len(batch_rows) >= 5000:
                _write_resolutions(con, batch_rows)
                batch_rows = []
        _write_resolutions(con, batch_rows)
        # films with no notes at all still get a resolved_ver so they are not re-scanned
        with con:
            con.executemany("UPDATE films SET resolved_ver=?, resolved_at=? WHERE tmdb_id=?",
                            [(RESOLVER_VERSION, utcnow(), i) for i in film_ids])
        rl.n["ok"] = len(film_ids)
        n_app = build_appearances(con, film_ids)
    if verbose:
        print(f"  {n_rows} noted events scored: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        print(f"  {n_app} distinct festival appearances for these films")
    return {"films": len(film_ids), "rows": n_rows, "bands": dict(counts), "appearances": n_app}


def _write_resolutions(con, rows):
    if not rows:
        return
    with con:
        con.executemany("INSERT OR REPLACE INTO resolutions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)


def build_appearances(con, film_ids: Iterable[int]) -> int:
    """Dedup key is (film, festival, edition) -- NOT (country, note). One
    appearance entered under three countries in two languages is one appearance."""
    ids = list(film_ids)
    groups: dict[tuple, dict] = {}
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        q = ",".join("?" * len(part))
        for tid, cc, idx, qid, label, score, en, ey, sec, d, t, note in con.execute(
                f"SELECT r.tmdb_id, r.country, r.src_idx, r.fest_qid, r.fest_label, r.score, r.edition_num, "
                f"r.edition_year, r.section, e.date, e.type, e.note_raw FROM resolutions r "
                f"JOIN events e ON e.tmdb_id=r.tmdb_id AND e.country=r.country AND e.src_idx=r.src_idx "
                f"WHERE r.band='accept' AND r.tmdb_id IN ({q})", part):
            k = (tid, qid, ed_key(en, ey, d or ""))
            g = groups.setdefault(k, {"label": label, "date": d, "score": score, "types": set(), "cc": set(),
                                      "notes": set(), "section": sec, "en": en, "ey": ey})
            g["types"].add(t if t is not None else -1)
            g["cc"].add(cc)
            g["notes"].add(note)
            if d and (not g["date"] or d < g["date"]):
                g["date"] = d
            if score > g["score"]:
                g["score"], g["label"] = score, label
            if sec and not g["section"]:
                g["section"] = sec
    with con:
        for i in range(0, len(ids), 500):
            part = ids[i:i + 500]
            con.execute(f"DELETE FROM appearances WHERE tmdb_id IN ({','.join('?' * len(part))})", part)
        con.executemany(
            "INSERT OR REPLACE INTO appearances VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(k[0], k[1], k[2], g["label"], g["date"], g["score"],
              min(t for t in g["types"] if t >= 0) if any(t >= 0 for t in g["types"]) else None,
              ",".join(str(t) for t in sorted(g["types"])), "|".join(sorted(g["cc"])),
              " || ".join(sorted(g["notes"]))[:500], g["section"], g["en"], g["ey"])
             for k, g in groups.items()])
    return len(groups)


# =============================================================================
# 7b. METADATA  -- one film-level row from TMDb payloads + credits + imdb.db
# =============================================================================
# Offline. TMDb fields come out of the stored /movie payload, TMDb people out
# of the stored /credits payload, IMDb fields out of imdb_match/imdb.db (the
# staged IMDb dumps the matching notebook built). Absent sources are recorded
# as absent (imdb_source / credits_source = 'missing'), never as blanks that
# look like "no director".

def imdb_db_path(cfg: Config) -> Path:
    env = os.environ.get("IMDB_DB")
    if env:
        return Path(env).expanduser()
    for cand in (cfg.root / "imdb_match" / "imdb.db", cfg.root / "imdb.db"):
        if cand.exists():
            return cand
    return cfg.root / "imdb_match" / "imdb.db"


def _join(vals, sep="|") -> str:
    return sep.join(str(v) for v in vals if v not in (None, ""))


def tmdb_metadata_fields(movie: dict) -> dict:
    """The film-level columns worth keeping from a /movie payload."""
    g = movie.get
    return {
        "title": g("title"), "original_title": g("original_title"), "release_date_tmdb": g("release_date") or None,
        "runtime_tmdb": g("runtime") or None,
        "genres_tmdb": _join(x.get("name") for x in (g("genres") or []) if isinstance(x, dict)),
        "original_language": g("original_language"),
        "spoken_languages": _join(x.get("iso_639_1") for x in (g("spoken_languages") or []) if isinstance(x, dict)),
        "origin_country": _join(g("origin_country") or []),
        "production_countries": _join(x.get("iso_3166_1") for x in (g("production_countries") or []) if isinstance(x, dict)),
        "production_companies": _join(x.get("name") for x in (g("production_companies") or []) if isinstance(x, dict)),
        "company_countries": _join(x.get("origin_country") for x in (g("production_companies") or []) if isinstance(x, dict)),
        "budget_tmdb": g("budget") or None, "revenue_tmdb": g("revenue") or None,
        "popularity": g("popularity"), "vote_average": g("vote_average"), "vote_count": g("vote_count"),
        "collection": (g("belongs_to_collection") or {}).get("name") if isinstance(g("belongs_to_collection"), dict) else None,
        "status": g("status"), "adult": int(bool(g("adult"))) if g("adult") is not None else None,
        "tagline": g("tagline") or None, "overview": g("overview") or None,
        "imdb_id_tmdb": (g("external_ids") or {}).get("imdb_id") or g("imdb_id"),
        "wikidata_id": (g("external_ids") or {}).get("wikidata_id"),
    }


CREW_JOBS = {"directors_tmdb": ("Director",), "writers_tmdb": ("Writer", "Screenplay", "Story", "Novel", "Author"),
             "producers_tmdb": ("Producer",), "dop_tmdb": ("Director of Photography",),
             "editor_tmdb": ("Editor",), "composer_tmdb": ("Original Music Composer",)}


def tmdb_credit_fields(credits: dict, top_cast=5) -> dict:
    out = {k: "" for k in CREW_JOBS}
    out.update({"director_tmdb_ids": "", "cast_tmdb": "", "cast_tmdb_ids": ""})
    crew = [c for c in (credits.get("crew") or []) if isinstance(c, dict)]
    seen: dict[str, list] = defaultdict(list)
    for c in crew:
        for col, jobs in CREW_JOBS.items():
            if c.get("job") in jobs and c.get("name") and c["name"] not in seen[col]:
                seen[col].append(c["name"])
                if col == "directors_tmdb":
                    seen["director_tmdb_ids"].append(str(c.get("id")))
    for col in CREW_JOBS:
        out[col] = _join(seen[col])
    out["director_tmdb_ids"] = _join(seen["director_tmdb_ids"])
    cast = sorted([c for c in (credits.get("cast") or []) if isinstance(c, dict)],
                  key=lambda c: (c.get("order") if isinstance(c.get("order"), int) else 10 ** 6))[:top_cast]
    out["cast_tmdb"] = _join(c.get("name") for c in cast)
    out["cast_tmdb_ids"] = _join(c.get("id") for c in cast)
    return out


class ImdbLookup:
    """Chunked reads from imdb.db: titles, ratings, crew, top-billed cast, names."""

    def __init__(self, path: Path):
        self.ok = path.exists()
        self.con = sqlite3.connect(f"file:{path}?mode=ro", uri=True) if self.ok else None
        self.has = set()
        if self.ok:
            self.has = {r[0] for r in self.con.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def batch(self, tconsts: list[str]) -> dict[str, dict]:
        out = {t: {} for t in tconsts}
        if not self.ok or not tconsts:
            return out
        q = ",".join("?" * len(tconsts))
        if "titles" in self.has:
            for t, tt, yr, rt, gen in self.con.execute(
                    f"SELECT tconst, titleType, startYear, runtimeMinutes, genres FROM titles WHERE tconst IN ({q})", tconsts):
                out[t].update(title_type_imdb=tt, year_imdb=yr, runtime_imdb=rt,
                              genres_imdb=(gen or "").replace(",", "|") if gen and gen != "\\N" else "")
        if "ratings" in self.has:
            for t, r, v in self.con.execute(f"SELECT tconst, averageRating, numVotes FROM ratings WHERE tconst IN ({q})", tconsts):
                out[t].update(imdb_rating=r, imdb_votes=v)
        need_names: set[str] = set()
        if "crew" in self.has:
            for t, d, w in self.con.execute(f"SELECT tconst, directors, writers FROM crew WHERE tconst IN ({q})", tconsts):
                d = [x for x in (d or "").split(",") if x.startswith("nm")]
                w = [x for x in (w or "").split(",") if x.startswith("nm")]
                out[t].update(director_nconsts=_join(d), _writer_nconsts=w)
                need_names.update(d, w)
        if "principals" in self.has:
            cast: dict[str, list] = defaultdict(list)
            for t, nc, cat, o in self.con.execute(
                    f"SELECT tconst, nconst, category, ordering FROM principals WHERE tconst IN ({q}) "
                    f"AND category IN ('actor','actress','self') ORDER BY tconst, ordering", tconsts):
                if len(cast[t]) < 5:
                    cast[t].append(nc)
                    need_names.add(nc)
            for t, ncs in cast.items():
                out[t]["cast_nconsts"] = _join(ncs)
                out[t]["_cast_nconsts"] = ncs
        names = {}
        if "names" in self.has and need_names:
            nl = sorted(need_names)
            for i in range(0, len(nl), 900):
                part = nl[i:i + 900]
                for nc, nm in self.con.execute(
                        f"SELECT nconst, primaryName FROM names WHERE nconst IN ({','.join('?' * len(part))})", part):
                    names[nc] = nm
        for t, d in out.items():
            if not d:            # nothing in imdb.db for this tconst: leave it empty, not name-filled
                continue
            d["directors_imdb"] = _join(names.get(n, n) for n in (d.get("director_nconsts") or "").split("|") if n)
            d["writers_imdb"] = _join(names.get(n, n) for n in d.pop("_writer_nconsts", []))
            d["cast_imdb"] = _join(names.get(n, n) for n in d.pop("_cast_nconsts", []))
        return out


META_COLS = ["tmdb_id", "tconst", "tconsts", "title", "original_title", "release_date_tmdb", "year_imdb",
             "title_type_imdb", "runtime_tmdb", "runtime_imdb", "genres_tmdb", "genres_imdb", "original_language",
             "spoken_languages", "origin_country", "production_countries", "production_companies", "company_countries",
             "budget_tmdb", "revenue_tmdb", "popularity", "vote_average", "vote_count", "imdb_rating", "imdb_votes",
             "collection", "status", "adult", "tagline", "overview", "directors_imdb", "director_nconsts",
             "writers_imdb", "cast_imdb", "cast_nconsts", "directors_tmdb", "director_tmdb_ids", "writers_tmdb",
             "producers_tmdb", "dop_tmdb", "editor_tmdb", "composer_tmdb", "cast_tmdb", "cast_tmdb_ids",
             "imdb_id_tmdb", "imdb_id_agrees", "wikidata_id", "imdb_source", "credits_source", "built_at"]


def metadata_stage(cfg: Config, con, rebuild=False, imdb_path: Path | None = None, verbose=True) -> dict:
    """Build film_metadata for every fetched film (or every film lacking a row)."""
    imdb = ImdbLookup(imdb_path or imdb_db_path(cfg))
    if rebuild:
        ids = [r[0] for r in con.execute("SELECT tmdb_id FROM films WHERE status='ok' ORDER BY tmdb_id")]
    else:
        ids = [r[0] for r in con.execute("SELECT f.tmdb_id FROM films f LEFT JOIN film_metadata m USING(tmdb_id) "
                                          "WHERE f.status='ok' AND m.tmdb_id IS NULL ORDER BY f.tmdb_id")]
    if verbose:
        print(f"METADATA  {len(ids)} films; imdb.db {'found' if imdb.ok else 'NOT FOUND'} at "
              f"{imdb_path or imdb_db_path(cfg)}; tables {sorted(imdb.has) if imdb.ok else '-'}")
    n = 0
    stats = Counter()
    with RunLog(con, "metadata", f"rebuild={rebuild} n={len(ids)}") as rl:
        for i in range(0, len(ids), 500):
            part = ids[i:i + 500]
            q = ",".join("?" * len(part))
            films = {r[0]: r[1:] for r in con.execute(
                f"SELECT tmdb_id, tconsts, credits_status FROM films WHERE tmdb_id IN ({q})", part)}
            payloads = dict(iter_payloads(con, part))
            credits = {tid: decompress_payload(b) for tid, b in con.execute(
                f"SELECT tmdb_id, json_z FROM tmdb_credits WHERE tmdb_id IN ({q})", part)}
            primary = {tid: ((films[tid][0] or "").split("|") or [""])[0] for tid in part}
            imdb_rows = imdb.batch([t for t in primary.values() if t])
            rows = []
            now = utcnow()
            for tid in part:
                mv = payloads.get(tid)
                if mv is None:
                    continue
                row = {c: None for c in META_COLS}
                row.update(tmdb_id=tid, tconst=primary[tid] or None, tconsts=films[tid][0], built_at=now)
                row.update(tmdb_metadata_fields(mv))
                ir = imdb_rows.get(primary[tid], {})
                if ir:
                    row.update({k: v for k, v in ir.items() if k in row})
                    row["imdb_source"] = "imdb.db"
                    stats["imdb"] += 1
                else:
                    row["imdb_source"] = "missing"
                cr = credits.get(tid)
                if cr is not None:
                    row.update(tmdb_credit_fields(cr))
                    row["credits_source"] = "tmdb"
                    stats["credits"] += 1
                else:
                    row["credits_source"] = "missing"
                row["imdb_id_agrees"] = (1 if row["imdb_id_tmdb"] == primary[tid] else 0) if row["imdb_id_tmdb"] and primary[tid] else None
                rows.append(tuple(row[c] for c in META_COLS))
            with con:
                con.executemany(f"INSERT OR REPLACE INTO film_metadata({','.join(META_COLS)}) "
                                f"VALUES ({','.join('?' * len(META_COLS))})", rows)
            n += len(rows)
            rl.n["ok"] = n
            if verbose and n % 50000 < 500:
                print(f"  {n:>7} films")
    if verbose:
        print(f"  done: {n} films; IMDb rows found for {stats['imdb']}, TMDb credits for {stats['credits']}")
        if n:
            dis = con.execute("SELECT COUNT(*) FROM film_metadata WHERE imdb_id_agrees=0").fetchone()[0]
            print(f"  TMDb's own imdb_id disagrees with the crosswalk tconst on {dis} films (worth a look in review)")
    return {"films": n, "imdb": stats["imdb"], "credits": stats["credits"]}


# =============================================================================
# 8. EXPORT + REPORT
# =============================================================================
# Bridge from Wikidata Q-ids to the comp engine's legacy festival ids/tiers
# (comp_3_festivals_tags.FESTIVALS), for the --compat export only.
LEGACY_TIER = {
    "Q42369": ("cannes", "big_three"), "Q49024": ("venice", "big_three"), "Q130871": ("berlin", "big_three"),
    "Q189887": ("sundance", "major"), "Q55122717": ("sxsw", "major"), "Q390018": ("tiff", "major"),
    "Q2402350": ("telluride", "major"), "Q853325": ("tribeca", "major"), "Q25648": ("nyff", "major"),
    "Q502053": ("locarno", "major"), "Q1666554": ("rotterdam", "major"), "Q622423": ("sansebastian", "major"),
    "Q332869": ("karlovyvary", "major"), "Q482633": ("busan", "major"), "Q1323689": ("bfi_london", "major"),
    "Q5252683": ("melbourne", "major"), "Q260979": ("sydney", "major"), "Q1071936": ("chicago", "major"),
    "Q1043431": ("tokyo", "major"), "Q975945": ("sitges", "genre"), "Q5434188": ("fantasticfest", "genre"),
    "Q3066295": ("fantasia", "genre"),
}
_SLUG_STOP = {"the", "of", "de", "di", "du", "la", "le", "el", "and", "for", "international", "film",
              "festival", "cinema", "movie", "fest", "internacional", "internazionale", "filmfestival",
              "filmfest", "annual", "opening", "virtual"}


def slug_unlisted(note_norm: str) -> str:
    base = re.sub(r"^\s*\d{1,3}(st|nd|rd|th)?\.?\s+", "", note_norm)
    base = re.sub(r"\b(19|20)\d{2}\b", "", base)
    toks = [t for t in re.findall(r"[a-z0-9]+", base) if t not in _SLUG_STOP][:6]
    return "other:" + ("_".join(toks) if toks else "unnamed")


def _atomic_csv(path: Path, header: list[str], rows: Iterable) -> int:
    """Write to a temp file and rename, so a crash mid-write cannot destroy the previous good output."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    n = 0
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
            n += 1
    os.replace(tmp, path)
    return n


def export_stage(cfg: Config, con, compat=False, verbose=True) -> dict:
    out = cfg.out_dir
    written = {}
    film_meta = {tid: (tc or "", title or "") for tid, tc, title in
                 con.execute("SELECT tmdb_id, tconsts, title FROM films")}

    def events_rows():
        q = ("SELECT e.tmdb_id, e.country, e.src_idx, e.type, e.type_name, e.date, e.note_raw, e.note_class, "
             "e.cert, e.lang, e.descriptors, r.fest_qid, r.fest_label, r.score, r.band, r.edition_num, "
             "r.edition_year, r.section FROM events e LEFT JOIN resolutions r ON r.tmdb_id=e.tmdb_id "
             "AND r.country=e.country AND r.src_idx=e.src_idx ORDER BY e.tmdb_id, e.country, e.src_idx")
        for row in con.execute(q):
            tid, cc, idx, t, tn, d, note, cls, cert, lang, desc, qid, label, score, band, en, ey, sec = row
            tc, title = film_meta.get(tid, ("", ""))
            accepted = band == "accept"
            channel = "festival" if accepted else TYPE_CHANNEL.get(t, "unknown")
            yield (tid, tc, title, cc, t, tn, d, note, "festival" if accepted else cls, channel,
                   qid if accepted else "", label if accepted else "", score if qid else "", band or "",
                   en if accepted else "", ey if accepted else "", sec if accepted else "", cert, lang, desc)

    written["events.csv"] = _atomic_csv(out / "events.csv",
        ["tmdb_id", "tconsts", "title", "country", "type", "type_name", "date", "note_raw", "note_class",
         "channel", "fest_qid", "fest_label", "fest_score", "fest_band", "edition_num", "edition_year",
         "section", "certification", "lang", "descriptors"], events_rows())

    written["festival_appearances.csv"] = _atomic_csv(out / "festival_appearances.csv",
        ["tmdb_id", "tconsts", "title", "fest_qid", "fest_label", "date", "edition_num", "edition_year",
         "section", "min_type", "types", "countries", "best_score", "notes"],
        ((tid, *film_meta.get(tid, ("", "")), qid, label, d, en, ey, sec, mt, ty, ccs, sc, notes)
         for tid, qid, ek, label, d, sc, mt, ty, ccs, notes, sec, en, ey in
         con.execute("SELECT * FROM appearances ORDER BY tmdb_id, date")))

    written["windows.csv"] = _atomic_csv(out / "windows.csv",
        ["tmdb_id", "tconsts", "title", "country", "premiere_date", "festival_date", "theatrical_ltd",
         "theatrical_wide", "first_theatrical", "digital_date", "physical_date", "tv_date",
         "days_theatrical_to_digital", "days_theatrical_to_physical", "n_entries", "theatrical_bom",
         "first_theatrical_source"],
        ((r[0], *film_meta.get(r[0], ("", "")), *r[1:]) for r in
         con.execute(f"SELECT {','.join(WINDOW_COLS)} FROM windows ORDER BY tmdb_id, country")))

    written["film_summary.csv"] = _atomic_csv(out / "film_summary.csv",
        ["tmdb_id", "tconsts", "title", "status", "earliest_any", "first_festival_date", "first_festival_qid",
         "first_festival_label", "n_festivals", "first_theatrical", "first_theatrical_cc", "us_premiere",
         "us_theatrical", "us_digital", "us_physical", "n_countries", "n_entries"],
        ((f[0], f[1] or "", f[2] or "", f[3], *s) for f, s in (
            (row[:4], row[4:]) for row in con.execute(
                "SELECT f.tmdb_id, f.tconsts, f.title, f.status, s.earliest_any, s.first_festival_date, "
                "s.first_festival_qid, s.first_festival_label, s.n_festivals, s.first_theatrical, "
                "s.first_theatrical_cc, s.us_premiere, s.us_theatrical, s.us_digital, s.us_physical, "
                "s.n_countries, s.n_entries FROM films f LEFT JOIN film_summary s ON s.tmdb_id=f.tmdb_id "
                "ORDER BY f.tmdb_id"))))

    # review queue: review-band rows that are NOT a duplicate of an already-accepted appearance
    accepted = {(tid, qid, ek) for tid, qid, ek in con.execute("SELECT tmdb_id, fest_qid, ed_key FROM appearances")}

    def review_rows():
        for tid, cc, idx, qid, label, score, band, en, ey, ev, reason, d, note, t in con.execute(
                "SELECT r.tmdb_id, r.country, r.src_idx, r.fest_qid, r.fest_label, r.score, r.band, r.edition_num, "
                "r.edition_year, r.evidence, r.reason, e.date, e.note_raw, e.type FROM resolutions r JOIN events e "
                "ON e.tmdb_id=r.tmdb_id AND e.country=r.country AND e.src_idx=r.src_idx WHERE r.band='review' "
                "ORDER BY r.score DESC"):
            dup = (tid, qid, ed_key(en, ey, d or "")) in accepted
            yield (tid, *film_meta.get(tid, ("", "")), cc, t, d, note, qid, label, score,
                   "duplicate_of_accepted" if dup else "review", reason, ev)

    written["review_queue.csv"] = _atomic_csv(out / "review_queue.csv",
        ["tmdb_id", "tconsts", "title", "country", "type", "date", "note_raw", "fest_qid", "fest_label",
         "score", "status", "reason", "evidence"], review_rows())

    # the unmatched festival-shaped tail, by frequency: the promotion list for the registry
    written["unlisted_festival_notes.csv"] = _atomic_csv(out / "unlisted_festival_notes.csv",
        ["note_norm", "n_events", "n_films", "example_note", "countries", "reason"],
        con.execute(
            "SELECT e.note_norm, COUNT(*), COUNT(DISTINCT e.tmdb_id), MIN(e.note_raw), "
            "GROUP_CONCAT(DISTINCT e.country), MIN(r.reason) FROM events e LEFT JOIN resolutions r "
            "ON r.tmdb_id=e.tmdb_id AND r.country=e.country AND r.src_idx=e.src_idx "
            "WHERE e.note_class='festival' AND (r.band IS NULL OR r.band='reject') "
            "GROUP BY e.note_norm ORDER BY COUNT(*) DESC"))

    if con.execute("SELECT COUNT(*) FROM film_metadata").fetchone()[0]:
        written["films.csv"] = _atomic_csv(out / "films.csv", META_COLS,
            con.execute(f"SELECT {','.join(META_COLS)} FROM film_metadata ORDER BY tmdb_id"))

    cov = coverage_ledger(con)
    written["coverage.csv"] = _atomic_csv(out / "coverage.csv", ["type", "type_name", "n", "with_note", "fill_pct"],
        ((t, d["name"], d["n"], d["with_note"], round(100 * d["with_note"] / d["n"], 1) if d["n"] else 0)
         for t, d in cov["types"].items()))
    written["registry.csv"] = _atomic_csv(out / "registry.csv",
        ["qid", "label", "country", "month", "n_aliases", "n_editions", "n_exact_windows", "source"],
        ((q, f["label"], f["country"], f["month"], len(f["aliases"]), len(f["editions"]),
          sum(1 for e in f["editions"] if e.get("precision") == "exact"), f["source"])
         for q, f in sorted(registry_rows(con).items())))

    if compat:
        written["01_events_tmdb_compat.csv"] = _atomic_csv(out / "01_events_tmdb_compat.csv",
            ["tconst", "source", "type_id", "type_label", "channel", "channel_source", "note_raw", "note_norm",
             "festival_id", "festival_tier", "festival_name", "is_festival", "country_iso", "country_raw",
             "region_status", "date", "certification", "opening_usd", "gross_usd", "match_status"],
            _compat_rows(con, film_meta))
    if verbose:
        print("EXPORT")
        for k, v in written.items():
            print(f"  {k:<32}{v:>9} rows")
        print(f"  -> {out}")
    return written


def _compat_rows(con, film_meta):
    """01_events.csv shape: one row per (tconst, entry). Festival identity is a
    Wikidata Q-id (wd:Qnnn), a legacy id where one exists, or other:<slug>.
    Reads `events_bo` when Box Office Mojo has been reconciled, so matched
    theatrical rows carry opening/gross and BOM-only dates appear as
    source='bom' rows -- the same shape comp_2_pipeline.reconcile_events made."""
    src = events_source(con)
    extra = ("e.source, e.opening_usd, e.gross_usd, e.match_status, e.bom_market" if src == "events_bo"
             else "'tmdb', NULL, NULL, 'tmdb_only', NULL")
    q = (f"SELECT e.tmdb_id, e.country, e.type, e.type_name, e.date, e.note_raw, e.note_norm, e.note_class, e.cert, "
         f"r.fest_qid, r.fest_label, r.band, {extra} FROM {src} e LEFT JOIN resolutions r ON r.tmdb_id=e.tmdb_id "
         f"AND r.country=e.country AND r.src_idx=e.src_idx ORDER BY e.tmdb_id, e.country, e.src_idx")
    for tid, cc, t, tn, d, note, nn, cls, cert, qid, label, band, source, opening, gross, mstat, market in con.execute(q):
        tconsts = [x for x in (film_meta.get(tid, ("", ""))[0]).split("|") if x]
        if not tconsts:
            continue
        if source == "bom":
            fid, tier, channel, chansrc, is_f, name = "", "", "theatrical", "bom_implicit", False, ""
        elif band == "accept":
            fid, tier = LEGACY_TIER.get(qid, (f"wd:{qid}", ""))
            channel, chansrc, is_f, name = "festival", "festival_match", True, label
        elif cls == "festival":
            fid, tier, channel, chansrc, is_f, name = slug_unlisted(nn), "regional", "festival", "festival_match", True, \
                nn.title()
        else:
            fid, tier, channel, chansrc, is_f, name = "", "", TYPE_CHANNEL.get(t, ""), "type_id", False, ""
        for tc in tconsts:
            yield (tc, source, float(t) if t is not None else "", tn, channel, chansrc, note or "", nn or "", fid, tier, name,
                   is_f, cc, market if source == "bom" else cc, "ok", d, cert or "",
                   opening if opening is not None else "", gross if gross is not None else "", mstat or "tmdb_only")


def report(cfg: Config, con):
    print_coverage(con)
    n_res = con.execute("SELECT COUNT(*) FROM resolutions").fetchone()[0]
    if n_res:
        print("\nRESOLUTION")
        for band, n in con.execute("SELECT band, COUNT(*) FROM resolutions GROUP BY band ORDER BY band"):
            print(f"  {band:<10}{n:>9}")
        n_app = con.execute("SELECT COUNT(*) FROM appearances").fetchone()[0]
        n_films = con.execute("SELECT COUNT(DISTINCT tmdb_id) FROM appearances").fetchone()[0]
        lost = con.execute("SELECT COUNT(*) FROM appearances WHERE min_type != 1").fetchone()[0]
        print(f"\n  {n_app} festival appearances across {n_films} films")
        if n_app:
            print(f"  a `type == 1` filter would lose {lost} of them ({100*lost/n_app:.0f}%)")
        print("\n  top festivals by appearances:")
        for label, n in con.execute("SELECT fest_label, COUNT(*) c FROM appearances GROUP BY fest_qid "
                                    "ORDER BY c DESC LIMIT 15"):
            print(f"    {label[:50]:<52}{n:>7}")
    runs = con.execute("SELECT stage, started, finished, n_done, n_ok, n_gone, n_error, note FROM runs "
                       "ORDER BY run_id DESC LIMIT 8").fetchall()
    if runs:
        print("\nRECENT RUNS")
        for r in runs:
            print(f"  {r[0]:<9} {r[1]} -> {r[2] or '...':<20} done={r[3]} ok={r[4]} gone={r[5]} err={r[6]} {r[7] or ''}")


# =============================================================================
# 9. SELF-TEST  -- offline checks + the live code path over real HTTP
# =============================================================================
# Written to FAIL when the code breaks, not to pass when it works: mechanisms,
# boundaries and the reported numbers, on the two REAL TMDb payloads captured
# for Parasite and Monster (fixtures/), plus a fault-injecting fake TMDb and a
# fake WDQS so the transport, retry, redaction and cache paths execute.

class _Scripted:
    """Scenario table for the fake servers: path -> list of (status, headers, body).
    The last response repeats; earlier ones are consumed once."""

    def __init__(self):
        self.script: dict[str, list] = {}
        self.seen: list[tuple[str, dict]] = []
        self.lock = threading.Lock()

    def add(self, path, *responses):
        self.script[path] = list(responses)

    def next(self, path):
        with self.lock:
            seq = self.script.get(path)
            if not seq:
                return 404, {}, b'{"status_code":34,"status_message":"not found"}'
            item = seq.pop(0) if len(seq) > 1 else seq[0]
            return item


def _fake_server(scripted: _Scripted):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _serve(self, path, body_in=b""):
            hdrs = dict(self.headers)
            if body_in:
                hdrs["_body"] = urllib.parse.unquote_plus(body_in.decode("utf-8", "replace"))
            scripted.seen.append((self.path, hdrs))
            status, headers, body = scripted.next(path)
            if callable(body):
                body = body(body_in)
            if isinstance(body, (dict, list)):
                body = json.dumps(body).encode()
            self.send_response(status)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            self._serve(self.path.split("?")[0])

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            self._serve(self.path.split("?")[0], self.rfile.read(n))

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def selftest(cfg: Config | None = None, verbose=True) -> tuple[int, int]:
    global BACKOFF_SCALE
    import tempfile
    cfg = cfg or Config.from_env()
    checks: list[tuple[str, bool, str]] = []

    def ck(name, cond, detail=""):
        checks.append((name, bool(cond), str(detail)[:200]))

    fx = {fid: json.loads((cfg.fixtures_dir / f"tmdb_{fid}.json").read_text(encoding="utf-8"))
          for fid in (496243, 1050035)}
    ck("real payload fixtures present", len(fx) == 2)

    # ---- normalisation and parsing ------------------------------------------
    ck("latin diacritics folded", norm("Filmfest München") == "filmfest munchen", norm("Filmfest München"))
    ck("japanese dakuten preserved", "ベ" in strip_accents("ベルリン国際映画祭"))
    ck("voiced kana stay distinct", norm("パリ") != norm("ハリ"))
    ck("hangul preserved", strip_accents("부산국제영화제") == "부산국제영화제")
    ck("devanagari vowel signs survive norm", norm("दिल") != norm("दुल"), f"{norm('दिल')!r} vs {norm('दुल')!r}")
    ck("ampersand normalised", norm("Black & White") == "black and white")
    p = parse_note("76th Cannes Film Festival")
    ck("western ordinal parsed", p.ordinal == 76 and norm(p.residual) == "cannes film festival", p)
    p = parse_note("第76回カンヌ国際映画祭")
    ck("japanese ordinal parsed", p.ordinal == 76 and p.residual == "カンヌ国際映画祭", p)
    ck("korean ordinal parsed", parse_note("제28회 부산국제영화제").ordinal == 28)
    ck("trailing year parsed", parse_note("Festival de Cannes 2021").year == 2021)
    ck("leading year parsed", parse_note("2005 Toronto International Film Festival").year == 2005)
    ck("CNC visa number is not a year", parse_note("Visa CNC 150985").year is None)
    ck("parenthetical section parsed",
       parse_note("San Sebastian International Film Festival (Perlak)").section == "Perlak")
    ck("dash section parsed", parse_note("Berlinale - Panorama").section == "Panorama")
    p = parse_note("Toronto International Film Festival - World Premiere")
    ck("premiere marker not mistaken for a section", p.premiere and p.section is None, f"{p.premiere}/{p.section}")
    ck("whitespace-only note parses cleanly", parse_note("   ").raw == "")
    for note, want in [("Blu-ray", "format"), ("4K Ultra HD Blu-ray Disc", "format"),
                       ("Bong Joon Ho Collection (Blu-ray)", "format"), ("Netflix", "platform"),
                       ("Arte•tv", "platform"), ("VOD", "platform"), ("Disney+", "platform"), ("MUBI", "platform"),
                       ("Theatrical re-release and IMAX release", "version"),
                       ("Colour / Black-and-White Edition", "version"), ("Visa CNC 150985", "admin"),
                       ("Cannes Film Festival", "festival"), ("Viennale", "festival"), ("NewFest", "festival"),
                       ("The Cannes Film Week (Gaumont)", "festival"), ("Los Angeles, California", "other"),
                       ("Leeds", "other"), ("", "empty")]:
        ck(f"classify {note!r} -> {want}", classify_note(note) == want, classify_note(note))

    # ---- flatten on the real payloads ---------------------------------------
    rows_p = flatten(496243, fx[496243])
    rows_m = flatten(1050035, fx[1050035])
    ck("Parasite: 138 entries across 63 countries", len(rows_p) == 138 and len({r["country"] for r in rows_p}) == 63,
       f"{len(rows_p)} / {len({r['country'] for r in rows_p})}")
    ck("Monster: 62 entries across 37 countries", len(rows_m) == 62 and len({r['country'] for r in rows_m}) == 37)
    ck("date is sliced, not converted", all(len(r["date"]) == 10 for r in rows_p) and
       any(r["date"] == "2019-05-21" and r["country"] == "FR" for r in rows_p))
    ck("src_idx is per country", [r["src_idx"] for r in rows_p if r["country"] == "US"] == list(range(8)))
    weird = flatten(1, {"release_dates": {"results": [
        {"iso_3166_1": "XX", "release_dates": [{"release_date": None, "type": None, "note": None},
                                              {"release_date": "2020-01-01T00:00:00.000Z"}, "junk"]},
        "junk", {"iso_3166_1": "YY"}]}})
    ck("null date / missing type / junk entries do not crash and are not dropped",
       len(weird) == 2 and weird[0]["date"] == "" and weird[0]["type"] is None and weird[1]["type_name"] == "unknown",
       weird)
    ck("no release_dates at all -> zero rows, no crash", flatten(2, {"id": 2}) == [])
    ck("bool is not a type", flatten(3, {"release_dates": {"results": [{"iso_3166_1": "A",
       "release_dates": [{"type": True, "release_date": "2020-01-01T00:00:00.000Z"}]}]}})[0]["type"] is None)

    # ---- registry + name scoring ---------------------------------------------
    seed = load_seed(cfg)
    ck("seed has 40 festivals with 35 edition windows", len(seed) == 40 and
       sum(len(f["editions"]) for f in seed) == 35)
    reg = Registry({f["qid"]: dict(f, source="seed") for f in seed})
    ck("exact alias hit scores 1.0", reg.name_score(norm("Festival de Cannes")).score == 1.0)
    ck("exact hit resolves an alias-only name", reg.name_score("viennale").qid == "Q609508")
    sit = reg.name_score(norm("Sitges International Fantastic Film Festival"))
    ck("boilerplate does not pull Sitges toward Cannes", sit.qid == "Q975945", sit)
    ck("recall-weighted (F2): extra descriptors cost little when every alias token is present",
       sit.score >= 0.8, sit)
    mami = reg.name_score(norm("Jio MAMI Mumbai Film Festival"))
    ck("sponsor-prefixed name still scores high", mami.qid == "Q17075409" and mami.score >= 0.85, mami)
    typo = reg.name_score(norm("Karlovy Varry International Film Festival"))
    ck("token-level typo tolerance", typo.qid == "Q332869" and typo.score >= 0.9 and not typo.extra_tokens, typo)
    ck("a short token is not typo-matched (vari != vary)", reg.typo_equiv("vari") is None)
    nyaff = reg.name_score(norm("New York Asian Film Festival"))
    ck("rival festival: distinguishing token flagged", nyaff.qid == "Q25648" and "asian" in nyaff.extra_tokens, nyaff)
    ck("junk note has no candidate above the floor",
       reg.name_score(norm("Los Angeles, California")).score < NAME_FLOOR)
    cjk = reg.name_score("カンヌ国際映画祭")
    ck("CJK alias hit", cjk.qid == "Q42369" and cjk.score == 1.0, cjk)
    ck("name scores are cached", reg.name_score("viennale") is reg.name_score("viennale"))
    for junk in ["tokyo drift", "berlin wall documentary", "sundance kid rodeo", "new york comic con",
                 "london marathon", "1899 cannes"]:
        rj, _ = resolve(reg, junk, "US", "2023-10-05")
        ck(f"junk rejected: {junk!r}", rj.band == "reject", f"{rj.band} {rj.score:.0f} {rj.qid} {rj.reason}")
    ck("festival-marker gate names itself",
       "no festival marker" in resolve(reg, "tokyo drift", "JP", "2023-10-05")[0].reason)

    # ---- resolver: the decisive cases ---------------------------------------
    r, _ = resolve(reg, "Cannes Film Festival", "FR", "2019-05-21")
    ck("Cannes premiere accepted at the 72nd edition", r.band == "accept" and r.qid == "Q42369"
       and r.edition and r.edition["num"] == 72, f"{r.band} {r.score} {r.edition}")
    r, _ = resolve(reg, "第76回カンヌ国際映画祭", "JP", "2023-05-17")
    ck("japanese note pinned to the 76th edition", r.qid == "Q42369" and r.band == "accept"
       and r.edition["num"] == 76, f"{r.qid} {r.band} {r.score}")
    r, _ = resolve(reg, "New York", "US", "2023-11-22")
    ck("city decoy 'New York' rejected on date evidence", r.band == "reject", f"{r.band} {r.score} {r.evidence}")
    r, _ = resolve(reg, "New York Film Festival", "US", "2019-10-07")
    ck("real NYFF screening accepted", r.band == "accept" and r.qid == "Q25648", f"{r.band} {r.score}")
    r, _ = resolve(reg, "New York Asian Film Festival", "US", "2019-10-07")
    ck("rival festival capped at review, named", r.band == "review" and "asian" in r.reason, f"{r.band} {r.reason}")
    r, _ = resolve(reg, "The Cannes Film Week (Gaumont)", "AR", "2023-11-30")
    ck("a touring 'Cannes Film Week' in Argentina in November is not Cannes",
       r.band != "accept", f"{r.band} {r.score} {r.evidence}")
    ck("a bare one-word alias is damped when the note says more",
       reg.name_score(norm("Cannes Film Week")).score < 0.9 and reg.name_score("cannes").score == 1.0,
       reg.name_score(norm("Cannes Film Week")))
    ck("a note that is entirely in brackets is a name, not a section",
       parse_note("(Hong Kong International Film Festival)").residual == "Hong Kong International Film Festival"
       and resolve(reg, "(Cannes Film Festival)", "FR", "2019-05-21")[0].band == "accept")
    two = dict(reg.f)
    two["Q1"] = dict(qid="Q1", label="SXSW (conference)", country="US", month=3, aliases=["SXSW"], sections=[], editions=[], source="wikidata")
    two["Q2"] = dict(qid="Q2", label="Sydney Film Festival (dup)", country="AU", month=6, aliases=["Sydney Film Festival"], sections=[], editions=[], source="wikidata")
    two["Q3"] = dict(qid="Q3", label="Sydney Film Festival (Nova Scotia)", country="CA", month=9, aliases=["Sydney Film Festival"], sections=[], editions=[], source="wikidata")
    reg2 = Registry(two)
    r2, _ = resolve(reg2, "SXSW", "US", "2019-03-10")
    ck("a shared name that ties on evidence is accepted by tie-break, preferring the seed, and says so",
       r2.qid == "Q55122717" and r2.band == "accept" and "tie-break" in r2.evidence[-1], (r2.qid, r2.band, r2.evidence[-1]))
    r3, _ = resolve(reg2, "Sydney Film Festival", "CA", "2019-09-20")
    ck("a shared name is separated by country evidence when it can be", r3.qid == "Q3" and "by evidence" in r3.evidence[-1], (r3.qid, r3.evidence[-1]))
    r4, _ = resolve(reg2, "Sydney Film Festival", "AU", "2023-06-11")
    ck("the Wikidata duplicate loses to the seed item with the edition window", r4.qid == "Q260979" and r4.band == "accept", r4.qid)
    ck("NewFest resolves despite a very different canonical label",
       resolve(reg, "NewFest", "US", "2023-10-21")[0].qid == "Q7013706")
    sc = lambda n, c, d: resolve(reg, n, c, d)[0].score
    ck("country agreement raises the score", sc("Sydney Film Festival", "AU", "2023-06-11") >
       sc("Sydney Film Festival", "US", "2023-06-11"))
    ck("country disagreement is a penalty, not merely a missing bonus",
       sc("Sydney Film Festival", "US", "2023-06-11") < sc("Sydney Film Festival", "AU", "2023-06-11") - W_COUNTRY)
    ck("no country on the festival: no signal either way",
       sc("Sydney Film Festival", "AU", "2023-06-11") - W_COUNTRY ==
       resolve(Registry({"Q1": dict(reg.f["Q260979"], country=None)}), "Sydney Film Festival", "AU", "2023-06-11")[0].score)
    ck("date-in-window raises the score", sc("Sydney Film Festival", "AU", "2023-06-11") >
       sc("Sydney Film Festival", "AU", "2023-03-11"))
    ck("a recognised section raises the score",
       sc("San Sebastian International Film Festival (Perlak)", "ES", "2023-09-24") >
       sc("San Sebastian International Film Festival", "ES", "2023-09-24"))
    ck("a foreign section earns nothing",
       sc("San Sebastian International Film Festival (Midnight Madness)", "ES", "2023-09-24") ==
       sc("San Sebastian International Film Festival", "ES", "2023-09-24"))
    ck("ordinal adds exactly W_ORDINAL and no month bonus",
       sc("72nd Locarno Film Festival", "CH", "2019-08-12") == sc("Locarno Film Festival", "CH", "2019-08-12") + W_ORDINAL)
    r, _ = resolve(reg, "44th Cannes Film Festival", "FR", "2019-05-21")
    ck("ordinal contradicting a numbered edition is not accepted", r.band != "accept", f"{r.band} {r.score} {r.evidence}")
    r, _ = resolve(reg, "5th Munich International Film Festival", "DE", "2023-06-27")
    ck("ordinal with no numbering to check is NOT penalised", r.band == "accept" and "no numbered edition" in " ".join(r.evidence),
       f"{r.band} {r.score} {r.evidence}")
    r, _ = resolve(reg, "Sundance Film Festival", "US", "2019-12-20")
    ck("month distance wraps around the year end", "vs usual" not in " ".join(r.evidence))
    ck("decoy short-circuits before scoring", "non-festival use" in resolve(reg, "Blu-ray", "US", "2020-01-14")[0].reason)
    for bad in ["", "not-a-date", "2019-13-45", None, "2019-05-21T00:00:00.000Z"]:
        try:
            rr, _ = resolve(reg, "Cannes Film Festival", "FR", bad)
            ok, det = True, rr.band
        except Exception as exc:
            ok, det = False, f"{type(exc).__name__}: {exc}"
        ck(f"malformed date does not crash: {bad!r}", ok, det)
    ck("raw TMDb date form resolves", resolve(reg, "Cannes Film Festival", "FR", "2019-05-21T00:00:00.000Z")[0].edition["num"] == 72)
    ck("window boundary inclusive with 2-day grace",
       pick_edition(reg.f["Q42369"], "2019-05-27", None, None)[1] == "window" and
       pick_edition(reg.f["Q42369"], "2019-05-28", None, None)[1] != "window")
    ck("ed_key separates years when the edition is unknown", ed_key(None, None, "2019-08-10") != ed_key(None, None, "2023-08-10"))
    ck("ed_key never falls back to 0 for a real date", ed_key(None, None, "2019-08-10") == "-1|2019")

    # ---- transport helpers ---------------------------------------------------
    ck("Retry-After integer", parse_retry_after("3", 9) == 3.0)
    ck("Retry-After HTTP-date", 0 <= parse_retry_after(email.utils.format_datetime(
        datetime.now(timezone.utc) + timedelta(seconds=5)), 9) <= 5.5)
    ck("Retry-After garbage -> default", parse_retry_after("soon", 9) == 9)
    ck("Retry-After far future is capped", parse_retry_after("99999", 9) == RETRY_AFTER_CAP)
    ck("redact strips a key from a URL", "abc123" not in redact("https://x/y?api_key=abc123&z=1"))
    ck("redact strips a bearer token", "tok" not in redact("Authorization: Bearer tok"))
    blob = compress_payload(fx[496243])
    ck("payload round-trips through zlib", decompress_payload(blob) == fx[496243] and len(blob) < 12000, len(blob))
    tb = TokenBucket(1000)
    t0 = time.monotonic(); tb.pause(0.3); tb.acquire()
    ck("a 429 pause blocks every acquirer", time.monotonic() - t0 >= 0.25)

    # ---- end-to-end offline pipeline on a temp database ---------------------
    with tempfile.TemporaryDirectory() as td:
        tcfg = Config(root=Path(td), db_path=Path(td) / "t.db", out_dir=Path(td) / "out",
                      crosswalk_csv=Path(td) / "cw.csv", fixtures_dir=cfg.fixtures_dir)
        (Path(td) / "cw.csv").write_text(
            "tconst,tmdb_id,source,tier,score,flags,decided_by,decided_at,run_date\n"
            "tt6751668,496243,tmdb_find,enriched_verified,1.0,,auto,x,y\n"
            "tt23736044,1050035,tmdb_find,confirmed,1.0,,auto,x,y\n"
            "tt0000001,,tmdb_find,no_tmdb_match,,,auto,x,y\n"
            "tt9999999,1050035,tmdb_find,enriched_weak,0.7,,auto,x,y\n"
            "tt0000002,777,tmdb_find,enriched_weak,0.7,,auto,x,y\n"
            "tt0000003,0,tmdb_find,enriched_weak,0.7,,auto,x,y\n"
            "tt0000004,-5,tmdb_find,enriched_weak,0.7,,auto,x,y\n", encoding="utf-8")
        con = connect(tcfg)
        st = load_universe(tcfg, con, verbose=False)
        ck("universe: rows without tmdb_id counted, not stored as 0",
           st["rows_without_tmdb_id"] == 3 and st["distinct_tmdb_ids"] == 3 and
           con.execute("SELECT COUNT(*) FROM films WHERE tmdb_id<=0").fetchone()[0] == 0, st)
        tc, tier = con.execute("SELECT tconsts, tier_best FROM films WHERE tmdb_id=1050035").fetchone()
        ck("universe: two tconsts on one tmdb_id are joined, best tier kept",
           tc == "tt23736044|tt9999999" and tier == "confirmed", (tc, tier))
        ck("universe re-run is idempotent", load_universe(tcfg, con, verbose=False)["films_in_table"] == 3)
        write_results(con, [FetchResult(496243, "ok", 200, fx[496243]), FetchResult(1050035, "ok", 200, fx[1050035]),
                            FetchResult(777, "gone", 404)], tcfg)
        ck("fetch outcomes stored as distinct states",
           dict(con.execute("SELECT status, COUNT(*) FROM films GROUP BY status")) == {"ok": 2, "gone": 1})
        ck("film summary columns filled from the payload",
           con.execute("SELECT title, imdb_id_tmdb, n_countries, n_entries FROM films WHERE tmdb_id=496243").fetchone()
           == ("Parasite", "tt6751668", 63, 138))
        ck("pending_ids skips fetched films", pending_ids(con, refresh=False) == [] and
           len(pending_ids(con, refresh=True)) == 3)
        st = ledger_stage(tcfg, con, verbose=False)
        ck("ledger flattened both films", st == {"films": 2, "events": 200}, st)
        ck("ledger re-run does nothing", ledger_stage(tcfg, con, verbose=False)["films"] == 0)
        ck("ledger rebuild redoes both", ledger_stage(tcfg, con, rebuild=True, verbose=False)["films"] == 2)
        cov = coverage_ledger(con)
        ck("coverage: type counts add up", sum(d["n"] for d in cov["types"].values()) == 200 and
           cov["types"][1]["n"] == 47 and cov["types"][1]["with_note"] == 46, cov["types"])
        seed_registry(tcfg, con, verbose=False)
        ck("registry seeded", con.execute("SELECT COUNT(*) FROM festival_registry").fetchone()[0] == 40)
        ck("seeding twice does not duplicate aliases",
           (seed_registry(tcfg, con, verbose=False), len(registry_rows(con)["Q42369"]["aliases"]))[1] == 7)
        sample = json.loads((cfg.fixtures_dir / "wdqs_editions_sample.json").read_text())
        regd = registry_rows(con)
        n_before = len(regd["Q42369"]["editions"])
        added = merge_editions(regd, sample["results"]["bindings"])
        ck("real WDQS shape merges editions, only adds", added > 100 and len(regd["Q42369"]["editions"]) > n_before
           and all(e["num"] == 72 for e in regd["Q42369"]["editions"] if e["year"] == 2019), added)
        ck("merging is idempotent and never duplicates an edition",
           merge_editions(regd, sample["results"]["bindings"]) == 0 and
           len({(e["num"], e["year"]) for e in regd["Q42369"]["editions"]}) == len(regd["Q42369"]["editions"]))
        ck("a seed window is never overwritten by a sparser live one",
           [e for e in regd["Q42369"]["editions"] if e["year"] == 2019][0]["start"] == "2019-05-14")
        ck("a blank-node date skips only its own binding",
           merge_editions(regd, [{"parent": {"value": "http://www.wikidata.org/entity/Q42369"},
                                  "start": {"value": "t123"}, "num": {"value": "1"}},
                                 {"parent": {"value": "http://www.wikidata.org/entity/Q42369"},
                                  "start": {"value": "1900-09-20T00:00:00Z"}, "end": {"value": "1900-10-05T00:00:00Z"},
                                  "num": {"value": "0"}}]) == 1)
        save_registry(con, regd)
        st = resolve_stage(tcfg, con, fetch_editions_live=False, verbose=False)
        ck("resolve covered both films", st["films"] == 2, st)
        ck("resolve re-run does nothing", resolve_stage(tcfg, con, fetch_editions_live=False, verbose=False)["films"] == 0)
        app = {(t, q): (d, en) for t, q, d, en in con.execute("SELECT tmdb_id, fest_qid, date, edition_num FROM appearances")}
        ck("Parasite Cannes 2019 pinned to edition 72", app.get((496243, "Q42369")) == ("2019-05-21", 72), app.get((496243, "Q42369")))
        ck("Monster Cannes 2023 pinned to edition 76", app.get((1050035, "Q42369")) == ("2023-05-17", 76))
        ck("Monster BFI London / Busan / Chicago found",
           all((1050035, q) in app for q in ("Q1323689", "Q482633", "Q1071936")))
        ck("Parasite Telluride found, Leeds bare city not accepted",
           (496243, "Q2402350") in app and not any(q == "Q3070482" for (t, q) in app if t == 496243))
        bands = dict(con.execute("SELECT band, COUNT(*) FROM resolutions GROUP BY band"))
        ck("nothing silently dropped: every noted event has a resolution row",
           sum(bands.values()) == con.execute("SELECT COUNT(*) FROM events WHERE note_raw != ''").fetchone()[0], bands)
        ck("every non-accepted note carries a reason", con.execute(
            "SELECT COUNT(*) FROM resolutions WHERE band!='accept' AND (reason IS NULL OR reason='')").fetchone()[0] == 0)
        ck("Visa CNC rows never became festivals", con.execute(
            "SELECT COUNT(*) FROM resolutions r JOIN events e USING(tmdb_id,country,src_idx) "
            "WHERE e.note_raw LIKE 'Visa CNC%' AND r.band != 'reject'").fetchone()[0] == 0)
        # one appearance entered under two countries with two types is ONE appearance
        with con:
            con.execute("INSERT INTO films(tmdb_id, status, ledger_ver) VALUES (999,'ok',?)", (LEDGER_VERSION,))
            con.executemany("INSERT INTO events(tmdb_id,country,src_idx,type,type_name,date,note_raw,note_norm,note_class,"
                            "cert,lang,descriptors) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                            [(999, "FR", 0, 1, "Premiere", "2019-05-21", "Cannes Film Festival", "cannes film festival", "festival", "", "", ""),
                             (999, "IT", 0, 2, "Theatrical (limited)", "2019-05-24", "Festival de Cannes", "festival de cannes", "festival", "", "", "")])
            for cc in ("FR", "IT"):
                con.execute("INSERT INTO resolutions VALUES (999,?,0,'Q42369','Cannes Film Festival',100,'accept',72,2019,'window',NULL,'','',?)", (cc, RESOLVER_VERSION))
        build_appearances(con, [999])
        a = con.execute("SELECT min_type, types, countries FROM appearances WHERE tmdb_id=999").fetchall()
        ck("dedup merges countries and languages; min_type is the EARLIEST type", a == [(1, "1,2", "FR|IT")], a)
        with con:
            for t in ("appearances", "resolutions", "events", "films"):
                con.execute(f"DELETE FROM {t} WHERE tmdb_id=999")
        n_app = con.execute("SELECT COUNT(*) FROM appearances").fetchone()[0]
        ck("appearances on real data with the 40-festival seed (the live notebook run found 16)",
           14 <= n_app <= 24, n_app)
        st = windows_stage(tcfg, con, verbose=False)
        w = con.execute("SELECT premiere_date, festival_date, theatrical_ltd, theatrical_wide, first_theatrical, "
                        "digital_date, physical_date, days_thea_to_dig, days_thea_to_phys FROM windows "
                        "WHERE tmdb_id=496243 AND country='US'").fetchone()
        ck("US window for Parasite: Telluride -> limited -> wide -> digital -> disc",
           w == ("2019-08-30", "2019-08-30", "2019-10-11", "2019-11-01", "2019-10-11", "2020-01-14", "2020-01-28", 95, 109), w)
        s = con.execute("SELECT earliest_any, first_festival_label, first_theatrical, first_theatrical_cc, us_theatrical, "
                        "n_countries FROM film_summary WHERE tmdb_id=496243").fetchone()
        ck("film summary: Parasite premiered at Cannes, opened first in Korea",
           s[0] == "2019-05-21" and s[1].startswith("Cannes") and s[2] == "2019-05-30" and s[3] == "KR"
           and s[4] == "2019-10-11" and s[5] == 63, s)
        ck("no-data is not zero: a gone film has no summary row",
           con.execute("SELECT COUNT(*) FROM film_summary WHERE tmdb_id=777").fetchone()[0] == 0)
        # ---- credits + metadata -------------------------------------------
        fake_credits = {"id": 496243, "cast": [
            {"id": 20738, "name": "Song Kang-ho", "order": 0}, {"id": 1315946, "name": "Lee Sun-kyun", "order": 1},
            {"id": 83384, "name": "Cho Yeo-jeong", "order": 2}, {"id": 1, "name": "Extra", "order": 9}],
            "crew": [{"id": 21684, "name": "Bong Joon-ho", "job": "Director", "department": "Directing"},
                     {"id": 21684, "name": "Bong Joon-ho", "job": "Screenplay", "department": "Writing"},
                     {"id": 2, "name": "Han Jin-won", "job": "Screenplay", "department": "Writing"},
                     {"id": 3, "name": "Hong Kyung-pyo", "job": "Director of Photography", "department": "Camera"},
                     {"id": 4, "name": "Jung Jae-il", "job": "Original Music Composer", "department": "Sound"}]}
        write_credit_results(con, [FetchResult(496243, "ok", 200, fake_credits), FetchResult(1050035, "error", 500, None, "HTTP 500")], tcfg)
        ck("credits outcomes stored per film",
           dict(con.execute("SELECT tmdb_id, credits_status FROM films WHERE credits_status IS NOT NULL")) == {496243: "ok", 1050035: "error"})
        ck("pending credit ids = fetched films without ok credits", pending_credit_ids(con, refresh=False) == [1050035])
        cf = tmdb_credit_fields(fake_credits)
        ck("credit fields: director once, writers deduped, cast in billing order",
           cf["directors_tmdb"] == "Bong Joon-ho" and cf["director_tmdb_ids"] == "21684"
           and cf["writers_tmdb"] == "Bong Joon-ho|Han Jin-won" and cf["cast_tmdb"] == "Song Kang-ho|Lee Sun-kyun|Cho Yeo-jeong|Extra"
           and cf["dop_tmdb"] == "Hong Kyung-pyo" and cf["composer_tmdb"] == "Jung Jae-il", cf)
        ck("credit fields survive an empty payload", tmdb_credit_fields({})["cast_tmdb"] == "")
        idb = Path(td) / "imdb.db"
        ic = sqlite3.connect(str(idb))
        ic.executescript("""
            CREATE TABLE titles(tconst TEXT PRIMARY KEY, titleType TEXT, primaryTitle TEXT, originalTitle TEXT,
                                isAdult INT, startYear INT, runtimeMinutes INT, genres TEXT);
            CREATE TABLE ratings(tconst TEXT PRIMARY KEY, averageRating REAL, numVotes INT);
            CREATE TABLE crew(tconst TEXT PRIMARY KEY, directors TEXT, writers TEXT);
            CREATE TABLE principals(tconst TEXT, ordering INT, nconst TEXT, category TEXT);
            CREATE TABLE names(nconst TEXT PRIMARY KEY, primaryName TEXT, birthYear INT);
            INSERT INTO titles VALUES ('tt6751668','movie','Parasite','Gisaengchung',0,2019,132,'Drama,Thriller');
            INSERT INTO ratings VALUES ('tt6751668',8.5,1000000);
            INSERT INTO crew VALUES ('tt6751668','nm0094435','nm0094435,nm10467544');
            INSERT INTO principals VALUES ('tt6751668',1,'nm0814280','actor'),('tt6751668',2,'nm1032856','actor'),
                                          ('tt6751668',3,'nm0094435','director');
            INSERT INTO names VALUES ('nm0094435','Bong Joon Ho',1969),('nm0814280','Song Kang-ho',1967),
                                     ('nm1032856','Lee Sun-kyun',1975);
        """)
        ic.commit(); ic.close()
        st = metadata_stage(tcfg, con, imdb_path=idb, verbose=False)
        ck("metadata built for both fetched films", st["films"] == 2 and st["imdb"] == 1 and st["credits"] == 1, st)
        m = dict(zip(META_COLS, con.execute(f"SELECT {','.join(META_COLS)} FROM film_metadata WHERE tmdb_id=496243").fetchone()))
        ck("metadata: TMDb fields from the payload",
           m["title"] == "Parasite" and m["original_language"] == "ko" and m["runtime_tmdb"] == 133 and m["origin_country"] == "KR"
           and m["release_date_tmdb"] == "2019-05-30", {k: m[k] for k in ("title", "original_language", "runtime_tmdb", "origin_country", "release_date_tmdb")})
        ck("metadata: IMDb fields joined and names resolved",
           m["year_imdb"] == 2019 and m["genres_imdb"] == "Drama|Thriller" and m["imdb_rating"] == 8.5
           and m["directors_imdb"] == "Bong Joon Ho" and m["writers_imdb"] == "Bong Joon Ho|nm10467544"
           and m["cast_imdb"] == "Song Kang-ho|Lee Sun-kyun" and m["imdb_source"] == "imdb.db", m)
        ck("metadata: TMDb credits joined", m["directors_tmdb"] == "Bong Joon-ho" and m["credits_source"] == "tmdb")
        ck("metadata: TMDb's own imdb_id agrees with the crosswalk", m["imdb_id_agrees"] == 1)
        m2 = dict(zip(META_COLS, con.execute(f"SELECT {','.join(META_COLS)} FROM film_metadata WHERE tmdb_id=1050035").fetchone()))
        ck("metadata: absent sources are recorded as missing, not blank",
           m2["imdb_source"] == "missing" and m2["credits_source"] == "missing" and m2["directors_imdb"] is None
           and m2["title"] == "Monster", (m2["imdb_source"], m2["credits_source"], m2["directors_imdb"]))
        ck("metadata re-run does nothing; rebuild redoes both",
           metadata_stage(tcfg, con, imdb_path=idb, verbose=False)["films"] == 0
           and metadata_stage(tcfg, con, rebuild=True, imdb_path=idb, verbose=False)["films"] == 2)
        ck("metadata without imdb.db still builds, flagged missing",
           metadata_stage(tcfg, con, rebuild=True, imdb_path=Path(td) / "nope.db", verbose=False)["imdb"] == 0)
        metadata_stage(tcfg, con, rebuild=True, imdb_path=idb, verbose=False)

        written = export_stage(tcfg, con, compat=True, verbose=False)
        ck("export wrote films.csv", written.get("films.csv") == 2, written)
        ck("export wrote every file with rows", written["events.csv"] == 200 and written["festival_appearances.csv"] == n_app
           and written["film_summary.csv"] == 3 and written["01_events_tmdb_compat.csv"] >= 262, written)
        with open(tcfg.out_dir / "01_events_tmdb_compat.csv", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        ck("compat export: one row per tconst, 01_events columns, Cannes carries the legacy id",
           len(rows) == 200 + 62 and set(rows[0]) >= {"tconst", "type_id", "festival_id", "is_festival", "channel"}
           and any(r["festival_id"] == "cannes" and r["festival_tier"] == "big_three" for r in rows), len(rows))
        ck("compat export: unlisted festival kept as other:<slug>, never dropped",
           any(r["festival_id"].startswith("other:") and r["festival_tier"] == "regional" for r in rows))
        with open(tcfg.out_dir / "events.csv", newline="", encoding="utf-8") as fh:
            ev = list(csv.DictReader(fh))
        ck("events export: accepted festival rows carry qid and channel=festival",
           any(r["fest_qid"] == "Q42369" and r["channel"] == "festival" for r in ev)
           and all(r["fest_qid"] == "" for r in ev if r["fest_band"] != "accept"))
        runs = con.execute("SELECT stage, finished FROM runs").fetchall()
        ck("every stage logged a finished run", len(runs) >= 4 and all(f for _, f in runs), runs)

        # ---- the LIVE code path over real HTTP: fake TMDb + fake WDQS -----
        old_scale, BACKOFF_SCALE = BACKOFF_SCALE, 0.001
        sc_ = _Scripted()
        srv, base = _fake_server(sc_)
        ok_body = fx[1050035]
        sc_.add("/3/movie/1050035", (200, {}, ok_body))
        sc_.add("/3/movie/404404", (404, {}, b'{"status_code":34}'))
        sc_.add("/3/movie/429429", (429, {"Retry-After": "0"}, b"slow down"), (200, {}, dict(ok_body, id=429429)))
        sc_.add("/3/movie/500500", (500, {}, b"boom"), (502, {}, b"boom"), (200, {}, dict(ok_body, id=500500)))
        sc_.add("/3/movie/222222", (200, {}, b"<html>not json</html>"), (200, {}, dict(ok_body, id=222222)))
        sc_.add("/3/movie/333333", (200, {}, dict(ok_body, id=1)))                      # wrong id
        sc_.add("/3/movie/444444", (200, {}, {"id": 444444, "title": "Empty", "release_dates": None}))
        sc_.add("/3/movie/555555", (429, {"Retry-After": email.utils.format_datetime(
            datetime.now(timezone.utc) + timedelta(seconds=1))}, b""), (200, {}, dict(ok_body, id=555555)))
        sc_.add("/3/movie/666666", (503, {}, b"down"))                                # never recovers
        sc_.add("/3/movie/1050035/credits", (200, {}, {"id": 1050035, "cast": [{"id": 9, "name": "Sakura Ando", "order": 0}],
                                                        "crew": [{"id": 8, "name": "Hirokazu Kore-eda", "job": "Director"}]}))
        sc_.add("/3/movie/429429/credits", (404, {}, b"{}"))
        sc_.add("/3/movie/500500/credits", (503, {}, b"down"))
        sc_.add("/3/movie/changes", (200, {}, {"results": [{"id": 1050035}, {"id": 5}], "page": 1, "total_pages": 2}),
                (200, {}, {"results": [{"id": 777}, {"id": "bad"}], "page": 2, "total_pages": 2}))
        try:
            lcfg = Config(root=Path(td), db_path=Path(td) / "live.db", out_dir=Path(td) / "lout",
                          crosswalk_csv=Path(td) / "cw.csv", fixtures_dir=cfg.fixtures_dir,
                          api_key="SECRETKEY123", rps=200, workers=4)
            lcon = connect(lcfg)
            ids = [1050035, 404404, 429429, 500500, 222222, 333333, 444444, 555555, 666666, 777]
            load_universe(lcfg, lcon, ids=ids, verbose=False)
            client = TmdbClient(lcfg, base=base + "/3")
            counts = fetch_stage(lcfg, lcon, client=client, verbose=False)
            ck("live path: outcomes", counts == Counter(ok=6, gone=2, error=2), counts)
            status = dict(lcon.execute("SELECT tmdb_id, status FROM films"))
            ck("live path: 404 is 'gone', not error", status[404404] == "gone")
            ck("live path: 429 with Retry-After recovers", status[429429] == "ok")
            ck("live path: 5xx retried then recovers", status[500500] == "ok")
            ck("live path: non-JSON 200 retried then recovers", status[222222] == "ok")
            ck("live path: Retry-After as HTTP-date recovers", status[555555] == "ok")
            ck("live path: a 200 with the wrong id is an error, not a fact", status[333333] == "error")
            ck("live path: a film with release_dates=null is ok with 0 entries",
               status[444444] == "ok" and lcon.execute("SELECT n_entries FROM films WHERE tmdb_id=444444").fetchone()[0] == 0)
            ck("live path: sustained 5xx ends as error after MAX_TRIES",
               status[666666] == "error" and sum(1 for p, _ in sc_.seen if p.startswith("/3/movie/666666")) == MAX_TRIES)
            errs = [e for (e,) in lcon.execute("SELECT error FROM films WHERE error IS NOT NULL")]
            ck("live path: the key never reaches the database", all("SECRETKEY123" not in (e or "") for e in errs), errs)
            ck("live path: v3 key travels as the api_key query param, never a header",
               all("api_key=SECRETKEY123" in p for p, _ in sc_.seen if "/movie/" in p)
               and all("Authorization" not in h for _, h in sc_.seen))
            ck("live path: raw payload stored verbatim", decompress_payload(
                lcon.execute("SELECT json_z FROM raw_payloads WHERE tmdb_id=1050035").fetchone()[0]) == ok_body)
            ck("live path: errors are retried next run", pending_ids(lcon, refresh=False) == [333333, 666666])
            cc_ = fetch_stage(lcfg, lcon, client=client, verbose=False, kind="credits", ids=[1050035, 429429, 500500])
            ck("live path: credits pass stores ok / gone / error per film",
               cc_ == Counter(ok=1, gone=1, error=1) and dict(lcon.execute(
                   "SELECT tmdb_id, credits_status FROM films WHERE credits_status IS NOT NULL")) ==
               {1050035: "ok", 429429: "gone", 500500: "error"}, cc_)
            ck("live path: credits payload stored verbatim", decompress_payload(lcon.execute(
                "SELECT json_z FROM tmdb_credits WHERE tmdb_id=1050035").fetchone()[0])["crew"][0]["name"] == "Hirokazu Kore-eda")
            n = changes_stage(lcfg, lcon, days=1, client=client, verbose=False)
            ck("changes: paged, ids outside the universe ignored, junk ignored, hits marked stale",
               n == 2 and lcon.execute("SELECT status FROM films WHERE tmdb_id=1050035").fetchone()[0] == "stale"
               and lcon.execute("SELECT status FROM films WHERE tmdb_id=777").fetchone()[0] == "stale", n)
            # fatal key
            sc_.add("/3/movie/1050035", (401, {}, b'{"status_code":7}'))
            lcon.execute("UPDATE films SET status='pending' WHERE tmdb_id=1050035"); lcon.commit()
            try:
                fetch_stage(lcfg, lcon, client=client, verbose=False, ids=[1050035])
                fatal = None
            except FatalApiError as e:
                fatal = e
            ck("live path: a rejected key aborts the run immediately", fatal is not None and "SECRETKEY" not in str(fatal))
            # bearer token path
            bcfg = Config(root=Path(td), db_path=Path(td) / "b.db", out_dir=Path(td), crosswalk_csv=Path(td) / "cw.csv",
                          fixtures_dir=cfg.fixtures_dir, read_token="TOKEN456", rps=200)
            sc_.seen.clear(); sc_.add("/3/movie/1050035", (200, {}, ok_body))
            TmdbClient(bcfg, base=base + "/3").movie(1050035, DEFAULT_APPEND)
            ck("live path: v4 token goes in the Authorization header, no api_key param",
               any(h.get("Authorization") == "Bearer TOKEN456" and "api_key" not in p for p, h in sc_.seen))
            # fake WDQS
            sc_.add("/sparql", (429, {"Retry-After": "0"}, b""), (200, {}, sample))
            w = WdqsClient(lcon, url=base + "/sparql", min_gap=0)
            seed_registry(lcfg, lcon, verbose=False)
            n_ed = fetch_editions(lcfg, lcon, ["Q42369", "Q49024", "Q999999"], wdqs=w, verbose=False)
            ck("WDQS: 429 then 200; editions merged for known qids only", n_ed > 100 and w.calls == 2, (n_ed, w.calls))
            ck("WDQS: response cached, second call is free",
               (fetch_editions(lcfg, lcon, ["Q42369", "Q49024", "Q999999"], wdqs=w, verbose=False), w.calls)[1] == 2)
            # registry refresh: festival list, then names (one language)
            ent = "http://www.wikidata.org/entity/"
            sc_.add("/sparql",
                    (200, {}, {"results": {"bindings": [
                        {"f": {"value": ent + "Q42369"}, "iso": {"value": "FR"}, "monthLabel": {"value": "May"}},
                        {"f": {"value": ent + "Q7777"}, "iso": {"value": "AR"}, "monthLabel": {"value": "November"}},
                        {"f": {"value": ent + "Q8888"}}]}}),
                    (200, {}, {"results": {"bindings": [
                        {"f": {"value": ent + "Q7777"}, "name": {"value": "Mar del Plata International Film Festival"}, "kind": {"value": "label"}, "lang": {"value": "en"}},
                        {"f": {"value": ent + "Q7777"}, "name": {"value": "Festival Internacional de Cine de Mar del Plata"}, "kind": {"value": "alias"}, "lang": {"value": "es"}},
                        {"f": {"value": ent + "Q42369"}, "name": {"value": "Cannes Film Festival"}, "kind": {"value": "label"}, "lang": {"value": "en"}},
                        {"f": {"value": ent + "Q42369"}, "name": {"value": "Festival de Cannes 2.0"}, "kind": {"value": "alias"}, "lang": {"value": "fr"}},
                        {"f": {"value": ent + "Q8888"}, "name": {"value": "Festival du film de Nulle Part"}, "kind": {"value": "label"}, "lang": {"value": "fr"}},
                        {"f": {"value": ent + "Q0000"}, "name": {"value": "orphan"}, "kind": {"value": "label"}, "lang": {"value": "en"}}]}}))
            w2 = WdqsClient(lcon, url=base + "/sparql", min_gap=0)
            rr = refresh_registry(lcfg, lcon, wdqs=w2, langs=("en",), verbose=False)
            regl = registry_rows(lcon)
            ck("refresh: new festival created with label, alias, country, month",
               regl.get("Q7777", {}).get("label") == "Mar del Plata International Film Festival"
               and "Festival Internacional de Cine de Mar del Plata" in regl["Q7777"]["aliases"]
               and regl["Q7777"]["country"] == "AR" and regl["Q7777"]["month"] == 11, regl.get("Q7777"))
            ck("refresh: seed festival keeps its label, gains the alias, keeps its editions",
               regl["Q42369"]["label"] == "Cannes Film Festival" and "Festival de Cannes 2.0" in regl["Q42369"]["aliases"]
               and len(regl["Q42369"]["editions"]) > 2 and regl["Q42369"]["source"] == "seed+wikidata")
            ck("refresh: no English label -> next language's label, not duplicated as an alias",
               regl["Q8888"]["label"] == "Festival du film de Nulle Part" and regl["Q8888"]["aliases"] == [], regl["Q8888"])
            last_body = sc_.seen[-1][1].get("_body", "")
            ck("refresh: names were fetched in a VALUES batch naming the festivals, all languages at once",
               "VALUES ?f" in last_body and "wd:Q7777" in last_body and 'LANG(?name) IN ("en")' in last_body
               and "P279" not in last_body, [k for k in ("VALUES ?f", "wd:Q7777", '"ja"', "P279") if k in last_body])
            ck("refresh: names for unknown items are ignored", "Q0000" not in regl and rr["new"] == 2, rr)
            ck("refresh: a Registry builds from the refreshed table and resolves the Spanish name exactly",
               Registry(regl).name_score(norm("Festival Internacional de Cine de Mar del Plata")).exact)
            sc_.add("/sparql", (500, {}, b"x"))
            try:
                WdqsClient(lcon, url=base + "/sparql", min_gap=0).query("SELECT 1", "fresh")
                wd_err = None
            except RuntimeError as e:
                wd_err = e
            ck("WDQS: sustained failure raises, does not cache", wd_err is not None and
               lcon.execute("SELECT COUNT(*) FROM wdqs_cache WHERE key='fresh'").fetchone()[0] == 0)
            sc_.add("/sparql", (400, {}, b"malformed query"))
            try:
                WdqsClient(lcon, url=base + "/sparql", min_gap=0).query("SELECT 2", "fresh2")
                wd_err = None
            except RuntimeError as e:
                wd_err = e
            ck("WDQS: a 400 (bad query) raises at once and is not cached as an empty result",
               wd_err is not None and "400" in str(wd_err) and
               lcon.execute("SELECT COUNT(*) FROM wdqs_cache WHERE key='fresh2'").fetchone()[0] == 0)
            # transport failure: nothing listening
            srv.shutdown(); srv.server_close()
            dead = TmdbClient(lcfg, base=base + "/3").movie(1050035, DEFAULT_APPEND)
            ck("transport failure is an error outcome, not a crash", dead.outcome == "error" and "transport" in dead.error, dead)
        finally:
            BACKOFF_SCALE = old_scale
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
# 10. CLI
# =============================================================================
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", help="path to release_events.db (default: <project>/release_events/release_events.db)")
    ap.add_argument("--rps", type=float, default=DEFAULT_RPS)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--append", default=DEFAULT_APPEND, help="append_to_response value")
    sub = ap.add_subparsers(dest="cmd", required=True)
    u = sub.add_parser("universe"); u.add_argument("--source", help="crosswalk CSV (default 28_imdb_tmdb_crosswalk.csv)")
    u.add_argument("--ids", help="text file of tmdb_ids, one per line")
    f = sub.add_parser("fetch"); f.add_argument("--limit", type=int); f.add_argument("--refresh", action="store_true",
                                                                                    help="re-fetch everything")
    cr = sub.add_parser("credits", help="/movie/{id}/credits for every fetched film")
    cr.add_argument("--limit", type=int); cr.add_argument("--refresh", action="store_true")
    md = sub.add_parser("metadata", help="film_metadata from TMDb payloads + credits + imdb.db")
    md.add_argument("--rebuild", action="store_true"); md.add_argument("--imdb-db", help="path to imdb.db")
    c = sub.add_parser("changes"); c.add_argument("--days", type=int, default=1)
    l = sub.add_parser("ledger"); l.add_argument("--rebuild", action="store_true")
    r = sub.add_parser("registry"); r.add_argument("--refresh", action="store_true", help="pull from Wikidata")
    r.add_argument("--editions", action="store_true", help="fetch edition windows for every registry festival")
    s = sub.add_parser("resolve"); s.add_argument("--rebuild", action="store_true")
    s.add_argument("--no-editions", action="store_true", help="do not contact Wikidata for edition windows")
    sub.add_parser("windows")
    e = sub.add_parser("export"); e.add_argument("--compat", action="store_true", help="also write a 01_events-shaped CSV")
    sub.add_parser("report")
    sub.add_parser("selftest")
    a = ap.parse_args(argv)

    cfg = Config.from_env()
    if a.db:
        cfg.db_path = Path(a.db); cfg.out_dir = cfg.db_path.parent / "out"
    cfg.rps, cfg.workers, cfg.append = a.rps, a.workers, a.append
    if a.cmd == "selftest":
        passed, total = selftest(cfg)
        return 0 if passed == total else 1
    con = connect(cfg)
    print(f"[{cfg.db_path}]")
    if a.cmd == "universe":
        ids = [int(x) for x in Path(a.ids).read_text().split()] if a.ids else None
        load_universe(cfg, con, source=a.source, ids=ids)
    elif a.cmd == "fetch":
        fetch_stage(cfg, con, limit=a.limit, refresh=a.refresh)
    elif a.cmd == "credits":
        fetch_stage(cfg, con, limit=a.limit, refresh=a.refresh, kind="credits")
    elif a.cmd == "metadata":
        metadata_stage(cfg, con, rebuild=a.rebuild, imdb_path=Path(a.imdb_db) if a.imdb_db else None)
    elif a.cmd == "changes":
        changes_stage(cfg, con, days=a.days)
    elif a.cmd == "ledger":
        ledger_stage(cfg, con, rebuild=a.rebuild)
        print_coverage(con)
    elif a.cmd == "registry":
        if a.refresh:
            refresh_registry(cfg, con)
        else:
            seed_registry(cfg, con)
        if a.editions:
            fetch_editions(cfg, con, registry_rows(con).keys())
    elif a.cmd == "resolve":
        resolve_stage(cfg, con, rebuild=a.rebuild, fetch_editions_live=not a.no_editions)
    elif a.cmd == "windows":
        windows_stage(cfg, con)
    elif a.cmd == "export":
        export_stage(cfg, con, compat=a.compat)
    elif a.cmd == "report":
        report(cfg, con)
    return 0


if __name__ == "__main__":
    sys.exit(main())
