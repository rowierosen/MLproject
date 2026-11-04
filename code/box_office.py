#!/usr/bin/env python3
"""
BOX OFFICE -- Box Office Mojo grosses for the films in release_events.db.

Per film (keyed on tconst, which is how BOM addresses titles):
  * headline grosses: domestic / international / worldwide, domestic distributor,
    domestic opening, budget, MPAA, earliest release date
  * every territory: release date, opening, gross, lifetime gross, BOM release id
  * the domestic release: opening theaters, widest release, release date, and
    the WEEKLY table (rank, gross, theaters, average, to-date)
  * the UK release: distributor, opening, widest, and the WEEKEND table
  * each territory row matched to the TMDb theatrical event it corresponds to

Stages (each a CLI sub-command and a function):

  scope     choose films: any TMDb theatrical entry (type 2/3) dated >= --min-year
  fetch     polite single-connection capture of the pages, cached verbatim
  parse     pure function over the cached HTML -> bo_* tables
  match     BOM territory rows <-> TMDb theatrical events (ISO + date window)
  reconcile matched TMDb events carry BOM opening/gross; BOM-only dates become
            source='bom' theatrical events (events_bo); windows rebuilt from it
  export    CSVs, incl. 06_weekly_domestic- and 14_uk_weekend-shaped files
  report
  selftest  parsers on real cached BOM pages + a fault-injecting fake BOM

Shares release_events.db with release_events.py (same folder). Nothing in
release_events' own tables is modified.

Pages per film: 1 title page for every film in scope (most have no BOM page
and stop there), then for films with data 1 page per release group, 1 for the
domestic weekly table and 1 for the UK weekend table: 3-5 pages. At the
default 1.5 s between requests that is ~1.5 s per film without data and ~7 s
per film with data. Resumable; Ctrl-C safe.

A note on footprint: Box Office Mojo is an IMDb/Amazon property whose
conditions of use restrict automated access. This tool fetches one page at a
time with a pause, caches everything so nothing is fetched twice, and stops
itself when the site starts answering with a robot check.
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
import threading
import time
import zlib
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_events as R   # noqa: E402  (Config, connect, RunLog, redact, utcnow)

__version__ = "0.1.0"
PARSER_VERSION = 3
BOM_BASE = os.environ.get("BOM_BASE", "https://www.boxofficemojo.com")
BOM_SLEEP = float(os.environ.get("BOM_SLEEP", "1.5"))     # seconds between requests
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
HTTP_TIMEOUT = 30
MAX_TRIES = 4
MAX_GROUPS = 4                # release groups fetched per film (original + re-releases)
BLOCK_PAUSE = 300             # seconds to wait after a robot check or a throttling burst
BLOCK_ABORT = 3               # consecutive robot checks that end the run
THROTTLE_ABORT = 8            # consecutive throttling pauses (40 min) that end the run
MATCH_WINDOW_DAYS = 7
BACKOFF_SCALE = 1.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS bo_films (
  tmdb_id      INTEGER PRIMARY KEY,
  tconst       TEXT NOT NULL,
  tconsts      TEXT,
  title        TEXT,
  scope_reason TEXT,
  status       TEXT NOT NULL DEFAULT 'pending',   -- pending | ok | none | error
  fetched_at   TEXT,
  error        TEXT,
  n_pages      INTEGER,
  parsed_ver   INTEGER
);
CREATE INDEX IF NOT EXISTS bo_films_status ON bo_films(status);

CREATE TABLE IF NOT EXISTS bo_pages (
  key         TEXT PRIMARY KEY,      -- title:tt.. | group:gr.. | release:rl..:weekly | release:rl..:weekend
  tmdb_id     INTEGER,
  url         TEXT,
  fetched_at  TEXT,
  http_status INTEGER,
  html_z      BLOB
);
CREATE INDEX IF NOT EXISTS bo_pages_film ON bo_pages(tmdb_id);

CREATE TABLE IF NOT EXISTS bo_summary (
  tmdb_id                INTEGER PRIMARY KEY,
  tconst                 TEXT,
  domestic_usd           INTEGER,
  international_usd      INTEGER,
  worldwide_usd          INTEGER,
  distributor            TEXT,
  domestic_opening_usd   INTEGER,
  opening_theaters       INTEGER,
  widest_release         INTEGER,
  domestic_release_date  TEXT,
  domestic_release_id    TEXT,
  budget_usd             INTEGER,
  earliest_release_date  TEXT,
  earliest_release_note  TEXT,
  mpaa                   TEXT,
  n_release_groups       INTEGER,
  release_groups         TEXT,
  uk_release_id          TEXT,
  uk_distributor         TEXT,
  uk_opening_usd         INTEGER,
  uk_opening_theaters    INTEGER,
  uk_widest_release      INTEGER,
  uk_release_date        TEXT,
  uk_gross_usd           INTEGER,
  parsed_at              TEXT
);

CREATE TABLE IF NOT EXISTS bo_territory (
  tmdb_id            INTEGER NOT NULL,
  tconst             TEXT,
  release_group      TEXT NOT NULL,     -- grNNN, or 'lifetime' for the title-page totals
  group_label        TEXT,
  market_raw         TEXT NOT NULL,
  iso                TEXT,
  region_status      TEXT,              -- ok | unresolved | aggregate
  release_date       TEXT,
  opening_usd        INTEGER,
  gross_usd          INTEGER,
  release_id         TEXT,
  PRIMARY KEY (tmdb_id, release_group, market_raw)
);

CREATE TABLE IF NOT EXISTS bo_weekly_us (
  tmdb_id INTEGER NOT NULL, tconst TEXT, release_id TEXT NOT NULL,
  week_number INTEGER, bom_year INTEGER, bom_week INTEGER, date_display TEXT,
  week_start TEXT, week_end TEXT, rank INTEGER, weekly_gross_usd INTEGER,
  pct_change_lw REAL, theaters INTEGER, theaters_change INTEGER,
  avg_per_theater_usd INTEGER, to_date_usd INTEGER,
  PRIMARY KEY (tmdb_id, release_id, bom_year, bom_week)
);

CREATE TABLE IF NOT EXISTS bo_weekend_uk (
  tmdb_id INTEGER NOT NULL, tconst TEXT, release_id TEXT NOT NULL, currency TEXT,
  weekend_number INTEGER, bom_year INTEGER, bom_week INTEGER, date_display TEXT,
  weekend_start TEXT, weekend_end TEXT, rank INTEGER, weekend_gross INTEGER,
  pct_change_lw REAL, theaters INTEGER, theaters_change INTEGER,
  avg_per_theater INTEGER, to_date INTEGER,
  PRIMARY KEY (tmdb_id, release_id, bom_year, bom_week)
);

CREATE TABLE IF NOT EXISTS events_bo (
  tmdb_id       INTEGER NOT NULL,
  country       TEXT NOT NULL,
  src_idx       INTEGER NOT NULL,     -- TMDb index, or a negative index for a BOM-only row
  source        TEXT NOT NULL,        -- tmdb | bom
  type          INTEGER,
  type_name     TEXT,
  date          TEXT,
  note_raw      TEXT,
  note_norm     TEXT,
  note_class    TEXT,
  cert          TEXT,
  lang          TEXT,
  descriptors   TEXT,
  opening_usd   INTEGER,
  gross_usd     INTEGER,
  bom_release_id TEXT,
  bom_market    TEXT,
  bom_release_group TEXT,
  gap_days      INTEGER,
  match_status  TEXT,                 -- tmdb_only | matched_bom_gap<N>d | bom_only
  PRIMARY KEY (tmdb_id, country, src_idx)
);

CREATE TABLE IF NOT EXISTS bo_event_match (
  tmdb_id       INTEGER NOT NULL,
  country       TEXT NOT NULL,
  src_idx       INTEGER NOT NULL,
  release_group TEXT,
  market_raw    TEXT,
  release_id    TEXT,
  bom_date      TEXT,
  gap_days      INTEGER,       -- TMDb date minus BOM date, in days
  match_status  TEXT,          -- matched | bom_only
  PRIMARY KEY (tmdb_id, country, src_idx, release_group, market_raw)
);
"""

# BOM market label -> ISO 3166-1 alpha-2. "Domestic" is US + Canada on BOM and
# is mapped to US because that is where the comp engine files it; the
# region_status column says 'aggregate' so nobody mistakes it for US-only.
MARKET_ISO = {
    "domestic": "US", "united states": "US", "canada": "CA", "united kingdom": "GB", "uk": "GB",
    "united kingdom and ireland": "GB", "ireland": "IE", "france": "FR", "germany": "DE", "italy": "IT",
    "spain": "ES", "portugal": "PT", "netherlands": "NL", "belgium": "BE", "luxembourg": "LU",
    "switzerland": "CH", "austria": "AT", "denmark": "DK", "sweden": "SE", "norway": "NO", "finland": "FI",
    "iceland": "IS", "poland": "PL", "czech republic": "CZ", "czechia": "CZ", "slovakia": "SK", "hungary": "HU",
    "romania": "RO", "bulgaria": "BG", "greece": "GR", "cyprus": "CY", "turkey": "TR", "türkiye": "TR",
    "russia": "RU", "russia cis": "RU", "ukraine": "UA", "croatia": "HR", "serbia": "RS",
    "serbia and montenegro": "RS", "slovenia": "SI", "bosnia and herzegovina": "BA", "north macedonia": "MK",
    "lithuania": "LT", "latvia": "LV", "estonia": "EE", "israel": "IL", "united arab emirates": "AE",
    "saudi arabia": "SA", "egypt": "EG", "south africa": "ZA", "nigeria": "NG", "kenya": "KE", "morocco": "MA",
    "lebanon": "LB", "jordan": "JO", "kuwait": "KW", "qatar": "QA", "bahrain": "BH", "oman": "OM",
    "india": "IN", "pakistan": "PK", "bangladesh": "BD", "sri lanka": "LK", "nepal": "NP",
    "china": "CN", "hong kong": "HK", "taiwan": "TW", "japan": "JP", "south korea": "KR", "korea": "KR",
    "singapore": "SG", "malaysia": "MY", "thailand": "TH", "vietnam": "VN", "philippines": "PH",
    "indonesia": "ID", "cambodia": "KH", "mongolia": "MN", "australia": "AU", "new zealand": "NZ",
    "mexico": "MX", "brazil": "BR", "argentina": "AR", "chile": "CL", "colombia": "CO", "peru": "PE",
    "venezuela": "VE", "ecuador": "EC", "bolivia": "BO", "uruguay": "UY", "paraguay": "PY",
    "central america": None, "east africa": None, "west africa": None, "middle east": None,
    "dominican republic": "DO", "puerto rico": "PR", "jamaica": "JM", "trinidad": "TT",
    "trinidad and tobago": "TT", "aruba": "AW", "bahamas": "BS", "guatemala": "GT", "costa rica": "CR",
    "panama": "PA", "honduras": "HN", "el salvador": "SV", "nicaragua": "NI",
    # BOM's combined and regional markets (region_status 'aggregate'; iso = the lead country or None)
    "russiacis": "RU", "russia cis ukraine": "RU", "czech republic slovakia": "CZ", "lesser antilles": None,
    "west indies": None, "baltic states": None, "middle east uae": "AE", "middle east other": None,
    "palestine": "PS", "lebanon united arab emirates": "LB", "south africa nigeria": "ZA", "s e w africa": None,
    "trinidad and tobago": "TT", "yugoslavia": "YU", "greece and cyprus": "GR", "east asia": None,
    "switzerland german": "CH", "switzerland french": "CH", "switzerland italian": "CH", "switzerland french italian": "CH",
    "ghana": "GH", "ethiopia": "ET", "curacao": "CW", "curaçao": "CW", "iraq": "IQ", "albania": "AL", "bosnia": "BA",
    "syria": "SY", "laos": "LA", "e w africa": None, "netherlands antilles": "AN", "mauritius": "MU", "tunisia": "TN",
    "malta": "MT", "kazakhstan": "KZ", "uzbekistan": "UZ", "suriname": "SR", "soviet union": "SU", "reunion": "RE",
    "new caledonia": "NC", "iran": "IR", "djibouti": "DJ", "azerbaijan": "AZ", "armenia": "AM", "georgia": "GE",
    "belarus": "BY", "moldova": "MD", "montenegro": "ME", "kosovo": "XK", "macedonia": "MK", "luxemburg": "LU",
    "southeast asia": None, "latin america": None, "europe": None, "africa": None, "asia": None,
}
AGGREGATE_MARKETS = {"domestic", "central america", "east africa", "west africa", "middle east", "russia cis",
                     "russiacis", "russia cis ukraine", "czech republic slovakia", "lesser antilles", "west indies",
                     "baltic states", "middle east uae", "middle east other", "lebanon united arab emirates",
                     "south africa nigeria", "s e w africa", "greece and cyprus", "east asia", "southeast asia",
                     "latin america", "europe", "africa", "asia", "e w africa",
                     "switzerland german", "switzerland french", "switzerland italian", "switzerland french italian"}

# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------
_MONEY = re.compile(r"[-–—]?\s*[$£€]?\s*([\d,]+(?:\.\d+)?)")
_MONTHS = {m: i for i, m in enumerate(["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}


def parse_money(v) -> int | None:
    """'$53,847,897' -> 53847897; '–', '-', '' -> None. Never 0 for a dash."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("–", "-", "—", "n/a", "N/A"):
        return None
    m = re.search(r"([\d,]+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return int(round(float(m.group(1).replace(",", ""))))
    except ValueError:
        return None


def parse_int(v) -> int | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    m = re.search(r"[+-]?\d+", s)
    return int(m.group(0)) if m else None


def parse_pct(v) -> float | None:
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    m = re.search(r"([+-]?\d+(?:\.\d+)?)\s*%", s)
    return float(m.group(1)) if m else None


def parse_bom_date(v) -> str | None:
    """'Oct 11, 2019' / 'July 10, 2024' -> ISO; anything else -> None."""
    if not v:
        return None
    m = re.search(r"([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})", str(v))
    if not m:
        return None
    mon = _MONTHS.get(m.group(1)[:3].lower())
    if not mon:
        return None
    try:
        return date(int(m.group(3)), mon, int(m.group(2))).isoformat()
    except ValueError:
        return None


def parse_span(display: str, year: int, week: int | None = None) -> tuple[str | None, str | None]:
    """'Oct 11-17' / 'Dec 29-Jan 4, 1996' / 'Feb 7-9' + the BOM year (and week) -> (start, end).

    A span that crosses New Year is labelled by BOM as week 52/53 of the
    START year ('/weekly/1995W52/' for Dec 29-Jan 4, 1996) and the display
    carries the END year. Both are honoured; without either, the year given
    is taken as the end year.
    """
    if not display:
        return None, None
    m = re.match(r"\s*([A-Za-z]{3})\.?\s+(\d{1,2})\s*[-–]\s*(?:([A-Za-z]{3})\.?\s+)?(\d{1,2})(?:,\s*(\d{4}))?", display)
    if not m:
        return None, None
    m1, d1, m2, d2 = _MONTHS.get(m.group(1).lower()), int(m.group(2)), \
        _MONTHS.get((m.group(3) or m.group(1)).lower()), int(m.group(4))
    if not m1 or not m2:
        return None, None
    crosses = m2 < m1
    try:
        if not crosses:
            return date(year, m1, d1).isoformat(), date(year, m2, d2).isoformat()
        if m.group(5):
            end_year = int(m.group(5))
        elif week is not None and week >= 50:
            end_year = year + 1
        else:
            end_year = year
        return date(end_year - 1, m1, d1).isoformat(), date(end_year, m2, d2).isoformat()
    except ValueError:
        return None, None


def market_iso(raw: str) -> tuple[str | None, str]:
    key = (raw or "").strip().lower().replace("&", " and ").replace("/", " ")
    key = re.sub(r"[^\w\s]", "", key)
    key = re.sub(r"\s+", " ", key).strip()
    if not key:
        return None, "empty"
    if key in AGGREGATE_MARKETS:
        return MARKET_ISO.get(key), "aggregate"
    if key in MARKET_ISO:
        return MARKET_ISO[key], "ok" if MARKET_ISO[key] else "aggregate"
    return None, "unresolved"


def looks_like_uk(label: str) -> bool:
    return market_iso(label)[0] == "GB"


def is_robot_check(html: str) -> bool:
    h = html[:4000].lower()
    return ("robot check" in h or "automated access" in h or "api-services-support@amazon.com" in h
            or "to discuss automated access" in h)


# ---------------------------------------------------------------------------
# page parsers (pure functions over HTML)
# ---------------------------------------------------------------------------
def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _cells(row) -> list[str]:
    return [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]


def _rl(href: str) -> str | None:
    m = re.search(r"/release/(rl\d+)", href or "")
    return m.group(1) if m else None


def parse_headline(soup: BeautifulSoup) -> dict:
    """The 'Domestic / International / Worldwide' strip and the summary label box."""
    out = {"domestic_usd": None, "international_usd": None, "worldwide_usd": None, "distributor": None,
           "domestic_opening_usd": None, "opening_theaters": None, "widest_release": None, "budget_usd": None,
           "earliest_release_date": None, "earliest_release_note": None, "mpaa": None, "release_date": None,
           "market_gross_usd": None, "market_label": None, "distributor_scope": None, "opening_scope": None}
    strip = soup.find("div", class_="mojo-performance-summary-table")
    if strip:
        for sec in strip.find_all("div", class_="a-section"):
            lab = sec.find("span", class_="a-size-small")
            money = sec.find("span", class_="money")
            if not lab or not money:
                continue
            l = lab.get_text(" ", strip=True).lower()
            v = parse_money(money.get_text(strip=True))
            if l.startswith("domestic"):
                out["domestic_usd"] = v
            elif l.startswith("international"):
                out["international_usd"] = v
            elif l.startswith("worldwide"):
                out["worldwide_usd"] = v
            else:                                   # a territory page: "United Kingdom $14,696,724"
                out["market_gross_usd"], out["market_label"] = v, lab.get_text(" ", strip=True)
    box = soup.find("div", class_="mojo-summary-values")
    if box:
        for row in box.find_all("div", class_="a-section"):
            spans = row.find_all("span", recursive=False) or row.find_all("span")
            if len(spans) < 2:
                continue
            label = spans[0].get_text(" ", strip=True).lower()
            val = " ".join(s.get_text(" ", strip=True) for s in spans[1:]).replace("See full company information", "").strip()
            if "distributor" in label:
                out["distributor"] = val or None
                out["distributor_scope"] = "domestic" if "domestic" in label else "release"
            elif "widest release" in label:
                out["widest_release"] = parse_int(val)
            elif "opening" in label:
                out["domestic_opening_usd"] = parse_money(val)
                out["opening_scope"] = "domestic" if "domestic" in label else "release"
                m = re.search(r"([\d,]+)\s*theaters?", val)
                out["opening_theaters"] = parse_int(m.group(1)) if m else None
            elif "budget" in label:
                out["budget_usd"] = parse_money(val)
            elif "earliest release date" in label:
                out["earliest_release_date"] = parse_bom_date(val)
                m = re.search(r"\(([^)]+)\)", val)
                out["earliest_release_note"] = m.group(1).strip() if m else None
            elif label.startswith("release date"):
                out["release_date"] = parse_bom_date(val)
            elif label.startswith("mpaa"):
                out["mpaa"] = val or None
    return out


def parse_release_groups(soup: BeautifulSoup) -> list[tuple[str, str]]:
    """[(label, grNNN)] from the release-group selector, in page order, deduped."""
    out, seen = [], set()
    for sel in soup.find_all("select"):
        for o in sel.find_all("option"):
            m = re.search(r"/releasegroup/(gr\d+)", o.get("value", ""))
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                out.append((o.get_text(" ", strip=True), m.group(1)))
    return out


def parse_territory_tables(soup: BeautifulSoup) -> list[dict]:
    """Rows of every 'Market/Area | Release Date | Opening | Gross' table."""
    rows = []
    for table in soup.find_all("table"):
        heads = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if not any("release date" in h for h in heads) or not any(h in ("market", "area") for h in heads):
            continue
        for tr in table.find_all("tr"):
            cells = _cells(tr)
            if len(cells) < 4 or cells[0].lower() in ("market", "area"):
                continue
            rid = next((_rl(a["href"]) for a in tr.find_all("a", href=True) if _rl(a["href"])), None)
            rows.append({"market_raw": cells[0], "release_date": parse_bom_date(cells[1]),
                         "opening_usd": parse_money(cells[2]), "gross_usd": parse_money(cells[3]),
                         "release_id": rid})
    return rows


def parse_lifetime_tables(soup: BeautifulSoup) -> list[dict]:
    """Title-page 'Region | # Releases | Lifetime Gross | Rank' tables (multi-group films)."""
    rows = []
    for table in soup.find_all("table"):
        heads = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if not any("lifetime gross" in h for h in heads):
            continue
        for tr in table.find_all("tr"):
            cells = _cells(tr)
            if len(cells) < 3 or "lifetime" in cells[2].lower():
                continue
            rows.append({"market_raw": cells[0], "n_releases": parse_int(cells[1]),
                         "gross_usd": parse_money(cells[2])})
    return rows


def parse_period_table(soup: BeautifulSoup, kind: str) -> list[dict]:
    """The /weekly/ or /weekend/ table. kind in {'weekly', 'weekend'}.

    Columns are found by header name, never by position. The BOM year and
    week come from the date cell's own link (/weekly/2019W41/), which is the
    only place the year lives.
    """
    link_rx = re.compile(r"/%s/(\d{4})W(\d{1,2})" % kind)
    out = []
    for table in soup.find_all("table"):
        heads = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if "date" not in heads or not any(h.startswith(kind) for h in heads):
            continue

        def idx(*names):
            for n in names:
                for i, h in enumerate(heads):
                    if h == n:
                        return i
            return None
        gross_i = idx(kind)                                   # 'weekly' / 'weekend' gross column
        num_name = {"weekly": "week", "weekend": "weekend"}[kind]
        num_i = next((i for i, h in enumerate(heads) if h == num_name and i != gross_i), None)
        ix = {"date": idx("date"), "rank": idx("rank"), "gross": gross_i, "pct": idx("%± lw", "%±lw", "% lw"),
              "theaters": idx("theaters"), "change": idx("change"), "avg": idx("avg"),
              "to_date": idx("to date"), "num": num_i}
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [c.get_text(" ", strip=True) for c in tds]

            def cell(k):
                i = ix.get(k)
                return cells[i] if i is not None and i < len(cells) else None
            a = tds[ix["date"]].find("a", href=link_rx) if ix["date"] is not None else None
            m = link_rx.search(a["href"]) if a else None
            yr, wk = (int(m.group(1)), int(m.group(2))) if m else (None, None)
            start, end = parse_span(cell("date") or "", yr, wk) if yr else (None, None)
            out.append({"date_display": cell("date"), "bom_year": yr, "bom_week": wk, "start": start, "end": end,
                        "rank": parse_int(cell("rank")), "gross": parse_money(cell("gross")),
                        "pct_change_lw": parse_pct(cell("pct")), "theaters": parse_int(cell("theaters")),
                        "theaters_change": parse_int(cell("change")), "avg": parse_money(cell("avg")),
                        "to_date": parse_money(cell("to_date")), "number": parse_int(cell("num"))})
        if out:
            break
    return out


def detect_currency(soup: BeautifulSoup) -> str:
    strip = soup.find("div", class_="mojo-performance-summary-table")
    txt = strip.get_text(" ", strip=True) if strip else ""
    return "GBP" if "£" in txt else ("EUR" if "€" in txt else "USD")


def find_domestic_release_id(soup: BeautifulSoup, territory_rows: list[dict]) -> str | None:
    """Only a 'Domestic' territory row names the domestic release. The first
    /release/ link on a page is NOT a fallback: for a film with no US release
    it is the earliest foreign release, and the first full run filed Austrian
    and Spanish distributors and dates as domestic that way."""
    for r in territory_rows:
        if r["market_raw"].strip().lower() == "domestic" and r["release_id"]:
            return r["release_id"]
    return None


def has_box_office_data(soup: BeautifulSoup) -> bool:
    h = parse_headline(soup)
    return any(h[k] for k in ("domestic_usd", "international_usd", "worldwide_usd")) or bool(parse_release_groups(soup))


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------
class RobotCheck(RuntimeError):
    pass


class Throttled(RuntimeError):
    """BOM answered 503 on every retry: a throttling episode, not a bad film.
    The run pauses and retries the same film rather than recording an error."""


class BomClient:
    """One connection, one request at a time, a pause between them."""

    def __init__(self, base: str | None = None, sleep: float | None = None):
        self.base = (base or BOM_BASE).rstrip("/")
        self.sleep = BOM_SLEEP if sleep is None else sleep
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9",
                               "Accept": "text/html,application/xhtml+xml"})
        self._last = 0.0
        self.calls = 0

    def get(self, path: str) -> tuple[int, str]:
        """(status, html). 404 returned as such; 429/5xx retried; robot check raises."""
        last_status, last_err = None, ""
        for attempt in range(1, MAX_TRIES + 1):
            gap = self.sleep * random.uniform(0.8, 1.3) - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()
            try:
                r = self.s.get(f"{self.base}{path}", timeout=HTTP_TIMEOUT, allow_redirects=True)
            except requests.RequestException as e:
                last_err = f"transport: {type(e).__name__}"
                time.sleep(min(5 * attempt, 30) * BACKOFF_SCALE)
                continue
            self.calls += 1
            last_status = r.status_code
            if r.status_code == 200:
                if is_robot_check(r.text):
                    raise RobotCheck(f"robot check served for {path}")
                return 200, r.text
            if r.status_code == 404:
                return 404, ""
            if r.status_code in (429, 503) and is_robot_check(r.text):
                raise RobotCheck(f"HTTP {r.status_code} robot check for {path}")
            if r.status_code in (429, 500, 502, 503, 504):
                wait = R.parse_retry_after(r.headers.get("Retry-After"), 10.0 * attempt * BACKOFF_SCALE)
                time.sleep(wait)
                last_err = f"HTTP {r.status_code}"
                continue
            return r.status_code, r.text
        if last_status in (429, 503):
            raise Throttled(f"HTTP {last_status} on every one of {MAX_TRIES} tries for {path}")
        raise RuntimeError(f"failed after {MAX_TRIES} tries: {last_err or last_status}")


def connect(cfg: R.Config) -> sqlite3.Connection:
    con = R.connect(cfg)
    con.executescript(SCHEMA)
    return con


_STRIP_TAGS = ("script", "style", "svg", "noscript", "link", "meta", "iframe")


def strip_page(html: str) -> str:
    """Drop scripts, styles and SVG before caching. A BOM page is ~160 KB of
    which ~90% is boilerplate JS/CSS; stripped and compressed it is ~6 KB
    instead of ~56 KB, and every table, label and link the parsers read is
    untouched (the self-test proves parse output is identical)."""
    if not html:
        return html
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(_STRIP_TAGS):
        t.decompose()
    return re.sub(r"\s+", " ", str(soup))


def _store_page(con, key, tmdb_id, url, status, html):
    html = strip_page(html) if status == 200 else ""
    con.execute("INSERT OR REPLACE INTO bo_pages(key, tmdb_id, url, fetched_at, http_status, html_z) VALUES (?,?,?,?,?,?)",
                (key, tmdb_id, url, R.utcnow(), status, zlib.compress(html.encode("utf-8"), 6) if html else None))


def _page(con, key) -> tuple[int | None, str | None]:
    row = con.execute("SELECT http_status, html_z FROM bo_pages WHERE key=?", (key,)).fetchone()
    if not row:
        return None, None
    return row[0], (zlib.decompress(row[1]).decode("utf-8") if row[1] else "")


def scope_stage(cfg: R.Config, con, min_year=2000, us_only=False, verbose=True) -> dict:
    """Films with a TMDb theatrical entry (type 2/3) dated >= min_year. Never removes a film already scoped."""
    where = "e.type IN (2,3) AND e.date >= ?" + (" AND e.country='US'" if us_only else "")
    rows = con.execute(
        f"SELECT f.tmdb_id, f.tconsts, f.title, MIN(e.date), COUNT(DISTINCT e.country) FROM films f "
        f"JOIN events e ON e.tmdb_id=f.tmdb_id WHERE f.status='ok' AND f.tconsts != '' AND {where} "
        f"GROUP BY f.tmdb_id", (f"{min_year}-01-01",)).fetchall()
    added = 0
    with con:
        for tid, tconsts, title, first, ncc in rows:
            tconst = tconsts.split("|")[0]
            cur = con.execute(
                "INSERT OR IGNORE INTO bo_films(tmdb_id, tconst, tconsts, title, scope_reason) VALUES (?,?,?,?,?)",
                (tid, tconst, tconsts, title, f"theatrical {first} in {ncc} countr{'y' if ncc == 1 else 'ies'}"))
            added += cur.rowcount
    total = con.execute("SELECT COUNT(*) FROM bo_films").fetchone()[0]
    if verbose:
        print(f"SCOPE  {len(rows)} films with a theatrical entry >= {min_year}{' in the US' if us_only else ''}; "
              f"{added} newly scoped; {total} in bo_films")
    return {"matched": len(rows), "added": added, "total": total}


def fetch_film(con, client: BomClient, tmdb_id: int, tconst: str, with_uk=True, with_weekly=True,
               alternates: Iterable[str] = ()) -> tuple[str, int, str]:
    """Fetch the pages one film needs. Returns (status, n_pages, error).

    When the crosswalk maps two tconsts to this TMDb id and the first has no
    BOM page, the others are tried; bo_films.tconst is updated to the one that
    answered so `parse` reads the right cache key."""
    n = 0
    st, html = None, None
    for cand in [tconst, *[a for a in alternates if a and a != tconst]]:
        key = f"title:{cand}"
        st, html = _page(con, key)
        if st is None:
            st, html = client.get(f"/title/{cand}/")
            _store_page(con, key, tmdb_id, f"/title/{cand}/", st, html)
            n += 1
        if st == 200 and html and has_box_office_data(_soup(html)):
            if cand != tconst:
                con.execute("UPDATE bo_films SET tconst=? WHERE tmdb_id=?", (cand, tmdb_id))
            tconst = cand
            break
    if st != 200 or not html:
        return "none", n, ""
    soup = _soup(html)
    if not has_box_office_data(soup):
        return "none", n, ""
    groups = parse_release_groups(soup)[:MAX_GROUPS]
    terr = parse_territory_tables(soup)
    if groups and not terr:
        for label, gr in groups:
            k = f"group:{gr}"
            gst, ghtml = _page(con, k)
            if gst is None:
                gst, ghtml = client.get(f"/releasegroup/{gr}/")
                _store_page(con, k, tmdb_id, f"/releasegroup/{gr}/", gst, ghtml)
                n += 1
            if gst == 200 and ghtml:
                terr.extend(parse_territory_tables(_soup(ghtml)))
    dom = find_domestic_release_id(soup, terr)
    if with_weekly and dom:
        k = f"release:{dom}:weekly"
        if _page(con, k)[0] is None:
            wst, whtml = client.get(f"/release/{dom}/weekly/")
            _store_page(con, k, tmdb_id, f"/release/{dom}/weekly/", wst, whtml)
            n += 1
    if with_uk:
        uk = next((r["release_id"] for r in terr if r["release_id"] and looks_like_uk(r["market_raw"])), None)
        if uk:
            k = f"release:{uk}:weekend"
            if _page(con, k)[0] is None:
                ust, uhtml = client.get(f"/release/{uk}/weekend/")
                _store_page(con, k, tmdb_id, f"/release/{uk}/weekend/", ust, uhtml)
                n += 1
    return "ok", n, ""


def fetch_stage(cfg: R.Config, con, limit: int | None = None, retry_errors=True, with_uk=True, with_weekly=True,
                client: BomClient | None = None, verbose=True) -> Counter:
    client = client or BomClient()
    statuses = ("pending", "error") if retry_errors else ("pending",)
    todo = con.execute(f"SELECT tmdb_id, tconst, tconsts FROM bo_films WHERE status IN ({','.join('?' * len(statuses))}) "
                       f"ORDER BY tmdb_id", statuses).fetchall()
    if limit:
        todo = todo[:limit]
    counts = Counter()
    blocks = throttles = 0
    t0 = time.monotonic()
    if verbose:
        print(f"FETCH  {len(todo)} films ({client.sleep:.1f}s between requests; uk={with_uk} weekly={with_weekly})")
    with R.RunLog(con, "bo_fetch", f"limit={limit} n={len(todo)}") as rl:
        try:
            for i, (tid, tconst, tconsts) in enumerate(todo, 1):
                while True:
                    try:
                        status, n, err = fetch_film(con, client, tid, tconst, with_uk, with_weekly,
                                                    alternates=(tconsts or "").split("|"))
                        blocks = throttles = 0
                        break
                    except Throttled as e:
                        # a burst of 503s: wait it out and retry the SAME film at normal speed
                        throttles += 1
                        counts["throttled"] += 1
                        con.rollback()
                        if throttles >= THROTTLE_ABORT:
                            raise RuntimeError(f"{THROTTLE_ABORT} throttling pauses in a row -- stopping; "
                                               f"try again later or with a longer --sleep") from e
                        if verbose:
                            print(f"  throttled ({e}); pausing {BLOCK_PAUSE}s  [{time.strftime('%H:%M')}]", flush=True)
                        time.sleep(BLOCK_PAUSE * BACKOFF_SCALE)
                    except RobotCheck as e:
                        # pause and retry the SAME film; nothing about it is cached
                        blocks += 1
                        counts["blocked"] += 1
                        con.rollback()
                        if blocks >= BLOCK_ABORT:
                            raise RuntimeError(f"{BLOCK_ABORT} robot checks in a row -- stopping; try again later "
                                               f"with a longer --sleep") from e
                        if verbose:
                            print(f"  robot check; pausing {BLOCK_PAUSE}s")
                        time.sleep(BLOCK_PAUSE * BACKOFF_SCALE)
                    except Exception as e:      # one film's failure never ends the run
                        status, n, err = "error", 0, R.redact(e, cfg)[:300]
                        con.rollback()
                        break
                con.execute("UPDATE bo_films SET status=?, fetched_at=?, error=?, n_pages=COALESCE(n_pages,0)+?, "
                            "parsed_ver=NULL WHERE tmdb_id=?", (status, R.utcnow(), err or None, n, tid))
                con.commit()
                counts[status] += 1
                rl.n[status if status in ("ok", "error") else "gone"] += 1
                if verbose and i % 200 == 0:
                    el = time.monotonic() - t0
                    print(f"  {i:>6}/{len(todo)}  ok={counts['ok']} none={counts['none']} err={counts['error']}  "
                          f"{client.calls} requests  {el/i:.1f}s/film  ETA {(len(todo)-i)*el/i/3600:.1f} h")
        except KeyboardInterrupt:
            if verbose:
                print("\n  interrupted -- everything fetched so far is committed")
            raise
    if verbose:
        print(f"  done: ok={counts['ok']} none={counts['none']} err={counts['error']} blocked={counts['blocked']} "
              f"throttle-pauses={counts['throttled']} in {(time.monotonic()-t0)/60:.1f} min, {client.calls} requests")
    return counts


# ---------------------------------------------------------------------------
# parse (pure over the cache)
# ---------------------------------------------------------------------------
def parse_film(con, tmdb_id: int, tconst: str) -> dict:
    """Everything the cached pages of one film say. No network."""
    out = {"summary": None, "territory": [], "weekly": [], "weekend": []}
    st, html = _page(con, f"title:{tconst}")
    if st != 200 or not html:
        return out
    soup = _soup(html)
    head = parse_headline(soup)
    groups = parse_release_groups(soup)
    terr = parse_territory_tables(soup)
    terr_rows = []
    if terr:
        gl = groups[0][1] if len(groups) == 1 else "title"
        for r in terr:
            terr_rows.append(dict(r, release_group=gl, group_label=groups[0][0] if len(groups) == 1 else None))
    else:
        for label, gr in groups[:MAX_GROUPS]:
            gst, ghtml = _page(con, f"group:{gr}")
            if gst == 200 and ghtml:
                for r in parse_territory_tables(_soup(ghtml)):
                    terr_rows.append(dict(r, release_group=gr, group_label=label))
    for r in parse_lifetime_tables(soup):
        terr_rows.append({"market_raw": r["market_raw"], "release_date": None, "opening_usd": None,
                          "gross_usd": r["gross_usd"], "release_id": None, "release_group": "lifetime",
                          "group_label": f"{r['n_releases']} releases" if r["n_releases"] else None})
    dom = find_domestic_release_id(soup, terr_rows)
    dom_dist = head["distributor"] if head["distributor_scope"] == "domestic" else None
    dom_open = head["domestic_opening_usd"] if head["opening_scope"] == "domestic" else None
    dom_open_th = head["opening_theaters"] if head["opening_scope"] == "domestic" else None
    summary = {"tconst": tconst, "domestic_usd": head["domestic_usd"], "international_usd": head["international_usd"],
               "worldwide_usd": head["worldwide_usd"], "distributor": dom_dist,
               "domestic_opening_usd": dom_open, "opening_theaters": dom_open_th,
               "widest_release": head["widest_release"] if dom else None, "domestic_release_date": None, "domestic_release_id": dom,
               "budget_usd": head["budget_usd"], "earliest_release_date": head["earliest_release_date"],
               "earliest_release_note": head["earliest_release_note"], "mpaa": head["mpaa"],
               "n_release_groups": len(groups), "release_groups": "|".join(f"{l}={g}" for l, g in groups),
               "uk_release_id": None, "uk_distributor": None, "uk_opening_usd": None, "uk_opening_theaters": None,
               "uk_widest_release": None, "uk_release_date": None, "uk_gross_usd": None}
    if dom:
        wst, whtml = _page(con, f"release:{dom}:weekly")
        if wst == 200 and whtml:
            ws = _soup(whtml)
            wh = parse_headline(ws)
            summary["opening_theaters"] = summary["opening_theaters"] or wh["opening_theaters"]
            summary["widest_release"] = summary["widest_release"] or wh["widest_release"]
            summary["domestic_release_date"] = wh["release_date"]
            summary["distributor"] = summary["distributor"] or wh["distributor"]
            if summary["domestic_opening_usd"] is None:
                summary["domestic_opening_usd"] = wh["domestic_opening_usd"]
            out["weekly"] = [dict(r, release_id=dom) for r in parse_period_table(ws, "weekly")]
    uk = next((r["release_id"] for r in terr_rows if r.get("release_id") and looks_like_uk(r["market_raw"])), None)
    if uk:
        summary["uk_release_id"] = uk
        ust, uhtml = _page(con, f"release:{uk}:weekend")
        if ust == 200 and uhtml:
            us_ = _soup(uhtml)
            uh = parse_headline(us_)
            summary.update(uk_distributor=uh["distributor"], uk_opening_usd=uh["domestic_opening_usd"],
                           uk_opening_theaters=uh["opening_theaters"], uk_widest_release=uh["widest_release"],
                           uk_release_date=uh["release_date"], uk_gross_usd=uh["market_gross_usd"])
            out["weekend"] = [dict(r, release_id=uk, currency=detect_currency(us_)) for r in parse_period_table(us_, "weekend")]
    out["summary"] = summary
    out["territory"] = terr_rows
    return out


def parse_stage(cfg: R.Config, con, rebuild=False, verbose=True) -> dict:
    if rebuild:
        films = con.execute("SELECT tmdb_id, tconst FROM bo_films WHERE status='ok'").fetchall()
    else:
        films = con.execute("SELECT tmdb_id, tconst FROM bo_films WHERE status='ok' AND (parsed_ver IS NULL OR parsed_ver < ?)",
                            (PARSER_VERSION,)).fetchall()
    if verbose:
        print(f"PARSE  {len(films)} films (parser v{PARSER_VERSION})")
    n = Counter()
    now = R.utcnow()
    with R.RunLog(con, "bo_parse", f"rebuild={rebuild} n={len(films)}") as rl:
        for i, (tid, tconst) in enumerate(films, 1):
            p = parse_film(con, tid, tconst)
            with con:
                for t in ("bo_summary", "bo_territory", "bo_weekly_us", "bo_weekend_uk"):
                    con.execute(f"DELETE FROM {t} WHERE tmdb_id=?", (tid,))
                s = p["summary"]
                if s:
                    cols = list(s)
                    con.execute(f"INSERT INTO bo_summary(tmdb_id,{','.join(cols)},parsed_at) VALUES (?,{','.join('?' * len(cols))},?)",
                                (tid, *[s[c] for c in cols], now))
                    n["summary"] += 1
                seen = set()
                for r in p["territory"]:
                    k = (r["release_group"], r["market_raw"])
                    if k in seen:
                        continue
                    seen.add(k)
                    iso, rs = market_iso(r["market_raw"])
                    con.execute("INSERT INTO bo_territory VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                                (tid, tconst, r["release_group"], r.get("group_label"), r["market_raw"], iso, rs,
                                 r["release_date"], r["opening_usd"], r["gross_usd"], r["release_id"]))
                    n["territory"] += 1
                for w in p["weekly"]:
                    if w["bom_year"] is None:
                        continue
                    con.execute("INSERT OR REPLACE INTO bo_weekly_us VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (tid, tconst, w["release_id"], w["number"], w["bom_year"], w["bom_week"], w["date_display"],
                                 w["start"], w["end"], w["rank"], w["gross"], w["pct_change_lw"], w["theaters"],
                                 w["theaters_change"], w["avg"], w["to_date"]))
                    n["weekly"] += 1
                for w in p["weekend"]:
                    if w["bom_year"] is None:
                        continue
                    con.execute("INSERT OR REPLACE INTO bo_weekend_uk VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (tid, tconst, w["release_id"], w["currency"], w["number"], w["bom_year"], w["bom_week"],
                                 w["date_display"], w["start"], w["end"], w["rank"], w["gross"], w["pct_change_lw"],
                                 w["theaters"], w["theaters_change"], w["avg"], w["to_date"]))
                    n["weekend"] += 1
                con.execute("UPDATE bo_films SET parsed_ver=? WHERE tmdb_id=?", (PARSER_VERSION, tid))
            rl.n["ok"] += 1
            if verbose and i % 2000 == 0:
                print(f"  {i:>6}/{len(films)}")
    if verbose:
        print(f"  summaries={n['summary']} territory rows={n['territory']} weekly rows={n['weekly']} "
              f"uk weekend rows={n['weekend']}")
    return dict(n)


# ---------------------------------------------------------------------------
# match BOM territory rows to TMDb theatrical events
# ---------------------------------------------------------------------------
def match_stage(cfg: R.Config, con, window=MATCH_WINDOW_DAYS, verbose=True) -> dict:
    """For every dated BOM territory row with an ISO code, the nearest TMDb type-2/3
    event in that country within +/-window days. Domestic rows match US events."""
    with con:
        con.execute("DELETE FROM bo_event_match")
    rows = con.execute("SELECT tmdb_id, release_group, market_raw, iso, release_date, release_id FROM bo_territory "
                       "WHERE iso IS NOT NULL AND release_date IS NOT NULL AND release_group != 'lifetime'").fetchall()
    ids = sorted({r[0] for r in rows})
    ev: dict[tuple[int, str], list] = defaultdict(list)
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        for tid, cc, idx, d in con.execute(
                f"SELECT tmdb_id, country, src_idx, date FROM events WHERE type IN (2,3) AND date != '' "
                f"AND tmdb_id IN ({','.join('?' * len(part))})", part):
            ev[(tid, cc)].append((d, idx))
    out, n = [], Counter()
    used = set()
    for tid, gr, mk, iso, d, rid in rows:
        best = None
        for ed, idx in ev.get((tid, iso), []):
            gap = R._days(d, ed)          # TMDb date minus BOM date: positive = TMDb entry is later
            if gap is None:
                continue
            if abs(gap) <= window and (best is None or abs(gap) < abs(best[1])) and (tid, iso, idx) not in used:
                best = (idx, gap)
        if best:
            used.add((tid, iso, best[0]))
            out.append((tid, iso, best[0], gr, mk, rid, d, best[1], "matched"))
            n["matched"] += 1
        else:
            out.append((tid, iso, -1, gr, mk, rid, d, None, "bom_only"))
            n["bom_only"] += 1
    with con:
        con.executemany("INSERT OR REPLACE INTO bo_event_match VALUES (?,?,?,?,?,?,?,?,?)", out)
    if verbose:
        print(f"MATCH  {n['matched']} BOM territory rows matched to a TMDb theatrical event within {window} days; "
              f"{n['bom_only']} have no TMDb counterpart")
    return dict(n)


# ---------------------------------------------------------------------------
# reconcile: TMDb events + BOM figures -> events_bo
# ---------------------------------------------------------------------------
def reconcile_stage(cfg: R.Config, con, rebuild_windows=True, verbose=True) -> dict:
    """Build `events_bo`: every TMDb event, with BOM opening/gross on the rows
    `match` paired, plus one source='bom' theatrical event for every BOM
    territory row TMDb had no counterpart for. Then rebuild windows from it.

    The BOM 'Domestic' market is filed under US (region_status 'aggregate' in
    bo_territory); a matched row keeps its TMDb type, a BOM-only row has no
    TMDb type and the label 'Theatrical (BOM)'. Re-release groups keep their
    own rows: a 2025 re-release is a real event, labelled by bom_release_group.
    """
    if verbose:
        print("RECONCILE  TMDb events x Box Office Mojo -> events_bo")
    with con:
        con.execute("DELETE FROM events_bo")
        con.execute("""
            INSERT INTO events_bo
            SELECT e.tmdb_id, e.country, e.src_idx, 'tmdb', e.type, e.type_name, e.date, e.note_raw, e.note_norm,
                   e.note_class, e.cert, e.lang, e.descriptors, t.opening_usd, t.gross_usd, m.release_id, m.market_raw,
                   m.release_group, m.gap_days,
                   CASE WHEN m.tmdb_id IS NULL THEN 'tmdb_only' ELSE 'matched_bom_gap' || abs(m.gap_days) || 'd' END
            FROM events e
            LEFT JOIN bo_event_match m ON m.tmdb_id=e.tmdb_id AND m.country=e.country AND m.src_idx=e.src_idx
                 AND m.match_status='matched'
            LEFT JOIN bo_territory t ON t.tmdb_id=m.tmdb_id AND t.release_group=m.release_group
                 AND t.market_raw=m.market_raw""")
        con.execute("""
            INSERT INTO events_bo
            SELECT m.tmdb_id, m.country,
                   -ROW_NUMBER() OVER (PARTITION BY m.tmdb_id, m.country ORDER BY m.bom_date, m.release_group, m.market_raw),
                   'bom', NULL, 'Theatrical (BOM)', m.bom_date, '', '', 'empty', '', '', '',
                   t.opening_usd, t.gross_usd, m.release_id, m.market_raw, m.release_group, NULL, 'bom_only'
            FROM bo_event_match m
            JOIN bo_territory t ON t.tmdb_id=m.tmdb_id AND t.release_group=m.release_group AND t.market_raw=m.market_raw
            WHERE m.match_status='bom_only' AND m.bom_date IS NOT NULL""")
    n = dict(con.execute("SELECT source, COUNT(*) FROM events_bo GROUP BY source"))
    n_matched = con.execute("SELECT COUNT(*) FROM events_bo WHERE match_status LIKE 'matched%'").fetchone()[0]
    n_gross = con.execute("SELECT COUNT(*) FROM events_bo WHERE gross_usd IS NOT NULL").fetchone()[0]
    if verbose:
        print(f"  {n.get('tmdb', 0)} TMDb events ({n_matched} carrying BOM figures) + {n.get('bom', 0)} BOM-only "
              f"theatrical events; {n_gross} rows with a gross")
    stats = {"tmdb": n.get("tmdb", 0), "bom": n.get("bom", 0), "matched": n_matched, "with_gross": n_gross}
    if rebuild_windows:
        stats["windows"] = R.windows_stage(cfg, con, verbose=verbose, source="events_bo")
        filled = con.execute("SELECT COUNT(*) FROM windows WHERE first_theatrical_source='bom'").fetchone()[0]
        if verbose:
            print(f"  first_theatrical filled from BOM where TMDb had no theatrical date: {filled} film x country windows")
        stats["filled_from_bom"] = filled
    return stats


# ---------------------------------------------------------------------------
# export + report
# ---------------------------------------------------------------------------
def export_stage(cfg: R.Config, con, verbose=True) -> dict:
    out = cfg.out_dir
    w = {}
    meta = {t: (tc or "", ti or "") for t, tc, ti in con.execute("SELECT tmdb_id, tconsts, title FROM films")}
    cols = [r[1] for r in con.execute("PRAGMA table_info(bo_summary)")]
    w["box_office_summary.csv"] = R._atomic_csv(out / "box_office_summary.csv", ["title", "status", *cols],
        ((meta.get(r[0], ("", ""))[1], st, *r) for st, *r in con.execute(
            f"SELECT f.status, {','.join('s.' + c for c in cols)} FROM bo_films f LEFT JOIN bo_summary s USING(tmdb_id) "
            f"ORDER BY f.tmdb_id")))
    w["box_office_territory.csv"] = R._atomic_csv(out / "box_office_territory.csv",
        ["tmdb_id", "tconst", "title", "release_group", "group_label", "market_raw", "iso", "region_status",
         "release_date", "opening_usd", "gross_usd", "release_id"],
        ((r[0], r[1], meta.get(r[0], ("", ""))[1], *r[2:]) for r in con.execute(
            "SELECT * FROM bo_territory ORDER BY tmdb_id, release_group, market_raw")))
    w["weekly_domestic.csv"] = R._atomic_csv(out / "weekly_domestic.csv",
        ["tconst", "title", "release_id", "week_number", "bom_year", "bom_week", "date_display", "week_start", "week_end",
         "rank", "weekly_gross_usd", "pct_change_lw_bom", "theaters", "theaters_change", "avg_per_theater_usd", "to_date_usd", "tmdb_id"],
        ((r[1], meta.get(r[0], ("", ""))[1], *r[2:], r[0]) for r in con.execute(
            "SELECT * FROM bo_weekly_us ORDER BY tmdb_id, bom_year, bom_week")))
    w["weekend_uk.csv"] = R._atomic_csv(out / "weekend_uk.csv",
        ["tconst", "title", "release_id", "currency", "weekend_number", "bom_year", "bom_week", "date_display", "weekend_start",
         "weekend_end", "rank", "weekend_gross", "pct_change_lw", "theaters", "theaters_change", "avg_per_theater", "to_date", "tmdb_id"],
        ((r[1], meta.get(r[0], ("", ""))[1], *r[2:], r[0]) for r in con.execute(
            "SELECT * FROM bo_weekend_uk ORDER BY tmdb_id, bom_year, bom_week")))
    w["box_office_event_matches.csv"] = R._atomic_csv(out / "box_office_event_matches.csv",
        ["tmdb_id", "title", "country", "src_idx", "tmdb_date", "tmdb_type", "release_group", "market_raw", "release_id",
         "bom_date", "gap_days", "match_status"],
        ((m[0], meta.get(m[0], ("", ""))[1], m[1], m[2], m[9], m[10], *m[3:9]) for m in con.execute(
            "SELECT m.tmdb_id, m.country, m.src_idx, m.release_group, m.market_raw, m.release_id, m.bom_date, m.gap_days, "
            "m.match_status, e.date, e.type FROM bo_event_match m LEFT JOIN events e ON e.tmdb_id=m.tmdb_id "
            "AND e.country=m.country AND e.src_idx=m.src_idx ORDER BY m.tmdb_id, m.country")))
    if R.events_source(con) == "events_bo":
        w["events_reconciled.csv"] = R._atomic_csv(out / "events_reconciled.csv",
            ["tmdb_id", "tconsts", "title", "country", "src_idx", "source", "type", "type_name", "date", "note_raw",
             "note_class", "channel", "fest_qid", "fest_label", "fest_band", "opening_usd", "gross_usd", "bom_release_id",
             "bom_market", "bom_release_group", "gap_days", "match_status"],
            ((r[0], *meta.get(r[0], ("", "")), r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8],
              "festival" if r[11] == "accept" else ("theatrical" if r[3] == "bom" else R.TYPE_CHANNEL.get(r[4], "unknown")),
              r[9] if r[11] == "accept" else "", r[10] if r[11] == "accept" else "", r[11] or "", *r[12:])
             for r in con.execute(
                "SELECT e.tmdb_id, e.country, e.src_idx, e.source, e.type, e.type_name, e.date, e.note_raw, e.note_class, "
                "r.fest_qid, r.fest_label, r.band, e.opening_usd, e.gross_usd, e.bom_release_id, e.bom_market, "
                "e.bom_release_group, e.gap_days, e.match_status FROM events_bo e LEFT JOIN resolutions r "
                "ON r.tmdb_id=e.tmdb_id AND r.country=e.country AND r.src_idx=e.src_idx "
                "ORDER BY e.tmdb_id, e.country, e.date")))
    w["box_office_unresolved_markets.csv"] = R._atomic_csv(out / "box_office_unresolved_markets.csv",
        ["market_raw", "n_rows", "n_films"],
        con.execute("SELECT market_raw, COUNT(*), COUNT(DISTINCT tmdb_id) FROM bo_territory WHERE region_status='unresolved' "
                    "GROUP BY market_raw ORDER BY 2 DESC"))
    if verbose:
        print("EXPORT")
        for k, v in w.items():
            print(f"  {k:<36}{v:>9} rows")
        print(f"  -> {out}")
    return w


def report(cfg: R.Config, con):
    print("BOX OFFICE")
    print("  films by status : " + ", ".join(f"{k}={v}" for k, v in con.execute(
        "SELECT status, COUNT(*) FROM bo_films GROUP BY status")))
    print(f"  pages cached    : {con.execute('SELECT COUNT(*) FROM bo_pages').fetchone()[0]}")
    ns = con.execute("SELECT COUNT(*), SUM(domestic_usd IS NOT NULL), SUM(worldwide_usd IS NOT NULL), "
                     "SUM(widest_release IS NOT NULL), SUM(uk_release_id IS NOT NULL) FROM bo_summary").fetchone()
    print(f"  summaries       : {ns[0]}  (domestic gross on {ns[1] or 0}, worldwide on {ns[2] or 0}, "
          f"widest release on {ns[3] or 0}, UK release on {ns[4] or 0})")
    print(f"  territory rows  : {con.execute('SELECT COUNT(*) FROM bo_territory').fetchone()[0]}  "
          f"(unresolved market names: {con.execute(chr(39).join(['SELECT COUNT(DISTINCT market_raw) FROM bo_territory WHERE region_status=', 'unresolved', ''])).fetchone()[0]})")
    print(f"  weekly rows     : {con.execute('SELECT COUNT(*) FROM bo_weekly_us').fetchone()[0]} across "
          f"{con.execute('SELECT COUNT(DISTINCT tmdb_id) FROM bo_weekly_us').fetchone()[0]} films")
    print(f"  uk weekend rows : {con.execute('SELECT COUNT(*) FROM bo_weekend_uk').fetchone()[0]} across "
          f"{con.execute('SELECT COUNT(DISTINCT tmdb_id) FROM bo_weekend_uk').fetchone()[0]} films")
    m = dict(con.execute("SELECT match_status, COUNT(*) FROM bo_event_match GROUP BY match_status"))
    if m:
        print(f"  event matches   : {m}")
    if R.events_source(con) == "events_bo":
        rb = dict(con.execute("SELECT source, COUNT(*) FROM events_bo GROUP BY source"))
        print(f"  reconciled      : {rb.get('tmdb', 0)} TMDb events + {rb.get('bom', 0)} BOM-only events; "
              f"{con.execute(chr(39).join(['SELECT COUNT(*) FROM windows WHERE first_theatrical_source=', 'bom', ''])).fetchone()[0]} "
              f"windows take their first theatrical date from BOM")
    for r in con.execute("SELECT stage, started, finished, n_done, n_ok, n_gone, n_error, note FROM runs "
                         "WHERE stage LIKE 'bo_%' ORDER BY run_id DESC LIMIT 5"):
        print(f"  {r[0]:<9} {r[1]} -> {r[2] or '...':<20} done={r[3]} ok={r[4]} none={r[5]} err={r[6]} {r[7] or ''}")


# ---------------------------------------------------------------------------
# self-test
# ---------------------------------------------------------------------------
def _fixture_html(cfg: R.Config, name: str) -> str:
    p = cfg.fixtures_dir / "bom" / f"{name}.html.gz"
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        return fh.read()


def selftest(cfg: R.Config | None = None, verbose=True) -> tuple[int, int]:
    global BACKOFF_SCALE
    import tempfile
    cfg = cfg or R.Config.from_env()
    checks = []

    def ck(name, cond, detail=""):
        checks.append((name, bool(cond), str(detail)[:220]))

    # ---- helpers ------------------------------------------------------------
    ck("money", parse_money("$53,847,897") == 53847897 and parse_money("–") is None and parse_money("") is None
       and parse_money("$1.5") == 2 and parse_money("£2,500,000") == 2500000)
    ck("a dash is absent, not zero", parse_money("-") is None and parse_int("-") is None)
    ck("int / pct", parse_int("2,510 theaters") == 2510 and parse_int("+291") == 291 and parse_pct("+82.9%") == 82.9
       and parse_pct("-") is None)
    ck("dates", parse_bom_date("Oct 11, 2019") == "2019-10-11" and parse_bom_date("July 10, 2024") == "2024-07-10"
       and parse_bom_date("May 30, 2019 (South Korea)") == "2019-05-30" and parse_bom_date("") is None)
    ck("week span", parse_span("Oct 11-17", 2019) == ("2019-10-11", "2019-10-17")
       and parse_span("Dec 27-Jan 2", 2020) == ("2019-12-27", "2020-01-02") and parse_span("garbage", 2020) == (None, None)
       and parse_span("Dec 29-Jan 4, 1996", 1995, 52) == ("1995-12-29", "1996-01-04")
       and parse_span("Dec 26-Jan 1", 2003, 52) == ("2003-12-26", "2004-01-01")
       and parse_span("Dec 26-Jan 1", 2004, 1) == ("2003-12-26", "2004-01-01")
       and parse_span("Feb 7-9", 2020) == ("2020-02-07", "2020-02-09"), parse_span("Dec 27-Jan 2", 2020))
    ck("market iso", market_iso("United Kingdom") == ("GB", "ok") and market_iso("Domestic") == ("US", "aggregate")
       and market_iso("South Korea") == ("KR", "ok") and market_iso("Narnia") == (None, "unresolved")
       and market_iso("Türkiye") == ("TR", "ok") and market_iso("Russia/CIS") == ("RU", "aggregate")
       and market_iso("Trinidad & Tobago") == ("TT", "ok") and market_iso("Czech Republic/Slovakia") == ("CZ", "aggregate")
       and market_iso("Switzerland (German)") == ("CH", "aggregate") and market_iso("Curaçao") == ("CW", "ok")
       and market_iso("Ghana") == ("GH", "ok"))
    ck("uk detection", looks_like_uk("United Kingdom") and looks_like_uk("UK") and not looks_like_uk("Ukraine"))
    ck("robot check detection", is_robot_check("<html><title>Robot Check</title>") and not is_robot_check("<html><title>Parasite - Box Office Mojo</title>"))

    # ---- real pages: Parasite (multi-group) ---------------------------------
    t = _soup(_fixture_html(cfg, "bom_title_tt6751668"))
    h = parse_headline(t)
    ck("Parasite headline: domestic / international / worldwide",
       (h["domestic_usd"], h["international_usd"], h["worldwide_usd"]) == (53847897, 209565145, 263423899), h)
    ck("Parasite headline: distributor, opening, budget, earliest release, MPAA",
       h["distributor"] == "Neon" and h["domestic_opening_usd"] == 393216 and h["budget_usd"] == 11400000
       and h["earliest_release_date"] == "2019-05-30" and h["earliest_release_note"] == "South Korea" and h["mpaa"] == "R", h)
    g = parse_release_groups(t)
    ck("Parasite: three release groups from the selector", [x[1] for x in g] == ["gr2497925637", "gr799232517", "gr1667584773"]
       and g[2][0] == "2025 Re-release", g)
    ck("Parasite title page: no territory table (multi-group), lifetime tables instead",
       parse_territory_tables(t) == [] and len(parse_lifetime_tables(t)) >= 35)
    lt = {r["market_raw"]: r for r in parse_lifetime_tables(t)}
    ck("Parasite lifetime: South Korea 2 releases $71.4M; Domestic $53.8M",
       lt["South Korea"]["gross_usd"] == 71447351 and lt["South Korea"]["n_releases"] == 2 and lt["Domestic"]["gross_usd"] == 53847897, lt.get("South Korea"))
    og = _soup(_fixture_html(cfg, "bom_origrelease_tt6751668"))
    terr = parse_territory_tables(og)
    dom = [r for r in terr if r["market_raw"] == "Domestic"][0]
    ck("Parasite original release: Domestic row", dom["release_date"] == "2019-10-11" and dom["opening_usd"] == 393216
       and dom["gross_usd"] == 53369749 and dom["release_id"] == "rl1258849793", dom)
    cro = [r for r in terr if r["market_raw"] == "Croatia"][0]
    ck("a dash opening is None, not 0", cro["opening_usd"] is None and cro["gross_usd"] == 74871)
    bul = [r for r in terr if r["market_raw"] == "Bulgaria"][0]
    ck("a blank release date is None", bul["release_date"] is None and bul["opening_usd"] == 76)
    ck("find domestic release id from territory rows", find_domestic_release_id(og, terr) == "rl1258849793")
    ck("no Domestic row -> no domestic release id, whatever links the page carries", find_domestic_release_id(t, []) is None)
    nd = _soup(_fixture_html(cfg, "bom_title_tt0411267"))          # The Dark: Spain first, no US release
    hn = parse_headline(nd)
    ck("a film with no US release: international and worldwide only, no distributor, no opening",
       hn["domestic_usd"] is None and hn["international_usd"] == 6593579 and hn["worldwide_usd"] == 6593579
       and hn["distributor"] is None and hn["domestic_opening_usd"] is None and hn["earliest_release_note"] == "Spain", hn)
    tn = parse_territory_tables(nd)
    ck("its territory table has no Domestic row and its first release is Spain",
       find_domestic_release_id(nd, tn) is None and tn[0]["market_raw"] == "Spain" and tn[0]["release_id"] == "rl1047758081", tn[:1])
    ff = parse_headline(_soup(_fixture_html(cfg, "bom_title_tt0385700")))   # Advent Children: distributor, no numbers
    ck("a Domestic Distributor with dashes for every gross: distributor kept, grosses absent (not zero)",
       ff["distributor"] == "Fathom Events" and ff["distributor_scope"] == "domestic" and ff["domestic_usd"] is None
       and ff["worldwide_usd"] is None and ff["earliest_release_note"] == "Domestic", ff)
    rp = parse_headline(_soup(_fixture_html(cfg, "bom_uk_weekend_tt6751668")))
    ck("on a release page the plain 'Distributor' / 'Opening' labels are scoped to that release",
       rp["distributor_scope"] == "release" and rp["opening_scope"] == "release")
    uk = next(r for r in terr if looks_like_uk(r["market_raw"]))
    ck("UK release id found on the original-release page", uk["release_id"] == "rl1387758081", uk)
    wk = _soup(_fixture_html(cfg, "bom_weekly_tt6751668"))
    wh = parse_headline(wk)
    ck("domestic release page: opening theaters, widest, release date",
       wh["opening_theaters"] == 3 and wh["widest_release"] == 2001 and wh["release_date"] == "2019-10-11", wh)
    weeks = parse_period_table(wk, "weekly")
    ck("weekly table: 23 weeks, week 1 is Oct 11-17 2019 $589,642 in 3 theaters",
       len(weeks) >= 20 and weeks[0]["bom_year"] == 2019 and weeks[0]["bom_week"] == 41 and weeks[0]["gross"] == 589642
       and weeks[0]["theaters"] == 3 and weeks[0]["start"] == "2019-10-11" and weeks[0]["number"] == 1, weeks[0])
    ck("weekly table: week 2 % change and theater change parsed", weeks[1]["pct_change_lw"] == 183.9 and weeks[1]["theaters_change"] == 30
       and weeks[1]["to_date"] == 2263805, weeks[1])
    ck("weekly to-date is monotone", all(a["to_date"] <= b["to_date"] for a, b in zip(weeks, weeks[1:]) if a["to_date"] and b["to_date"]))
    ukw = _soup(_fixture_html(cfg, "bom_uk_weekend_tt6751668"))
    uh = parse_headline(ukw)
    ck("UK release page: StudioCanal, opening $1.8M in 137, widest 579, market gross",
       uh["distributor"] == "StudioCanal" and uh["domestic_opening_usd"] == 1800524 and uh["opening_theaters"] == 137
       and uh["widest_release"] == 579 and uh["release_date"] == "2020-02-07" and uh["market_gross_usd"] == 14696724, uh)
    wke = parse_period_table(ukw, "weekend")
    ck("UK weekend table: 6 weekends, first is 2020 W06 rank 4", len(wke) == 6 and wke[0]["bom_year"] == 2020 and wke[0]["bom_week"] == 6
       and wke[0]["rank"] == 4 and wke[0]["gross"] == 1800524 and wke[0]["number"] == 1, wke[0] if wke else None)
    ck("UK page currency detected as USD (BOM reports international in USD)", detect_currency(ukw) == "USD")
    reordered = _soup("""<table><tr><th>Week</th><th>To Date</th><th>Weekly</th><th>Theaters</th><th>Date</th><th>Rank</th></tr>
      <tr><td>3</td><td>$4,900,551</td><td>$2,636,746</td><td>129</td><td><a href="/weekly/2019W43/">Oct 25-31</a></td><td>11</td></tr></table>""")
    rr = parse_period_table(reordered, "weekly")
    ck("period table columns are found by header name, not position",
       rr and rr[0]["number"] == 3 and rr[0]["gross"] == 2636746 and rr[0]["theaters"] == 129 and rr[0]["rank"] == 11
       and rr[0]["to_date"] == 4900551 and rr[0]["bom_week"] == 43, rr)
    ck("weekly parser does not read a weekend table and vice versa",
       parse_period_table(ukw, "weekly") == [] and parse_period_table(wk, "weekend") == [])
    # ---- real pages: Longlegs (single group, wide) --------------------------
    t2 = _soup(_fixture_html(cfg, "bom_title_tt23468450"))
    h2 = parse_headline(t2)
    ck("Longlegs headline", h2["domestic_usd"] == 74346140 and h2["worldwide_usd"] == 127961936 and h2["domestic_opening_usd"] == 22400119
       and h2["earliest_release_note"] == "APAC, EMEA", h2)
    terr2 = parse_territory_tables(t2)
    ck("single-group title page carries the territory table directly", len(terr2) >= 20 and
       any(r["market_raw"] == "United Kingdom" and r["opening_usd"] == 1781300 and r["gross_usd"] == 10462722 for r in terr2))
    ck("Longlegs release page: 2,510 opening theaters, widest 2,850",
       (lambda x: x["opening_theaters"] == 2510 and x["widest_release"] == 2850)(parse_headline(_soup(_fixture_html(cfg, "bom_release_tt23468450")))))
    for name in ("bom_title_tt6751668", "bom_origrelease_tt6751668", "bom_weekly_tt6751668", "bom_uk_weekend_tt6751668",
                 "bom_title_tt23468450", "bom_release_tt23468450"):
        raw = _fixture_html(cfg, name)
        st_ = strip_page(raw)
        a, b = _soup(raw), _soup(st_)
        same = (parse_headline(a) == parse_headline(b) and parse_release_groups(a) == parse_release_groups(b)
                and parse_territory_tables(a) == parse_territory_tables(b) and parse_lifetime_tables(a) == parse_lifetime_tables(b)
                and parse_period_table(a, "weekly") == parse_period_table(b, "weekly")
                and parse_period_table(a, "weekend") == parse_period_table(b, "weekend")
                and find_domestic_release_id(a, []) == find_domestic_release_id(b, []))
        ck(f"stripped page parses identically and is <20% of the size: {name}",
           same and len(zlib.compress(st_.encode())) < 0.20 * len(zlib.compress(raw.encode())),
           (same, len(zlib.compress(st_.encode())), len(zlib.compress(raw.encode()))))
    ck("has_box_office_data on real pages", has_box_office_data(t) and has_box_office_data(t2)
       and not has_box_office_data(_soup("<html><body><h1>Nothing</h1></body></html>")))

    # ---- end to end on a temp database: scope / cache / parse / match / export
    with tempfile.TemporaryDirectory() as td:
        tcfg = R.Config(root=Path(td), db_path=Path(td) / "t.db", out_dir=Path(td) / "out", crosswalk_csv=Path(td) / "cw.csv",
                        fixtures_dir=cfg.fixtures_dir)
        con = connect(tcfg)
        fx = {fid: json.loads((cfg.fixtures_dir / f"tmdb_{fid}.json").read_text(encoding="utf-8")) for fid in (496243, 1050035)}
        R.load_universe(tcfg, con, ids=[496243, 1050035, 42], verbose=False)
        con.execute("UPDATE films SET tconsts='tt6751668' WHERE tmdb_id=496243")
        con.execute("UPDATE films SET tconsts='tt23736044' WHERE tmdb_id=1050035")
        con.execute("UPDATE films SET tconsts='tt0000042' WHERE tmdb_id=42"); con.commit()
        R.write_results(con, [R.FetchResult(f, "ok", 200, fx[f]) for f in fx] +
                        [R.FetchResult(42, "ok", 200, {"id": 42, "title": "Old", "release_dates": {"results": [
                            {"iso_3166_1": "US", "release_dates": [{"type": 3, "release_date": "1950-01-01T00:00:00.000Z", "note": ""}]}]}})], tcfg)
        R.ledger_stage(tcfg, con, verbose=False)
        st = scope_stage(tcfg, con, min_year=2000, verbose=False)
        ck("scope: both modern films scoped, the 1950 film not", st["matched"] == 2 and st["total"] == 2
           and con.execute("SELECT tconst FROM bo_films WHERE tmdb_id=496243").fetchone()[0] == "tt6751668", st)
        ck("scope: US-only is narrower or equal", scope_stage(tcfg, con, min_year=2000, us_only=True, verbose=False)["matched"] <= 2)
        ck("scope re-run adds nothing", scope_stage(tcfg, con, min_year=2000, verbose=False)["added"] == 0)

        # fake BOM over real HTTP, serving the real cached pages
        old, BACKOFF_SCALE = BACKOFF_SCALE, 0.001
        sc = R._Scripted()
        srv, base = R._fake_server(sc)
        try:
            pages = {"/title/tt6751668/": "bom_title_tt6751668", "/releasegroup/gr2497925637/": "bom_origrelease_tt6751668",
                     "/release/rl1258849793/weekly/": "bom_weekly_tt6751668", "/release/rl1387758081/weekend/": "bom_uk_weekend_tt6751668"}
            for path, name in pages.items():
                sc.add(path, (200, {}, _fixture_html(cfg, name).encode("utf-8")))
            sc.add("/releasegroup/gr799232517/", (503, {}, b"down"), (200, {}, _fixture_html(cfg, "bom_origrelease_tt6751668").encode("utf-8")))
            sc.add("/releasegroup/gr1667584773/", (404, {}, b""))
            sc.add("/title/tt23736044/", (404, {}, b""))
            client = BomClient(base=base, sleep=0)
            counts = fetch_stage(tcfg, con, client=client, verbose=False)
            ck("fetch: Parasite ok, Monster has no BOM page -> none", counts == Counter(ok=1, none=1), counts)
            n_pages = con.execute("SELECT COUNT(*) FROM bo_pages").fetchone()[0]
            ck("fetch: title + 3 group pages + weekly + UK weekend cached (404 group cached too)", n_pages == 7, n_pages)
            ck("fetch: 503 retried then succeeded", sum(1 for p, _ in sc.seen if p.startswith("/releasegroup/gr799232517")) == 2)
            calls_before = client.calls
            ck("fetch re-run touches nothing", fetch_stage(tcfg, con, client=client, verbose=False) == Counter() and client.calls == calls_before)
            # robot check handling
            con.execute("INSERT INTO bo_films(tmdb_id, tconst, status) VALUES (7, 'tt0000007', 'pending')"); con.commit()
            sc.add("/title/tt0000007/", (200, {}, b"<html><head><title>Robot Check</title></head><body>To discuss automated access to Amazon data please contact api-services-support@amazon.com</body></html>"))
            try:
                fetch_stage(tcfg, con, client=client, verbose=False)
                blocked_err = None
            except RuntimeError as e:
                blocked_err = e
            ck("robot check: paused, retried, then the run stops itself; nothing cached",
               blocked_err is not None and "robot" in str(blocked_err) and
               con.execute("SELECT COUNT(*) FROM bo_pages WHERE key='title:tt0000007'").fetchone()[0] == 0 and
               con.execute("SELECT status FROM bo_films WHERE tmdb_id=7").fetchone()[0] == "pending", blocked_err)
            con.execute("DELETE FROM bo_films WHERE tmdb_id=7"); con.commit()
            # a throttling burst: 503 on every try, then the site recovers
            con.execute("INSERT INTO bo_films(tmdb_id, tconst, status) VALUES (9, 'tt0000009', 'pending')"); con.commit()
            sc.add("/title/tt0000009/", *([(503, {}, b"Service Unavailable")] * MAX_TRIES), (404, {}, b""))
            c9 = fetch_stage(tcfg, con, client=client, verbose=False)
            ck("a 503 burst pauses and retries the same film instead of recording an error",
               c9["throttled"] == 1 and c9["none"] == 1 and c9["error"] == 0 and
               con.execute("SELECT status FROM bo_films WHERE tmdb_id=9").fetchone()[0] == "none", c9)
            con.execute("DELETE FROM bo_films WHERE tmdb_id=9"); con.execute("DELETE FROM bo_pages WHERE tmdb_id=9"); con.commit()
            # two tconsts on one film: the first has no BOM page, the second does
            con.execute("INSERT INTO bo_films(tmdb_id, tconst, tconsts, status) VALUES (8, 'tt0000008', 'tt0000008|tt23468450', 'pending')"); con.commit()
            sc.add("/title/tt0000008/", (404, {}, b""))
            sc.add("/title/tt23468450/", (200, {}, _fixture_html(cfg, "bom_title_tt23468450").encode("utf-8")))
            sc.add("/release/rl1040613377/weekly/", (200, {}, _fixture_html(cfg, "bom_weekly_tt23468450").encode("utf-8")))
            c8 = fetch_stage(tcfg, con, client=client, with_uk=False, verbose=False)
            ck("a second tconst is tried when the first has no BOM page, and recorded",
               c8 == Counter(ok=1) and con.execute("SELECT tconst FROM bo_films WHERE tmdb_id=8").fetchone()[0] == "tt23468450", c8)
            con.execute("DELETE FROM bo_films WHERE tmdb_id=8"); con.execute("DELETE FROM bo_pages WHERE tmdb_id=8"); con.commit()
        finally:
            BACKOFF_SCALE = old
            with contextlib.suppress(Exception):
                srv.shutdown(); srv.server_close()

        with con:
            con.execute("INSERT INTO bo_films(tmdb_id, tconst, status, fetched_at) VALUES (11, 'tt0411267', 'ok', 'x')")
            _store_page(con, "title:tt0411267", 11, "/title/tt0411267/", 200, _fixture_html(cfg, "bom_title_tt0411267"))
        pst = parse_stage(tcfg, con, verbose=False)
        nd_row = con.execute("SELECT distributor, domestic_opening_usd, opening_theaters, widest_release, domestic_release_date, "
                             "domestic_release_id, international_usd FROM bo_summary WHERE tmdb_id=11").fetchone()
        ck("end to end: no US release -> every domestic column empty, international kept",
           nd_row == (None, None, None, None, None, None, 6593579), nd_row)
        ck("end to end: its Spanish and Russia/CIS rows are territory rows with ISO codes",
           con.execute("SELECT COUNT(*) FROM bo_territory WHERE tmdb_id=11 AND iso IN ('ES','RU')").fetchone()[0] == 2)
        with con:
            for tb in ("bo_films", "bo_pages", "bo_summary", "bo_territory"):
                con.execute(f"DELETE FROM {tb} WHERE tmdb_id=11")
        pst = parse_stage(tcfg, con, rebuild=True, verbose=False)
        ck("parse: one summary, territory rows, weekly and UK weekend rows",
           pst["summary"] == 1 and pst["territory"] >= 70 and pst["weekly"] >= 20 and pst["weekend"] == 6, pst)
        s = dict(zip([r[1] for r in con.execute("PRAGMA table_info(bo_summary)")],
                     con.execute("SELECT * FROM bo_summary WHERE tmdb_id=496243").fetchone()))
        ck("summary row: grosses, distributor, opening + theaters, widest, domestic release date",
           s["domestic_usd"] == 53847897 and s["worldwide_usd"] == 263423899 and s["distributor"] == "Neon"
           and s["domestic_opening_usd"] == 393216 and s["opening_theaters"] == 3 and s["widest_release"] == 2001
           and s["domestic_release_date"] == "2019-10-11" and s["domestic_release_id"] == "rl1258849793", s)
        ck("summary row: UK block", s["uk_release_id"] == "rl1387758081" and s["uk_distributor"] == "StudioCanal"
           and s["uk_opening_usd"] == 1800524 and s["uk_widest_release"] == 579 and s["uk_gross_usd"] == 14696724
           and s["uk_release_date"] == "2020-02-07", s)
        ck("summary row: release groups recorded", s["n_release_groups"] == 3 and "2025 Re-release=gr1667584773" in s["release_groups"])
        tr = con.execute("SELECT release_group, market_raw, iso, region_status, release_date, gross_usd FROM bo_territory "
                         "WHERE tmdb_id=496243 AND market_raw='United Kingdom' ORDER BY release_group").fetchall()
        ck("territory: UK appears once per release group plus lifetime, iso GB",
           len(tr) >= 2 and all(r[2] == "GB" and r[3] == "ok" for r in tr) and any(r[0] == "lifetime" and r[5] == 14707907 for r in tr), tr)
        ck("territory: Domestic is flagged aggregate", con.execute(
            "SELECT region_status FROM bo_territory WHERE tmdb_id=496243 AND market_raw='Domestic' AND release_group!='lifetime'").fetchone()[0] == "aggregate")
        ck("parse re-run does nothing; rebuild redoes", parse_stage(tcfg, con, verbose=False) == {} and parse_stage(tcfg, con, rebuild=True, verbose=False)["summary"] == 1)
        with con:      # a BOM row a year before the only JP theatrical event must NOT match
            con.execute("INSERT INTO bo_territory VALUES (496243,'tt6751668','grX','x','Japan','JP','ok','2019-01-10',NULL,1,NULL)")
        m = match_stage(tcfg, con, verbose=False)
        ck("match: a BOM date outside the window is bom_only even when the country has an event",
           con.execute("SELECT match_status FROM bo_event_match WHERE tmdb_id=496243 AND release_group='grX'").fetchone()[0] == "bom_only")
        with con:
            con.execute("DELETE FROM bo_territory WHERE release_group='grX'")
        m = match_stage(tcfg, con, verbose=False)
        mm = con.execute("SELECT country, src_idx, gap_days, match_status FROM bo_event_match WHERE tmdb_id=496243 AND country='US' "
                         "AND match_status='matched'").fetchall()
        ck("match: BOM Domestic Oct 11 2019 matches TMDb US limited Oct 11 2019 (gap 0)",
           any(r[2] == 0 for r in mm), mm)
        gb = con.execute("SELECT gap_days, match_status FROM bo_event_match WHERE tmdb_id=496243 AND country='GB' "
                         "AND release_group!='lifetime'").fetchall()
        ck("match: UK Feb 7 2020 vs TMDb GB theatrical Feb 14 2020 -> gap 7 within window",
           any(r == (7, "matched") for r in gb), gb)
        ck("match: a territory with no TMDb theatrical event is bom_only, kept", m["bom_only"] > 0
           and con.execute("SELECT COUNT(*) FROM bo_event_match WHERE match_status='bom_only'").fetchone()[0] == m["bom_only"], m)
        ck("match: one TMDb event is never claimed by two BOM rows", con.execute(
            "SELECT COUNT(*) FROM (SELECT tmdb_id, country, src_idx, COUNT(*) c FROM bo_event_match WHERE match_status='matched' "
            "GROUP BY 1,2,3 HAVING c > 1)").fetchone()[0] == 0)
        # ---- reconcile -----------------------------------------------------
        with con:      # a BOM-only market with no TMDb theatrical event, dated
            con.execute("INSERT INTO bo_territory VALUES (496243,'tt6751668','gr2497925637','Original Release','Narnia','NA','ok','2020-03-06',1234,56789,'rlX')")
            con.execute("DELETE FROM events WHERE tmdb_id=496243 AND country='NA'")
        match_stage(tcfg, con, verbose=False)
        rs = reconcile_stage(tcfg, con, verbose=False)
        n_ev = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        n_bo = con.execute("SELECT COUNT(*) FROM bo_event_match WHERE match_status='bom_only' AND bom_date IS NOT NULL").fetchone()[0]
        ck("reconcile: every TMDb event kept, one row per dated BOM-only market", rs["tmdb"] == n_ev and rs["bom"] == n_bo > 0, (rs, n_ev, n_bo))
        us = con.execute("SELECT opening_usd, gross_usd, match_status, bom_market FROM events_bo WHERE tmdb_id=496243 AND country='US' "
                         "AND match_status LIKE 'matched%'").fetchall()
        ck("reconcile: the matched US limited opening carries BOM's opening and gross with a gap label",
           any(r == (393216, 53369749, "matched_bom_gap0d", "Domestic") for r in us), us)
        na = con.execute("SELECT source, type, type_name, date, opening_usd, gross_usd, match_status, src_idx FROM events_bo "
                         "WHERE tmdb_id=496243 AND country='NA'").fetchall()
        ck("reconcile: a BOM-only market becomes a source='bom' theatrical event with a negative src_idx",
           na == [("bom", None, "Theatrical (BOM)", "2020-03-06", 1234, 56789, "bom_only", -1)], na)
        ck("reconcile: unmatched TMDb rows are tmdb_only with no figures", con.execute(
            "SELECT COUNT(*) FROM events_bo WHERE match_status='tmdb_only' AND (gross_usd IS NOT NULL OR opening_usd IS NOT NULL)").fetchone()[0] == 0)
        wn = con.execute("SELECT first_theatrical, first_theatrical_source, theatrical_bom, theatrical_wide FROM windows "
                         "WHERE tmdb_id=496243 AND country='NA'").fetchone()
        ck("windows: first_theatrical filled from BOM where TMDb had none, and says so",
           wn == ("2020-03-06", "bom", "2020-03-06", None), wn)
        wu = con.execute("SELECT first_theatrical, first_theatrical_source FROM windows WHERE tmdb_id=496243 AND country='US'").fetchone()
        ck("windows: where TMDb has the date it stays TMDb's", wu == ("2019-10-11", "tmdb"), wu)
        ck("windows: film summary first theatrical unchanged (Korea, TMDb)",
           con.execute("SELECT first_theatrical, first_theatrical_cc FROM film_summary WHERE tmdb_id=496243").fetchone() == ("2019-05-30", "KR"))
        ck("reconcile is idempotent", reconcile_stage(tcfg, con, rebuild_windows=False, verbose=False)["bom"] == rs["bom"])
        cw_ = R.export_stage(tcfg, con, compat=True, verbose=False)
        with open(tcfg.out_dir / "01_events_tmdb_compat.csv", newline="", encoding="utf-8") as fh:
            crows = list(csv.DictReader(fh))
        ck("compat export: matched rows carry opening/gross and match_status; BOM-only rows have source=bom",
           any(r["source"] == "tmdb" and r["gross_usd"] == "53369749" and r["match_status"] == "matched_bom_gap0d" for r in crows)
           and any(r["source"] == "bom" and r["country_raw"] == "Narnia" and r["channel_source"] == "bom_implicit" for r in crows)
           and all(r["match_status"] for r in crows), len(crows))
        with con:
            con.execute("DELETE FROM bo_territory WHERE market_raw='Narnia'")
        match_stage(tcfg, con, verbose=False); reconcile_stage(tcfg, con, verbose=False)
        w = export_stage(tcfg, con, verbose=False)
        ck("export: events_reconciled.csv written", w.get("events_reconciled.csv", 0) >= 200, w.get("events_reconciled.csv"))
        ck("export: every file written", w["box_office_summary.csv"] == 2 and w["weekly_domestic.csv"] >= 20 and w["weekend_uk.csv"] == 6
           and w["box_office_territory.csv"] >= 70, w)
        with open(tcfg.out_dir / "weekly_domestic.csv", newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        ck("export: weekly_domestic.csv has the 06_weekly_domestic columns",
           {"tconst", "release_id", "week_number", "bom_year", "bom_week", "week_start", "weekly_gross_usd", "theaters", "to_date_usd"} <= set(rows[0])
           and rows[0]["tconst"] == "tt6751668" and rows[0]["weekly_gross_usd"] == "589642")
        with open(tcfg.out_dir / "box_office_summary.csv", newline="", encoding="utf-8") as fh:
            srows = list(csv.DictReader(fh))
        ck("export: a film with no BOM page is listed with status none and empty grosses, not zeros",
           any(r["status"] == "none" and r["domestic_usd"] == "" for r in srows), [(r["status"], r["domestic_usd"]) for r in srows])

    passed = sum(1 for _, ok, _ in checks if ok)
    if verbose:
        print("=" * 88)
        for name, ok, detail in checks:
            if not ok:
                print(f"  FAIL  {name}   [{detail}]")
        print(f"{passed}/{len(checks)} checks passed")
        print("=" * 88)
    return passed, len(checks)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", help="release_events.db path (default: <project>/release_events/release_events.db)")
    ap.add_argument("--sleep", type=float, default=None, help=f"seconds between requests (default {BOM_SLEEP})")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("scope"); s.add_argument("--min-year", type=int, default=2000); s.add_argument("--us-only", action="store_true")
    f = sub.add_parser("fetch"); f.add_argument("--limit", type=int); f.add_argument("--no-uk", action="store_true")
    f.add_argument("--no-weekly", action="store_true"); f.add_argument("--no-retry", action="store_true", help="skip films that errored before")
    p = sub.add_parser("parse"); p.add_argument("--rebuild", action="store_true")
    m = sub.add_parser("match"); m.add_argument("--window", type=int, default=MATCH_WINDOW_DAYS)
    sub.add_parser("reconcile", help="events_bo = TMDb events + BOM figures; rebuilds windows")
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
    if a.cmd == "scope":
        scope_stage(cfg, con, min_year=a.min_year, us_only=a.us_only)
    elif a.cmd == "fetch":
        fetch_stage(cfg, con, limit=a.limit, retry_errors=not a.no_retry, with_uk=not a.no_uk, with_weekly=not a.no_weekly,
                    client=BomClient(sleep=a.sleep))
    elif a.cmd == "parse":
        parse_stage(cfg, con, rebuild=a.rebuild)
    elif a.cmd == "match":
        match_stage(cfg, con, window=a.window)
    elif a.cmd == "reconcile":
        reconcile_stage(cfg, con)
    elif a.cmd == "export":
        export_stage(cfg, con)
    elif a.cmd == "report":
        report(cfg, con)
    return 0


if __name__ == "__main__":
    sys.exit(main())
