#!/usr/bin/env python3
"""Repair the TMDb awards sweep after the 2026-10-18 crash.

What went wrong
  When TMDb's firewall stopped accepting plain requests, enrich.py switched to
  fetching every page in the headless browser ("BROWSER-ONLY"). That path never
  checked that the browser had actually been served an awards page, so firewall
  challenge pages parsed as "no awards" and the film was recorded as
  tmdb_awards_status='none'. The sweep never revisits 'none' films. It happened
  in both runs so far:
      2026-10-14 21:09-21:38 UTC   876 films in a row
      2026-10-18 18:26-18:44 UTC   539 films in a row (then the disk filled)
  At the normal hit rate (15-35% of films have awards) runs that long are
  impossible by chance.

What this script does (the sweep must be stopped)
  1. Patches release_events/enrich.py, after saving a backup next to it:
       - a page from the browser must carry the awards-page markers, exactly as
         the plain-request path already required; anything else raises
         AwardsChallenged, so the film stays unfetched and three in a row stop
         the run (instead of being recorded as 'none')
       - the browser now waits for the firewall challenge to clear (it waited 0.8 s)
       - a 404 in the browser still means "film gone"
       - one new self-test check covers it
  2. Runs enrich.py's self-test (49 checks). If it fails, enrich.py is put back
     and the database is not touched.
  3. Resets the suspect films to "not fetched yet". Their ids and old statuses
     are saved to release_events/out/awards_reset_<time>.csv first.

Usage (from Terminal):
    python3 ~/NEON/ADP_Project/comp_atlas/tools/repair_awards_sweep.py --dry-run   # report only
    python3 ~/NEON/ADP_Project/comp_atlas/tools/repair_awards_sweep.py

Running it twice is safe: each step notices when it is already done.
"""
from __future__ import annotations

import argparse
import csv
import py_compile
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# the two runs of false 'none' results, from enrich_status.tmdb_awards_at (UTC)
WINDOWS = [("2026-10-14T21:09:29Z", "2026-10-14T21:38:33Z"),
           ("2026-10-18T18:26:54Z", "2026-10-18T18:44:41Z")]
EXPECTED = 876 + 539

PATCH_TAG = "_require_awards_page"

EDITS = [
    # 1. the shared check, right after the exception it raises
    ('''class AwardsChallenged(RuntimeError):
    """TMDb answered with something that is not an awards page (WAF challenge, block)."""
''',
     '''class AwardsChallenged(RuntimeError):
    """TMDb answered with something that is not an awards page (WAF challenge, block)."""


def _require_awards_page(html: str, where: str) -> str:
    """Pass a real awards page, or "" (film gone); raise AwardsChallenged for anything else.
    Without this the browser path recorded firewall challenge pages as 'none' (2026-10-18)."""
    if html and not any(m in html for m in AWARDS_PAGE_MARKERS):
        raise AwardsChallenged(f"{where}: {len(html)} bytes, no awards page markers")
    return html
'''),
    # 2. browser-assisted fetches from the plain fetcher
    ('''    def _prime(self, tmdb_id: int) -> str:
        """Fetch this film in the browser (solves the challenge) and copy its cookies."""
        html = self.browser.fetch(tmdb_id)
''',
     '''    def _prime(self, tmdb_id: int) -> str:
        """Fetch this film in the browser (solves the challenge) and copy its cookies.
        A browser that is not served an awards page either raises AwardsChallenged."""
        html = _require_awards_page(self.browser.fetch(tmdb_id), f"browser, tmdb {tmdb_id}")
'''),
    # 3. the browser itself (also used directly with --browser)
    ('''    def fetch(self, tmdb_id: int) -> str:
        page = self.ctx.new_page()
        try:
            page.goto(f"{self.site}/movie/{tmdb_id}/awards?language=en-US", timeout=self.timeout_ms, wait_until="domcontentloaded")
            try:
                page.wait_for_selector("div.space-y-12, text=/No awards/i, text=/Nominations?/", timeout=self.timeout_ms)
            except Exception:
                pass
            page.wait_for_timeout(800)
            return page.content()
        finally:
            page.close()
''',
     '''    def fetch(self, tmdb_id: int) -> str:
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
'''),
    # 4. self-test: a challenge page served to the browser must not parse as 'none'
    ('''            ck("plain fetcher + browser: five primes in a row -> browser-only, no more plain requests",
               pfb.browser_only and sb.calls == 7 and len(sc.seen) - n_before == 5, (pfb.browser_only, sb.calls, len(sc.seen) - n_before))
''',
     '''            ck("plain fetcher + browser: five primes in a row -> browser-only, no more plain requests",
               pfb.browser_only and sb.calls == 7 and len(sc.seen) - n_before == 5, (pfb.browser_only, sb.calls, len(sc.seen) - n_before))
            sb.fetch = lambda tid: "<html><script src='challenge.js'></script></html>"
            try:
                pfb.fetch(496243); bchal = False
            except AwardsChallenged:
                bchal = True
            ck("browser-only: a challenge page served to the browser raises AwardsChallenged instead of parsing as 'none'", bchal)
'''),
]


def say(msg=""):
    print(msg, flush=True)


def sweep_running() -> bool:
    try:
        r = subprocess.run(["pgrep", "-f", "enrich.py tmdb_awards"], capture_output=True, text=True)
    except FileNotFoundError:
        return False
    return bool(r.stdout.strip())


def patch(enrich: Path, dry: bool) -> Path | None:
    """Returns the backup path if the file was changed."""
    text = enrich.read_text(encoding="utf-8")
    if PATCH_TAG in text:
        say("1. enrich.py: already patched")
        return None
    for i, (old, _) in enumerate(EDITS, 1):
        n = text.count(old)
        if n != 1:
            sys.exit(f"1. enrich.py: edit {i} expected its original text once, found it {n} times. "
                     f"enrich.py has changed since this script was written; nothing was modified.")
    if dry:
        say("1. enrich.py: would apply 4 edits (dry run)")
        return None
    for old, new in EDITS:
        text = text.replace(old, new)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = enrich.with_name(f"enrich.py.bak-{stamp}")
    shutil.copy2(enrich, backup)
    enrich.write_text(text, encoding="utf-8")
    try:
        py_compile.compile(str(enrich), doraise=True)
    except py_compile.PyCompileError as e:
        shutil.copy2(backup, enrich)
        sys.exit(f"1. enrich.py: patched file did not compile ({e}); original restored")
    say(f"1. enrich.py: patched (backup: {backup.name})")
    return backup


def selftest(enrich: Path, backup: Path | None, dry: bool) -> None:
    if dry:
        say("2. self-test: skipped (dry run)")
        return
    r = subprocess.run([sys.executable, enrich.name, "selftest"], cwd=enrich.parent, capture_output=True, text=True)
    lines = [ln for ln in (r.stdout + r.stderr).splitlines() if "checks passed" in ln or "FAIL" in ln]
    if r.returncode != 0:
        say("\n".join((r.stdout + r.stderr).splitlines()[-30:]))
        if backup is not None:
            shutil.copy2(backup, enrich)
            sys.exit("2. self-test FAILED: enrich.py restored from the backup; the database was not touched")
        sys.exit("2. self-test FAILED; the database was not touched")
    say(f"2. self-test: {lines[-1].strip() if lines else 'passed'}")


def reset(db: Path, out_dir: Path, dry: bool) -> None:
    con = sqlite3.connect(str(db), timeout=60)
    where = " OR ".join("tmdb_awards_at BETWEEN ? AND ?" for _ in WINDOWS)
    args = [t for w in WINDOWS for t in w]
    rows = con.execute(f"SELECT tmdb_id, tconst, tmdb_awards_status, tmdb_awards_at FROM enrich_status "
                       f"WHERE tmdb_awards_status='none' AND ({where}) ORDER BY tmdb_awards_at", args).fetchall()
    if not rows:
        say("3. database: no suspect films left (already reset)")
        return
    flag = "" if len(rows) == EXPECTED else f"  (expected {EXPECTED}; check before going on)"
    if dry:
        say(f"3. database: would reset {len(rows):,} films to not-fetched (dry run){flag}")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    log = out_dir / f"awards_reset_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.csv"
    with open(log, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh); w.writerow(["tmdb_id", "tconst", "old_status", "old_at"]); w.writerows(rows)
    with con:
        con.executemany("UPDATE enrich_status SET tmdb_awards_status=NULL, tmdb_awards_at=NULL "
                        "WHERE tmdb_id=? AND tmdb_awards_status='none'", [(r[0],) for r in rows])
    left = con.execute(f"SELECT COUNT(*) FROM enrich_status WHERE tmdb_awards_status='none' AND ({where})", args).fetchone()[0]
    counts = dict(con.execute("SELECT COALESCE(tmdb_awards_status,'not fetched'), COUNT(*) FROM enrich_status GROUP BY 1").fetchall())
    con.close()
    say(f"3. database: reset {len(rows):,} films to not-fetched{flag}; {left} suspect left (ids saved to out/{log.name})")
    say("   statuses now: " + ", ".join(f"{k} {v:,}" for k, v in sorted(counts.items())))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--project", type=Path, default=Path(__file__).resolve().parents[2],
                    help="ADP_Project folder (default: two levels above this script)")
    ap.add_argument("--dry-run", action="store_true", help="report what would change; change nothing")
    a = ap.parse_args(argv)
    re_dir = a.project / "release_events"
    enrich, db = re_dir / "enrich.py", re_dir / "release_events.db"
    if not enrich.exists():
        sys.exit(f"not found: {enrich} (pass --project /path/to/ADP_Project)")
    if sweep_running():
        sys.exit("An 'enrich.py tmdb_awards' process is running. Stop it first (it would keep recording "
                 "challenge pages as 'none'), then run this again.")
    backup = patch(enrich, a.dry_run)
    selftest(enrich, backup, a.dry_run)
    if db.exists():
        reset(db, re_dir / "out", a.dry_run)
    else:
        say(f"3. database: {db} not found; skipped")
    if not a.dry_run:
        say("\nDone. Restart the sweep from Terminal:\n"
            f"  cd {re_dir}\n"
            "  nohup caffeinate -i python3 -u enrich.py tmdb_awards > awards.log 2>&1 &\n"
            "  tail -f awards.log      # ok= should keep climbing; if TMDb blocks the browser too, the run now stops itself")
    return 0


if __name__ == "__main__":
    sys.exit(main())
