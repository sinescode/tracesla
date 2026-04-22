"""
send_all.py — Standalone PayTrack sender
========================================
Reads Config/Config.json and CSV/*.csv,
computes every user's net pending balance,
generates a Pillow card, and sends to ALL
users via Telegram.  No Flask, no database.

Usage:
    python send_all.py

Requires:
    pip install pillow aiohttp python-dotenv
    TELEGRAM_BOT_TOKEN in .env or environment
    Fonts/LobsterTwo-Regular.ttf in repo
"""

from __future__ import annotations

import asyncio
import csv
import glob
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Sequence

from dotenv import load_dotenv
from card_generator import generate_card

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Env ───────────────────────────────────────────────────────────────────────
load_dotenv()
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN not set in environment / .env")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# ── Telegram tuning ───────────────────────────────────────────────────────────
MAX_CONCURRENT  = 10
MAX_RETRIES     = 4
BASE_DELAY      = 0.4
MAX_DELAY       = 16.0
CONNECT_TIMEOUT = 8
READ_TIMEOUT    = 20

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIG LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_config(path: str = "Config/Config.json") -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def get_tier_definitions(cfg_data: dict) -> list[dict]:
    return cfg_data["config"]["tier_definitions"]


def get_user_tiers(cfg_data: dict) -> dict[str, list[int]]:
    return {k: v for k, v in cfg_data["config"]["user_tiers"].items()}


def get_custom_names(cfg_data: dict) -> dict[str, str]:
    return cfg_data["config"].get("custom_names", {})


def get_balances(cfg_data: dict) -> dict[str, float]:
    return {k: float(v) for k, v in cfg_data.get("balances", {}).items()}


def build_tier_index(tier_defs: list[dict]) -> dict[int, dict]:
    return {t["id"]: t for t in tier_defs}


# ─────────────────────────────────────────────────────────────────────────────
# 2. TIER / PRICE CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def get_tiers_for_user(user_id: str,
                        user_tier_map: dict[str, list[int]],
                        tier_index: dict[int, dict]) -> list[dict]:
    ids = user_tier_map.get(user_id, [])
    return sorted(
        [tier_index[i] for i in ids if i in tier_index],
        key=lambda t: t["min_ok"]
    )


def calculate_total(ok_count: int, tiers: list[dict]) -> tuple[float, float]:
    for tier in tiers:
        if tier["min_ok"] <= ok_count <= tier["max_ok"]:
            return tier["price_per_ok"], ok_count * tier["price_per_ok"]
    return 0.0, 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 3. CSV LOADING  —  also extracts the latest date from filenames
# ─────────────────────────────────────────────────────────────────────────────

# Date formats tried against the CSV filename stem (without extension).
# Add more formats here if your filenames differ.
_DATE_FORMATS = [
    "%Y-%m-%d",   # 2024-07-15
    "%Y_%m_%d",   # 2024_07_15
    "%d-%m-%Y",   # 15-07-2024
    "%d_%m_%Y",   # 15_07_2024
    "%Y-%m",      # 2024-07
    "%Y_%m",      # 2024_07
    "%B_%Y",      # July_2024
    "%b_%Y",      # Jul_2024
    "%B-%Y",      # July-2024
    "%b-%Y",      # Jul-2024
]


def _date_from_stem(stem: str) -> str:
    """Try to parse a date from a CSV filename stem.
    Returns 'YYYY-MM-DD' string, or '' if nothing matched."""
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(stem, fmt)
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def load_csv_entries(csv_dir: str = "CSV") -> tuple[list[dict], str]:
    """Load all CSV rows and return (entries, latest_date).

    latest_date is the most recent date found in any CSV filename,
    formatted as 'YYYY-MM-DD'.  Falls back to '' if none could be parsed.
    """
    entries: list[dict] = []
    latest_date: str = ""

    pattern = os.path.join(csv_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("No CSV files found in '%s'", csv_dir)

    for fpath in files:
        filename = os.path.basename(fpath)
        stem     = os.path.splitext(filename)[0]

        # ── Detect date from filename ──────────────────────────────────────
        candidate = _date_from_stem(stem)
        if candidate and candidate > latest_date:
            latest_date = candidate
            log.debug("Latest date updated to %s (from %s)", latest_date, filename)
        # ──────────────────────────────────────────────────────────────────

        try:
            with open(fpath, encoding="utf-8-sig", errors="replace") as f:
                reader = csv.DictReader(f)
                for raw in reader:
                    row = {k.strip(): (v.strip() if v else "") for k, v in raw.items()}
                    user_id = row.get("User ID", "").strip()
                    if not user_id:
                        continue
                    ok_str = row.get("OK Count", "").strip()
                    try:
                        ok_count = int(ok_str)
                    except ValueError:
                        continue
                    if ok_count < 0:
                        continue
                    entries.append({
                        "filename":    filename,
                        "user_id":     user_id,
                        "username":    row.get("Username", ""),
                        "ok_count":    ok_count,
                        "rate_str":    row.get("Rate", "").strip(),
                        "bkash":       row.get("Bkash", "") or "Not Provided",
                        "rocket":      row.get("Rocket", "") or "Not Provided",
                        "paid_status": row.get("Paid Status", "").lower(),
                    })
            log.info("Loaded %s", filename)
        except Exception as exc:
            log.error("Failed to read %s: %s", filename, exc)

    if latest_date:
        log.info("Latest CSV date detected: %s", latest_date)
    else:
        log.warning("Could not detect a date from any CSV filename.")

    return entries, latest_date


# ─────────────────────────────────────────────────────────────────────────────
# 4. BUILD USER SUMMARIES
# ─────────────────────────────────────────────────────────────────────────────

def build_user_summaries(cfg_data: dict, csv_dir: str = "CSV") -> tuple[list[dict], str]:
    """Returns (summaries, latest_date_from_csvs)."""
    tier_defs     = get_tier_definitions(cfg_data)
    tier_index    = build_tier_index(tier_defs)
    user_tier_map = get_user_tiers(cfg_data)
    custom_names  = get_custom_names(cfg_data)
    balances      = get_balances(cfg_data)

    entries, latest_date = load_csv_entries(csv_dir)   # ← unpacks latest_date

    user_totals: dict[str, float] = {}
    for entry in entries:
        uid      = entry["user_id"]
        ok_count = entry["ok_count"]
        paid     = entry["paid_status"]

        if paid in ("paid", "p"):
            continue

        user_tiers = get_tiers_for_user(uid, user_tier_map, tier_index)
        if user_tiers:
            _, total = calculate_total(ok_count, user_tiers)
        else:
            try:
                rate = float(entry["rate_str"])
                total = ok_count * rate
            except (ValueError, TypeError):
                total = 0.0

        user_totals[uid] = user_totals.get(uid, 0.0) + total

    all_uids = set(user_totals) | {k for k, v in balances.items() if v != 0}
    summaries: list[dict] = []
    for uid in all_uids:
        raw_total = user_totals.get(uid, 0.0)
        balance   = balances.get(uid, 0.0)
        pending   = raw_total + balance
        summaries.append({
            "user_id":      uid,
            "display_name": custom_names.get(uid, uid),
            "pending":      pending,
        })

    summaries.sort(key=lambda x: x["pending"], reverse=True)
    return summaries, latest_date   # ← also returns latest_date


# ─────────────────────────────────────────────────────────────────────────────
# 5. TELEGRAM SERVICE
# ─────────────────────────────────────────────────────────────────────────────

import aiohttp


@dataclass
class SendResult:
    user_id : str
    ok      : bool
    blocked : bool  = False
    error   : str   = ""
    attempts: int   = 0
    elapsed : float = 0.0


@dataclass
class BatchReport:
    total    : int
    succeeded: int
    blocked  : int
    failed   : int
    elapsed  : float
    results  : list[SendResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.succeeded / self.total * 100 if self.total else 0.0


def _is_blocked(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in
               ["blocked", "not found", "deleted",
                "deactivated", "chat not found", "forbidden"])


def _build_caption(user_id: str, display_name: str, pending: float, date: str) -> str:
    """Build the Telegram HTML caption for a payment card.

    date  —  the latest CSV date, shown as "Report till: <date>"
    """
    import html as _html
    uid  = _html.escape(user_id)
    name = _html.escape(display_name)
    dt   = _html.escape(date)
    sign = "CREDIT" if pending < 0 else "DEBIT"
    amt  = f"{abs(pending):,.2f}"
    return (
        "🧾 <b>Payment Receipt</b>\n\n"

        f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
        f"📅 <b>Report till:</b> {dt}\n"        # ← "Date" → "Report till: <latest CSV date>"
        f"💰 <b>{sign}:</b> ৳{amt}\n\n"          # ← amount line added

        "📨 Please <b>forward this message</b> to admin for verification.\n"
        "👨‍💼 <b>Admin:</b> @turja_un"
    )


def _extract_retry_after(body: str) -> float | None:
    try:
        data = json.loads(body)
        return float(data.get("parameters", {}).get("retry_after", 0)) or None
    except Exception:
        return None


class TelegramService:
    def __init__(self, max_concurrent=MAX_CONCURRENT, max_retries=MAX_RETRIES):
        self._sem       = asyncio.Semaphore(max_concurrent)
        self._max_retry = max_retries
        self._connector = None
        self._session   = None

    async def __aenter__(self):
        self._connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT + 5, limit_per_host=MAX_CONCURRENT + 5,
            ttl_dns_cache=300, ssl=True)
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=aiohttp.ClientTimeout(connect=CONNECT_TIMEOUT, total=READ_TIMEOUT))
        return self

    async def __aexit__(self, *_):
        if self._session:   await self._session.close()
        if self._connector: await self._connector.close()
        await asyncio.sleep(0.1)

    async def send_photo(self, user_id: str, display_name: str,
                         photo_bytes: bytes, pending: float, date: str) -> SendResult:
        t0      = time.monotonic()
        caption = _build_caption(user_id, display_name, pending, date)
        delay   = BASE_DELAY
        result  = SendResult(user_id=user_id, ok=False)

        for attempt in range(1, self._max_retry + 1):
            result.attempts = attempt
            async with self._sem:
                try:
                    status, body = await self._post(user_id, caption, photo_bytes)
                except aiohttp.ClientError as exc:
                    result.error = str(exc)
                    log.warning("uid=%s attempt=%d network: %s", user_id, attempt, exc)
                    if attempt < self._max_retry:
                        await asyncio.sleep(delay + random.uniform(0, 0.3))
                        delay = min(delay * 2, MAX_DELAY)
                    continue

            if status == 200 and '"ok":true' in body:
                result.ok = True; result.error = ""; break

            if status == 429:
                wait = _extract_retry_after(body) or delay
                log.warning("uid=%s rate-limited, sleeping %.1fs", user_id, wait)
                await asyncio.sleep(wait + random.uniform(0, 0.5))
                delay = min(wait * 1.5, MAX_DELAY)
                continue

            if _is_blocked(body):
                result.blocked = True; result.error = "User blocked bot"; break

            if status in (400, 403):
                result.error = body; break

            result.error = body
            if attempt < self._max_retry:
                await asyncio.sleep(delay + random.uniform(0, 0.3))
                delay = min(delay * 2, MAX_DELAY)

        result.elapsed = time.monotonic() - t0
        return result

    async def _post(self, user_id, caption, photo_bytes):
        data = aiohttp.FormData()
        data.add_field("chat_id",    user_id)
        data.add_field("caption",    caption)
        data.add_field("parse_mode", "HTML")
        data.add_field("photo", photo_bytes,
                       filename="payment.png", content_type="image/png")
        async with self._session.post(f"{TELEGRAM_API}/sendPhoto", data=data) as resp:
            return resp.status, await resp.text()

    async def send_many(self, users: Sequence[tuple]) -> BatchReport:
        t0      = time.monotonic()
        tasks   = [self.send_photo(uid, name, pb, pend, dt)
                   for uid, name, pb, pend, dt in users]
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - t0
        succeeded = sum(1 for r in results if r.ok)
        blocked   = sum(1 for r in results if r.blocked)
        return BatchReport(
            total=len(results), succeeded=succeeded,
            blocked=blocked, failed=len(results) - succeeded - blocked,
            elapsed=elapsed, results=list(results))


# ─────────────────────────────────────────────────────────────────────────────
# 6. MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    log.info("Loading config...")
    cfg_data = load_config("Config/Config.json")

    log.info("Building user summaries from CSVs...")
    users, csv_latest_date = build_user_summaries(cfg_data, csv_dir="CSV")  # ← unpacks

    if not users:
        log.warning("No users found — nothing to send.")
        return

    # Use the latest CSV date as the report date.
    # Falls back to today if no date could be parsed from filenames.
    date = csv_latest_date or datetime.today().strftime("%Y-%m-%d")
    log.info("Report till: %s  |  users: %d", date, len(users))
    print()

    card_cache: dict[str, tuple] = {}
    for u in users:
        uid  = u["user_id"]
        name = u["display_name"]
        pend = u["pending"]
        try:
            log.info("[CARD] Generating for %-20s  pending=%.2f", name, pend)
            pb = generate_card(user_id=uid, display_name=name,
                               pending=pend, date=date)
            card_cache[uid] = (name, pb, pend)
        except Exception as exc:
            log.error("[CARD] Failed for %s: %s", uid, exc)

    print()
    log.info("Cards generated: %d / %d", len(card_cache), len(users))

    if not card_cache:
        log.warning("No cards to send.")
        return

    payload = [
        (uid, name, pb, pend, date)
        for uid, (name, pb, pend) in card_cache.items()
    ]

    log.info("Sending to %d users via Telegram...", len(payload))
    print()

    async with TelegramService() as svc:
        report = await svc.send_many(payload)

    print()
    print("=" * 55)
    print(f"  SEND REPORT  —  Report till {date}")
    print("=" * 55)
    print(f"  Total    : {report.total}")
    print(f"  Succeeded: {report.succeeded}")
    print(f"  Blocked  : {report.blocked}")
    print(f"  Failed   : {report.failed}")
    print(f"  Time     : {report.elapsed:.1f}s")
    print(f"  Rate     : {report.success_rate:.1f}%")
    print("=" * 55)

    if report.failed or report.blocked:
        print("\nDetails for non-OK results:")
        for r in report.results:
            if not r.ok:
                tag = "BLOCKED" if r.blocked else "FAILED"
                print(f"  [{tag}] {r.user_id:>15}  —  {r.error[:80]}")
    print()


if __name__ == "__main__":
    asyncio.run(main())
