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
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

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
    """Returns {user_id: [tier_id, ...]}"""
    return {k: v for k, v in cfg_data["config"]["user_tiers"].items()}


def get_custom_names(cfg_data: dict) -> dict[str, str]:
    return cfg_data["config"].get("custom_names", {})


def get_balances(cfg_data: dict) -> dict[str, float]:
    return {k: float(v) for k, v in cfg_data.get("balances", {}).items()}


def build_tier_index(tier_defs: list[dict]) -> dict[int, dict]:
    """Returns {tier_id: tier_dict}"""
    return {t["id"]: t for t in tier_defs}


# ─────────────────────────────────────────────────────────────────────────────
# 2. TIER / PRICE CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def get_tiers_for_user(user_id: str,
                        user_tier_map: dict[str, list[int]],
                        tier_index: dict[int, dict]) -> list[dict]:
    """Return ordered tier list for a user, or empty list if not configured."""
    ids = user_tier_map.get(user_id, [])
    return sorted(
        [tier_index[i] for i in ids if i in tier_index],
        key=lambda t: t["min_ok"]
    )


def calculate_total(ok_count: int, tiers: list[dict]) -> tuple[float, float]:
    """Returns (price_per_ok, total)  — 0,0 if no tier matches."""
    for tier in tiers:
        if tier["min_ok"] <= ok_count <= tier["max_ok"]:
            return tier["price_per_ok"], ok_count * tier["price_per_ok"]
    return 0.0, 0.0


# ─────────────────────────────────────────────────────────────────────────────
# 3. CSV LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_csv_entries(csv_dir: str = "CSV") -> list[dict]:
    """
    Read every *.csv in csv_dir.
    Returns a list of row-dicts with keys:
        filename, user_id, username, ok_count,
        rate_str, bkash, rocket, paid_status
    """
    entries: list[dict] = []
    pattern = os.path.join(csv_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        log.warning("No CSV files found in '%s'", csv_dir)
    for fpath in files:
        filename = os.path.basename(fpath)
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
    return entries


# ─────────────────────────────────────────────────────────────────────────────
# 4. BUILD USER SUMMARIES (net pending per user)
# ─────────────────────────────────────────────────────────────────────────────

def build_user_summaries(cfg_data: dict, csv_dir: str = "CSV") -> list[dict]:
    """
    Returns a list of:
        {user_id, display_name, pending}
    sorted by pending descending (largest debt first).
    """
    tier_defs    = get_tier_definitions(cfg_data)
    tier_index   = build_tier_index(tier_defs)
    user_tier_map = get_user_tiers(cfg_data)
    custom_names  = get_custom_names(cfg_data)
    balances      = get_balances(cfg_data)

    entries = load_csv_entries(csv_dir)

    # Accumulate totals per user
    user_totals: dict[str, float] = {}
    for entry in entries:
        uid      = entry["user_id"]
        ok_count = entry["ok_count"]
        paid     = entry["paid_status"]

        # Skip already-paid rows
        if paid in ("paid", "p"):
            continue

        # Determine price: user tiers first, then CSV rate, then 0
        user_tiers = get_tiers_for_user(uid, user_tier_map, tier_index)
        if user_tiers:
            _, total = calculate_total(ok_count, user_tiers)
        else:
            # Fallback: use Rate column from CSV
            try:
                rate = float(entry["rate_str"])
                total = ok_count * rate
            except (ValueError, TypeError):
                total = 0.0

        user_totals[uid] = user_totals.get(uid, 0.0) + total

    # Merge with balance adjustments
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
    return summaries


# ─────────────────────────────────────────────────────────────────────────────
# 5. CARD GENERATOR  (inline Pillow — no separate file needed)
# ─────────────────────────────────────────────────────────────────────────────

import io as _io
try:
    from PIL import Image, ImageDraw, ImageFont
    _PIL_AVAILABLE = True
except ImportError:
    _PIL_AVAILABLE = False
    log.warning("Pillow not installed — card generation will produce blank PNGs")


def _find_font(bold: bool = False, mono: bool = False):
    free_dir = "/usr/share/fonts/truetype/freefont/"
    local_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = []
    if bold and not mono:
        candidates += [
            os.path.join(local_dir, "LobsterTwo-Bold.ttf"),
            os.path.join(free_dir,  "FreeSansBold.ttf"),
        ]
    elif mono and bold:
        candidates += [os.path.join(free_dir, "FreeMonoBold.ttf")]
    elif mono:
        candidates += [os.path.join(free_dir, "FreeMono.ttf")]
    else:
        candidates += [
            os.path.join(local_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(free_dir,  "FreeSans.ttf"),
        ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    return None


def _font(bold=False, size=14, mono=False):
    path = _find_font(bold, mono)
    if path:
        return ImageFont.truetype(path, size)
    return ImageFont.load_default()


def _lerp(a, b, t):
    return int(a * (1 - t) + b * t)


def _blend(color, alpha, bg):
    return tuple(int(c * alpha + b * (1 - alpha)) for c, b in zip(color, bg))


def generate_card(user_id: str, display_name: str,
                  pending: float, date: str) -> bytes:
    """Generate a payment card PNG and return its bytes."""
    if not _PIL_AVAILABLE:
        # Return a 1×1 transparent PNG as fallback
        buf = _io.BytesIO()
        Image.new("RGB", (1, 1)).save(buf, "PNG")
        return buf.getvalue()

    W, H = 960, 540
    is_credit = pending < 0
    abs_pend  = abs(pending)
    abs_str   = f"{abs_pend:,.2f}"
    initial   = (display_name[0] if display_name else user_id[0]).upper()

    # Colours
    BG_DARK  = (15,  23,  42)
    BG_CARD  = (30,  41,  59)
    SLATE600 = (71,  85, 105)
    SLATE700 = (51,  65,  85)
    SLATE400 = (148, 163, 184)
    SLATE50  = (248, 250, 252)
    EMERALD  = (16,  185, 129)
    RED      = (239,  68,  68)
    accent   = EMERALD if is_credit else RED
    status   = "CREDIT" if is_credit else "DEBIT"
    bal_lbl  = "CREDIT BALANCE" if is_credit else "PENDING BALANCE"

    pad_x       = int(W * 0.06)
    pad_y       = int(H * 0.06)
    av_size     = int(W * 0.13)
    av_radius   = av_size // 5
    divider_y   = int(H * 0.38)
    rpanel_w    = int(W * 0.28)
    rpanel_x    = W - rpanel_w - pad_x

    def fs(factor): return int(H * factor)

    img  = Image.new("RGB", (W, H), BG_CARD)
    draw = ImageDraw.Draw(img)

    # Gradient background
    for y in range(H):
        t = y / H
        ts = t * t * (3 - 2 * t)
        r = _lerp(BG_DARK[0], BG_CARD[0], ts)
        g = _lerp(BG_DARK[1], BG_CARD[1], ts)
        b = _lerp(BG_DARK[2], BG_CARD[2], ts)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # Glow orb
    orb = Image.new("RGB", (W, H), (0, 0, 0))
    od  = ImageDraw.Draw(orb)
    cx, cy, rad = int(W * 0.15), int(H * 0.22), int(W * 0.20)
    for rr in range(rad, 0, -3):
        a = 0.12 * (1 - rr / rad) ** 2
        od.ellipse([cx - rr, cy - rr, cx + rr, cy + rr],
                   fill=_blend(accent, a, (0, 0, 0)))
    img  = Image.blend(img, orb, alpha=0.6)
    draw = ImageDraw.Draw(img)

    # Top accent stripe
    for x in range(int(W * 0.60)):
        a = (1.0 - x / (W * 0.60)) * 0.8
        draw.line([(x, 0), (x, 3)], fill=_blend(accent, a, BG_CARD))

    # Avatar
    av_x, av_y = pad_x, int(pad_y * 1.2)
    draw.rounded_rectangle(
        [av_x, av_y, av_x + av_size, av_y + av_size],
        radius=av_radius,
        fill=_blend(accent, 0.55, BG_CARD)
    )
    f_init = _font(bold=True, size=fs(0.08))
    bb = draw.textbbox((0, 0), initial, font=f_init)
    draw.text(
        (av_x + (av_size - (bb[2] - bb[0])) // 2,
         av_y + (av_size - (bb[3] - bb[1])) // 2),
        initial, font=f_init, fill=SLATE50
    )

    # Name & ID
    name_x = av_x + av_size + pad_x
    name_y = int(pad_y * 1.2)
    f_name = _font(bold=True, size=fs(0.055))
    draw.text((name_x, name_y), display_name, font=f_name, fill=SLATE50)
    f_id = _font(size=fs(0.028))
    draw.text((name_x, name_y + int(H * 0.07)), f"ID: {user_id}", font=f_id, fill=SLATE400)

    # Logo
    f_logo = _font(bold=True, size=fs(0.03))
    draw.text((rpanel_x - pad_x, int(pad_y * 1.2)), "PayTrack", font=f_logo,
              fill=_blend(accent, 0.85, BG_CARD))

    # Divider
    draw.line([(pad_x, divider_y), (rpanel_x - pad_x, divider_y)],
              fill=SLATE600, width=1)

    # Balance section
    bal_y = divider_y + int(H * 0.055)
    f_lbl = _font(bold=True, size=fs(0.024))
    draw.text((pad_x, bal_y), bal_lbl, font=f_lbl, fill=SLATE400)
    amt_y = bal_y + int(H * 0.045)
    f_sym = _font(size=fs(0.065))
    f_amt = _font(bold=True, size=fs(0.12), mono=True)
    sym_bb = draw.textbbox((0, 0), "৳", font=f_sym)
    sym_w  = sym_bb[2] - sym_bb[0]; sym_h = sym_bb[3] - sym_bb[1]
    amt_bb = draw.textbbox((0, 0), abs_str, font=f_amt)
    amt_h  = amt_bb[3] - amt_bb[1]
    base_y = amt_y + max(sym_h, amt_h)
    draw.text((pad_x, base_y - sym_h), "৳",
              font=f_sym, fill=_blend(accent, 0.75, BG_CARD))
    draw.text((pad_x + sym_w + int(W * 0.02), base_y - amt_h), abs_str,
              font=f_amt, fill=accent)

    # Status pill
    pill_h = int(H * 0.07)
    pill_y = H - pad_y - pill_h - int(H * 0.015)
    f_stat = _font(bold=True, size=fs(0.028))
    stbb   = draw.textbbox((0, 0), status, font=f_stat)
    st_w   = stbb[2] - stbb[0]
    pp = 16; dot_d = 8; sp = 10
    pill_w = pp + dot_d + sp + st_w + pp
    draw.rounded_rectangle([pad_x, pill_y, pad_x + pill_w, pill_y + pill_h],
                           radius=pill_h // 2,
                           fill=_blend(accent, 0.10, BG_CARD))
    draw.rounded_rectangle([pad_x, pill_y, pad_x + pill_w, pill_y + pill_h],
                           radius=pill_h // 2,
                           outline=_blend(accent, 0.20, BG_CARD), width=1)
    dcx = pad_x + pp + dot_d // 2
    dcy = pill_y + pill_h // 2
    dr  = dot_d // 2
    draw.ellipse([dcx - dr, dcy - dr, dcx + dr, dcy + dr], fill=accent)
    draw.text((dcx + dr + sp,
               pill_y + (pill_h - (stbb[3] - stbb[1])) // 2),
              status, font=f_stat, fill=accent)

    # Date
    f_date = _font(size=fs(0.026))
    dbb    = draw.textbbox((0, 0), date, font=f_date)
    draw.text((W - pad_x - (dbb[2] - dbb[0]),
               pill_y + (pill_h - (dbb[3] - dbb[1])) // 2),
              date, font=f_date, fill=SLATE400)

    # Right summary panel
    draw.rounded_rectangle(
        [rpanel_x, pad_y, rpanel_x + rpanel_w, H - pad_y],
        radius=24, fill=SLATE700
    )
    f_ph = _font(bold=True, size=fs(0.028))
    draw.text((rpanel_x + int(rpanel_w * 0.1), pad_y + int((H - 2 * pad_y) * 0.06)),
              "SUMMARY", font=f_ph, fill=_blend(SLATE50, 0.7, SLATE700))

    # Stat rows
    stats = [("DATE", date), ("TYPE", "Credit" if is_credit else "Debit"),
             ("AMOUNT", f"৳{abs_str}")]
    panel_h   = H - 2 * pad_y
    row_start = pad_y + int(panel_h * 0.45)
    row_h_    = int(panel_h * 0.16)
    f_sl = _font(bold=True, size=fs(0.022))
    f_sv = _font(bold=True, size=fs(0.032))
    chart_pad = int(rpanel_w * 0.12)
    for i, (lbl, val) in enumerate(stats):
        ry = row_start + i * row_h_
        if i > 0:
            draw.line([(rpanel_x + chart_pad, ry - int(row_h_ * 0.3)),
                       (rpanel_x + rpanel_w - chart_pad, ry - int(row_h_ * 0.3))],
                      fill=_blend(SLATE600, 0.3, SLATE700), width=1)
        draw.text((rpanel_x + chart_pad, ry), lbl, font=f_sl, fill=SLATE400)
        vcol = accent if lbl == "AMOUNT" else SLATE50
        draw.text((rpanel_x + chart_pad, ry + int(row_h_ * 0.38)), val,
                  font=f_sv, fill=_blend(vcol, 0.9, SLATE700))

    buf = _io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# 6. TELEGRAM SERVICE  (async, inline)
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
    import html as _html
    uid  = _html.escape(user_id)
    name = _html.escape(display_name)
    dt   = _html.escape(date)
    sign = "CREDIT" if pending < 0 else "DEBIT"
    amt  = f"{abs(pending):,.2f}"
    return (
        "🧾 <b>Payment Receipt</b>\n\n"
        f"👤 <b>Name:</b> {name}\n"
        f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
        f"📅 <b>Date:</b> {dt}\n"
        f"💰 <b>{sign}:</b> ৳{amt}\n\n"
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
        data.add_field("chat_id", user_id)
        data.add_field("caption", caption)
        data.add_field("parse_mode", "HTML")
        data.add_field("photo", photo_bytes,
                       filename="payment.png", content_type="image/png")
        async with self._session.post(f"{TELEGRAM_API}/sendPhoto", data=data) as resp:
            return resp.status, await resp.text()

    async def send_many(self, users: Sequence[tuple]) -> BatchReport:
        """users: list of (user_id, display_name, photo_bytes, pending, date)"""
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
# 7. MAIN ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    # ── Load config ───────────────────────────────────────────────────────────
    log.info("Loading config...")
    cfg_data = load_config("Config/Config.json")

    # ── Build user summaries ──────────────────────────────────────────────────
    log.info("Building user summaries from CSVs...")
    users = build_user_summaries(cfg_data, csv_dir="CSV")

    if not users:
        log.warning("No users found — nothing to send.")
        return

    date = datetime.today().strftime("%Y-%m-%d")
    log.info("Processing %d users | date=%s", len(users), date)
    print()

    # ── Generate cards ────────────────────────────────────────────────────────
    card_cache: dict[str, tuple] = {}   # user_id → (display_name, bytes, pending)
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

    # ── Send all via Telegram ─────────────────────────────────────────────────
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

    # ── Print report ──────────────────────────────────────────────────────────
    print()
    print("=" * 55)
    print(f"  SEND REPORT  —  {date}")
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
