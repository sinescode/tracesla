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
    Fonts in repo: Fonts/LobsterTwo-Regular.ttf
                   Fonts/LobsterTwo-Bold.ttf
                   Fonts/Monoton-Regular.ttf
"""

from __future__ import annotations

import asyncio
import csv
import glob
import io
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Sequence, Tuple

from dotenv import load_dotenv
from PIL import Image, ImageDraw, ImageFont

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
# 3. CSV LOADING
# ─────────────────────────────────────────────────────────────────────────────

def load_csv_entries(csv_dir: str = "CSV") -> list[dict]:
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
# 4. BUILD USER SUMMARIES
# ─────────────────────────────────────────────────────────────────────────────

def build_user_summaries(cfg_data: dict, csv_dir: str = "CSV") -> list[dict]:
    tier_defs     = get_tier_definitions(cfg_data)
    tier_index    = build_tier_index(tier_defs)
    user_tier_map = get_user_tiers(cfg_data)
    custom_names  = get_custom_names(cfg_data)
    balances      = get_balances(cfg_data)
    entries       = load_csv_entries(csv_dir)

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
    return summaries


# ─────────────────────────────────────────────────────────────────────────────
# 5. CARD GENERATOR  — exact original, Lobster Two fonts only
# ─────────────────────────────────────────────────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))

def _find_font(bold: bool = False, mono: bool = False) -> Optional[str]:
    """
    Resolve font path — Lobster Two family first, then FreeFonts as fallback.
    Mono flag still falls back to FreeMono since Lobster Two has no mono variant.
    """
    fonts_dir = os.path.join(_HERE, "Fonts")
    free_dir  = "/usr/share/fonts/truetype/freefont/"

    if mono and bold:
        candidates = [
            os.path.join(fonts_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(free_dir,  "FreeMonoBold.ttf"),
        ]
    elif mono:
        candidates = [
            os.path.join(fonts_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(free_dir,  "FreeMono.ttf"),
        ]
    elif bold:
        candidates = [
            os.path.join(fonts_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(fonts_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(free_dir,  "FreeSansBold.ttf"),
        ]
    else:
        candidates = [
            os.path.join(fonts_dir, "LobsterTwo-Regular.ttf"),
            os.path.join(free_dir,  "FreeSans.ttf"),
        ]

    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def _font(bold: bool = False, size: int = 14, mono: bool = False) -> ImageFont.FreeTypeFont:
    path = _find_font(bold, mono)
    if path:
        return ImageFont.truetype(path, size)
    return ImageFont.load_default()


# ── CardConfig ────────────────────────────────────────────────────────────────

@dataclass
class CardConfig:
    width: int = 960
    height: int = 540

    padding_h: float = 0.06
    padding_v: float = 0.06
    avatar_size: float = 0.13
    divider_y: float = 0.38
    right_panel_width: float = 0.28

    bg_dark: Tuple[int, int, int] = (15, 23, 42)
    bg_card: Tuple[int, int, int] = (30, 41, 59)
    accent_credit: Tuple[int, int, int] = (16, 185, 129)
    accent_debit: Tuple[int, int, int] = (239, 68, 68)
    surface_light: Tuple[int, int, int] = (51, 65, 85)
    surface_dark: Tuple[int, int, int] = (30, 41, 59)
    text_primary: Tuple[int, int, int] = (248, 250, 252)
    text_muted: Tuple[int, int, int] = (148, 163, 184)
    divider_color: Tuple[int, int, int] = (71, 85, 105)

    font_initial: float = 0.08
    font_name: float = 0.055
    font_id: float = 0.028
    font_logo: float = 0.03
    font_balance_label: float = 0.024
    font_balance_symbol: float = 0.065
    font_balance_amount: float = 0.12
    font_status: float = 0.028
    font_date: float = 0.026
    font_stat_label: float = 0.022
    font_stat_value: float = 0.032


def _alpha_blend(color, alpha: float, bg):
    return tuple(int(c * alpha + b * (1 - alpha)) for c, b in zip(color, bg))


def _lerp(a, b, t):
    return int(a * (1 - t) + b * t)


# ── generate_card ─────────────────────────────────────────────────────────────

def generate_card(user_id: str, display_name: str, pending: float, date: str,
                  config: Optional[CardConfig] = None) -> bytes:
    if config is None:
        config = CardConfig()

    W, H = config.width, config.height
    is_credit = pending < 0
    abs_pend = abs(pending)
    abs_str = f"{abs_pend:,.2f}"
    initial = (display_name[0] if display_name else user_id[0]).upper()
    accent = config.accent_credit if is_credit else config.accent_debit
    status_lbl = "CREDIT" if is_credit else "DEBIT"
    bal_label = "CREDIT BALANCE" if is_credit else "PENDING BALANCE"

    pad_x = int(W * config.padding_h)
    pad_y = int(H * config.padding_v)
    avatar_size = int(W * config.avatar_size)
    avatar_radius = avatar_size // 5
    name_x = pad_x + avatar_size + int(pad_x * 1.0)
    divider_y = int(H * config.divider_y)
    right_panel_x = W - int(W * config.right_panel_width) - pad_x
    right_panel_w = int(W * config.right_panel_width)

    def fs(factor: float) -> int:
        return int(H * factor)

    img = Image.new("RGB", (W, H), config.bg_card)
    draw = ImageDraw.Draw(img)

    # 1. Gradient Background
    for y in range(H):
        t = y / H
        t_smooth = t * t * (3 - 2 * t)
        r = _lerp(config.bg_dark[0], config.bg_card[0], t_smooth)
        g = _lerp(config.bg_dark[1], config.bg_card[1], t_smooth)
        b = _lerp(config.bg_dark[2], config.bg_card[2], t_smooth)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # 2. Glow Orb
    orb = Image.new("RGB", (W, H), (0, 0, 0))
    od = ImageDraw.Draw(orb)
    cx, cy, rad = int(W * 0.15), int(H * 0.22), int(W * 0.20)
    for r in range(rad, 0, -3):
        alpha_val = 0.12 * (1 - r / rad) ** 2
        col = _alpha_blend(accent, alpha_val, (0, 0, 0))
        od.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)
    img = Image.blend(img, orb, alpha=0.6)
    draw = ImageDraw.Draw(img)

    # 3. Grid Pattern
    grid_color = _alpha_blend(config.divider_color, 0.15, config.bg_card)
    grid_spacing_x = int(W * 0.06)
    grid_spacing_y = int(H * 0.12)
    for gx in range(pad_x, W, grid_spacing_x):
        draw.line([(gx, 0), (gx, H)], fill=grid_color, width=1)
    for gy in range(pad_y, H, grid_spacing_y):
        draw.line([(0, gy), (W, gy)], fill=grid_color, width=1)

    # 4. Card Border
    border_alpha = 0.18
    draw.rounded_rectangle(
        [pad_x//2, pad_y//2, W - pad_x//2, H - pad_y//2],
        radius=28,
        outline=_alpha_blend(accent, border_alpha, config.bg_card),
        width=1
    )

    # 5. Top Accent Stripe
    stripe_height = 3
    for x in range(int(W * 0.60)):
        t = x / (W * 0.60)
        alpha_stripe = 1.0 - t * 0.7
        col = _alpha_blend(accent, alpha_stripe * 0.8, config.bg_card)
        draw.line([(x, 0), (x, stripe_height)], fill=col)

    # 6. Avatar
    av_x, av_y = pad_x, int(pad_y * 1.2)
    for g in range(8, 0, -1):
        glow_alpha = 0.12 * (1 - g / 8)
        draw.rounded_rectangle(
            [av_x - 3 - g, av_y - 3 - g,
             av_x + avatar_size + 3 + g, av_y + avatar_size + 3 + g],
            radius=avatar_radius + 3 + g,
            outline=_alpha_blend(accent, glow_alpha, config.bg_card),
            width=1
        )
    draw.rounded_rectangle(
        [av_x, av_y, av_x + avatar_size, av_y + avatar_size],
        radius=avatar_radius,
        fill=_alpha_blend(accent, 0.55, config.bg_card)
    )
    draw.rounded_rectangle(
        [av_x, av_y, av_x + avatar_size, av_y + avatar_size],
        radius=avatar_radius,
        outline=_alpha_blend(accent, 0.3, config.bg_card),
        width=1
    )
    f_init = _font(bold=True, size=fs(config.font_initial))
    bbox = draw.textbbox((0, 0), initial, font=f_init)
    text_w, text_h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    text_x = av_x + avatar_size // 2 - text_w // 2
    text_y = av_y + avatar_size // 2 - text_h // 2 - 2
    draw.text((text_x, text_y), initial, font=f_init, fill=config.text_primary)

    # 7. Name and ID
    f_name = _font(bold=True, size=fs(config.font_name))
    name_y = av_y + int(H * 0.01)
    draw.text((name_x, name_y), display_name, font=f_name, fill=config.text_primary)
    f_id = _font(size=fs(config.font_id))
    id_y = name_y + fs(config.font_name) + int(H * 0.015)
    draw.text((name_x, id_y), user_id.upper(), font=f_id, fill=config.text_muted)

    # 8. Logo
    f_logo = _font(bold=True, size=fs(config.font_logo))
    logo_text = "PAYTRACK"
    bbox = draw.textbbox((0, 0), logo_text, font=f_logo)
    logo_w = bbox[2] - bbox[0]
    logo_y = av_y + int(H * 0.04)
    draw.text((W - pad_x - logo_w, logo_y), logo_text, font=f_logo,
              fill=_alpha_blend(accent, 0.65, config.bg_card))

    # 9. Divider Line
    for x in range(pad_x, W - pad_x):
        t = (x - pad_x) / (W - 2 * pad_x)
        div_alpha = 0.4 + 0.3 * (1 - abs(t - 0.5) * 2)
        div_col = _alpha_blend(config.divider_color, div_alpha, config.bg_card)
        draw.point((x, divider_y), fill=div_col)

    # 10. Balance Section
    bal_label_y = divider_y + int(H * 0.055)
    f_label = _font(bold=True, size=fs(config.font_balance_label))
    draw.text((pad_x, bal_label_y), bal_label, font=f_label, fill=config.text_muted)

    bal_amt_y = bal_label_y + int(H * 0.045)
    f_symbol = _font(size=fs(config.font_balance_symbol))
    symbol_bbox = draw.textbbox((0, 0), "৳", font=f_symbol)
    symbol_h = symbol_bbox[3] - symbol_bbox[1]
    symbol_w = symbol_bbox[2] - symbol_bbox[0]
    f_amount = _font(bold=True, size=fs(config.font_balance_amount), mono=True)
    amount_bbox = draw.textbbox((0, 0), abs_str, font=f_amount)
    amount_h = amount_bbox[3] - amount_bbox[1]
    baseline_y = bal_amt_y + max(symbol_h, amount_h)
    sym_x = pad_x
    draw.text((sym_x, baseline_y - symbol_h), "৳",
              font=f_symbol, fill=_alpha_blend(accent, 0.75, config.bg_card))
    amt_x = sym_x + symbol_w + int(W * 0.02)
    draw.text((amt_x, baseline_y - amount_h), abs_str, font=f_amount, fill=accent)

    # 11. Status Pill
    pill_h = int(H * 0.07)
    pill_y = H - pad_y - pill_h - int(H * 0.015)
    f_status = _font(bold=True, size=fs(config.font_status))
    status_bbox = draw.textbbox((0, 0), status_lbl, font=f_status)
    status_w = status_bbox[2] - status_bbox[0]
    pill_inner_pad = 16
    dot_diameter = 8
    spacing = 10
    pill_w = pill_inner_pad + dot_diameter + spacing + status_w + pill_inner_pad
    draw.rounded_rectangle(
        [pad_x, pill_y, pad_x + pill_w, pill_y + pill_h],
        radius=pill_h // 2,
        fill=_alpha_blend(accent, 0.10, config.bg_card)
    )
    draw.rounded_rectangle(
        [pad_x, pill_y, pad_x + pill_w, pill_y + pill_h],
        radius=pill_h // 2,
        outline=_alpha_blend(accent, 0.20, config.bg_card),
        width=1
    )
    dot_cx = pad_x + pill_inner_pad + dot_diameter // 2
    dot_cy = pill_y + pill_h // 2
    dot_r = dot_diameter // 2
    for g in range(4, 0, -1):
        draw.ellipse(
            [dot_cx - dot_r - g, dot_cy - dot_r - g,
             dot_cx + dot_r + g, dot_cy + dot_r + g],
            outline=_alpha_blend(accent, 0.25 * (1 - g / 4), config.bg_card),
            width=1
        )
    draw.ellipse(
        [dot_cx - dot_r, dot_cy - dot_r, dot_cx + dot_r, dot_cy + dot_r],
        fill=accent
    )
    status_text_x = dot_cx + dot_r + spacing
    status_text_y = pill_y + (pill_h - (status_bbox[3] - status_bbox[1])) // 2
    draw.text((status_text_x, status_text_y), status_lbl, font=f_status, fill=accent)

    # 12. Date
    f_date = _font(size=fs(config.font_date))
    date_bbox = draw.textbbox((0, 0), date, font=f_date)
    date_w = date_bbox[2] - date_bbox[0]
    date_h = date_bbox[3] - date_bbox[1]
    date_x = W - pad_x - date_w
    date_y = pill_y + (pill_h - date_h) // 2
    draw.text((date_x, date_y), date, font=f_date, fill=config.text_muted)

    # 13. Right Stats Panel
    panel_y = pad_y
    panel_h = H - 2 * pad_y
    draw.rounded_rectangle(
        [right_panel_x, panel_y, right_panel_x + right_panel_w, panel_y + panel_h],
        radius=24,
        fill=config.surface_light
    )
    draw.rounded_rectangle(
        [right_panel_x, panel_y, right_panel_x + right_panel_w, panel_y + panel_h],
        radius=24,
        outline=_alpha_blend(accent, 0.08, config.bg_card),
        width=1
    )
    header_y = panel_y + int(panel_h * 0.06)
    f_header = _font(bold=True, size=fs(0.028))
    draw.text((right_panel_x + int(right_panel_w * 0.1), header_y), "SUMMARY",
              font=f_header, fill=_alpha_blend(config.text_primary, 0.7, config.surface_light))

    bars = [0.35, 0.55, 0.45, 0.75, 0.60, 0.85, 0.70, 0.95]
    chart_pad = int(right_panel_w * 0.12)
    chart_w = right_panel_w - 2 * chart_pad
    bar_gap = 4
    bar_w = (chart_w - (len(bars) - 1) * bar_gap) / len(bars)
    chart_top = header_y + int(panel_h * 0.07)
    chart_bottom = panel_y + int(panel_h * 0.34)
    chart_h = chart_bottom - chart_top
    for i, bv in enumerate(bars):
        bx = right_panel_x + chart_pad + i * (bar_w + bar_gap)
        bh = bv * chart_h * 0.85
        by = chart_bottom - bh
        if i == len(bars) - 1:
            color = accent
            for g in range(3, 0, -1):
                draw.rounded_rectangle(
                    [bx - g, by - g, bx + bar_w + g, by + bh + g],
                    radius=3,
                    outline=_alpha_blend(accent, 0.2 * (1 - g / 3), config.surface_light),
                    width=1
                )
        else:
            color = _alpha_blend(accent, 0.30, config.surface_light)
        draw.rounded_rectangle([bx, by, bx + bar_w, by + bh], radius=3, fill=color)

    stat_start_y = chart_bottom + int(panel_h * 0.09)
    row_h = int(panel_h * 0.11)
    f_stat_label = _font(bold=True, size=fs(config.font_stat_label))
    f_stat_value = _font(bold=True, size=fs(config.font_stat_value))
    stat_data = [
        ("DATE",   date),
        ("TYPE",   "Credit" if is_credit else "Debit"),
        ("AMOUNT", f"৳{abs_str}"),
    ]
    for idx, (label, value) in enumerate(stat_data):
        row_y = stat_start_y + idx * row_h
        if idx > 0:
            sep_y = row_y - int(row_h * 0.3)
            draw.line(
                [(right_panel_x + chart_pad, sep_y),
                 (right_panel_x + right_panel_w - chart_pad, sep_y)],
                fill=_alpha_blend(config.divider_color, 0.3, config.surface_light),
                width=1
            )
        draw.text((right_panel_x + chart_pad, row_y), label,
                  font=f_stat_label,
                  fill=_alpha_blend(config.text_muted, 0.8, config.surface_light))
        val_y = row_y + int(row_h * 0.38)
        value_color = accent if label == "AMOUNT" else config.text_primary
        draw.text((right_panel_x + chart_pad, val_y), value,
                  font=f_stat_value,
                  fill=_alpha_blend(value_color, 0.9, config.surface_light))

    # 14. Save to bytes
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# 6. TELEGRAM SERVICE
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
        
        f"🆔 <b>User ID:</b> <code>{uid}</code>\n"
        f"📅 <b>Date:</b> {dt}\n\n"

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
# 7. MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    log.info("Loading config...")
    cfg_data = load_config("Config/Config.json")

    log.info("Building user summaries from CSVs...")
    users = build_user_summaries(cfg_data, csv_dir="CSV")

    if not users:
        log.warning("No users found — nothing to send.")
        return

    date = datetime.today().strftime("%Y-%m-%d")
    log.info("Processing %d users | date=%s", len(users), date)
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
