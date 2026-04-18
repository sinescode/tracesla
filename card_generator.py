"""
card_generator.py — PayTrack Pillow card generator
Uses Lobster Two Regular exclusively.
Font expected at: Fonts/LobsterTwo-Regular.ttf
"""

import io
import os
from dataclasses import dataclass
from typing import Optional, Tuple
from PIL import Image, ImageDraw, ImageFont

# ─────────────────────────────────────────────────────────────────────────────
# Font resolution — Lobster Two Regular only
# ─────────────────────────────────────────────────────────────────────────────

_HERE = os.path.dirname(os.path.abspath(__file__))
_FONT_PATH = os.path.join(_HERE, "Fonts", "LobsterTwo-Regular.ttf")


def _font(bold: bool = False, size: int = 14, mono: bool = False) -> ImageFont.FreeTypeFont:
    """Always returns Lobster Two Regular at the requested size."""
    if os.path.isfile(_FONT_PATH):
        return ImageFont.truetype(_FONT_PATH, size)
    return ImageFont.load_default()


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# Main generation
# ─────────────────────────────────────────────────────────────────────────────

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

    # ================================================================
    # 1. Gradient Background
    # ================================================================
    for y in range(H):
        t = y / H
        t_smooth = t * t * (3 - 2 * t)
        r = _lerp(config.bg_dark[0], config.bg_card[0], t_smooth)
        g = _lerp(config.bg_dark[1], config.bg_card[1], t_smooth)
        b = _lerp(config.bg_dark[2], config.bg_card[2], t_smooth)
        draw.line([(0, y), (W, y)], fill=(r, g, b))

    # ================================================================
    # 2. Glow Orb
    # ================================================================
    orb = Image.new("RGB", (W, H), (0, 0, 0))
    od = ImageDraw.Draw(orb)
    cx, cy, rad = int(W * 0.15), int(H * 0.22), int(W * 0.20)
    for r in range(rad, 0, -3):
        alpha_val = 0.12 * (1 - r / rad) ** 2
        col = _alpha_blend(accent, alpha_val, (0, 0, 0))
        od.ellipse([cx - r, cy - r, cx + r, cy + r], fill=col)
    img = Image.blend(img, orb, alpha=0.6)
    draw = ImageDraw.Draw(img)

    # ================================================================
    # 3. Grid Pattern
    # ================================================================
    grid_color = _alpha_blend(config.divider_color, 0.15, config.bg_card)
    grid_spacing_x = int(W * 0.06)
    grid_spacing_y = int(H * 0.12)
    for gx in range(pad_x, W, grid_spacing_x):
        draw.line([(gx, 0), (gx, H)], fill=grid_color, width=1)
    for gy in range(pad_y, H, grid_spacing_y):
        draw.line([(0, gy), (W, gy)], fill=grid_color, width=1)

    # ================================================================
    # 4. Card Border
    # ================================================================
    border_alpha = 0.18
    draw.rounded_rectangle(
        [pad_x//2, pad_y//2, W - pad_x//2, H - pad_y//2],
        radius=28,
        outline=_alpha_blend(accent, border_alpha, config.bg_card),
        width=1
    )

    # ================================================================
    # 5. Top Accent Stripe
    # ================================================================
    stripe_height = 3
    for x in range(int(W * 0.60)):
        t = x / (W * 0.60)
        alpha_stripe = 1.0 - t * 0.7
        col = _alpha_blend(accent, alpha_stripe * 0.8, config.bg_card)
        draw.line([(x, 0), (x, stripe_height)], fill=col)

    # ================================================================
    # 6. Avatar
    # ================================================================
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

    # ================================================================
    # 7. Name and ID
    # ================================================================
    f_name = _font(bold=True, size=fs(config.font_name))
    name_y = av_y + int(H * 0.01)
    draw.text((name_x, name_y), display_name, font=f_name, fill=config.text_primary)
    f_id = _font(size=fs(config.font_id))
    id_y = name_y + fs(config.font_name) + int(H * 0.015)
    draw.text((name_x, id_y), user_id.upper(), font=f_id, fill=config.text_muted)

    # ================================================================
    # 8. Logo
    # ================================================================
    f_logo = _font(bold=True, size=fs(config.font_logo))
    logo_text = "PAYTRACK"
    bbox = draw.textbbox((0, 0), logo_text, font=f_logo)
    logo_w = bbox[2] - bbox[0]
    logo_y = av_y + int(H * 0.04)
    draw.text((W - pad_x - logo_w, logo_y), logo_text, font=f_logo,
              fill=_alpha_blend(accent, 0.65, config.bg_card))

    # ================================================================
    # 9. Divider Line
    # ================================================================
    for x in range(pad_x, W - pad_x):
        t = (x - pad_x) / (W - 2 * pad_x)
        div_alpha = 0.4 + 0.3 * (1 - abs(t - 0.5) * 2)
        div_col = _alpha_blend(config.divider_color, div_alpha, config.bg_card)
        draw.point((x, divider_y), fill=div_col)

    # ================================================================
    # 10. Balance Section
    # ================================================================
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

    # ================================================================
    # 11. Status Pill
    # ================================================================
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

    # ================================================================
    # 12. Date
    # ================================================================
    f_date = _font(size=fs(config.font_date))
    date_bbox = draw.textbbox((0, 0), date, font=f_date)
    date_w = date_bbox[2] - date_bbox[0]
    date_h = date_bbox[3] - date_bbox[1]
    date_x = W - pad_x - date_w
    date_y = pill_y + (pill_h - date_h) // 2
    draw.text((date_x, date_y), date, font=f_date, fill=config.text_muted)

    # ================================================================
    # 13. Right Stats Panel
    # ================================================================
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

    # ================================================================
    # 14. Save to bytes
    # ================================================================
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
