import json
import csv
import os
import sys
import tty
import re
import termios
import shutil
from datetime import date
from glob import glob

# ─── Paths ───────────────────────────────────────────────
CONFIG_PATH  = "Config/Config.json"
CSV_DIR      = "CSV"
ADMIN_RATE   = 4.1
DEFAULT_RATE = 3.5

# ─── ANSI ────────────────────────────────────────────────
ESC = "\033"

def esc(*codes): return ESC + "[" + ";".join(str(c) for c in codes) + "m"
def cur(r, c):   return ESC + f"[{r};{c}H"
def clear():     return ESC + "[2J" + ESC + "[H"
def hide_cur():  return ESC + "[?25l"
def show_cur():  return ESC + "[?25h"

RESET  = esc(0)
BOLD   = esc(1)
DIM    = esc(2)

def fg(r, g, b): return ESC + f"[38;2;{r};{g};{b}m"
def bg(r, g, b): return ESC + f"[48;2;{r};{g};{b}m"

C_BG     = (8,   12,  18)
C_PANEL  = (14,  20,  30)
C_BORDER = (30,  45,  65)
C_HEADER = (15,  25,  40)
C_ACCENT = (56,  189, 248)
C_GREEN  = (34,  197, 94)
C_RED    = (239, 68,  68)
C_YELLOW = (251, 191, 36)
C_PURPLE = (167, 139, 250)
C_TEXT   = (226, 232, 240)
C_DIM    = (71,  85,  105)
C_WHITE  = (248, 250, 252)
C_TITLE  = (99,  209, 255)

def F(*c): return fg(*c)
def B(*c): return bg(*c)

H  = "\u2500"; V  = "\u2502"
TL = "\u256d"; TR = "\u256e"; BL = "\u2570"; BR = "\u256f"

# ─── Helpers ─────────────────────────────────────────────
def term_size():
    return shutil.get_terminal_size((120, 40))

def write(s): sys.stdout.write(s)
def flush():  sys.stdout.flush()

def strip_ansi(s):
    return re.sub(r'\x1b\[[^m]*m', '', s)

def pad(s, width, align="left"):
    s = str(s)
    plain = strip_ansi(s)
    pad_n = max(0, width - len(plain))
    if align == "right":  return " " * pad_n + s
    if align == "center":
        l = pad_n // 2
        return " " * l + s + " " * (pad_n - l)
    return s + " " * pad_n

def draw_box(r, c, h, w, title="", bc=C_BORDER, tc=C_ACCENT):
    b = F(*bc); t = F(*tc) + BOLD
    write(cur(r, c) + b + TL + H * (w - 2) + TR + RESET)
    for i in range(1, h - 1):
        write(cur(r+i, c) + b + V + RESET)
        write(cur(r+i, c+w-1) + b + V + RESET)
    write(cur(r+h-1, c) + b + BL + H * (w - 2) + BR + RESET)
    if title:
        s = f" {title} "
        col = c + (w - len(s)) // 2
        write(cur(r, col) + t + s + RESET)

def fill_box(r, c, h, w):
    panel = B(*C_PANEL)
    for i in range(1, h - 1):
        write(cur(r+i, c+1) + panel + " " * (w-2) + RESET)

# ─── Keyboard ────────────────────────────────────────────
def getch():
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
        if ch == '\x1b':
            ch2 = sys.stdin.read(1)
            if ch2 == '[':
                ch3 = sys.stdin.read(1)
                return '\x1b[' + ch3
            return '\x1b'
        return ch
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)

# ─── Data ────────────────────────────────────────────────
def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    cfg      = data.get("config", data)
    balances = data.get("balances", {})
    return cfg, balances

def build_tier_map(config):
    return {t["id"]: t for t in config.get("tier_definitions", [])}

def get_admin_rates(config):
    rates = set()
    for t in config.get("tier_definitions", []):
        if "admin" in t.get("name", "").lower():
            rates.add(t["price_per_ok"])
    return rates

def resolve_rate(uid, ok_count, config, tier_map, admin_rates):
    tier_ids = config.get("user_tiers", {}).get(str(uid), [])
    for tid in tier_ids:
        t = tier_map.get(tid)
        if t and t["min_ok"] <= ok_count <= t["max_ok"]:
            return t["price_per_ok"], t["price_per_ok"] in admin_rates
    return DEFAULT_RATE, False

def list_csv_files():
    files = sorted(glob(os.path.join(CSV_DIR, "*.csv")), reverse=True)
    return [(os.path.splitext(os.path.basename(f))[0], f) for f in files]

def load_report_from(csv_path, config, tier_map, admin_rates, names):
    rows, skipped = [], []
    if not csv_path or not os.path.exists(csv_path):
        return rows, skipped
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uid = row["User ID"].strip()
            ok  = int(row["OK Count"].strip())
            if ok <= 0:
                continue
            rate, is_admin = resolve_rate(uid, ok, config, tier_map, admin_rates)
            name = names.get(uid, row["Username"].strip())
            if is_admin:
                skipped.append({"name": name, "uid": uid, "ok": ok})
                continue
            earning = ADMIN_RATE * ok
            payout  = rate * ok
            rows.append({
                "name": name, "uid": uid, "ok": ok, "rate": rate,
                "earning": earning, "payout": payout, "profit": earning - payout,
            })
    return rows, skipped

# ─── TUI ─────────────────────────────────────────────────
TABS = ["REPORT", "TIERS", "USERS", "BALANCES"]

class TUI:
    def __init__(self):
        self.tab     = 0
        self.scroll  = 0
        self.running = True

        self.show_picker  = False
        self.picker_sel   = 0
        self.picker_scroll = 0

        self.config, self.balances = load_config()
        self.tier_map    = build_tier_map(self.config)
        self.admin_rates = get_admin_rates(self.config)
        self.names       = self.config.get("custom_names", {})

        self.csv_files = list_csv_files()

        today = date.today().strftime("%Y-%m-%d")
        self.active_idx = next(
            (i for i, (lbl, _) in enumerate(self.csv_files) if lbl == today), 0
        )
        self._reload()

    def _reload(self):
        if not self.csv_files:
            self.rows, self.skipped, self.date_label = [], [], "N/A"
            return
        label, path = self.csv_files[self.active_idx]
        self.date_label = label
        self.rows, self.skipped = load_report_from(
            path, self.config, self.tier_map, self.admin_rates, self.names)
        self.scroll = 0

    # ── Loop ─────────────────────────────────────────────
    def run(self):
        write(hide_cur() + clear())
        try:
            while self.running:
                cols, rows = term_size()
                self.render(rows, cols)
                flush()
                self.handle_key()
        finally:
            write(show_cur() + clear())
            flush()

    def handle_key(self):
        k = getch()

        if self.show_picker:
            if k in ('q', '\x1b', 'd', 'D'):
                self.show_picker = False
            elif k in ('\x1b[B', 'j'):
                self.picker_sel = min(self.picker_sel + 1, len(self.csv_files) - 1)
            elif k in ('\x1b[A', 'k'):
                self.picker_sel = max(self.picker_sel - 1, 0)
            elif k in ('\r', '\n', ' '):
                self.active_idx = self.picker_sel
                self._reload()
                self.show_picker = False
            return

        if k in ('q', 'Q', '\x03'):
            self.running = False
        elif k in ('d', 'D'):
            self.show_picker = True
            self.picker_sel  = self.active_idx
            self.picker_scroll = 0
        elif k in ('\x1b[C', 'l', '\t'):
            self.tab = (self.tab + 1) % len(TABS)
            self.scroll = 0
        elif k in ('\x1b[D', 'h'):
            self.tab = (self.tab - 1) % len(TABS)
            self.scroll = 0
        elif k in ('\x1b[B', 'j'):
            self.scroll += 1
        elif k in ('\x1b[A', 'k'):
            self.scroll = max(0, self.scroll - 1)
        elif k in ('[', ','):
            self.active_idx = min(self.active_idx + 1, len(self.csv_files) - 1)
            self._reload()
        elif k in (']', '.'):
            self.active_idx = max(self.active_idx - 1, 0)
            self._reload()

    # ── Render ───────────────────────────────────────────
    def render(self, rows, cols):
        write(clear() + B(*C_BG))
        self.draw_topbar(cols)
        self.draw_tabs(cols)
        ct = 5
        ch = rows - ct - 2
        if   self.tab == 0: self.render_report(ct, ch, cols)
        elif self.tab == 1: self.render_tiers(ct, ch, cols)
        elif self.tab == 2: self.render_users(ct, ch, cols)
        elif self.tab == 3: self.render_balances(ct, ch, cols)
        self.draw_statusbar(rows, cols)
        if self.show_picker:
            self.render_picker(rows, cols)

    def draw_topbar(self, cols):
        write(cur(1, 1) + B(*C_HEADER) + " " * cols)
        write(cur(1, 3) + F(*C_TITLE) + BOLD + "\u258c  PayTrack  \u2590  Profit Dashboard" + RESET)

        n = len(self.csv_files)
        idx = self.active_idx
        nav = ""
        if n > 1:
            nav = (F(*C_DIM) + "[ " if idx < n-1 else "  ") + \
                  F(*C_YELLOW) + BOLD + f" {self.date_label} " + RESET + \
                  (F(*C_DIM) + " ]" if idx > 0 else "  ")
        else:
            nav = F(*C_YELLOW) + BOLD + f" {self.date_label} " + RESET

        nav_plain = f"  {self.date_label}  "
        col = cols - len(nav_plain) - 4
        write(cur(1, col) + nav + RESET)
        write(cur(2, 1) + F(*C_BORDER) + H * cols + RESET)

    def draw_tabs(self, cols):
        write(cur(3, 1) + B(*C_BG) + " " * cols)
        x = 3
        for i, name in enumerate(TABS):
            label = f"  {name}  "
            if i == self.tab:
                style = B(*C_ACCENT) + F(*C_BG) + BOLD
            else:
                style = B(*C_HEADER) + F(*C_DIM)
            write(cur(3, x) + style + label + RESET)
            x += len(label) + 1
        write(cur(4, 1) + F(*C_BORDER) + H * cols + RESET)

    def draw_statusbar(self, rows, cols):
        write(cur(rows, 1) + B(*C_HEADER) + " " * cols)
        s = (F(*C_ACCENT) + "d" + F(*C_DIM) + ":date picker  " +
             F(*C_ACCENT) + "[ ]" + F(*C_DIM) + ":prev/next date  " +
             F(*C_ACCENT) + "\u2190 \u2192" + F(*C_DIM) + ":tab  " +
             F(*C_ACCENT) + "\u2191 \u2193" + F(*C_DIM) + ":scroll  " +
             F(*C_ACCENT) + "q" + F(*C_DIM) + ":quit")
        write(cur(rows, 3) + s + RESET)

    # ── Date picker overlay ───────────────────────────────
    def render_picker(self, term_rows, cols):
        pw     = 38
        ph     = min(len(self.csv_files) + 6, term_rows - 6, 24)
        pr     = (term_rows - ph) // 2
        pc     = (cols - pw) // 2
        inner  = ph - 4

        # shadow
        for i in range(ph + 2):
            write(cur(pr-1+i, pc-2) + B(*C_BG) + " " * (pw+4) + RESET)

        draw_box(pr, pc, ph, pw, " \U0001f4c5 Select Date ", bc=C_ACCENT, tc=(8,12,18))
        fill_box(pr, pc, ph, pw)

        total = len(self.csv_files)

        # clamp scroll
        if self.picker_sel < self.picker_scroll:
            self.picker_scroll = self.picker_sel
        if self.picker_sel >= self.picker_scroll + inner:
            self.picker_scroll = self.picker_sel - inner + 1

        # sub-header
        write(cur(pr+1, pc+2) +
              F(*C_DIM) + f"{total} file{'s' if total != 1 else ''}  " +
              F(*C_ACCENT) + "Enter" + F(*C_DIM) + ":load  " +
              F(*C_ACCENT) + "Esc/q" + F(*C_DIM) + ":close" + RESET)
        write(cur(pr+2, pc+1) + F(*C_BORDER) + H*(pw-2) + RESET)

        today = date.today().strftime("%Y-%m-%d")
        y = pr + 3
        for i in range(self.picker_scroll, min(self.picker_scroll + inner, total)):
            label, _ = self.csv_files[i]
            is_sel    = (i == self.picker_sel)
            is_active = (i == self.active_idx)
            is_today  = (label == today)

            if is_sel:
                row_bg = B(*C_ACCENT)
                row_fg = F(*C_BG) + BOLD
                arrow  = " \u25b6 "
            elif is_active:
                row_bg = B(18, 32, 52)
                row_fg = F(*C_ACCENT)
                arrow  = " \u2713 "
            else:
                row_bg = ""
                row_fg = F(*C_TEXT)
                arrow  = "   "

            tag = ""
            if is_today:
                tag_fg = F(*C_BG) if is_sel else F(*C_GREEN)
                tag = tag_fg + " today" + row_fg
            elif is_active and not is_sel:
                tag = F(*C_DIM) + " loaded" + row_fg

            content      = arrow + label + tag
            content_plain = arrow + label + (" today" if is_today else " loaded" if (is_active and not is_sel) else "")
            padding      = " " * max(0, pw - 2 - len(content_plain))

            write(cur(y, pc+1) + row_bg + row_fg + content + padding + RESET)
            y += 1

        # scrollbar
        if total > inner:
            sb_h   = max(1, inner * inner // total)
            sb_top = int(self.picker_scroll / max(1, total - inner) * (inner - sb_h))
            for i in range(inner):
                ch = "\u2588" if sb_top <= i < sb_top + sb_h else "\u2591"
                write(cur(pr+3+i, pc+pw-1) + F(*C_BORDER) + ch + RESET)

        write(cur(pr+ph-1, pc+1) + F(*C_BORDER) + H*(pw-2) + RESET)

    # ── Report tab ───────────────────────────────────────
    def render_report(self, top, h, cols):
        total_ok     = sum(r["ok"]      for r in self.rows)
        total_payout = sum(r["payout"]  for r in self.rows)
        total_profit = sum(r["profit"]  for r in self.rows)
        total_earn   = ADMIN_RATE * total_ok

        stats = [
            ("TOTAL OK",  f"{total_ok:,}",              C_ACCENT),
            ("EARNING",   f"{total_earn:,.1f} \u09f3",   C_PURPLE),
            ("PAYOUT",    f"{total_payout:,.1f} \u09f3",  C_YELLOW),
            ("PROFIT",    f"{total_profit:,.1f} \u09f3",
             C_GREEN if total_profit >= 0 else C_RED),
        ]
        sw = (cols - 2) // len(stats)
        for i, (label, val, color) in enumerate(stats):
            bx = 1 + i * sw
            draw_box(top, bx, 4, sw-1, bc=color)
            fill_box(top, bx, 4, sw-1)
            write(cur(top+1, bx+2) + F(*C_DIM)  + label + RESET)
            write(cur(top+2, bx+2) + F(*color) + BOLD + val + RESET)

        tt = top + 5
        th = h - 6
        draw_box(tt, 1, th, cols, f"  Profit Report \u2014 {self.date_label}  ")
        fill_box(tt, 1, th, cols)

        COL = [18, 7, 7, 12, 12, 12]
        HDR = ["Name", "OK", "Rate", "Earning \u09f3", "Payout \u09f3", "Profit \u09f3"]
        ALN = ["left","right","right","right","right","right"]

        hy = tt + 1; x = 3
        write(cur(hy, x) + F(*C_ACCENT) + BOLD)
        for i, lbl in enumerate(HDR):
            write(pad(lbl, COL[i], ALN[i]) + "  ")
        write(RESET)
        write(cur(hy+1, 2) + F(*C_BORDER) + H*(cols-3) + RESET)

        lines = []
        if self.skipped:
            for s in self.skipped:
                lines.append(("skip", s))
            lines.append(("sep", None))
        for r in self.rows:
            lines.append(("row", r))
        lines.append(("sep", None))
        lines.append(("total", {
            "ok": total_ok, "earning": total_earn,
            "payout": total_payout, "profit": total_profit
        }))

        vis = th - 4
        self.scroll = min(self.scroll, max(0, len(lines) - vis))
        shown = lines[self.scroll: self.scroll + vis]

        y = hy + 2
        for kind, data in shown:
            if kind == "sep":
                write(cur(y, 2) + F(*C_BORDER) + H*(cols-3) + RESET)
                y += 1; continue
            x = 3
            if kind == "skip":
                write(cur(y, x) + F(*C_DIM) + DIM)
                write(pad(f"\u23ed {data['name']}", COL[0]) + "  ")
                write(pad(str(data["ok"]),   COL[1], "right") + "  ")
                write(pad("admin",           COL[2], "right") + "  ")
                write(pad("\u2014",          COL[3], "right") + "  ")
                write(pad("\u2014",          COL[4], "right") + "  ")
                write(pad("skipped",         COL[5], "right") + RESET)
            elif kind == "row":
                pc = C_GREEN if data["profit"] >= 0 else C_RED
                write(cur(y, x) + F(*C_TEXT)   + pad(data["name"],             COL[0])         + "  ")
                write(            F(*C_YELLOW)  + pad(f"{data['ok']:,}",         COL[1],"right") + "  ")
                write(            F(*C_DIM)     + pad(f"{data['rate']:.1f}",     COL[2],"right") + "  ")
                write(            F(*C_PURPLE)  + pad(f"{data['earning']:,.1f}", COL[3],"right") + "  ")
                write(            F(*C_ACCENT)  + pad(f"{data['payout']:,.1f}",  COL[4],"right") + "  ")
                write(            F(*pc)+BOLD   + pad(f"{data['profit']:,.1f}",  COL[5],"right") + RESET)
            elif kind == "total":
                pc = C_GREEN if data["profit"] >= 0 else C_RED
                write(cur(y, x) + F(*C_WHITE) + BOLD)
                write(pad("TOTAL",                          COL[0])         + "  ")
                write(pad(f"{data['ok']:,}",                COL[1],"right") + "  ")
                write(pad("",                               COL[2],"right") + "  ")
                write(F(*C_PURPLE)+pad(f"{data['earning']:,.1f}", COL[3],"right") + "  ")
                write(F(*C_ACCENT)+pad(f"{data['payout']:,.1f}",  COL[4],"right") + "  ")
                write(F(*pc)      +pad(f"{data['profit']:,.1f} \u09f3", COL[5],"right") + RESET)
            y += 1

        total_lines = len(lines)
        if total_lines > vis:
            pct = int(self.scroll / max(1, total_lines - vis) * (th - 6))
            for i in range(th - 4):
                ch = "\u2588" if i == pct else "\u2591"
                write(cur(hy+2+i, cols-1) + F(*C_BORDER) + ch + RESET)

    # ── Tiers tab ────────────────────────────────────────
    def render_tiers(self, top, h, cols):
        tiers = sorted(self.config.get("tier_definitions", []), key=lambda t: t["id"])
        draw_box(top, 1, h, cols, "  Tier Definitions  ")
        fill_box(top, 1, h, cols)
        COL = [4, 14, 10, 12, 12, 14]
        HDR = ["ID", "Name", "Type", "Min OK", "Max OK", "Rate / OK"]
        ALN = ["right","left","left","right","right","right"]
        y = top+1; x = 3
        write(cur(y, x) + F(*C_ACCENT) + BOLD)
        for i, lbl in enumerate(HDR):
            write(pad(lbl, COL[i], ALN[i]) + "  ")
        write(RESET)
        y += 1
        write(cur(y, 2) + F(*C_BORDER) + H*(cols-3) + RESET)
        y += 1
        for t in tiers[self.scroll: self.scroll+h-5]:
            if "admin" in t["name"].lower():
                ttype, tc = "ADMIN", C_RED
            elif t["name"].lower().startswith("mp"):
                ttype, tc = "MP",    C_PURPLE
            else:
                ttype, tc = "PR",    C_ACCENT
            rs = f"{t['price_per_ok']:.1f}" if t["price_per_ok"] >= 0.01 else f"{t['price_per_ok']:.1e}"
            write(cur(y, x))
            write(F(*C_DIM)   + pad(str(t["id"]),      COL[0],"right") + "  ")
            write(F(*C_WHITE) + pad(t["name"],          COL[1],"left")  + "  ")
            write(F(*tc)+BOLD + pad(ttype,              COL[2],"left")  + "  " + RESET)
            write(F(*C_DIM)   + pad(f"{t['min_ok']:,}", COL[3],"right") + "  ")
            write(F(*C_DIM)   + pad(f"{t['max_ok']:,}", COL[4],"right") + "  ")
            write(F(*C_YELLOW)+BOLD + pad(f"\u09f3 {rs}", COL[5],"right") + RESET)
            y += 1

    # ── Users tab ────────────────────────────────────────
    def render_users(self, top, h, cols):
        users = sorted(self.config.get("user_tiers", {}).items(),
                       key=lambda x: self.names.get(x[0], x[0]))
        draw_box(top, 1, h, cols, "  Users & Tiers  ")
        fill_box(top, 1, h, cols)
        y = top+1; x = 3
        write(cur(y, x) + F(*C_ACCENT) + BOLD)
        write(pad("Name", 18) + "  " + pad("UID", 14) + "  Assigned Tiers" + RESET)
        y += 1
        write(cur(y, 2) + F(*C_BORDER) + H*(cols-3) + RESET)
        y += 1
        for uid, tids in users[self.scroll: self.scroll+h-5]:
            name = self.names.get(uid, uid)
            is_admin = False
            badges = []
            for tid in tids:
                t = self.tier_map.get(tid)
                if t:
                    is_admin |= "admin" in t["name"].lower()
                    tc = (C_RED if "admin" in t["name"].lower()
                          else C_PURPLE if t["name"].lower().startswith("mp")
                          else C_ACCENT)
                    badges.append(F(*tc) + f"[{t['name']}]" + RESET)
            write(cur(y, x))
            write(F(*(C_RED if is_admin else C_TEXT))+BOLD+pad(name,18)+RESET+"  ")
            write(F(*C_DIM)+pad(uid,14)+RESET+"  ")
            write("  ".join(badges)+RESET)
            y += 1

    # ── Balances tab ─────────────────────────────────────
    def render_balances(self, top, h, cols):
        draw_box(top, 1, h, cols, "  Account Balances  ")
        fill_box(top, 1, h, cols)
        total    = sum(self.balances.values())
        sorted_b = sorted(self.balances.items(), key=lambda x: x[1])
        max_abs  = max((abs(v) for v in self.balances.values()), default=1)
        bar_w    = cols - 44
        y = top+1; x = 3
        tc = C_GREEN if total >= 0 else C_RED
        write(cur(y, x)+F(*C_DIM)+"Total outstanding: "+F(*tc)+BOLD+f"{total:+,.2f} \u09f3"+RESET)
        y += 1
        write(cur(y, 2)+F(*C_BORDER)+H*(cols-3)+RESET)
        y += 1
        for uid, bal in sorted_b[self.scroll: self.scroll+h-5]:
            name   = self.names.get(uid, uid)
            bc     = C_GREEN if bal >= 0 else C_RED
            filled = int(abs(bal)/max_abs*bar_w)
            bar    = F(*bc)+"\u2588"*filled+F(*C_BORDER)+"\u2591"*(bar_w-filled)+RESET
            write(cur(y, x))
            write(F(*C_TEXT)+pad(name,18)+RESET+"  ")
            write(bar+"  ")
            write(F(*bc)+BOLD+f"{bal:+,.2f} \u09f3"+RESET)
            y += 1


# ─── Entry ───────────────────────────────────────────────
if __name__ == "__main__":
    if not sys.stdout.isatty():
        print("Run in a real terminal.")
        sys.exit(1)
    TUI().run()
