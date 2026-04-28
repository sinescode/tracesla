#!/usr/bin/env python3
"""
PayTrack Tier Price Manager
Usage:
  python pricetier_changer.py          → interactive menu (Termux safe)
  python pricetier_changer.py --summary → show payment summary
  python pricetier_changer.py --tier-price --tier ID [--set|--add|--sub VAL] [--include-admin] [--no-rebalance]
"""

import os, sys, json, csv, argparse
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
#  ANSI Colors (Termux safe)
# ═══════════════════════════════════════════════════════════════

class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    # Foreground
    BLACK  = "\033[30m"
    RED    = "\033[31m"
    GREEN  = "\033[32m"
    YELLOW = "\033[33m"
    BLUE   = "\033[34m"
    MAGENTA= "\033[35m"
    CYAN   = "\033[36m"
    WHITE  = "\033[37m"
    # Bright
    BRED   = "\033[91m"
    BGREEN = "\033[92m"
    BYELLOW= "\033[93m"
    BBLUE  = "\033[94m"
    BMAGENTA="\033[95m"
    BCYAN  = "\033[96m"
    BWHITE = "\033[97m"
    # Background
    BG_BLACK  = "\033[40m"
    BG_BLUE   = "\033[44m"
    BG_CYAN   = "\033[46m"

def c(color, text):
    return f"{color}{text}{C.RESET}"

def clear():
    os.system("clear")

def hr(char="═", width=58):
    print(c(C.BLUE, char * width))

def header(title):
    clear()
    hr("═")
    pad = (58 - len(title) - 4) // 2
    print(c(C.CYAN + C.BOLD, "║" + " " * pad + f"  {title}  " + " " * pad + "║"))
    hr("═")
    print()

def section(title):
    print()
    print(c(C.BBLUE, f"  ┌─ {title} " + "─" * max(0, 48 - len(title)) + "┐"))

def endsection():
    print(c(C.BBLUE, "  └" + "─" * 55 + "┘"))

def prompt(msg, default=None):
    hint = f" [{c(C.DIM, default)}]" if default else ""
    try:
        val = input(f"  {c(C.BYELLOW, '❯')} {msg}{hint}: ").strip()
        return val if val else default
    except (EOFError, KeyboardInterrupt):
        print()
        return None

def ok(msg):    print(f"  {c(C.BGREEN,  '✔')}  {c(C.GREEN,  msg)}")
def warn(msg):  print(f"  {c(C.BYELLOW, '⚠')}  {c(C.YELLOW, msg)}")
def err(msg):   print(f"  {c(C.BRED,    '✘')}  {c(C.RED,    msg)}")
def info(msg):  print(f"  {c(C.BCYAN,   '·')}  {c(C.CYAN,   msg)}")

def press_enter():
    try:
        input(f"\n  {c(C.DIM, 'Press Enter to continue...')}")
    except (EOFError, KeyboardInterrupt):
        pass


# ═══════════════════════════════════════════════════════════════
#  Config I/O
# ═══════════════════════════════════════════════════════════════

def load_config(config_path="Config/Config.json"):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_config(config, config_path="Config/Config.json"):
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


# ═══════════════════════════════════════════════════════════════
#  CSV Loading
# ═══════════════════════════════════════════════════════════════

def load_all_csvs(csv_dir="CSV"):
    user_data = defaultdict(lambda: {
        "username": "", "total_ok": 0, "total_csv_amount": 0.0,
        "bkash": "Not Provided", "rocket": "Not Provided",
        "paid_status": "No", "per_file": [],
    })
    if not os.path.isdir(csv_dir):
        return user_data, []
    csv_files = sorted(f for f in os.listdir(csv_dir) if f.endswith(".csv"))
    if not csv_files:
        return user_data, []
    for filename in csv_files:
        filepath = os.path.join(csv_dir, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = row["User ID"].strip()
                ok  = int(row.get("OK Count", 0))
                amt = float(row.get("Total Amount", 0.0))
                user_data[uid]["username"]         = row.get("Username", "").strip()
                user_data[uid]["total_ok"]         += ok
                user_data[uid]["total_csv_amount"] += amt
                user_data[uid]["per_file"].append((filename, ok, amt))
                if row.get("Bkash", "").strip() not in ("", "Not Provided"):
                    user_data[uid]["bkash"] = row["Bkash"].strip()
                if row.get("Rocket", "").strip() not in ("", "Not Provided"):
                    user_data[uid]["rocket"] = row["Rocket"].strip()
                if row.get("Paid Status", "No").strip() == "Yes":
                    user_data[uid]["paid_status"] = "Yes"
    return user_data, csv_files


# ═══════════════════════════════════════════════════════════════
#  Tier Helpers
# ═══════════════════════════════════════════════════════════════

def get_tier_for_ok(tier_ids, tier_defs, total_ok):
    tier_map = {t["id"]: t for t in tier_defs}
    for tid in tier_ids:
        tier = tier_map.get(tid)
        if tier and tier["min_ok"] <= total_ok <= tier["max_ok"]:
            return tier
    for tid in tier_ids:
        if tid in tier_map:
            return tier_map[tid]
    return None

def is_admin(tier):
    return "admin" in tier.get("name", "").lower()

def fmt_price(p):
    return f"{p:.4f}" if p >= 0.001 else f"{p:.2e}"

def compute_payables(user_data, tier_defs, user_tiers, balances):
    result = {}
    for uid, data in user_data.items():
        tier_ids = user_tiers.get(uid, [])
        calc = 0.0
        for _, ok, csv_amt in data["per_file"]:
            tier = get_tier_for_ok(tier_ids, tier_defs, ok) if tier_ids else None
            calc += ok * tier["price_per_ok"] if tier else csv_amt
        result[uid] = (calc, calc + balances.get(uid, 0.0))
    return result

def do_rebalance(user_data, tier_defs, user_tiers, old_payables, balances):
    new_pay = compute_payables(user_data, tier_defs, user_tiers, balances)
    nb = {}
    for uid, (_, old_total) in old_payables.items():
        nb[uid] = round(old_total - new_pay[uid][0], 4)
    return nb

def apply_op(old_price, op, value):
    if op == "set": return round(value, 4)
    if op == "add": return round(old_price + value, 4)
    if op == "sub": return round(old_price - value, 4)
    return old_price


# ═══════════════════════════════════════════════════════════════
#  Display Tier Table
# ═══════════════════════════════════════════════════════════════

def print_tier_table(tier_defs, highlight_ids=None):
    highlight_ids = highlight_ids or set()
    section("All Tier Definitions")
    print(c(C.BBLUE, "  │") +
          c(C.BOLD, f"  {'#':<3} {'ID':<5} {'Name':<18} {'Min':>6} {'Max':>9} {'Price':>10}  {'Tag'}"))
    print(c(C.BBLUE, "  │") + c(C.DIM, "  " + "─" * 54))

    for i, t in enumerate(tier_defs):
        num     = str(i + 1)
        tid     = str(t["id"])
        name    = t["name"]
        mn      = f"{t['min_ok']:,}"
        mx      = f"{t['max_ok']:,}"
        price   = fmt_price(t["price_per_ok"])
        admin   = is_admin(t)
        tag     = c(C.MAGENTA, "ADMIN") if admin else c(C.DIM, "     ")

        if t["id"] in highlight_ids:
            row = c(C.BGREEN + C.BOLD,
                    f"  {num:<3} {tid:<5} {name:<18} {mn:>6} {mx:>9} {price:>10}  ") + tag
        elif admin:
            row = c(C.MAGENTA,
                    f"  {num:<3} {tid:<5} {name:<18} {mn:>6} {mx:>9} {price:>10}  ") + tag
        else:
            row = c(C.WHITE,
                    f"  {num:<3} {tid:<5} {name:<18} {mn:>6} {mx:>9} {price:>10}  ") + tag

        print(c(C.BBLUE, "  │") + row)
    endsection()


# ═══════════════════════════════════════════════════════════════
#  Core Change Executor
# ═══════════════════════════════════════════════════════════════

def execute_change(config, config_path, csv_dir, targets, op, value, do_rebal=True):
    tier_defs  = config["config"]["tier_definitions"]
    user_tiers = config["config"]["user_tiers"]
    balances   = config.get("balances", {})

    user_data, _ = load_all_csvs(csv_dir)
    has_csv = bool(user_data)

    old_payables = compute_payables(user_data, tier_defs, user_tiers, balances) if has_csv else {}

    section("Price Changes")
    changed_ids = set()
    for t in targets:
        old = t["price_per_ok"]
        new = apply_op(old, op, value)
        t["price_per_ok"] = new
        changed_ids.add(t["id"])
        arrow = c(C.DIM, "→")
        old_s = c(C.YELLOW, fmt_price(old))
        new_s = c(C.BGREEN, fmt_price(new))
        delta = new - old
        d_s   = c(C.BGREEN if delta >= 0 else C.BRED,
                  f"({'+' if delta >= 0 else ''}{delta:.4f})")
        print(c(C.BBLUE, "  │") +
              f"  [{c(C.CYAN, str(t['id']))}] {c(C.BOLD, t['name']):<18}"
              f"  {old_s}  {arrow}  {new_s}  {d_s}")
    endsection()

    if do_rebal:
        if has_csv:
            nb = do_rebalance(user_data, tier_defs, user_tiers, old_payables, balances)
            config["balances"] = nb
            section("Balance Adjustments  (payable stays identical)")
            for uid, new_bal in sorted(nb.items()):
                old_bal = balances.get(uid, 0.0)
                delta   = new_bal - old_bal
                name    = user_data[uid]["username"] or uid
                d_s     = c(C.BGREEN if delta >= 0 else C.BRED,
                            f"({'+' if delta >= 0 else ''}{delta:.4f})")
                print(c(C.BBLUE, "  │") +
                      f"  {name:<22} {old_bal:>+10.4f}  →  {new_bal:>+10.4f}  {d_s}")
            endsection()
        else:
            warn("No CSV data found – rebalance skipped.")
    else:
        warn("Rebalance skipped (--no-rebalance). Payables will change.")

    save_config(config, config_path)
    ok(f"Config saved → {config_path}")
    return changed_ids


# ═══════════════════════════════════════════════════════════════
#  Interactive Menu
# ═══════════════════════════════════════════════════════════════

def pick_operation():
    """Ask user for operation type and value. Returns (op, value) or None."""
    print()
    print(c(C.BOLD, "  Select operation:"))
    print(f"  {c(C.CYAN, '1')}  Set exact price")
    print(f"  {c(C.CYAN, '2')}  Add to current price")
    print(f"  {c(C.CYAN, '3')}  Subtract from current price")
    print(f"  {c(C.DIM,  '0')}  Cancel")
    print()
    choice = prompt("Choice")
    if choice not in ("1", "2", "3"):
        return None, None

    op_map = {"1": "set", "2": "add", "3": "sub"}
    op     = op_map[choice]
    label  = {"set": "New exact price", "add": "Amount to add", "sub": "Amount to subtract"}[op]

    val_s = prompt(label)
    if val_s is None:
        return None, None
    try:
        val = float(val_s)
        return op, val
    except ValueError:
        err("Invalid number.")
        return None, None


def pick_rebalance():
    ans = prompt("Rebalance balances so payables stay the same? (Y/n)", default="y")
    return (ans or "y").lower() != "n"


def menu_single_tier(config, config_path, csv_dir):
    """Change one specific tier."""
    tier_defs = config["config"]["tier_definitions"]

    while True:
        header("Change Single Tier")
        print_tier_table(tier_defs)
        print()
        val = prompt("Enter tier ID or # number  (0 = back)")
        if val in (None, "0", ""):
            return

        # Resolve by # (row number) or ID
        target = None
        if val.isdigit():
            n = int(val)
            # Try as row number first (1-based)
            if 1 <= n <= len(tier_defs):
                # But also check if it's a valid tier ID
                by_id = [t for t in tier_defs if t["id"] == n]
                by_num = tier_defs[n - 1]
                if by_id and by_id[0] != by_num:
                    # Ambiguous – ask
                    print()
                    print(f"  {c(C.YELLOW, '?')}  Did you mean:")
                    print(f"  {c(C.CYAN, '1')}  Row #{n} → [{by_num['id']}] {by_num['name']}")
                    print(f"  {c(C.CYAN, '2')}  Tier ID {n} → [{by_id[0]['id']}] {by_id[0]['name']}")
                    ans = prompt("Choice", default="1")
                    target = by_num if ans != "2" else by_id[0]
                elif by_id:
                    target = by_id[0]
                else:
                    target = by_num
            else:
                by_id = [t for t in tier_defs if t["id"] == n]
                if by_id:
                    target = by_id[0]

        if target is None:
            err(f"Tier '{val}' not found.")
            press_enter()
            continue

        # Show selected tier
        print()
        admin_tag = c(C.MAGENTA, " [ADMIN]") if is_admin(target) else ""
        info(f"Selected: [{c(C.CYAN, str(target['id']))}] "
             f"{c(C.BOLD, target['name'])}{admin_tag}  "
             f"price = {c(C.YELLOW, fmt_price(target['price_per_ok']))}")

        op, value = pick_operation()
        if op is None:
            continue

        do_rebal = pick_rebalance()
        print()
        execute_change(config, config_path, csv_dir, [target], op, value, do_rebal)
        press_enter()
        return  # back to main after apply


def menu_all_tiers(config, config_path, csv_dir):
    """Change all tiers at once."""
    tier_defs = config["config"]["tier_definitions"]

    while True:
        header("Change All Tiers")
        print_tier_table(tier_defs)

        print()
        inc_admin_ans = prompt("Include ADMIN tiers? (y/N)", default="n")
        inc_admin = (inc_admin_ans or "n").lower() == "y"

        if inc_admin:
            targets = list(tier_defs)
            info(f"Targeting ALL {len(targets)} tiers (including admin).")
        else:
            targets = [t for t in tier_defs if not is_admin(t)]
            skipped = [t for t in tier_defs if is_admin(t)]
            info(f"Targeting {len(targets)} non-admin tier(s). "
                 f"Skipping: {', '.join(t['name'] for t in skipped) or 'none'}")

        if not targets:
            err("No tiers to change.")
            press_enter()
            return

        op, value = pick_operation()
        if op is None:
            return

        do_rebal = pick_rebalance()
        print()
        execute_change(config, config_path, csv_dir, targets, op, value, do_rebal)
        press_enter()
        return


def menu_bulk_custom(config, config_path, csv_dir):
    """Pick multiple specific tiers to change together."""
    tier_defs = config["config"]["tier_definitions"]

    while True:
        header("Bulk Custom – Pick Multiple Tiers")
        print_tier_table(tier_defs)

        print()
        info("Enter tier IDs or # row numbers separated by commas.")
        info("Example:  1,3,9   or   3,10,11")
        raw = prompt("Tier IDs / row numbers  (0 = back)")
        if raw in (None, "0", ""):
            return

        selected = []
        seen_ids = set()
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok.isdigit():
                warn(f"Skipping invalid token: '{tok}'")
                continue
            n = int(tok)
            # Try row number first if <= len
            if 1 <= n <= len(tier_defs):
                t = tier_defs[n - 1]
            else:
                match = [t for t in tier_defs if t["id"] == n]
                t = match[0] if match else None

            if t is None:
                warn(f"Not found: {tok}")
            elif t["id"] in seen_ids:
                warn(f"Duplicate, skipping: {t['name']}")
            else:
                selected.append(t)
                seen_ids.add(t["id"])

        if not selected:
            err("No valid tiers selected.")
            press_enter()
            continue

        print()
        section("Selected Tiers")
        for t in selected:
            tag = c(C.MAGENTA, " [ADMIN]") if is_admin(t) else ""
            print(c(C.BBLUE, "  │") +
                  f"  [{c(C.CYAN, str(t['id']))}] {c(C.BOLD, t['name'])}{tag}"
                  f"  →  {c(C.YELLOW, fmt_price(t['price_per_ok']))}")
        endsection()

        op, value = pick_operation()
        if op is None:
            continue

        do_rebal = pick_rebalance()
        print()
        execute_change(config, config_path, csv_dir, selected, op, value, do_rebal)
        press_enter()
        return


def menu_view_tiers(config):
    """Just display the tier table."""
    header("View All Tiers")
    print_tier_table(config["config"]["tier_definitions"])
    press_enter()


# ═══════════════════════════════════════════════════════════════
#  Payment Summary (plain text)
# ═══════════════════════════════════════════════════════════════

def show_summary(config, csv_dir):
    header("Payment Summary")
    tier_defs    = config["config"]["tier_definitions"]
    user_tiers   = config["config"]["user_tiers"]
    custom_names = config["config"].get("custom_names", {})
    balances     = config.get("balances", {})

    user_data, csv_files = load_all_csvs(csv_dir)
    if not user_data:
        err("No CSV data found.")
        press_enter()
        return

    info(f"CSV files: {', '.join(csv_files)}")
    print()

    grand_csv = grand_calc = grand_pay = 0.0
    results = []

    for uid, data in sorted(user_data.items()):
        display_name = custom_names.get(uid, data["username"] or uid)
        tier_ids     = user_tiers.get(uid, [])
        calc_amt     = 0.0
        file_details = []

        for fname, ok_cnt, csv_amt in data["per_file"]:
            matched = get_tier_for_ok(tier_ids, tier_defs, ok_cnt) if tier_ids else None
            if matched:
                rate, tname, fc = matched["price_per_ok"], matched["name"], ok_cnt * matched["price_per_ok"]
            else:
                rate, tname, fc = None, "No Tier", csv_amt
            calc_amt += fc
            file_details.append((fname, ok_cnt, csv_amt, rate, tname, fc))

        balance = balances.get(uid, 0.0)
        payable = calc_amt + balance
        grand_csv  += data["total_csv_amount"]
        grand_calc += calc_amt
        grand_pay  += payable

        disp_tier = file_details[0][4] if len(file_details) == 1 else "Multiple"
        disp_rate = file_details[0][3] if len(file_details) == 1 else None
        results.append(dict(uid=uid, name=display_name,
                            total_ok=data["total_ok"],
                            tier=disp_tier, rate=disp_rate,
                            csv_amt=data["total_csv_amount"],
                            calc=calc_amt, balance=balance,
                            payable=payable, paid=data["paid_status"],
                            files=file_details))

    # Table header
    hr("─")
    h = (f"{'Name':<22} {'OKs':>6}  {'Tier':<12} {'Rate':>8}"
         f"  {'Calc':>10}  {'Balance':>10}  {'Payable':>10}")
    print(c(C.BOLD + C.BWHITE, h))
    hr("─")

    for r in results:
        rs = fmt_price(r["rate"]) if r["rate"] is not None else "N/A"
        pay_color = C.BRED if r["payable"] > 0 else C.BGREEN
        bal_color = C.YELLOW if r["balance"] != 0 else C.DIM
        bal_str = f"{r['balance']:>+10.2f}"
        pay_str = f"{r['payable']:>10.2f}"
        print(
            f"{c(C.BWHITE, r['name']):<22}"
            f" {r['total_ok']:>6}  "
            f"{c(C.CYAN, r['tier']):<12} "
            f"{c(C.YELLOW, rs):>8}  "
            f"{r['calc']:>10.2f}  "
            f"{c(bal_color, bal_str)}  "
            f"{c(pay_color + C.BOLD, pay_str)}"
        )

    hr("─")
    print(f"  {c(C.DIM, 'Total CSV    :')}  ৳ {grand_csv:>12.2f}")
    print(f"  {c(C.DIM, 'Total Calc   :')}  ৳ {grand_calc:>12.2f}")
    grand_pay_str = f"{grand_pay:>12.2f}"
    print(f"  {c(C.BOLD,'Total Payable:')}  ৳ {c(C.BGREEN, grand_pay_str)}")
    hr("═")

    # Per-file breakdown
    print()
    for r in results:
        if not r["files"]: continue
        print(c(C.BOLD + C.BCYAN, f"  {r['name']}") + c(C.DIM, f"  ({r['uid']})"))
        print(c(C.DIM, f"  {'File':<22} {'OKs':>6} {'Rate':>8} {'Amount':>10}"))
        for fname, ok_cnt, csv_amt, rate, tier, fc in r["files"]:
            rs = fmt_price(rate) if rate else "N/A"
            print(f"  {fname:<22} {ok_cnt:>6} {rs:>8} {fc:>10.2f}")
        print(c(C.DIM, f"  {'─'*22} {'─'*6} {'─'*8} {'─'*10}"))
        calc_str = f"{r['calc']:>10.2f}"
        print(f"  {'Total':<22} {r['total_ok']:>6} {'':>8} {c(C.BOLD, calc_str)}")
        print()

    # Unpaid
    unpaid = [r for r in results if r["paid"] == "No" and r["payable"] != 0]
    if unpaid:
        hr("─")
        print(c(C.BRED + C.BOLD, "  UNPAID USERS"))
        hr("─")
        for r in unpaid:
            pay_s = f"{r['payable']:>10.2f}"
            print(f"  {r['name']:<22}  ৳ {c(C.BRED, pay_s)}")
        total_unpaid = f"{sum(r['payable'] for r in unpaid):>10.2f}"
        print(f"\n  {c(C.BOLD,'Total Unpaid:')}  ৳ {c(C.BRED, total_unpaid)}")
        hr("═")

    press_enter()


# ═══════════════════════════════════════════════════════════════
#  Main Interactive Menu
# ═══════════════════════════════════════════════════════════════

def interactive(config_path, csv_dir):
    while True:
        config = load_config(config_path)   # reload each loop to stay fresh
        tier_defs = config["config"]["tier_definitions"]
        n_admin   = sum(1 for t in tier_defs if is_admin(t))
        n_normal  = len(tier_defs) - n_admin

        header("PayTrack Tier Manager")

        print(c(C.BOLD, "  Tier Price Management"))
        print(f"  {c(C.CYAN,'1')}  Change a {c(C.BOLD,'single')} tier")
        print(f"  {c(C.CYAN,'2')}  Change {c(C.BOLD,'all')} tiers  "
              f"{c(C.DIM, f'({n_normal} normal + {n_admin} admin)')}")
        print(f"  {c(C.CYAN,'3')}  Change {c(C.BOLD,'custom selection')} of tiers")
        print()
        print(c(C.BOLD, "  View / Info"))
        print(f"  {c(C.CYAN,'4')}  View all tiers")
        print(f"  {c(C.CYAN,'5')}  Payment summary")
        print()
        print(f"  {c(C.DIM,'0')}  Exit")
        print()
        hr("─")

        choice = prompt("Choose")

        if choice == "1":
            menu_single_tier(config, config_path, csv_dir)
        elif choice == "2":
            menu_all_tiers(config, config_path, csv_dir)
        elif choice == "3":
            menu_bulk_custom(config, config_path, csv_dir)
        elif choice == "4":
            menu_view_tiers(config)
        elif choice == "5":
            show_summary(config, csv_dir)
        elif choice in ("0", None, "q", "Q"):
            print(c(C.DIM, "\n  Bye.\n"))
            break
        else:
            err("Invalid choice.")


# ═══════════════════════════════════════════════════════════════
#  CLI: --tier-price mode
# ═══════════════════════════════════════════════════════════════

def cmd_tier_price(args, config_path, csv_dir):
    config    = load_config(config_path)
    tier_defs = config["config"]["tier_definitions"]
    user_tiers = config["config"]["user_tiers"]
    balances  = config.get("balances", {})

    if args.tier is not None:
        targets = [t for t in tier_defs if str(t["id"]) == str(args.tier)]
        if not targets:
            ids = ", ".join(str(t["id"]) for t in tier_defs)
            err(f"Tier ID '{args.tier}' not found. Available: {ids}")
            return
    else:
        if args.include_admin:
            targets = list(tier_defs)
        else:
            targets = [t for t in tier_defs if not is_admin(t)]

    if not targets:
        err("No tiers matched."); return

    if args.set is None and args.add is None and args.sub is None:
        err("Specify --set, --add, or --sub VALUE"); return

    op    = "set" if args.set is not None else ("add" if args.add is not None else "sub")
    value = args.set if args.set is not None else (args.add if args.add is not None else args.sub)

    execute_change(config, config_path, csv_dir, targets, op, value,
                   do_rebal=not args.no_rebalance)


# ═══════════════════════════════════════════════════════════════
#  CLI: --summary mode
# ═══════════════════════════════════════════════════════════════

def cmd_summary(config_path, csv_dir):
    config = load_config(config_path)
    show_summary(config, csv_dir)


# ═══════════════════════════════════════════════════════════════
#  Argument Parsing & Entry
# ═══════════════════════════════════════════════════════════════

def build_parser():
    parser = argparse.ArgumentParser(
        prog=os.path.basename(sys.argv[0]),
        description="PayTrack Tier Price Manager",
    )
    parser.add_argument("--summary",    action="store_true", help="Show payment summary.")
    parser.add_argument("--tier-price", action="store_true", help="CLI tier price mode.")

    target = parser.add_mutually_exclusive_group()
    target.add_argument("--tier",      metavar="ID")
    target.add_argument("--all-tiers", action="store_true")

    parser.add_argument("--include-admin",  action="store_true")

    op = parser.add_mutually_exclusive_group()
    op.add_argument("--set", type=float, metavar="VALUE")
    op.add_argument("--add", type=float, metavar="VALUE")
    op.add_argument("--sub", type=float, metavar="VALUE")

    parser.add_argument("--no-rebalance", action="store_true")
    parser.add_argument("--csv-dir",      default="CSV",                metavar="DIR")
    parser.add_argument("--config-path",  default="Config/Config.json", metavar="FILE")
    return parser


def main():
    parser      = build_parser()
    args        = parser.parse_args()
    csv_dir     = args.csv_dir
    config_path = args.config_path

    if not os.path.isfile(config_path):
        err(f"Config not found: '{config_path}'"); sys.exit(1)

    if args.summary:
        cmd_summary(config_path, csv_dir)
        return

    if args.tier_price:
        cmd_tier_price(args, config_path, csv_dir)
        return

    # Default: interactive menu
    try:
        interactive(config_path, csv_dir)
    except KeyboardInterrupt:
        print(c(C.DIM, "\n\n  Interrupted. Bye.\n"))


if __name__ == "__main__":
    main()
