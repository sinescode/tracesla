import json
import csv
import os
from datetime import date
from glob import glob

# ─── Paths ───────────────────────────────────────────────
CONFIG_PATH = "Config/Config.json"
CSV_DIR     = "CSV"

# ─── Admin earns this per OK ────────────────────────────
ADMIN_RATE   = 4.1
# ─── Default rate if user NOT found in config ───────────
DEFAULT_RATE = 3.5


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    # JSON wraps everything under a "config" key — unwrap it
    if "config" in data and isinstance(data["config"], dict):
        return data["config"]
    return data


def get_admin_rates(config):
    """Collect rates that belong to any 'Admin' tier definition."""
    rates = set()
    for tier in config.get("tier_definitions", []):
        if "admin" in tier.get("name", "").lower():
            rates.add(tier["price_per_ok"])
    return rates


def resolve_rate(user_id, ok_count, config, admin_rates):
    """
    Returns (rate, is_admin).
    Matches user_tiers first, then falls back to DEFAULT_RATE.
    If the matched rate is an admin rate → marks as admin.
    """
    uid = str(user_id)
    tiers = config.get("user_tiers", {}).get(uid, [])

    for t in tiers:
        if t["min_ok"] <= ok_count <= t["max_ok"]:
            if t["price_per_ok"] in admin_rates:
                return t["price_per_ok"], True
            return t["price_per_ok"], False

    return DEFAULT_RATE, False


def pick_csv():
    """Try today's file first; otherwise list available files."""
    today = date.today().strftime("%Y-%m-%d")
    today_path = os.path.join(CSV_DIR, f"{today}.csv")
    if os.path.exists(today_path):
        return today_path, today

    files = sorted(glob(os.path.join(CSV_DIR, "*.csv")), reverse=True)
    if not files:
        return None, None
    label = os.path.splitext(os.path.basename(files[0]))[0]
    return files[0], label


def calculate_profit():
    config      = load_config()
    admin_rates = get_admin_rates(config)
    names       = config.get("custom_names", {})

    csv_path, date_label = pick_csv()
    if csv_path is None:
        print("❌ No CSV file found in", CSV_DIR)
        return

    rows    = []
    skipped = []
    total_ok = 0
    total_profit = 0.0
    total_payout = 0.0

    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uid = row["User ID"].strip()
            ok  = int(row["OK Count"].strip())
            if ok <= 0:
                continue

            rate, is_admin = resolve_rate(uid, ok, config, admin_rates)
            name = names.get(uid, row["Username"].strip())

            if is_admin:
                skipped.append((name, uid, ok))
                continue

            earning = ADMIN_RATE * ok
            payout  = rate * ok
            profit  = earning - payout

            rows.append({
                "name":    name,
                "uid":     uid,
                "ok":      ok,
                "rate":    rate,
                "earning": earning,
                "payout":  payout,
                "profit":  profit,
            })
            total_ok      += ok
            total_payout  += payout
            total_profit  += profit

    # ── Print ────────────────────────────────────────────
    print()
    print("=" * 72)
    print(f"  📊  PROFIT REPORT  —  {date_label}")
    print("=" * 72)

    if skipped:
        print("  ⏭  Skipped (admin accounts):")
        for n, u, o in skipped:
            print(f"       {n} ({u})  —  {o} OK")
        print()

    print(f"  {'Name':<18} {'OK':>5} {'Rate':>6} {'Earning':>10}"
          f" {'Payout':>10} {'Profit':>10}")
    print(f"  {'─' * 64}")

    for r in rows:
        print(f"  {r['name']:<18} {r['ok']:>5} {r['rate']:>6.1f}"
              f" {r['earning']:>10.1f} {r['payout']:>10.1f} {r['profit']:>10.1f}")

    print(f"  {'─' * 64}")
    print(f"  {'TOTAL':<18} {total_ok:>5} {'':>6}"
          f" {ADMIN_RATE * total_ok:>10.1f}"
          f" {total_payout:>10.1f}"
          f" {total_profit:>10.1f} টাকা")
    print("=" * 72)
    print()


if __name__ == "__main__":
    calculate_profit()