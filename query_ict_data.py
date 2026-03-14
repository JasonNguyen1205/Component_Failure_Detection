"""
query_ict_data.py
Fetches ICT failure records from DATA_ICT where STATUS = 0,
from December 2024 up to now.

RESULT_DATA is a JSON field containing the list of failure components.
The script provides:
  - Monthly failure count summary
  - Top failing components (overall)
  - Components broken down by month
  - Optional full detail per record

Usage:
    python query_ict_data.py
    python query_ict_data.py --component IC10
    python query_ict_data.py --component IC10 --export-csv ic10.csv
    python query_ict_data.py --top 20
    python query_ict_data.py --export-csv results.csv
    python query_ict_data.py --detail
"""

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime

from db_connection import get_connection

# Fixed window: 01-DEC-2024 00:00:00  →  current moment
P_FROM = "01-DEC-24 00:00:00"

QUERY = """
SELECT
    RESULT_DATA,
    INPUT_DATE,
    MACHINE_ID,
    PART_NO,
    ORDER_NO,
    IDX
FROM DATA_ICT
WHERE STATUS = 0
  AND INPUT_DATE BETWEEN TO_DATE(:p_from, 'DD-MON-YY HH24:MI:SS')
                     AND TO_DATE(:p_to,   'DD-MON-YY HH24:MI:SS')
ORDER BY INPUT_DATE
"""

COLUMNS = ["RESULT_DATA", "INPUT_DATE", "MACHINE_ID", "PART_NO", "ORDER_NO", "IDX"]
MONTHLY_COLS = ["MONTH", "FAILURE_COUNT", "COMPONENT_COUNT"]
COMPONENT_COLS = ["COMPONENT", "TOTAL_COUNT"]
MONTHLY_COMPONENT_COLS = ["MONTH", "COMPONENT", "COUNT"]


# ---------------------------------------------------------------------------
# JSON parsing
# ---------------------------------------------------------------------------

def parse_components(result_data) -> list[str]:
    """
    Parse RESULT_DATA JSON and return a flat list of component name strings.
    Handles:
      - JSON list of strings:  ["C1", "R5", "U3"]
      - JSON list of objects:  [{"name": "C1", ...}, ...]  (tries common keys)
      - JSON object with a list value: {"failures": [...]}
      - Already a Python list (driver decoded it): returned as-is
    Returns an empty list on any parse error.
    """
    if result_data is None:
        return []

    # If the driver already deserialized it
    if isinstance(result_data, list):
        raw_list = result_data
    elif isinstance(result_data, dict):
        # Unwrap the first list-valued key
        raw_list = next((v for v in result_data.values() if isinstance(v, list)), [])
    else:
        try:
            parsed = json.loads(str(result_data))
        except (json.JSONDecodeError, TypeError):
            return []
        if isinstance(parsed, list):
            raw_list = parsed
        elif isinstance(parsed, dict):
            raw_list = next((v for v in parsed.values() if isinstance(v, list)), [])
        else:
            return []

    components = []
    _COMPONENT_KEYS = ("name", "component", "comp", "ref", "reference",
                       "COMPONENT", "NAME", "REF", "REFERENCE", "part", "PART")
    for item in raw_list:
        if isinstance(item, str):
            components.append(item.strip())
        elif isinstance(item, dict):
            for key in _COMPONENT_KEYS:
                if key in item:
                    components.append(str(item[key]).strip())
                    break
            else:
                # Fallback: first string value in the dict
                for v in item.values():
                    if isinstance(v, str):
                        components.append(v.strip())
                        break
    return [c for c in components if c]


def filter_by_component(records: list[dict], component: str) -> list[dict]:
    """
    Return only records whose parsed component list contains a case-insensitive
    match for *component* (substring match, e.g. 'IC10' matches 'IC10', 'IC100').
    Use an exact pattern like '^IC10$' or add word boundaries if needed.
    """
    needle = component.upper()
    return [
        rec for rec in records
        if any(needle in c.upper() for c in parse_components(rec["RESULT_DATA"]))
    ]


def fetch_ict_failures(p_from: str, p_to: str) -> list[dict]:
    """Query DATA_ICT for STATUS=0 records between p_from and p_to."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY, p_from=p_from, p_to=p_to)
            raw_rows = cur.fetchall()
            # RESULT_DATA is a CLOB — read LOB objects NOW while the
            # connection is still open; they become invalid after conn.close()
            rows = [
                tuple(col.read() if hasattr(col, "read") else col for col in row)
                for row in raw_rows
            ]
    finally:
        conn.close()
    return [dict(zip(COLUMNS, row)) for row in rows]


# ---------------------------------------------------------------------------
# Month key helper
# ---------------------------------------------------------------------------

def _month_key(date_val) -> str:
    if isinstance(date_val, datetime):
        return date_val.strftime("%Y-%m")
    for fmt in ("%d-%b-%y %H:%M:%S", "%d-%b-%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(str(date_val), fmt).strftime("%Y-%m")
        except ValueError:
            continue
    return str(date_val)[:7]


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------

def group_by_month(records: list[dict]) -> list[dict]:
    """Return [{MONTH, FAILURE_COUNT, COMPONENT_COUNT}] sorted chronologically."""
    failure_counts: dict[str, int] = defaultdict(int)
    component_counts: dict[str, int] = defaultdict(int)
    for rec in records:
        key = _month_key(rec["INPUT_DATE"])
        failure_counts[key] += 1
        component_counts[key] += len(parse_components(rec["RESULT_DATA"]))

    return [
        {"MONTH": k, "FAILURE_COUNT": failure_counts[k], "COMPONENT_COUNT": component_counts[k]}
        for k in sorted(failure_counts)
    ]


def group_by_component(records: list[dict]) -> list[dict]:
    """Return [{COMPONENT, TOTAL_COUNT}] sorted by count descending."""
    counts: dict[str, int] = defaultdict(int)
    for rec in records:
        for comp in parse_components(rec["RESULT_DATA"]):
            counts[comp] += 1
    return [
        {"COMPONENT": comp, "TOTAL_COUNT": cnt}
        for comp, cnt in sorted(counts.items(), key=lambda x: -x[1])
    ]


def group_by_month_component(records: list[dict]) -> list[dict]:
    """Return [{MONTH, COMPONENT, COUNT}] sorted by month then count desc."""
    counts: dict[tuple, int] = defaultdict(int)
    for rec in records:
        key = _month_key(rec["INPUT_DATE"])
        for comp in parse_components(rec["RESULT_DATA"]):
            counts[(key, comp)] += 1
    return [
        {"MONTH": m, "COMPONENT": c, "COUNT": cnt}
        for (m, c), cnt in sorted(counts.items(), key=lambda x: (x[0][0], -x[1]))
    ]


def print_monthly_summary(monthly: list[dict]) -> None:
    if not monthly:
        print("No failure records found for Dec 2024 – present.")
        return

    col_m = max(len("MONTH"), max(len(r["MONTH"]) for r in monthly))
    col_f = max(len("FAILURES"), max(len(str(r["FAILURE_COUNT"])) for r in monthly))
    col_c = max(len("COMPONENTS"), max(len(str(r["COMPONENT_COUNT"])) for r in monthly))

    print(f"\n{'MONTH'.ljust(col_m)}  {'FAILURES'.rjust(col_f)}  {'COMPONENTS'.rjust(col_c)}")
    print(f"{'-' * col_m}  {'-' * col_f}  {'-' * col_c}")
    for row in monthly:
        print(
            f"{row['MONTH'].ljust(col_m)}  "
            f"{str(row['FAILURE_COUNT']).rjust(col_f)}  "
            f"{str(row['COMPONENT_COUNT']).rjust(col_c)}"
        )
    total_f = sum(r["FAILURE_COUNT"] for r in monthly)
    total_c = sum(r["COMPONENT_COUNT"] for r in monthly)
    print(f"\nGrand total: {total_f} failed boards  |  {total_c} component failures  |  {len(monthly)} month(s)")


def print_component_summary(components: list[dict], top: int) -> None:
    if not components:
        print("No component data found.")
        return

    shown = components[:top]
    col_comp = max(len("COMPONENT"), max(len(r["COMPONENT"]) for r in shown))
    col_cnt  = max(len("COUNT"),     max(len(str(r["TOTAL_COUNT"])) for r in shown))

    print(f"\n--- Top {top} Failing Components (all months) ---")
    print(f"{'COMPONENT'.ljust(col_comp)}  {'COUNT'.rjust(col_cnt)}")
    print(f"{'-' * col_comp}  {'-' * col_cnt}")
    for row in shown:
        print(f"{row['COMPONENT'].ljust(col_comp)}  {str(row['TOTAL_COUNT']).rjust(col_cnt)}")
    print(f"\n({len(components)} unique components total)")


def _unique_barcode(rec: dict) -> str:
    """Return the best available unique board identifier."""
    return str(rec.get("ORDER_NO") or rec.get("IDX") or "")


def print_component_filter_summary(component: str, records: list[dict]) -> None:
    """Print monthly unique-barcode counts for boards that failed on *component*."""
    if not records:
        print(f"\nNo records found containing component matching '{component}'.")
        return

    # Deduplicate: each barcode (ORDER_NO) counted only once per month
    month_barcodes: dict[str, set] = defaultdict(set)
    for rec in records:
        key = _month_key(rec["INPUT_DATE"])
        month_barcodes[key].add(_unique_barcode(rec))

    col_m = max(len("MONTH"),          max(len(k) for k in month_barcodes))
    col_b = max(len("UNIQUE BARCODES"), max(len(str(len(v))) for v in month_barcodes.values()))

    print(f"\n--- Failures containing '{component}' by month (unique barcodes) ---")
    print(f"{'MONTH'.ljust(col_m)}  {'UNIQUE BARCODES'.rjust(col_b)}")
    print(f"{'-' * col_m}  {'-' * col_b}")
    for key in sorted(month_barcodes):
        print(f"{key.ljust(col_m)}  {str(len(month_barcodes[key])).rjust(col_b)}")

    total_unique = len({_unique_barcode(r) for r in records})
    print(
        f"\nTotal: {total_unique} unique barcodes with '{component}' failures  |  "
        f"{len(month_barcodes)} month(s)"
    )


def print_detail(records: list[dict]) -> None:
    if not records:
        return
    DETAIL_COLS = ["INPUT_DATE", "MACHINE_ID", "PART_NO", "ORDER_NO", "IDX", "COMPONENTS"]
    rows = []
    for rec in records:
        rows.append({
            "INPUT_DATE": rec["INPUT_DATE"],
            "MACHINE_ID": rec["MACHINE_ID"],
            "PART_NO":    rec["PART_NO"],
            "ORDER_NO":   rec["ORDER_NO"],
            "IDX":        rec["IDX"],
            "COMPONENTS": ", ".join(parse_components(rec["RESULT_DATA"])) or "(none)",
        })

    widths = {col: len(col) for col in DETAIL_COLS}
    for row in rows:
        for col in DETAIL_COLS:
            widths[col] = max(widths[col], len(str(row[col])))

    header    = "  ".join(col.ljust(widths[col]) for col in DETAIL_COLS)
    separator = "  ".join("-" * widths[col] for col in DETAIL_COLS)
    print("\n--- Detail Records ---")
    print(header)
    print(separator)
    for row in rows:
        print("  ".join(str(row[col]).ljust(widths[col]) for col in DETAIL_COLS))
    print(f"\nTotal records: {len(records)}")


def export_csv(
    monthly: list[dict],
    components: list[dict],
    monthly_components: list[dict],
    records: list[dict],
    filepath: str,
) -> None:
    base = filepath.removesuffix(".csv") if filepath.endswith(".csv") else filepath

    # 1. Monthly summary
    p = f"{base}_monthly.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=MONTHLY_COLS).writeheader()
        csv.DictWriter(f, fieldnames=MONTHLY_COLS).writerows(monthly)
    # re-open cleanly
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MONTHLY_COLS)
        w.writeheader(); w.writerows(monthly)
    print(f"Monthly summary          → {p}")

    # 2. Top components
    p = f"{base}_components.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COMPONENT_COLS)
        w.writeheader(); w.writerows(components)
    print(f"Component totals         → {p}")

    # 3. Month × component breakdown
    p = f"{base}_monthly_components.csv"
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=MONTHLY_COMPONENT_COLS)
        w.writeheader(); w.writerows(monthly_components)
    print(f"Month × component detail → {p}")

    # 4. Full raw detail (components expanded)
    p = f"{base}_detail.csv"
    DETAIL_COLS = ["INPUT_DATE", "MACHINE_ID", "PART_NO", "ORDER_NO", "IDX", "COMPONENTS"]
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DETAIL_COLS)
        w.writeheader()
        for rec in records:
            w.writerow({
                "INPUT_DATE": rec["INPUT_DATE"],
                "MACHINE_ID": rec["MACHINE_ID"],
                "PART_NO":    rec["PART_NO"],
                "ORDER_NO":   rec["ORDER_NO"],
                "IDX":        rec["IDX"],
                "COMPONENTS": ", ".join(parse_components(rec["RESULT_DATA"])),
            })
    print(f"Full detail              → {p}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Query ICT failures Dec 2024 → now, grouped by month and component."
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Number of top failing components to display (default: 10)",
    )
    parser.add_argument(
        "--export-csv",
        dest="csv_path",
        default=None,
        help="Base path for CSV export (creates 4 files: *_monthly, *_components, *_monthly_components, *_detail)",
    )
    parser.add_argument(
        "--component",
        default=None,
        metavar="NAME",
        help="Filter to records containing this component (case-insensitive substring, e.g. IC10)",
    )
    parser.add_argument(
        "--detail",
        action="store_true",
        help="Also print every raw record with parsed components",
    )
    args = parser.parse_args()

    p_to = datetime.now().strftime("%d-%b-%y %H:%M:%S").upper()
    print(f"Querying DATA_ICT  STATUS=0  from {P_FROM}  to {p_to} ...")

    try:
        records = fetch_ict_failures(P_FROM, p_to)
    except Exception as exc:
        print(f"ERROR: Could not fetch data — {exc}", file=sys.stderr)
        sys.exit(1)

    # Apply component filter if requested
    if args.component:
        filtered = filter_by_component(records, args.component)
        unique_matches = len({_unique_barcode(r) for r in filtered})
        print(f"Filter '{args.component}': {unique_matches} unique barcodes ({len(filtered)} total records) match.")
        print_component_filter_summary(args.component, filtered)
        if args.detail:
            print_detail(filtered)
        if args.csv_path:
            monthly_f   = group_by_month(filtered)
            comps_f     = group_by_component(filtered)
            monthly_c_f = group_by_month_component(filtered)
            export_csv(monthly_f, comps_f, monthly_c_f, filtered, args.csv_path)
        return

    monthly            = group_by_month(records)
    components         = group_by_component(records)
    monthly_components = group_by_month_component(records)

    print_monthly_summary(monthly)
    print_component_summary(components, args.top)

    if args.detail:
        print_detail(records)

    if args.csv_path:
        export_csv(monthly, components, monthly_components, records, args.csv_path)


if __name__ == "__main__":
    main()
