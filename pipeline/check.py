"""Quick health check of the warehouse after a run: `python -m pipeline.check`.

Exits with status 1 when the reporting tables are empty, so it can gate a deployment.
"""

import sys

import duckdb

from pipeline.warehouse import WAREHOUSE_PATH

CHECKS = {
    "orders": "select count(*) from marts.fct_orders",
    "order_lines": "select count(*) from marts.fct_order_lines",
    "days_with_sales": "select count(distinct order_date) from marts.agg_daily_sales",
    "quarantined_rows": "select count(*) from quality.dq_rejected_rows",
    "missing_files": "select count(*) from quality.dq_missing_files",
}


def main() -> None:
    con = duckdb.connect(str(WAREHOUSE_PATH), read_only=True)
    try:
        results = {name: con.execute(sql).fetchall()[0][0] for name, sql in CHECKS.items()}
    finally:
        con.close()
    for name, value in results.items():
        print(f"{name:18} {value}")
    if results["orders"] == 0 or results["days_with_sales"] == 0:
        sys.exit("Reporting tables are empty")


if __name__ == "__main__":
    main()
