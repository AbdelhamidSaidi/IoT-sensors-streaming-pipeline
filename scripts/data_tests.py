#!/usr/bin/env python3
"""Simple data-quality checks between medallion layers.

Usage examples:
  python scripts/data_tests.py --boundary consumer_silver
  python scripts/data_tests.py --boundary silver_gold
  python scripts/data_tests.py --boundary gold_dim

The script uses `medallion.silver.silver.get_connection()` for a DB connection.
"""
import argparse
import sys
import traceback

from medallion.silver.silver import get_connection


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute("SELECT 1 FROM sys.tables WHERE name = ?", (table_name,))
    return cursor.fetchone() is not None


def count_rows(cursor, table_name: str) -> int:
    # table_name is expected to be a trusted literal (internal use)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    r = cursor.fetchone()
    return int(r[0]) if r and r[0] is not None else 0


def test_consumer_silver(conn):
    cur = conn.cursor()
    bronze = 'sensor_logs'
    if not table_exists(cur, bronze):
        raise RuntimeError(f"Bronze table '{bronze}' not found")
    c = count_rows(cur, bronze)
    if c < 1:
        raise RuntimeError(f"Bronze table '{bronze}' has no rows")
    print(f"OK: {bronze} has {c} rows")


def test_silver_gold(conn):
    cur = conn.cursor()
    silver = 'sensor_logs_silver'
    gold = 'gold_central'
    for t in (silver,):
        if not table_exists(cur, t):
            raise RuntimeError(f"Silver table '{t}' not found")
        c = count_rows(cur, t)
        if c < 1:
            raise RuntimeError(f"Silver table '{t}' has no rows")
        print(f"OK: {t} has {c} rows")

    # gold may not yet exist until the gold pipeline runs; we only assert silver here


def test_gold_dim(conn):
    cur = conn.cursor()
    gold = 'gold_central'
    dim_sensor = 'dim_sensor'
    if not table_exists(cur, gold):
        raise RuntimeError(f"Gold table '{gold}' not found")
    if not table_exists(cur, dim_sensor):
        raise RuntimeError(f"Dim table '{dim_sensor}' not found")
    cg = count_rows(cur, gold)
    cd = count_rows(cur, dim_sensor)
    if cg < 1:
        raise RuntimeError(f"Gold table '{gold}' has no rows")
    if cd < 1:
        raise RuntimeError(f"Dim table '{dim_sensor}' has no rows")
    print(f"OK: {gold} rows={cg}, {dim_sensor} rows={cd}")


BOUNDARY_MAP = {
    'consumer_silver': test_consumer_silver,
    'silver_gold': test_silver_gold,
    'gold_dim': test_gold_dim,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--boundary', choices=list(BOUNDARY_MAP.keys()), required=True)
    args = p.parse_args()

    conn = None
    try:
        conn = get_connection()
        func = BOUNDARY_MAP[args.boundary]
        func(conn)
    except Exception as e:
        print('DATA TEST FAILED:', e)
        traceback.print_exc()
        sys.exit(2)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    print('DATA TESTS PASSED')


if __name__ == '__main__':
    main()
