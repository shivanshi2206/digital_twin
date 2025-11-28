
# transform.py
# Purpose: Aggregate raw time-series data from TimescaleDB into daily analytics
#          and upsert results into PostgreSQL (analytics DB).

import os
import sys
import logging
from datetime import datetime
from typing import Optional, List, Tuple

import psycopg2
from psycopg2.extras import execute_values

# ---------- Config ----------
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "timescaledb")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB   = os.getenv("TIMESCALE_DB", "sensordata")
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "admin")
TIMESCALE_PASS = os.getenv("TIMESCALE_PASS", "admin")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "admin")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

# ---------- Helpers ----------

def get_timescale_conn():
    return psycopg2.connect(
        host=TIMESCALE_HOST, port=TIMESCALE_PORT,
        dbname=TIMESCALE_DB, user=TIMESCALE_USER, password=TIMESCALE_PASS
    )

def get_pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def ensure_unique_index_on_analytics(conn_pg):
    """
    Ensure a unique index exists on (building, date) so ON CONFLICT works.
    """
    with conn_pg.cursor() as cur:
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS analytics_building_date_uidx
            ON analytics_data (building, date);
        """)
    conn_pg.commit()

def fetch_date_bounds(conn_ts) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get min and max timestamp available in raw sensor_data.
    """
    with conn_ts.cursor() as cur:
        cur.execute("SELECT min(timestamp), max(timestamp) FROM sensor_data;")
        row = cur.fetchone()
        return row[0], row[1]

def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        # Accept YYYY-MM-DD or full ISO
        return datetime.fromisoformat(date_str)
    except Exception:
        logging.error("Invalid date format: %s (use YYYY-MM-DD or ISO8601)", date_str)
        sys.exit(1)

# ---------- Core Transform ----------

def compute_daily_aggregates(conn_ts, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> List[Tuple]:
    """
    Compute daily aggregates per building directly in SQL for performance.
    Returns list of tuples: (building, date, avg_temp, avg_humidity, occupancy_rate)
    - occupancy_rate = fraction of intervals with occupancy > 0
    """
    logging.info("Computing daily aggregates from TimescaleDB...")
    sql_base = """
        SELECT
            building,
            DATE_TRUNC('day', timestamp)::date AS date,
            AVG(temperature) AS avg_temperature,
            AVG(humidity)    AS avg_humidity,
            AVG(CASE WHEN occupancy > 0 THEN 1.0 ELSE 0.0 END) AS occupancy_rate
        FROM sensor_data
        WHERE 1=1
    """
    params = []
    if start_dt:
        sql_base += " AND timestamp >= %s"
        params.append(start_dt)
    if end_dt:
        sql_base += " AND timestamp < %s"
        params.append(end_dt)
    sql_base += """
        GROUP BY building, DATE_TRUNC('day', timestamp)::date
        ORDER BY building, DATE_TRUNC('day', timestamp)::date;
    """

    with conn_ts.cursor() as cur:
        cur.execute(sql_base, params)
        rows = cur.fetchall()

    logging.info("Aggregated %d daily rows.", len(rows))
    return rows

def upsert_analytics(conn_pg, rows: List[Tuple]):
    """
    Upsert aggregated rows into analytics_data.
    ON CONFLICT (building, date) DO UPDATE.
    """
    if not rows:
        logging.info("No rows to upsert.")
        return

    ensure_unique_index_on_analytics(conn_pg)

    insert_sql = """
        INSERT INTO analytics_data (building, date, avg_temperature, avg_humidity, occupancy_rate)
        VALUES %s
        ON CONFLICT (building, date) DO UPDATE SET
            avg_temperature = EXCLUDED.avg_temperature,
            avg_humidity    = EXCLUDED.avg_humidity,
            occupancy_rate  = EXCLUDED.occupancy_rate;
    """

    with conn_pg.cursor() as cur:
        execute_values(cur, insert_sql, rows, page_size=1000)
    conn_pg.commit()
    logging.info("Upserted %d rows into analytics_data.", len(rows))

# ---------- CLI ----------

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Transform raw sensor data (TimescaleDB) into daily analytics (PostgreSQL)."
    )
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD or ISO). Optional.", default=None)
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD or ISO). Optional (exclusive).", default=None)
    args = parser.parse_args()

    start_dt = parse_date(args.start_date)
    end_dt   = parse_date(args.end_date)

    # Connect
    conn_ts = get_timescale_conn()
    conn_pg = get_pg_conn()

    try:
        # If no dates provided, auto-detect
        if not start_dt or not end_dt:
            min_dt, max_dt = fetch_date_bounds(conn_ts)
            if not min_dt or not max_dt:
                logging.error("No data found in sensor_data. Generate data first.")
                sys.exit(1)
            start_dt = start_dt or min_dt
            # Make end_dt exclusive by adding +1 day if only date part was given
            end_dt = end_dt or (max_dt.replace(hour=23, minute=59, second=59, microsecond=999999))
            logging.info("Using date range: %s to %s", start_dt.isoformat(), end_dt.isoformat())

        # Compute & Upsert
        rows = compute_daily_aggregates(conn_ts, start_dt, end_dt)
        upsert_analytics(conn_pg, rows)

    finally:
        conn_ts.close()
        conn_pg.close()

if __name__ == "__main__":
    main()
