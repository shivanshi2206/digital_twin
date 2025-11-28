
from fastapi import FastAPI, Query, Header, HTTPException
from typing import Optional, List
from datetime import datetime
from fastapi.responses import PlainTextResponse
import psycopg2
import os

# ---------------- Config ----------------
# TimescaleDB (raw)
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "timescaledb_poc")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB   = os.getenv("TIMESCALE_DB", "sensordata")
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "admin")
TIMESCALE_PASS = os.getenv("TIMESCALE_PASS", "admin")

# PostgreSQL (analytics)
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "admin")
PG_PASS = os.getenv("PG_PASS", "admin")

# API key
API_KEY = os.getenv("API_KEY", "supersecretkey123")

# ---------------- FastAPI App ----------------
app = FastAPI(
    title="Digital Twin Data API",
    description="Access raw sensor data (TimescaleDB) and daily analytics (PostgreSQL).",
    version="1.0.0",
)

# ---------------- Helpers ----------------
def require_api_key(x_api_key: Optional[str]):
    if API_KEY and (x_api_key != API_KEY):
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

def ts_conn():
    return psycopg2.connect(
        host=TIMESCALE_HOST, port=TIMESCALE_PORT,
        dbname=TIMESCALE_DB, user=TIMESCALE_USER, password=TIMESCALE_PASS
    )

def pg_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )

def rows_to_dicts(cursor, rows):
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, r)) for r in rows]

def parse_dt(dt_str: str) -> datetime:
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Invalid datetime format: {dt_str}. Use YYYY-MM-DD or ISO8601.")

# ---------------- Health ----------------
@app.get("/health", summary="Health check")
def health():
    try:
        with ts_conn() as cts, pg_conn() as cpg:
            pass
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------- Metadata ----------------
@app.get("/buildings", summary="List distinct buildings from sensor_data")
def list_buildings(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_api_key(x_api_key)
    with ts_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT building FROM sensor_data ORDER BY building;")
            rows = cur.fetchall()
    return {"buildings": [r[0] for r in rows]}

# ---------------- Raw Stats (TimescaleDB) ----------------
@app.get("/raw-stats", summary="Get raw data stats from TimescaleDB")
def raw_stats(x_api_key: Optional[str] = Header(default=None, alias="X-API-Key")):
    require_api_key(x_api_key)
    with ts_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sensor_data;")
            total = cur.fetchone()[0]
            cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM sensor_data;")
            min_ts, max_ts = cur.fetchone()
            cur.execute("SELECT building, COUNT(*) FROM sensor_data GROUP BY building ORDER BY building;")
            per_building = cur.fetchall()
    return {
        "total_rows": total,
        "min_timestamp": min_ts.isoformat() if min_ts else None,
        "max_timestamp": max_ts.isoformat() if max_ts else None,
        "rows_per_building": [{"building": b, "rows": c} for (b, c) in per_building]
    }

# ---------------- Raw Data (TimescaleDB) ----------------
@app.get("/raw-data", summary="Get raw sensor data from TimescaleDB")
def get_raw_data(
    start: Optional[str] = Query(default=None, description="Start datetime (ISO8601 or YYYY-MM-DD). Optional."),
    end: Optional[str] = Query(default=None, description="End datetime (exclusive). Optional."),
    building: Optional[str] = Query(default=None, description="Filter by building name. Optional."),
    limit: int = Query(default=500, ge=1, le=10000, description="Max rows to return."),
    offset: int = Query(default=0, ge=0, description="Offset for pagination."),
    order: str = Query(default="asc", pattern="^(asc|desc)$", description="Sort by timestamp asc/desc."),
    format: str = Query(default="json", pattern="^(json|csv)$", description="Response format."),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key", description="API key header"),
):
    require_api_key(x_api_key)
    start_dt = parse_dt(start) if start else None
    end_dt   = parse_dt(end) if end else None

    sql = """
        SELECT id, building, timestamp, temperature, humidity, occupancy
        FROM sensor_data
        WHERE 1=1
    """
    params: List = []
    if start_dt:
        sql += " AND timestamp >= %s"
        params.append(start_dt)
    if end_dt:
        sql += " AND timestamp < %s"
        params.append(end_dt)
    if building:
        sql += " AND building = %s"
        params.append(building)

    sql += f" ORDER BY timestamp {'ASC' if order=='asc' else 'DESC'}"
    sql += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with ts_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            if format == "csv":
                header = "id,building,timestamp,temperature,humidity,occupancy"
                lines = [header] + [
                    f"{r[0]},{r[1]},{r[2].isoformat()},{r[3]},{r[4]},{r[5]}"
                    for r in rows
                ]
                return PlainTextResponse("\n".join(lines), media_type="text/csv")
            else:
                data = rows_to_dicts(cur, rows)
                return {"count": len(data), "items": data}

# ---------------- Analytics (PostgreSQL) ----------------
@app.get("/analytics", summary="Get daily analytics from PostgreSQL (analytics_data)")
def get_analytics(
    start_date: Optional[str] = Query(default=None, description="Start date (YYYY-MM-DD). Optional."),
    end_date: Optional[str] = Query(default=None, description="End date (exclusive, YYYY-MM-DD). Optional."),
    building: Optional[str] = Query(default=None, description="Filter by building. Optional."),
    limit: int = Query(default=500, ge=1, le=10000, description="Max rows to return."),
    offset: int = Query(default=0, ge=0, description="Offset for pagination."),
    order: str = Query(default="asc", pattern="^(asc|desc)$", description="Sort order by date."),
    format: str = Query(default="json", pattern="^(json|csv)$", description="Response format."),
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key", description="API key header"),
):
    require_api_key(x_api_key)

    start_dt = parse_dt(start_date) if start_date else None
    end_dt   = parse_dt(end_date) if end_date else None

    sql = """
        SELECT id, building, date, avg_temperature, avg_humidity, occupancy_rate
        FROM analytics_data
        WHERE 1=1
    """
    params: List = []
    if start_dt:
        sql += " AND date >= %s"
        params.append(start_dt.date())
    if end_dt:
        sql += " AND date < %s"
        params.append(end_dt.date())
    if building:
        sql += " AND building = %s"
        params.append(building)

    sql += f" ORDER BY date {'ASC' if order=='asc' else 'DESC'}"
    sql += " LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            if format == "csv":
                header = "id,building,date,avg_temperature,avg_humidity,occupancy_rate"
                lines = [header] + [
                    f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]},{r[5]}"
                    for r in rows
                ]
                return PlainTextResponse("\n".join(lines), media_type="text/csv")
            else:
                data = rows_to_dicts(cur, rows)
                return {"count": len(data), "items": data}
