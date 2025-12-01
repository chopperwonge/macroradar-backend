import os
from datetime import date
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

# Render / Supabase connection
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="MacroBiscuit API", version="0.4")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set")
    return psycopg2.connect(DATABASE_URL)


# ---------------------------------------------------------
# Models
# ---------------------------------------------------------

class Observation(BaseModel):
    date: date
    value: float


# ---------------------------------------------------------
# Health + Ping
# ---------------------------------------------------------

@app.get("/healthz")
def health():
    return {"ok": True}


@app.get("/ping")
def ping():
    return {"status": "alive"}


# ---------------------------------------------------------
# Get single indicator (canonical)
# ---------------------------------------------------------

@app.get("/series/{series_id}")
def get_series(series_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # --- Get title + description (canonical)
            cur.execute("""
                SELECT title, description 
                FROM public.indicators
                WHERE id = %s
            """, (series_id,))
            base = cur.fetchone()

            if not base:
                raise HTTPException(status_code=404, detail="Indicator not found")

            # --- Get metadata
            cur.execute("""
                SELECT 
                    category,
                    frequency,
                    unit_display,
                    source,
                    source_url,
                    methodology_url,
                    release_schedule,
                    country,
                    display_priority,
                    decimal_places
                FROM public.indicator_metadata
                WHERE id = %s
            """, (series_id,))
            meta = cur.fetchone()

            metadata = dict(meta) if meta else {}

            # --- Get full timeseries
            cur.execute("""
                SELECT date, value
                FROM public.observations
                WHERE series_id = %s
                ORDER BY date ASC
            """, (series_id,))
            rows = cur.fetchall()

            full = [{"date": r["date"], "value": float(r["value"])} for r in rows]
            latest = full[-1] if full else None
            recent = full[-120:] if len(full) > 120 else full

            return {
                "id": series_id,
                "title": base["title"],
                "description": base["description"],
                "unit": metadata.get("unit_display"),
                "source": metadata.get("source"),
                "latest": latest,
                "recent": recent,
                "full": full,
                "metadata": metadata,
            }

    finally:
        conn.close()


# ---------------------------------------------------------
# List all indicators
# ---------------------------------------------------------

@app.get("/indicators")
def list_indicators():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("""
                SELECT 
                    i.id,
                    i.title,
                    i.description,
                    m.category,
                    m.frequency,
                    m.unit_display,
                    m.source,
                    m.source_url,
                    m.release_schedule,
                    m.country,
                    m.display_priority,
                    m.decimal_places
                FROM public.indicators i
                LEFT JOIN public.indicator_metadata m ON i.id = m.id
                ORDER BY m.display_priority NULLS LAST, i.id
            """)

            rows = cur.fetchall()

            result = []
            for r in rows:
                result.append({
                    "id": r["id"],
                    "title": r["title"],
                    "description": r["description"],
                    "metadata": {
                        "category": r["category"],
                        "frequency": r["frequency"],
                        "unit_display": r["unit_display"],
                        "source": r["source"],
                        "source_url": r["source_url"],
                        "release_schedule": r["release_schedule"],
                        "country": r["country"],
                        "display_priority": r["display_priority"],
                        "decimal_places": r["decimal_places"],
                    }
                })

            return result

    finally:
        conn.close()


# ---------------------------------------------------------
# Manual refresh
# ---------------------------------------------------------

@app.get("/refresh/{series_id}")
@app.post("/refresh/{series_id}")
def refresh(series_id: str):
    return {
        "ok": True,
        "message": "Update data directly in Supabase (public.observations)."
    }
