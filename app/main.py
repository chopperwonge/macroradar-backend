import os
from datetime import date
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

# Render / Supabase connection
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="MacroBiscuit API", version="0.3")


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
# Healthcheck
# ---------------------------------------------------------

@app.get("/healthz")
def health():
    return {"ok": True}


# ---------------------------------------------------------
# NEW: Keep-alive ping endpoint for Render
# ---------------------------------------------------------

@app.get("/ping")
def ping():
    """
    Lightweight endpoint to keep the Render service from idling.
    Used by the Next.js frontend every few minutes.
    """
    return {"status": "alive"}


# ---------------------------------------------------------
# Get single indicator with full timeseries
# ---------------------------------------------------------

@app.get("/series/{series_id}")
def get_series(series_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # --- Base metadata ---
            cur.execute("""
                SELECT id, title, source, unit
                FROM public.series
                WHERE id = %s
            """, (series_id,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Series not found")

            # --- Extended metadata ---
            cur.execute("""
                SELECT 
                    category,
                    description,
                    frequency,
                    unit_display,
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

            metadata = None
            if meta:
                metadata = dict(meta)

            # --- Full timeseries ---
            cur.execute("""
                SELECT date, value
                FROM public.observations
                WHERE series_id = %s
                ORDER BY date ASC
            """, (series_id,))
            full_rows = cur.fetchall()

            full = [{"date": r["date"], "value": float(r["value"])} for r in full_rows]

            latest = full[-1] if full else None
            recent = full[-120:] if len(full) > 120 else full

            return {
                "id": row["id"],
                "title": row["title"],
                "source": row["source"],
                "unit": row["unit"],
                "latest": latest,
                "recent": recent,
                "full": full,
                "metadata": metadata,
            }

    finally:
        conn.close()


# ---------------------------------------------------------
# NEW: List all indicators (frontend uses this)
# ---------------------------------------------------------

@app.get("/indicators")
def list_indicators():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            cur.execute("""
                SELECT
                    s.id,
                    s.title,
                    s.source,
                    s.unit,
                    m.category,
                    m.description,
                    m.frequency,
                    m.unit_display,
                    m.release_schedule,
                    m.country,
                    m.display_priority,
                    m.decimal_places
                FROM public.series s
                LEFT JOIN public.indicator_metadata m
                ON s.id = m.id
                ORDER BY m.display_priority NULLS LAST, s.id
            """)

            rows = cur.fetchall()

            result = []
            for r in rows:
                result.append({
                    "id": r["id"],
                    "title": r["title"],
                    "source": r["source"],
                    "unit": r["unit"],
                    "metadata": {
                        "category": r["category"],
                        "description": r["description"],
                        "frequency": r["frequency"],
                        "unit_display": r["unit_display"],
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
# Manual refresh endpoint
# ---------------------------------------------------------

@app.get("/refresh/{series_id}")
@app.post("/refresh/{series_id}")
def refresh(series_id: str):
    return {
        "ok": True,
        "mode": "manual",
        "message": "Update data directly in Supabase (public.observations)."
    }
