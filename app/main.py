import os
from datetime import date
from typing import List, Optional
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
    return {"status": "alive"}


# ---------------------------------------------------------
# Get single indicator with full metadata + timeseries
# ---------------------------------------------------------

@app.get("/series/{series_id}")
def get_series(series_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # --- Fetch title from public.indicators ---
            cur.execute("""
                SELECT title
                FROM public.indicators
                WHERE id = %s
            """, (series_id,))
            row_title = cur.fetchone()

            if not row_title:
                raise HTTPException(status_code=404, detail="Indicator not found")

            title = row_title["title"]

            # --- Extended metadata (includes source + unit + unit_display + decimals) ---
            cur.execute("""
                SELECT 
                    category,
                    description,
                    frequency,
                    unit_display,
                    unit,
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

            # --- Full timeseries ---
            cur.execute("""
                SELECT date, value
                FROM public.observations
                WHERE series_id = %s
                ORDER BY date ASC
            """, (series_id,))
            full_rows = cur.fetchall()

            full = [
                {"date": r["date"], "value": float(r["value"])}
                for r in full_rows
            ]

            latest = full[-1] if full else None
            recent = full[-120:] if len(full) > 120 else full

            return {
                "id": series_id,
                "title": title,
                "source": metadata.get("source"),
                "unit": metadata.get("unit"),
                "latest": latest,
                "recent": recent,
                "full": full,
                "metadata": metadata,
            }

    finally:
        conn.close()


# ---------------------------------------------------------
# List all indicators (for the indicators page)
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
                    m.source,
                    m.unit,
                    m.category,
                    m.description,
                    m.frequency,
                    m.unit_display,
                    m.release_schedule,
                    m.country,
                    m.display_priority,
                    m.decimal_places
                FROM public.indicators i
                LEFT JOIN public.indicator_metadata m
                    ON i.id = m.id
                ORDER BY m.display_priority NULLS LAST, i.id
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
