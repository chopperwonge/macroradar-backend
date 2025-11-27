import os
from datetime import date
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

# Use your Supabase Postgres connection string as DATABASE_URL on Render
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="MacroRadar API", version="0.2")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL env var not set")
    return psycopg2.connect(DATABASE_URL)


class Observation(BaseModel):
    date: date
    value: float


class SeriesResponse(BaseModel):
    id: str
    title: str
    unit: str
    source: str
    latest: Optional[Observation]
    recent: List[Observation]


@app.get("/healthz")
def health():
    return {"ok": True}


@app.get("/series/{series_id}")
def get_series(series_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

            # --- Fetch series metadata (title, unit, source) ---
            cur.execute("""
                select id, title, source, unit
                from public.series
                where id=%s
            """, (series_id,))
            row = cur.fetchone()

            if not row:
                raise HTTPException(status_code=404, detail="Series not found")

            # --- Fetch extended metadata from indicator_metadata ---
            cur.execute("""
                select 
                    category,
                    description,
                    frequency,
                    unit_display,
                    source_url,
                    methodology_url,
                    release_schedule,
                    country,
                    display_priority
                from public.indicator_metadata
                where id=%s
            """, (series_id,))
            meta = cur.fetchone()

            metadata = None
            if meta:
                metadata = {
                    "category": meta["category"],
                    "description": meta["description"],
                    "frequency": meta["frequency"],
                    "unit_display": meta["unit_display"],
                    "source_url": meta["source_url"],
                    "methodology_url": meta["methodology_url"],
                    "release_schedule": meta["release_schedule"],
                    "country": meta["country"],
                    "display_priority": meta["display_priority"],
                }

            # --- Fetch full history ---
            cur.execute("""
                select date, value
                from public.observations
                where series_id=%s
                order by date asc
            """, (series_id,))
            full_rows = cur.fetchall()

            full = [
                {"date": r["date"], "value": float(r["value"])}
                for r in full_rows
            ]

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
                "metadata": metadata,     # ⭐ NEW FIELD
            }

    finally:
        conn.close()


@app.post("/refresh/{series_id}")
@app.get("/refresh/{series_id}")
def refresh(series_id: str):
    return {
        "ok": True,
        "mode": "manual",
        "message": "Update data directly in Supabase (public.observations)."
    }