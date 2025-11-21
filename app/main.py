import os
from datetime import date
from typing import List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
import psycopg2.extras

# Use your Supabase Postgres connection string as DATABASE_URL on Render
DATABASE_URL = os.getenv("DATABASE_URL")

app = FastAPI(title="MacroRadar API", version="0.1")


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

            # --- Fetch series metadata ---
            cur.execute("""
                select id, title, source, unit 
                from public.series 
                where id=%s
            """, (series_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Series not found")

            # --- Fetch description from indicators table ---
            cur.execute("""
                select description
                from public.indicators
                where id=%s
            """, (series_id,))
            desc_row = cur.fetchone()
            description = desc_row["description"] if desc_row else None

            # --- Fetch full historical series (ascending order) ---
            cur.execute("""
                select date, value
                from public.observations
                where series_id=%s
                order by date asc
            """, (series_id,))
            full_rows = cur.fetchall()
            full = [{"date": r["date"], "value": float(r["value"])} for r in full_rows]

            # --- latest ---
            latest = full[-1] if full else None

            # --- recent (last 120 months) ---
            recent = full[-120:] if len(full) > 120 else full

            return {
                "id": row["id"],
                "title": row["title"],
                "source": row["source"],
                "unit": row["unit"],
                "latest": latest,
                "recent": recent,
                "full": full,
                "description": description  # ⭐ added field
            }

    finally:
        conn.close()


@app.post("/refresh/{series_id}")
@app.get("/refresh/{series_id}")
def refresh(series_id: str):
    """
    Manual Mode enabled:
    No automatic data fetching is performed.
    Please update the observations directly in Supabase.
    """
    return {
        "ok": True,
        "mode": "manual",
        "message": "Update data directly in Supabase (public.observations)."
    }
