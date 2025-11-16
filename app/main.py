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

# Dummy values for testing different series
DUMMY_VALUES = {
    "cpi": 2.4,
    "unemp": 4.3,
    # You can add more later:
    # "wages": 5.1,
    # "gdp": 0.2,
}

@app.get("/series/{series_id}", response_model=SeriesResponse)
def get_series(series_id: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("select id,title,source,unit from public.series where id=%s", (series_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Series not found")

            cur.execute("""
                select date, value
                from public.observations
                where series_id=%s
                order by date desc
                limit 120
            """, (series_id,))
            obs = cur.fetchall()
            recent = [{"date": r["date"], "value": float(r["value"])} for r in obs][::-1]  # chronological
            latest = recent[-1] if recent else None

            return {
                "id": row["id"],
                "title": row["title"],
                "source": row["source"],
                "unit": row["unit"],
                "latest": latest,
                "recent": recent
            }
    finally:
        conn.close()

@app.post("/refresh/{series_id}")
@app.get("/refresh/{series_id}")   # allow GET from browser too
def refresh(series_id: str):

    """
    v1 stub so you can see the pipeline.
    Later, this will fetch from ONS/BoE and insert the real value.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # ensure the series exists
            cur.execute("select 1 from public.series where id=%s", (series_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Series not found")

            # pick the correct dummy value for the series
            dummy = DUMMY_VALUES.get(series_id, 2.4)

            # check if data for today already exists
            cur.execute("""
                select 1 from public.observations
                where series_id=%s and date=%s
            """, (series_id, date.today()))
            exists = cur.fetchone()

            # insert a dummy data point for today if missing
            if not exists:
                cur.execute("""
                    insert into public.observations(series_id, date, value)
                    values (%s, %s, %s)
                """, (series_id, date.today(), dummy))
                conn.commit()

        return {"ok": True}
    finally:
        conn.close()

