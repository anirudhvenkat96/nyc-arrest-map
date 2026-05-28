import asyncio
import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

NYC_ARRESTS_URL = "https://data.cityofnewyork.us/resource/8h9b-rp9u.json"
PAGE_SIZE = 5000
MAX_PAGES = 6          # 30,000 records max per month
PAGE_TIMEOUT = 30.0    # seconds per page request
_YEAR_SEMAPHORE = asyncio.Semaphore(4)  # max concurrent month fetches


@app.get("/api/arrests")
async def get_arrests(month: int = Query(...), year: int = Query(...)):
    results = []
    offset = 0

    async with httpx.AsyncClient(timeout=PAGE_TIMEOUT) as client:
        while offset // PAGE_SIZE < MAX_PAGES:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$where": f"date_extract_m(arrest_date)={month} AND date_extract_y(arrest_date)={year}",
            }
            response = await client.get(NYC_ARRESTS_URL, params=params)
            response.raise_for_status()
            page = response.json()

            if not page:
                break

            for record in page:
                lat = record.get("latitude")
                lon = record.get("longitude")
                if lat and lon:
                    results.append(
                        {
                            "arrest_date": record.get("arrest_date"),
                            "latitude": float(lat),
                            "longitude": float(lon),
                            "law_cat_cd": record.get("law_cat_cd"),
                        }
                    )

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

    return results


async def _fetch_month(client: httpx.AsyncClient, year: int, month: int) -> list[dict]:
    records = []
    offset = 0
    async with _YEAR_SEMAPHORE:
        while offset // PAGE_SIZE < MAX_PAGES:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$where": f"date_extract_m(arrest_date)={month} AND date_extract_y(arrest_date)={year}",
            }
            response = await client.get(NYC_ARRESTS_URL, params=params)
            response.raise_for_status()
            page = response.json()

            if not page:
                break

            for record in page:
                lat = record.get("latitude")
                lon = record.get("longitude")
                if lat and lon:
                    records.append({"latitude": float(lat), "longitude": float(lon)})

            if len(page) < PAGE_SIZE:
                break

            offset += PAGE_SIZE

    return records


@app.get("/api/arrests/year")
async def get_arrests_year(year: int = Query(...)):
    async with httpx.AsyncClient(timeout=PAGE_TIMEOUT) as client:
        results = await asyncio.gather(
            *[_fetch_month(client, year, month) for month in range(1, 13)]
        )

    by_month = {month: records for month, records in zip(range(1, 13), results)}
    summary = {month: len(records) for month, records in by_month.items()}

    return {"by_month": by_month, "summary": summary}
