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


@app.get("/api/arrests")
async def get_arrests(month: int = Query(...), year: int = Query(...)):
    results = []
    offset = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
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
