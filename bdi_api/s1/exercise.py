import os
import shutil
from typing import Annotated
import sqlite3
import requests

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings

settings = Settings()

s1 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Something is wrong with the request"
        },
    },
    prefix="/api/s1",
    tags=["s1"],
)


# DOWNLOAD RAW FILES


@s1.post("/aircraft/download")
def download_data(
    file_limit: Annotated[
        int,
        Query(
            ...,
            description="""
Limits the number of files to download.
You must always start from the first page returns and
go in ascending order.
""",
        ),
    ] = 100,
) -> str:
    """
    Downloads the `file_limit` files from ADS-B Exchange
    and saves them locally as .json.gz in raw/day=20231101
    """

    download_dir = os.path.join(settings.raw_dir, "day=20231101")
    base_url = settings.source_url + "/2023/11/01/"

    # Clean folder before downloading
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir, exist_ok=True)

    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]

    for filename in filenames:
        url = f"{settings.source_url}/2023/11/01/{filename}"
        response = requests.get(url)
        if response.status_code == 200:
            path = os.path.join(download_dir, filename)
            with open(path, "wb") as f:
                f.write(response.content)
        else:
            print(f"Skipping {filename}, HTTP {response.status_code}")

    return "OK"



# PREPARE DATA (store in SQLite database)

@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Reads raw .json or .json.gz files and stores aircraft and positions
    in a SQLite database for fast querying by S1 endpoints.
    """
    import os
    import shutil
    import gzip
    import json
    import pandas as pd
    import sqlite3

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")

    if not os.path.exists(raw_dir):
        raise RuntimeError(f"Raw folder does not exist: {raw_dir}")

    # Clean prepared folder
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    aircraft_rows = []
    position_rows = []

    files = [f for f in sorted(os.listdir(raw_dir)) if f.endswith((".json", ".json.gz"))]

    for filename in files:
        file_path = os.path.join(raw_dir, filename)
        data = None
        try:
            # Try gzip first
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, gzip.BadGzipFile):
            # Fallback: treat as plain JSON
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

        if not data or "aircraft" not in data:
            continue

        for ac in data["aircraft"]:
            icao = ac.get("hex")
            if not icao:
                continue

            aircraft_rows.append({
                "icao": icao,
                "registration": ac.get("r"),
                "type": ac.get("t"),
            })

            if ac.get("lat") is not None and ac.get("lon") is not None:
                position_rows.append({
                    "icao": icao,
                    "timestamp": ac.get("seen_pos"),
                    "lat": ac.get("lat"),
                    "lon": ac.get("lon"),
                    "alt_baro": ac.get("alt_baro"),
                    "gs": ac.get("gs"),
                    "emergency": ac.get("emergency"),
                })

    # Convert to DataFrames
    aircraft_df = pd.DataFrame(aircraft_rows).drop_duplicates(subset="icao", keep="last")
    positions_df = pd.DataFrame(position_rows)

    # Make sure DataFrames have columns, even if empty
    if aircraft_df.empty:
        aircraft_df = pd.DataFrame(columns=["icao", "registration", "type"])
    if positions_df.empty:
        positions_df = pd.DataFrame(columns=["icao", "timestamp", "lat", "lon", "alt_baro", "gs", "emergency"])

    # Save to SQLite
    conn = sqlite3.connect(db_file)
    aircraft_df.to_sql("aircraft", conn, if_exists="replace", index=False)
    positions_df.to_sql("positions", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Prepared {len(aircraft_df)} aircraft and {len(positions_df)} positions in {db_file}")
    return "OK"

# LIST AIRCRAFT

@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """
    List all aircraft ordered by ICAO ascending.
    Uses SQLite as backend database.
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")

    if not os.path.exists(db_file):
        return []

    conn = sqlite3.connect(db_file)
    start = page * num_results

    query = """
        SELECT icao, registration, type
        FROM aircraft
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, (num_results, start)).fetchall()
    conn.close()

    return [{"icao": r[0], "registration": r[1], "type": r[2]} for r in rows]


# AIRCRAFT POSITIONS

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(
    icao: str, num_results: int = 1000, page: int = 0
) -> list[dict]:
    """
    Returns all positions of an aircraft ordered by timestamp ascending.
    Uses SQLite as the backend database.
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")

    if not os.path.exists(db_file):
        return []

    conn = sqlite3.connect(db_file)
    start = page * num_results

    query = """
        SELECT timestamp, lat, lon, alt_baro, gs, emergency
        FROM positions
        WHERE icao = ?
        ORDER BY timestamp ASC
        LIMIT ? OFFSET ?
    """
    rows = conn.execute(query, (icao, num_results, start)).fetchall()
    conn.close()

    return [
        {
            "timestamp": r[0],
            "lat": r[1],
            "lon": r[2],
            "alt_baro": r[3],
            "gs": r[4],
            "emergency": bool(r[5]),
        }
        for r in rows
    ]



# AIRCRAFT STATISTICS

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict: #works
    """
    Returns max_altitude_baro, max_ground_speed, had_emergency.
    Uses SQLite as backend database.
    """
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")

    if not os.path.exists(db_file):
        return {
            "max_altitude_baro": None,
            "max_ground_speed": None,
            "had_emergency": False,
        }

    conn = sqlite3.connect(db_file)
    query = """
        SELECT 
            MAX(alt_baro) AS max_altitude_baro,
            MAX(gs) AS max_ground_speed,
            MAX(CASE WHEN emergency THEN 1 ELSE 0 END) AS had_emergency
        FROM positions
        WHERE icao = ?
    """
    row = conn.execute(query, (icao,)).fetchone()
    conn.close()

    return {
        "max_altitude_baro": row[0],
        "max_ground_speed": row[1],
        "had_emergency": bool(row[2]),
    }
