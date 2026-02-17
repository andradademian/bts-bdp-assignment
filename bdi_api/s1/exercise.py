import os
import shutil
import gzip
import json
from typing import Annotated

import requests
import pandas as pd
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

# =====================================================
# DOWNLOAD RAW FILES
# =====================================================

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

@s1.post("/aircraft/prepare")
def prepare_data() -> str:
    """
    Reads raw .json or .json.gz files and stores aircraft and positions
    in a DuckDB database for fast querying by S1 endpoints.
    """

    import os, shutil, gzip, json
    import duckdb

    raw_dir = os.path.join(settings.raw_dir, "day=20231101")
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")

    # Ensure raw folder exists
    if not os.path.exists(raw_dir):
        raise RuntimeError(f"Raw folder does not exist: {raw_dir}")

    files = [f for f in sorted(os.listdir(raw_dir)) if f.endswith((".json", ".json.gz"))]
    if not files:
        raise RuntimeError(f"No raw files found in {raw_dir}")

    # Clean prepared folder
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    # Prepare database
    conn = duckdb.connect(db_file)
    conn.execute("CREATE TABLE IF NOT EXISTS aircraft (icao TEXT, registration TEXT, type TEXT)")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS positions (
            icao TEXT, timestamp DOUBLE, lat DOUBLE, lon DOUBLE,
            alt_baro DOUBLE, gs DOUBLE, emergency BOOLEAN
        )
    """)

    for filename in files:
        file_path = os.path.join(raw_dir, filename)
        try:
            if filename.endswith(".gz"):
                with gzip.open(file_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
        except Exception as e:
            print(f"[WARN] Skipping {filename}: {e}")
            continue

        aircraft_rows = []
        position_rows = []

        for ac in data.get("aircraft", []):
            icao = ac.get("hex")
            if not icao:
                continue
            aircraft_rows.append((icao, ac.get("r"), ac.get("t")))

            if ac.get("lat") is not None and ac.get("lon") is not None:
                position_rows.append((
                    icao,
                    ac.get("seen_pos"),
                    ac.get("lat"),
                    ac.get("lon"),
                    ac.get("alt_baro"),
                    ac.get("gs"),
                    ac.get("emergency")
                ))

        # Insert rows into DuckDB
        if aircraft_rows:
            conn.executemany("INSERT INTO aircraft VALUES (?, ?, ?)", aircraft_rows)
        if position_rows:
            conn.executemany("INSERT INTO positions VALUES (?, ?, ?, ?, ?, ?, ?)", position_rows)

    conn.close()
    print(f"Prepared database with DuckDB at {db_file}")
    return "OK"


# =====================================================
# LIST AIRCRAFT
# =====================================================

@s1.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """
    List all aircraft ordered by ICAO ascending.
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    aircraft_path = os.path.join(prepared_dir, "aircraft.parquet")

    if not os.path.exists(aircraft_path):
        return []

    df = pd.read_parquet(aircraft_path)
    df = df.sort_values("icao")

    start = page * num_results
    end = start + num_results

    return df.iloc[start:end].to_dict(orient="records")


# =====================================================
# AIRCRAFT POSITIONS
# =====================================================

@s1.get("/aircraft/{icao}/positions")
def get_aircraft_position(
    icao: str, num_results: int = 1000, page: int = 0
) -> list[dict]:
    """
    Returns all positions of an aircraft ordered by timestamp ascending.
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    positions_path = os.path.join(prepared_dir, "positions.parquet")

    if not os.path.exists(positions_path):
        return []

    df = pd.read_parquet(positions_path)
    df = df[df["icao"] == icao].sort_values("timestamp")

    if df.empty:
        return []

    start = page * num_results
    end = start + num_results

    return df.iloc[start:end][
        ["timestamp", "lat", "lon"]
    ].to_dict(orient="records")


# =====================================================
# AIRCRAFT STATISTICS
# =====================================================

@s1.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict: #works
    """
    Returns max_altitude_baro, max_ground_speed, had_emergency
    """

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    positions_path = os.path.join(prepared_dir, "positions.parquet")

    if not os.path.exists(positions_path):
        return {
            "max_altitude_baro": None,
            "max_ground_speed": None,
            "had_emergency": False,
        }

    df = pd.read_parquet(positions_path)
    df = df[df["icao"] == icao]

    if df.empty:
        return {
            "max_altitude_baro": None,
            "max_ground_speed": None,
            "had_emergency": False,
        }

    return {
        "max_altitude_baro": df["alt_baro"].max(),
        "max_ground_speed": df["gs"].max(),
        "had_emergency": df["emergency"].notna().any(),
    }
