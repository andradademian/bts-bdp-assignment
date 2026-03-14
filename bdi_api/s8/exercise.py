import sqlite3
import requests
from fastapi import APIRouter, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s8 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s8",
    tags=["s8"],
)

DB_PATH = r"C:\Users\andra\Documents\GitHub\bts-bdp-assignment\aircraft.db"
FUEL_RATES_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"


class AircraftReturn(BaseModel):
    icao: str
    registration: str | None
    type: str | None
    owner: str | None
    manufacturer: str | None
    model: str | None


class AircraftCO2Return(BaseModel):
    icao: str
    hours_flown: float
    co2: float | None


@s8.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[AircraftReturn]:
    """List all aircraft with enriched data, ordered by ICAO ascending."""
    conn = sqlite3.connect(DB_PATH)
    offset = page * num_results
    rows = conn.execute(
        """
        SELECT icao, registration, type, owner, manufacturer, model
        FROM aircraft
        ORDER BY icao ASC
        LIMIT ? OFFSET ?
        """,
        (num_results, offset),
    ).fetchall()
    conn.close()

    return [
        AircraftReturn(
            icao=r[0],
            registration=r[1],
            type=r[2],
            owner=r[3],
            manufacturer=r[4],
            model=r[5],
        )
        for r in rows
    ]


@s8.get("/aircraft/{icao}/co2")
def get_aircraft_co2(icao: str, day: str) -> AircraftCO2Return:
    """Calculate CO2 emissions for a given aircraft on a specific day."""

    # Count observations for this ICAO
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT type FROM aircraft WHERE icao = ?", (icao,)
    ).fetchone()
    conn.close()

    if not row:
        return AircraftCO2Return(icao=icao, hours_flown=0.0, co2=None)

    aircraft_type = row[0]

    # Count observations from bronze files for this day
    import os
    import gzip
    import json

    bronze_dir = "/tmp/bronze/aircraft"
    observations = 0

    for filename in os.listdir(bronze_dir):
        if not filename.endswith(".json.gz"):
            continue
        filepath = os.path.join(bronze_dir, filename)
        try:
            with gzip.open(filepath, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, gzip.BadGzipFile):
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

        if not data or "aircraft" not in data:
            continue

        for ac in data["aircraft"]:
            if ac.get("hex", "").lower() == icao.lower():
                observations += 1

    # Calculate hours flown
    hours_flown = (observations * 5) / 3600

    # Look up fuel consumption rate
    fuel_rates = requests.get(FUEL_RATES_URL, timeout=30).json()
    galph = None
    if aircraft_type and aircraft_type in fuel_rates:
        galph = fuel_rates[aircraft_type].get("galph")

    # Calculate CO2
    co2 = None
    if galph is not None:
        fuel_used_kg = hours_flown * galph * 3.04
        co2 = (fuel_used_kg * 3.15) / 907.185

    return AircraftCO2Return(icao=icao, hours_flown=hours_flown, co2=co2)