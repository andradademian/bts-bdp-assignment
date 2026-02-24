from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s6 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s6",
    tags=["s6"],
)


class AircraftPosition(BaseModel):
    icao: str
    registration: str | None = None
    type: str | None = None
    lat: float
    lon: float
    alt_baro: float | None = None
    ground_speed: float | None = None
    timestamp: str


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB.

    Use the BDI_MONGO_URL environment variable to configure the connection.
    Start MongoDB with: make mongo
    Database name: bdi_aircraft
    Collection name: positions
    """
    # TODO: Connect to MongoDB using pymongo.MongoClient(settings.mongo_url)
    # TODO: Insert the position document into the 'positions' collection
    # TODO: Return {"status": "ok"}
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return aggregated statistics: count of positions grouped by aircraft type.

    Response example: [{"type": "B738", "count": 42}, {"type": "A320", "count": 38}]

    Use MongoDB's aggregation pipeline with $group.
    """
    # TODO: Connect to MongoDB
    # TODO: Use collection.aggregate() with $group on 'type' field
    # TODO: Return list sorted by count descending
    return []


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[
        int,
        Query(description="Page number (1-indexed)", ge=1),
    ] = 1,
    page_size: Annotated[
        int,
        Query(description="Number of results per page", ge=1, le=100),
    ] = 20,
) -> list[dict]:
    """List all aircraft with pagination.

    Each result should include: icao, registration, type.
    Use MongoDB's skip() and limit() for pagination.
    """
    # TODO: Connect to MongoDB
    # TODO: Query distinct aircraft, apply skip/limit for pagination
    # TODO: Return list of dicts with icao, registration, type
    return []


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position data for a specific aircraft.

    Return the most recent document matching the given ICAO code.
    If not found, return 404.
    """
    # TODO: Connect to MongoDB
    # TODO: Find the latest document for this icao (sort by timestamp descending)
    # TODO: Return 404 if not found
    return {}


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Remove all position records for an aircraft.

    Returns the number of deleted documents.
    """
    # TODO: Connect to MongoDB
    # TODO: Delete all documents matching the icao
    # TODO: Return {"deleted": <count>}
    return {"deleted": 0}
