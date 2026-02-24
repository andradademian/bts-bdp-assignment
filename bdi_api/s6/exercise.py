from typing import Annotated

from fastapi import APIRouter, HTTPException, status
from fastapi.params import Query
from pydantic import BaseModel
from pymongo import MongoClient, DESCENDING

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


def get_collection():
    """Connect to MongoDB and return the positions collection."""
    client = MongoClient(settings.mongo_url)
    db = client["bdi_aircraft"]
    return db["positions"]


@s6.post("/aircraft")
def create_aircraft(position: AircraftPosition) -> dict:
    """Store an aircraft position document in MongoDB."""
    collection = get_collection()
    # Convert Pydantic model to dict and insert
    collection.insert_one(position.model_dump())
    return {"status": "ok"}


@s6.get("/aircraft/stats")
def aircraft_stats() -> list[dict]:
    """Return count of positions grouped by aircraft type, sorted descending."""
    collection = get_collection()
    pipeline = [
        # Group all documents by 'type' field and count them
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": DESCENDING}},
        # Rename _id to type for cleaner output
        {"$project": {"_id": 0, "type": "$_id", "count": 1}},
    ]
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/")
def list_aircraft(
    page: Annotated[int, Query(description="Page number (1-indexed)", ge=1)] = 1,
    page_size: Annotated[int, Query(description="Number of results per page", ge=1, le=100)] = 20,
) -> list[dict]:
    """List distinct aircraft with pagination (icao, registration, type)."""
    collection = get_collection()
    skip = (page - 1) * page_size

    # Use aggregation to get one document per icao (distinct aircraft)
    pipeline = [
        {"$group": {
            "_id": "$icao",
            "registration": {"$first": "$registration"},
            "type": {"$first": "$type"},
        }},
        {"$project": {"_id": 0, "icao": "$_id", "registration": 1, "type": 1}},
        {"$sort": {"icao": 1}},
        {"$skip": skip},
        {"$limit": page_size},
    ]
    return list(collection.aggregate(pipeline))


@s6.get("/aircraft/{icao}")
def get_aircraft(icao: str) -> dict:
    """Get the latest position document for a specific aircraft."""
    collection = get_collection()
    # Sort by timestamp descending to get the most recent entry
    doc = collection.find_one({"icao": icao}, sort=[("timestamp", DESCENDING)])
    if not doc:
        raise HTTPException(status_code=404, detail="Aircraft not found")
    doc.pop("_id")  # Remove MongoDB internal ID before returning
    return doc


@s6.delete("/aircraft/{icao}")
def delete_aircraft(icao: str) -> dict:
    """Delete all position records for an aircraft."""
    collection = get_collection()
    result = collection.delete_many({"icao": icao})
    return {"deleted": result.deleted_count}