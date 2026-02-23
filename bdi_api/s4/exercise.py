from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
import boto3
import requests

# Load app settings (S3 bucket name, source URL, local paths, etc.)
settings = Settings()

# Create a FastAPI router with a shared prefix and error responses
# All endpoints here will be under /api/s4
s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


# ENDPOINT 1: DOWNLOAD
# This endpoint fetches raw ADS-B aircraft JSON files from a public web source
# and stores them directly into an S3 bucket for later processing.
@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[int, Query(...)] = 100,  # Max number of files to download (default: 100)
) -> str:

    # Build the base URL pointing to Nov 1, 2023 ADS-B snapshot data
    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket

    # S3 destination path — partitioned by date for easy querying later
    s3_prefix_path = "raw/day=20231101/"

    # Create the S3 client using boto3 (uses IAM role or env credentials automatically)
    s3 = boto3.client("s3")

    # Generate one filename per hour of the day (00:00Z to 23:00Z)
    # Files are named like "000000Z.json.gz", "010000Z.json.gz", etc.
    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]

    count = 0
    for filename in filenames:
        # Stop once we've hit the requested file limit
        if count >= file_limit:
            break

        url = base_url + filename
        response = requests.get(url)

        if response.status_code == 200:
            # Upload the raw gzipped file content directly to S3 (no local disk needed)
            s3_key = s3_prefix_path + filename
            s3.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=response.content  # Raw bytes of the .json.gz file
            )
            count += 1
        else:
            # Skip files that don't exist or fail — not all hours may be available
            print(f"Skipping {filename}, HTTP {response.status_code}")

    return "OK"


# ENDPOINT 2: PREPARE
# This endpoint reads the raw files from S3, parses them,
# and stores structured data into a local SQLite database with two tables:
#   - aircraft: unique aircraft metadata (icao, registration, type)
#   - positions: timestamped GPS positions and flight metrics
@s4.post("/aircraft/prepare")
def prepare_data() -> str:
    import os
    import shutil
    import gzip
    import json
    import sqlite3
    import pandas as pd
    from io import BytesIO

    s3_bucket = settings.s3_bucket
    s3_prefix = "raw/day=20231101/"

    # Set up the output directory and SQLite DB file path
    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")
    print(f"[DEBUG] bucket={settings.s3_bucket}, prepared_dir={settings.prepared_dir}")

    # Clean and recreate the output directory to ensure a fresh run
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    s3 = boto3.client("s3")

    # Use a paginator to handle listing more than 1000 objects from S3
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    # Accumulators for the two tables we'll build
    aircraft_rows = []   # One row per unique aircraft (deduplicated later)
    position_rows = []   # One row per position reading
    file_count = 0

    for page in pages:
        if "Contents" not in page:
            print(f"[WARN] No contents in page, bucket={s3_bucket}, prefix={s3_prefix}")
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.lower().endswith(".json.gz"):
                continue  # Skip any non-data files (e.g. manifests)

            file_count += 1
            print(f"Processing S3 key: {key}")

            try:
                # Download the file content from S3 into memory
                file_obj = s3.get_object(Bucket=s3_bucket, Key=key)
                content = file_obj["Body"].read()

                # Try to decompress as gzip first; fall back to plain JSON if needed
                try:
                    with gzip.open(BytesIO(content), "rt", encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, gzip.BadGzipFile):
                    data = json.loads(content)

            except Exception as e:
                print(f"[ERROR] Failed to fetch/parse {key}: {e}")
                raise  # Bubble up the error so the endpoint returns a 500

            # Validate the expected top-level structure of ADS-B snapshot files
            if not isinstance(data, dict) or "aircraft" not in data:
                print(f"[WARN] Unexpected format in {key}, keys: {data.keys() if isinstance(data, dict) else type(data)}")
                continue

            # "now" is the Unix timestamp at the time the snapshot was recorded
            base_time = data.get("now")

            for ac in data.get("aircraft", []):
                icao = ac.get("hex")  # ICAO hex code — unique identifier for each aircraft
                if not icao:
                    continue  # Skip entries with no identifier

                # Store core aircraft metadata
                aircraft_rows.append({
                    "icao": icao,
                    "registration": ac.get("r"),   # Tail number (e.g. "N12345")
                    "type": ac.get("t"),            # Aircraft type code (e.g. "B738")
                })

                lat = ac.get("lat")
                lon = ac.get("lon")
                if lat is not None and lon is not None:
                    # "seen_pos" is how many seconds ago this position was last updated
                    # Subtract it from base_time to get the actual position timestamp
                    seen_pos = ac.get("seen_pos")
                    timestamp = (base_time - seen_pos) if (base_time and seen_pos is not None) else base_time

                    # alt_baro is barometric altitude in feet, but can be the string "ground"
                    # Normalize: "ground" → 0, any other string → None
                    alt_baro = ac.get("alt_baro")
                    if isinstance(alt_baro, str):
                        alt_baro = 0 if alt_baro == "ground" else None

                    position_rows.append({
                        "icao": icao,
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon,
                        "alt_baro": alt_baro,       # Altitude in feet (0 = on ground)
                        "gs": ac.get("gs"),         # Ground speed in knots
                        "emergency": ac.get("emergency"),  # Emergency status string if any
                    })

    print(f"Processed {file_count} files, {len(aircraft_rows)} aircraft records, {len(position_rows)} position records")

    # Deduplicate aircraft by ICAO — keep the last seen entry per aircraft
    aircraft_df = pd.DataFrame(aircraft_rows).drop_duplicates(subset="icao", keep="last") if aircraft_rows else pd.DataFrame(columns=["icao", "registration", "type"])
    positions_df = pd.DataFrame(position_rows) if position_rows else pd.DataFrame(columns=["icao", "timestamp", "lat", "lon", "alt_baro", "gs", "emergency"])

    # Write both DataFrames to SQLite tables, replacing any existing data
    conn = sqlite3.connect(db_file)
    aircraft_df.to_sql("aircraft", conn, if_exists="replace", index=False)
    positions_df.to_sql("positions", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Saved {len(aircraft_df)} unique aircraft and {len(positions_df)} positions to {db_file}")
    return "OK"