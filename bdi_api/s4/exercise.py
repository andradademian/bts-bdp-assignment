from typing import Annotated

from fastapi import APIRouter, status
from fastapi.params import Query

from bdi_api.settings import Settings
import boto3
import requests
settings = Settings()

s4 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s4",
    tags=["s4"],
)


@s4.post("/aircraft/download")
def download_data(
    file_limit: Annotated[int, Query(...)] = 100,
) -> str:

    base_url = settings.source_url + "/2023/11/01/"
    s3_bucket = settings.s3_bucket
    s3_prefix_path = "raw/day=20231101/"

    s3 = boto3.client("s3")

    # Generate filenames in ascending order
    filenames = [f"{hour:02d}0000Z.json.gz" for hour in range(24)]

    count = 0
    for filename in filenames:
        if count >= file_limit:
            break

        url = base_url + filename
        response = requests.get(url)

        if response.status_code == 200:
            s3_key = s3_prefix_path + filename
            s3.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=response.content
            )
            count += 1
        else:
            print(f"Skipping {filename}, HTTP {response.status_code}")

    return "OK"


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

    prepared_dir = os.path.join(settings.prepared_dir, "day=20231101")
    db_file = os.path.join(prepared_dir, "aircraft.db")
    print(f"[DEBUG] bucket={settings.s3_bucket}, prepared_dir={settings.prepared_dir}")
    if os.path.exists(prepared_dir):
        shutil.rmtree(prepared_dir)
    os.makedirs(prepared_dir, exist_ok=True)

    s3 = boto3.client("s3")

    # Handle pagination - list_objects_v2 returns max 1000 objects per call
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    aircraft_rows = []
    position_rows = []
    file_count = 0

    for page in pages:
        if "Contents" not in page:
            print(f"[WARN] No contents in page, bucket={s3_bucket}, prefix={s3_prefix}")
            continue

        for obj in page["Contents"]:
            key = obj["Key"]
            if not key.lower().endswith(".json.gz"):
                continue

            file_count += 1
            print(f"Processing S3 key: {key}")

            try:
                file_obj = s3.get_object(Bucket=s3_bucket, Key=key)
                content = file_obj["Body"].read()

                try:
                    with gzip.open(BytesIO(content), "rt", encoding="utf-8") as f:
                        data = json.load(f)
                except (OSError, gzip.BadGzipFile):
                    data = json.loads(content)

            except Exception as e:
                print(f"[ERROR] Failed to fetch/parse {key}: {e}")
                raise  # Don't silently swallow - let it surface

            if not isinstance(data, dict) or "aircraft" not in data:
                print(f"[WARN] Unexpected format in {key}, keys: {data.keys() if isinstance(data, dict) else type(data)}")
                continue

            # Use the file's timestamp as base
            base_time = data.get("now")

            for ac in data.get("aircraft", []):
                icao = ac.get("hex")
                if not icao:
                    continue

                aircraft_rows.append({
                    "icao": icao,
                    "registration": ac.get("r"),
                    "type": ac.get("t"),
                })

                lat = ac.get("lat")
                lon = ac.get("lon")
                if lat is not None and lon is not None:
                    # Compute real timestamp from base_time - seen_pos offset
                    seen_pos = ac.get("seen_pos")
                    timestamp = (base_time - seen_pos) if (base_time and seen_pos is not None) else base_time

                    # alt_baro can be "ground" string - normalize to None
                    alt_baro = ac.get("alt_baro")
                    if isinstance(alt_baro, str):
                        alt_baro = 0 if alt_baro == "ground" else None

                    position_rows.append({
                        "icao": icao,
                        "timestamp": timestamp,
                        "lat": lat,
                        "lon": lon,
                        "alt_baro": alt_baro,
                        "gs": ac.get("gs"),
                        "emergency": ac.get("emergency"),
                    })

    print(f"Processed {file_count} files, {len(aircraft_rows)} aircraft records, {len(position_rows)} position records")

    aircraft_df = pd.DataFrame(aircraft_rows).drop_duplicates(subset="icao", keep="last") if aircraft_rows else pd.DataFrame(columns=["icao", "registration", "type"])
    positions_df = pd.DataFrame(position_rows) if position_rows else pd.DataFrame(columns=["icao", "timestamp", "lat", "lon", "alt_baro", "gs", "emergency"])

    conn = sqlite3.connect(db_file)
    aircraft_df.to_sql("aircraft", conn, if_exists="replace", index=False)
    positions_df.to_sql("positions", conn, if_exists="replace", index=False)
    conn.close()

    print(f"Saved {len(aircraft_df)} unique aircraft and {len(positions_df)} positions to {db_file}")
    return "OK"
