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
    """Obtain the data from AWS s3 and store it in the local `prepared` directory
    as done in s1.

    All the `/api/s1/aircraft/` endpoints should work as usual
    """
    # TODO
    return "OK"
