import gzip
import json
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


# This simulates the JSON structure returned by the real data source.
# It contains two aircrafts:
#   - EC-MTI: an A320 airborne at 3200 ft
#   - N12345: a B737 on the ground (alt_baro = "ground" — tests our normalization logic)
MOCK_AIRCRAFT_DATA = {
    "now": 1698796800.0,  # Unix timestamp for Nov 1, 2023 00:00:00 UTC
    "aircraft": [
        {
            "hex": "06a0af",       # ICAO identifier
            "r": "EC-MTI",         # Registration (tail number)
            "t": "A320",           # Aircraft type
            "lat": 41.290039,
            "lon": 2.072664,
            "alt_baro": 3200,      # Altitude in feet
            "gs": 180.5,           # Ground speed in knots
            "seen_pos": 0.5,       # Position was recorded 0.5s before "now"
            "emergency": None,
        },
        {
            "hex": "aabbcc",
            "r": "N12345",
            "t": "B737",
            "lat": 40.0,
            "lon": 3.0,
            "alt_baro": "ground",  # Edge case: string instead of integer — should normalize to 0
            "gs": 0.0,
            "seen_pos": 1.0,
            "emergency": "none",
        },
    ],
}


def _make_gz_content(data: dict) -> bytes:
    """Helper: serialize a dict to JSON and compress it as gzip bytes.
    Mimics the exact format of the real .json.gz files from the source."""
    return gzip.compress(json.dumps(data).encode("utf-8"))


class TestS4:

    def test_download(self, client: TestClient) -> None:
        """Download endpoint stores files in S3 without error."""

        # Create a mock S3 client; no real AWS calls in tests
        mock_s3 = MagicMock()

        with patch("bdi_api.s4.exercise.boto3.client", return_value=mock_s3), \
             patch("bdi_api.s4.exercise.requests.get") as mock_get:

            # Simulate a successful HTTP response returning a valid gzipped file
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = _make_gz_content(MOCK_AIRCRAFT_DATA)
            mock_get.return_value = mock_response

            with client as c:
                # file_limit=1 keeps the test fast — only one file needs to be "downloaded"
                response = c.post("/api/s4/aircraft/download?file_limit=1")

        # Verify the endpoint completed successfully
        assert not response.is_error, "Download endpoint returned an error"
        assert response.json() == "OK"

        # Verify that exactly one file was uploaded to S3
        # This confirms the endpoint actually called put_object and didn't just silently skip
        mock_s3.put_object.assert_called_once()

    def test_prepare(self, client: TestClient) -> None:
        """Prepare endpoint reads from S3 and creates the SQLite DB without error."""

        mock_s3 = MagicMock()

        # Mock the S3 paginator
        # list_objects_v2 uses pagination; we simulate one page with one file in it
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "raw/day=20231101/000000Z.json.gz"},  # One file listed in the bucket
                ]
            }
        ]

        # Mock the S3 file download
        # get_object returns a dict with a "Body" that has a .read() method
        # We return our fake gzipped data to simulate what the real file would contain
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=_make_gz_content(MOCK_AIRCRAFT_DATA)))
        }

        # Inject the mock S3 client so no real AWS calls are made
        with patch("bdi_api.s4.exercise.boto3.client", return_value=mock_s3):
            with client as c:
                response = c.post("/api/s4/aircraft/prepare")

        # Verify the endpoint ran without errors and returned the expected response
        assert not response.is_error, "Prepare endpoint returned an error"
        assert response.json() == "OK"
        # Note: a more thorough test could open the resulting SQLite DB
        # and assert the aircraft/positions tables contain the expected rows