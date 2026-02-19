import gzip
import json
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


MOCK_AIRCRAFT_DATA = {
    "now": 1698796800.0,
    "aircraft": [
        {
            "hex": "06a0af",
            "r": "EC-MTI",
            "t": "A320",
            "lat": 41.290039,
            "lon": 2.072664,
            "alt_baro": 3200,
            "gs": 180.5,
            "seen_pos": 0.5,
            "emergency": None,
        },
        {
            "hex": "aabbcc",
            "r": "N12345",
            "t": "B737",
            "lat": 40.0,
            "lon": 3.0,
            "alt_baro": "ground",
            "gs": 0.0,
            "seen_pos": 1.0,
            "emergency": "none",
        },
    ],
}


def _make_gz_content(data: dict) -> bytes:
    return gzip.compress(json.dumps(data).encode("utf-8"))


class TestS4:

    def test_download(self, client: TestClient) -> None:
        """Download endpoint stores files in S3 without error."""
        mock_s3 = MagicMock()

        with patch("bdi_api.s4.exercise.boto3.client", return_value=mock_s3), \
             patch("bdi_api.s4.exercise.requests.get") as mock_get:

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.content = _make_gz_content(MOCK_AIRCRAFT_DATA)
            mock_get.return_value = mock_response

            with client as c:
                response = c.post("/api/s4/aircraft/download?file_limit=1")

            assert not response.is_error, "Download endpoint returned an error"
            assert response.json() == "OK"
            mock_s3.put_object.assert_called_once()

    def test_prepare(self, client: TestClient) -> None:
        """Prepare endpoint reads from S3 and creates the SQLite DB without error."""
        mock_s3 = MagicMock()

        # Mock list_objects_v2 paginator
        mock_paginator = MagicMock()
        mock_s3.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "raw/day=20231101/000000Z.json.gz"},
                ]
            }
        ]

        # Mock get_object to return our fake gzipped data
        mock_s3.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=_make_gz_content(MOCK_AIRCRAFT_DATA)))
        }

        with patch("bdi_api.s4.exercise.boto3.client", return_value=mock_s3):
            with client as c:
                response = c.post("/api/s4/aircraft/prepare")

        assert not response.is_error, "Prepare endpoint returned an error"
        assert response.json() == "OK"