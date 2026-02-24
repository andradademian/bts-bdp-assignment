from fastapi.testclient import TestClient


class TestS6Student:
    """
    Use this class to create your own tests for the MongoDB endpoints.
    Testing helps you verify your implementation works correctly.

    For more information on testing, search `pytest` and `fastapi.testclient`.
    """

    def test_first(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s6/aircraft",
                json={
                    "icao": "test01",
                    "registration": "N12345",
                    "type": "B738",
                    "lat": 41.3851,
                    "lon": 2.1734,
                    "alt_baro": 35000,
                    "ground_speed": 450,
                    "timestamp": "2026-02-19T10:30:00Z",
                },
            )
            assert True


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `pytest tests/s6/ -v` or it will be a 0!
    """

    def test_create_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s6/aircraft",
                json={
                    "icao": "a0b1c2",
                    "registration": "N12345",
                    "type": "B738",
                    "lat": 41.3851,
                    "lon": 2.1734,
                    "alt_baro": 35000,
                    "ground_speed": 450,
                    "timestamp": "2026-02-19T10:30:00Z",
                },
            )
            assert not response.is_error, "Error at the create aircraft endpoint"
            r = response.json()
            assert "status" in r, "Missing 'status' field in response"

    def test_create_second_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s6/aircraft",
                json={
                    "icao": "d3e4f5",
                    "registration": "EC-ABC",
                    "type": "A320",
                    "lat": 40.4168,
                    "lon": -3.7038,
                    "alt_baro": 28000,
                    "ground_speed": 420,
                    "timestamp": "2026-02-19T11:00:00Z",
                },
            )
            assert not response.is_error, "Error creating second aircraft"

    def test_list_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s6/aircraft/")
            assert not response.is_error, "Error at the list aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Result is empty"
            for field in ["icao", "registration", "type"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_list_aircraft_pagination(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s6/aircraft/?page=1&page_size=1")
            assert not response.is_error, "Error at pagination"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) <= 1, "Pagination not working: returned more than page_size"

    def test_get_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s6/aircraft/a0b1c2")
            assert not response.is_error, "Error at the get aircraft endpoint"
            r = response.json()
            assert "icao" in r, "Missing 'icao' field"
            assert r["icao"] == "a0b1c2", "Wrong aircraft returned"
            assert "lat" in r, "Missing 'lat' field"
            assert "lon" in r, "Missing 'lon' field"

    def test_aircraft_stats(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s6/aircraft/stats")
            assert not response.is_error, "Error at the stats endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) > 0, "Stats result is empty"
            for item in r:
                assert "type" in item, "Missing 'type' field in stats"
                assert "count" in item, "Missing 'count' field in stats"

    def test_delete_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.delete("/api/s6/aircraft/a0b1c2")
            assert not response.is_error, "Error at the delete endpoint"
            r = response.json()
            assert "deleted" in r, "Missing 'deleted' field"
            assert r["deleted"] >= 1, "Should have deleted at least 1 record"

    def test_get_deleted_aircraft(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s6/aircraft/a0b1c2")
            assert response.status_code == 404, "Deleted aircraft should return 404"
