from fastapi.testclient import TestClient


class TestS8Student:
    """
    Use this class to create your own tests for the S8 endpoints.
    Testing helps you verify your implementation works correctly.

    For more information on testing, search `pytest` and `fastapi.testclient`.
    """

    def test_first(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/")
            assert True


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `pytest tests/s8/ -v` or it will be a 0!
    """

    def test_list_aircraft_returns_list(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/")
            assert not response.is_error, "Error at the list aircraft endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"

    def test_list_aircraft_has_required_fields(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/?num_results=5")
            assert not response.is_error
            r = response.json()
            if len(r) > 0:
                for field in ["icao", "registration", "type", "owner", "manufacturer", "model"]:
                    assert field in r[0], f"Missing '{field}' field in aircraft response"

    def test_list_aircraft_pagination(self, client: TestClient) -> None:
        with client as client:
            response_p0 = client.get("/api/s8/aircraft/?num_results=5&page=0")
            response_p1 = client.get("/api/s8/aircraft/?num_results=5&page=1")
            assert not response_p0.is_error
            assert not response_p1.is_error
            r0 = response_p0.json()
            r1 = response_p1.json()
            assert len(r0) <= 5, "Page 0 should have at most 5 results"
            if len(r0) == 5 and len(r1) > 0:
                assert r0[0]["icao"] != r1[0]["icao"], "Page 0 and page 1 should have different results"

    def test_list_aircraft_ordered_by_icao(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/?num_results=20")
            assert not response.is_error
            r = response.json()
            if len(r) >= 2:
                icaos = [a["icao"] for a in r]
                assert icaos == sorted(icaos), "Aircraft should be ordered by ICAO ascending"

    def test_list_aircraft_has_data(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s8/aircraft/?num_results=100")
            assert not response.is_error
            r = response.json()
            assert len(r) > 0, "Aircraft list should not be empty after running the pipeline"

    def test_co2_endpoint_returns_correct_structure(self, client: TestClient) -> None:
        with client as client:
            aircraft_response = client.get("/api/s8/aircraft/?num_results=1")
            assert not aircraft_response.is_error
            aircraft_list = aircraft_response.json()
            if len(aircraft_list) > 0:
                icao = aircraft_list[0]["icao"]
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day=2023-11-01")
                assert not response.is_error, f"Error at the CO2 endpoint for {icao}"
                r = response.json()
                assert "icao" in r, "Missing 'icao' field in CO2 response"
                assert "hours_flown" in r, "Missing 'hours_flown' field in CO2 response"
                assert "co2" in r, "Missing 'co2' field in CO2 response"

    def test_co2_icao_matches(self, client: TestClient) -> None:
        with client as client:
            aircraft_response = client.get("/api/s8/aircraft/?num_results=1")
            assert not aircraft_response.is_error
            aircraft_list = aircraft_response.json()
            if len(aircraft_list) > 0:
                icao = aircraft_list[0]["icao"]
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day=2023-11-01")
                assert not response.is_error
                r = response.json()
                assert r["icao"] == icao, "ICAO in response should match the requested ICAO"

    def test_co2_hours_flown_is_number(self, client: TestClient) -> None:
        with client as client:
            aircraft_response = client.get("/api/s8/aircraft/?num_results=1")
            assert not aircraft_response.is_error
            aircraft_list = aircraft_response.json()
            if len(aircraft_list) > 0:
                icao = aircraft_list[0]["icao"]
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day=2023-11-01")
                assert not response.is_error
                r = response.json()
                assert isinstance(r["hours_flown"], (int, float)), "hours_flown should be a number"
                assert r["hours_flown"] >= 0, "hours_flown should be non-negative"

    def test_co2_value_is_number_or_none(self, client: TestClient) -> None:
        with client as client:
            aircraft_response = client.get("/api/s8/aircraft/?num_results=1")
            assert not aircraft_response.is_error
            aircraft_list = aircraft_response.json()
            if len(aircraft_list) > 0:
                icao = aircraft_list[0]["icao"]
                response = client.get(f"/api/s8/aircraft/{icao}/co2?day=2023-11-01")
                assert not response.is_error
                r = response.json()
                assert r["co2"] is None or isinstance(r["co2"], (int, float)), "co2 should be a number or None"
