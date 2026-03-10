from fastapi.testclient import TestClient


class TestS7Student:
    """
    Use this class to create your own tests for the Neo4J endpoints.
    Testing helps you verify your implementation works correctly.

    For more information on testing, search `pytest` and `fastapi.testclient`.
    """

    def test_first(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/person",
                json={
                    "name": "TestUser",
                    "city": "Barcelona",
                    "age": 25,
                },
            )
            assert True


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `pytest tests/s7/ -v` or it will be a 0!
    """

    def test_create_person(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/person",
                json={
                    "name": "Alice",
                    "city": "Barcelona",
                    "age": 28,
                },
            )
            assert not response.is_error, "Error at the create person endpoint"
            r = response.json()
            assert "status" in r, "Missing 'status' field in response"
            assert r["name"] == "Alice", "Name should match"

    def test_create_second_person(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/person",
                json={
                    "name": "Bob",
                    "city": "Madrid",
                    "age": 32,
                },
            )
            assert not response.is_error, "Error creating second person"

    def test_create_third_person(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/person",
                json={
                    "name": "Carol",
                    "city": "Barcelona",
                    "age": 25,
                },
            )
            assert not response.is_error, "Error creating third person"

    def test_list_persons(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/graph/persons")
            assert not response.is_error, "Error at the list persons endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) >= 3, "Should have at least 3 persons"
            for field in ["name", "city", "age"]:
                assert field in r[0], f"Missing '{field}' field."

    def test_create_relationship(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/relationship",
                json={
                    "from_person": "Alice",
                    "to_person": "Bob",
                },
            )
            assert not response.is_error, "Error creating relationship"
            r = response.json()
            assert "status" in r, "Missing 'status' field"

    def test_create_second_relationship(self, client: TestClient) -> None:
        with client as client:
            response = client.post(
                "/api/s7/graph/relationship",
                json={
                    "from_person": "Bob",
                    "to_person": "Carol",
                },
            )
            assert not response.is_error, "Error creating second relationship"

    def test_get_friends(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/graph/person/Alice/friends")
            assert not response.is_error, "Error at the get friends endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) >= 1, "Alice should have at least 1 friend"
            friend_names = [f["name"] for f in r]
            assert "Bob" in friend_names, "Bob should be Alice's friend"

    def test_get_friends_not_found(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/graph/person/NonExistent/friends")
            assert response.status_code == 404, "Non-existent person should return 404"

    def test_recommendations(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/graph/person/Alice/recommendations")
            assert not response.is_error, "Error at the recommendations endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"
            assert len(r) >= 1, "Alice should have at least 1 recommendation"
            rec_names = [rec["name"] for rec in r]
            assert "Carol" in rec_names, "Carol should be recommended (friend of friend via Bob)"
            for rec in r:
                assert "mutual_friends" in rec, "Missing 'mutual_friends' field"

    def test_recommendations_not_found(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s7/graph/person/NonExistent/recommendations")
            assert response.status_code == 404, "Non-existent person should return 404"
