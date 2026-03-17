from fastapi.testclient import TestClient


class TestS9Student:
    """
    Use this class to create your own tests for the S9 endpoints.
    Testing helps you verify your implementation works correctly.
    """

    def test_first(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines")
            assert True


class TestItCanBeEvaluated:
    """
    Those tests are just to be sure I can evaluate your exercise.
    Don't modify anything from here!

    Make sure all those tests pass with `pytest tests/s9/ -v` or it will be a 0!
    """

    def test_list_pipelines_returns_list(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines")
            assert not response.is_error, "Error at the list pipelines endpoint"
            r = response.json()
            assert isinstance(r, list), "Result is not a list"

    def test_list_pipelines_has_required_fields(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines?num_results=5")
            assert not response.is_error
            r = response.json()
            if len(r) > 0:
                required = ["id", "repository", "branch", "status", "triggered_by", "started_at", "stages"]
                for field in required:
                    assert field in r[0], f"Missing '{field}' field in pipeline response"

    def test_list_pipelines_pagination(self, client: TestClient) -> None:
        with client as client:
            response_p0 = client.get("/api/s9/pipelines?num_results=5&page=0")
            response_p1 = client.get("/api/s9/pipelines?num_results=5&page=1")
            assert not response_p0.is_error
            assert not response_p1.is_error
            r0 = response_p0.json()
            r1 = response_p1.json()
            assert len(r0) <= 5, "Page 0 should have at most 5 results"
            if len(r0) == 5 and len(r1) > 0:
                assert r0[0]["id"] != r1[0]["id"], "Page 0 and page 1 should have different results"

    def test_list_pipelines_has_data(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines?num_results=100")
            assert not response.is_error
            r = response.json()
            assert len(r) > 0, "Pipelines list should not be empty"

    def test_list_pipelines_ordered_by_started_at_desc(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines?num_results=20")
            assert not response.is_error
            r = response.json()
            if len(r) >= 2:
                dates = [p["started_at"] for p in r]
                assert dates == sorted(dates, reverse=True), "Pipelines should be ordered by started_at descending"

    def test_list_pipelines_filter_by_status(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines?status_filter=success")
            assert not response.is_error
            r = response.json()
            for pipeline in r:
                assert pipeline["status"] == "success", "All pipelines should have status 'success' when filtered"

    def test_pipeline_stages_returns_list(self, client: TestClient) -> None:
        with client as client:
            pipelines_response = client.get("/api/s9/pipelines?num_results=1")
            assert not pipelines_response.is_error
            pipelines = pipelines_response.json()
            if len(pipelines) > 0:
                pipeline_id = pipelines[0]["id"]
                response = client.get(f"/api/s9/pipelines/{pipeline_id}/stages")
                assert not response.is_error, f"Error at the stages endpoint for {pipeline_id}"
                r = response.json()
                assert isinstance(r, list), "Stages result is not a list"

    def test_pipeline_stages_has_required_fields(self, client: TestClient) -> None:
        with client as client:
            pipelines_response = client.get("/api/s9/pipelines?num_results=1")
            assert not pipelines_response.is_error
            pipelines = pipelines_response.json()
            if len(pipelines) > 0:
                pipeline_id = pipelines[0]["id"]
                response = client.get(f"/api/s9/pipelines/{pipeline_id}/stages")
                assert not response.is_error
                r = response.json()
                if len(r) > 0:
                    required = ["name", "status", "started_at", "logs_url"]
                    for field in required:
                        assert field in r[0], f"Missing '{field}' field in stage response"

    def test_pipeline_stages_not_empty(self, client: TestClient) -> None:
        with client as client:
            pipelines_response = client.get("/api/s9/pipelines?num_results=1")
            assert not pipelines_response.is_error
            pipelines = pipelines_response.json()
            if len(pipelines) > 0:
                pipeline_id = pipelines[0]["id"]
                response = client.get(f"/api/s9/pipelines/{pipeline_id}/stages")
                assert not response.is_error
                r = response.json()
                assert len(r) > 0, "Pipeline should have at least one stage"

    def test_pipeline_stages_match_pipeline_stages_list(self, client: TestClient) -> None:
        with client as client:
            pipelines_response = client.get("/api/s9/pipelines?num_results=1")
            assert not pipelines_response.is_error
            pipelines = pipelines_response.json()
            if len(pipelines) > 0:
                pipeline = pipelines[0]
                pipeline_id = pipeline["id"]
                response = client.get(f"/api/s9/pipelines/{pipeline_id}/stages")
                assert not response.is_error
                stages = response.json()
                stage_names = [s["name"] for s in stages]
                assert stage_names == pipeline["stages"], "Stage names should match the stages list in the pipeline"

    def test_pipeline_not_found_returns_404(self, client: TestClient) -> None:
        with client as client:
            response = client.get("/api/s9/pipelines/nonexistent-pipeline-id/stages")
            assert response.status_code == 404, "Should return 404 for non-existent pipeline"
