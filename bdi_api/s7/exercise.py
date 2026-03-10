from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J.

    Use the BDI_NEO4J_URL environment variable to configure the connection.
    Start Neo4J with: make neo4j
    """
    # TODO: Connect to Neo4J using neo4j.GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    # TODO: Create a Person node with the given properties
    # TODO: Return {"status": "ok", "name": person.name}
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes.

    Each result should include: name, city, age.
    """
    # TODO: Connect to Neo4J
    # TODO: MATCH (p:Person) RETURN p
    # TODO: Return list of dicts with name, city, age
    return []


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    If person not found, return 404.
    """
    # TODO: Connect to Neo4J
    # TODO: First check if person exists, return 404 if not
    # TODO: MATCH (p:Person {name: name})-[:FRIENDS_WITH]-(friend:Person)
    # TODO: Return list of friend dicts with name, city, age
    raise HTTPException(status_code=404, detail=f"Person '{name}' not found")


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    # TODO: Connect to Neo4J
    # TODO: Verify both persons exist
    # TODO: CREATE (a)-[:FRIENDS_WITH]->(b)
    # TODO: Return {"status": "ok", "from": rel.from_person, "to": rel.to_person}
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person.

    Recommend friends-of-friends who are NOT already direct friends.
    Return them sorted by number of mutual friends (descending).
    If person not found, return 404.

    Each result should include: name, city, mutual_friends (count).
    """
    # TODO: Connect to Neo4J
    # TODO: First check if person exists, return 404 if not
    # TODO: Find friends-of-friends not already friends
    # TODO: Count mutual friends and sort descending
    raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
