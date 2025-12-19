import pytest
import pytest_asyncio
import uuid
import datetime
import asyncio
import os
from httpx import AsyncClient

BASE_URL = os.getenv("BASE_URL", "http://aggregator:8080")

@pytest_asyncio.fixture
async def client():
    async with AsyncClient(base_url=BASE_URL, timeout=10.0) as c:
        yield c

def generate_payload(event_id=None, topic="test.integration"):
    if not event_id:
        event_id = str(uuid.uuid4())
    return {
        "topic": topic,
        "event_id": event_id,
        "timestamp": datetime.datetime.now().isoformat(),
        "source": "pytest-integration",
        "payload": {"value": 999}
    }

# ==========================================
# GROUP A: Validation (Negative Cases)
# ==========================================

@pytest.mark.asyncio
async def test_api_01_reject_missing_topic(client):
    data = generate_payload()
    del data["topic"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_api_02_reject_missing_event_id(client):
    data = generate_payload()
    del data["event_id"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_api_03_reject_missing_timestamp(client):
    data = generate_payload()
    del data["timestamp"]
    response = await client.post("/publish", json=data)
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_api_04_reject_malformed_json(client):
    response = await client.post("/publish", content="bukan json", headers={"Content-Type": "application/json"})
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_api_05_reject_empty_json(client):
    response = await client.post("/publish", json={})
    assert response.status_code == 422

# ==========================================
# GROUP B: Successful Publishing (Positive Cases)
# ==========================================

@pytest.mark.asyncio
async def test_api_06_accept_valid_event(client):
    data = generate_payload()
    response = await client.post("/publish", json=data)
    assert response.status_code == 202
    assert response.json()["status"] == "queued"

@pytest.mark.asyncio
async def test_api_07_accept_complex_payload(client):
    data = generate_payload()
    data["payload"] = {"nested": {"level": 1}, "list": [10, 20]}
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

@pytest.mark.asyncio
async def test_api_08_accept_long_topic_string(client):
    data = generate_payload(topic="long.topic." * 50)
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

@pytest.mark.asyncio
async def test_api_09_accept_minimal_fields(client):
    data = {
        "topic": "minimal.topic",
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.datetime.now().isoformat()
    }
    response = await client.post("/publish", json=data)
    assert response.status_code == 202

# ==========================================
# GROUP C: Logic & Processing
# ==========================================

@pytest.mark.asyncio
async def test_api_10_detect_duplicate_submission(client):
    data = generate_payload()
    
    r_stats = await client.get("/stats")
    initial_dupes = r_stats.json()["uptime_stats"]["duplicate_dropped"]

    await client.post("/publish", json=data)
    await client.post("/publish", json=data)

    await asyncio.sleep(2)

    r_stats_final = await client.get("/stats")
    final_dupes = r_stats_final.json()["uptime_stats"]["duplicate_dropped"]
    
    assert final_dupes > initial_dupes

# ==========================================
# GROUP D: Monitoring & Retrieval
# ==========================================

@pytest.mark.asyncio
async def test_api_11_verify_stats_format(client):
    response = await client.get("/stats")
    assert response.status_code == 200
    js = response.json()
    
    # Verifikasi key yang ada di aggregator.py
    assert "uptime_stats" in js
    assert "system_state" in js
    assert "received_api" in js["uptime_stats"]
    assert "queue_depth" in js["system_state"]
    assert "database_rows" in js["system_state"]

@pytest.mark.asyncio
async def test_api_12_retrieve_events_list(client):
    response = await client.get("/events")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

@pytest.mark.asyncio
async def test_api_13_filter_events_by_topic(client):
    unique_topic = f"search.{uuid.uuid4()}"
    await client.post("/publish", json=generate_payload(topic=unique_topic))
    
    await asyncio.sleep(1)

    response = await client.get(f"/events?topic={unique_topic}")
    data = response.json()
    assert len(data) >= 1
    assert data[0]["topic"] == unique_topic

# ==========================================
# GROUP E: Standard HTTP Errors
# ==========================================

@pytest.mark.asyncio
async def test_api_14_check_method_not_allowed(client):
    response = await client.get("/publish")
    assert response.status_code == 405

@pytest.mark.asyncio
async def test_api_15_check_route_not_found(client):
    response = await client.post("/unknown/route", json={})
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_api_16_check_service_health(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "alive"

@pytest.mark.asyncio
async def test_api_17_verify_pagination_limit(client):
    for _ in range(4):
        await client.post("/publish", json=generate_payload(topic="pagination.test"))
    
    await asyncio.sleep(1) 
    
    response = await client.get("/events?limit=3")
    assert response.status_code == 200
    assert len(response.json()) == 3

@pytest.mark.asyncio
async def test_api_18_verify_payload_integrity(client):
    unique_val = str(uuid.uuid4())
    complex_payload = {"key": "value", "codes": [1, 2, 3], "unique": unique_val}
    
    data = generate_payload(topic="integrity.test")
    data["payload"] = complex_payload
    
    await client.post("/publish", json=data)
    await asyncio.sleep(1)
    
    response = await client.get("/events?topic=integrity.test&limit=1")
    events = response.json()
    
    assert len(events) > 0
    assert events[0]["payload"] == complex_payload