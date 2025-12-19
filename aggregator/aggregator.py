import asyncio
import time
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError
from sqlalchemy import select, func

import redis.asyncio as redis
from contextlib import asynccontextmanager
from database import engine, Base, get_db
import models
import schemas
from config.config import BROKER_URL, QUEUE_NAME, logger
from initial_state import stats
from worker.worker import consume_events

redis_client = None
worker_tasks = []

# --- HELPER DB ---
async def init_db(retries=5, delay=3):
    """Retry koneksi DB saat startup"""
    for i in range(retries):
        try:
            logger.info(f"Startup: Mencoba koneksi database ({i+1}/{retries})...")
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            logger.info("Startup: Koneksi sukses & Tabel siap.")
            return
        except (OSError, OperationalError) as e:
            logger.warning(f"Database belum siap. Retrying in {delay}s... Error: {e}")
            await asyncio.sleep(delay)
    raise RuntimeError("Gagal konek ke database.")

# --- LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client
    
    await init_db()
    
    redis_client = redis.from_url(BROKER_URL, decode_responses=True)
    logger.info("Connected to Redis Broker")
    
    for i in range(5):
        task = asyncio.create_task(consume_events(i))
        worker_tasks.append(task)
    
    yield

    for task in worker_tasks:
        task.cancel()

    if worker_tasks:
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    if redis_client:
        await redis_client.close()

app = FastAPI(lifespan=lifespan)

# --- ENDPOINTS ---
@app.get("/")
async def root():
    return {"status": "alive", "service": "aggregator"}

@app.post("/publish", status_code=202)
async def publish_event(event: schemas.EventCreate):
    stats["received"] += 1
    try:
        event_json = event.model_dump_json()
        await redis_client.lpush(QUEUE_NAME, event_json)
        return {"status": "queued", "id": event.event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal Broker Error")

@app.get("/events", response_model=list[schemas.EventResponse])
async def get_events(topic: str = None, limit: int = 20, db: AsyncSession = Depends(get_db)):
    query = select(models.ProcessedEvent).order_by(models.ProcessedEvent.id.desc()).limit(limit)
    if topic:
        query = query.where(models.ProcessedEvent.topic == topic)
    result = await db.execute(query)
    return result.scalars().all()

@app.get("/stats")
async def get_stats(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(func.count(models.ProcessedEvent.id)))
    db_count = result.scalar()
    
    queue_depth = 0
    if redis_client:
        queue_depth = await redis_client.llen(QUEUE_NAME)

    return {
        "uptime_stats": {
            "received_api": stats["received"],
            "unique_processed": stats["unique_processed"],
            "duplicate_dropped": stats["duplicate_dropped"]
        },
        "system_state": {
            "database_rows": db_count,
            "queue_depth": queue_depth
        }
    }