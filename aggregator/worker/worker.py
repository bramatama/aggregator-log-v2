import asyncio
import json
import redis.asyncio as redis
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from database import AsyncSessionLocal
import models
from config.config import BROKER_URL, QUEUE_NAME, logger
from initial_state import stats

async def process_event_in_db(event_data):
    """Worker: Simpan ke DB & Hitung Latency"""
    async with AsyncSessionLocal() as db:
        try:
            try:
                event_ts = datetime.fromisoformat(event_data['timestamp'])
                if event_ts.tzinfo is None:
                    event_ts = event_ts.replace(tzinfo=None)
                
                arrival_ts = datetime.now()
                latency = (arrival_ts - event_ts).total_seconds()
                
                if latency > 0:
                    stats["total_latency"] += latency
            except Exception:
                pass 

            stmt = insert(models.ProcessedEvent).values(
                topic=event_data['topic'],
                event_id=event_data['event_id'],
                timestamp=event_data['timestamp'],
                source=event_data.get('source'),
                payload=event_data.get('payload')
            )
            stmt = stmt.on_conflict_do_nothing(index_elements=['topic', 'event_id'])
            
            result = await db.execute(stmt)
            await db.commit()
            
            if result.rowcount > 0:
                stats["unique_processed"] += 1
            else:
                stats["duplicate_dropped"] += 1
                
            # total_ops = stats["unique_processed"] + stats["duplicate_dropped"]
            # if total_ops % 500 == 0:
            #     logger.info(
            #         f"WORKER >> Processed: {stats['unique_processed']} | "
            #         f"Dropped: {stats['duplicate_dropped']}"
            #     )

        except Exception as e:
            logger.error(f"DB Error di Worker: {e}")

async def consume_events(worker_id):
    logger.info(f"Worker {worker_id} actived")
    await asyncio.sleep(2)

    try:
        consumer_redis = redis.from_url(BROKER_URL, decode_responses=True)
    except Exception as e:
        logger.error(f"Gagal konek Redis Consumer: {e}")
        return

    while True:
        try:
            result = await consumer_redis.brpop(QUEUE_NAME, timeout=1)
            if result:
                _, data_str = result
                event_data = json.loads(data_str)
                await process_event_in_db(event_data)
            await asyncio.sleep(0.001)
        except Exception as e:
            logger.error(f"Worker Loop Error: {e}")
            await asyncio.sleep(1)
