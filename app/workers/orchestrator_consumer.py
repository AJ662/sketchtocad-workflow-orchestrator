import asyncio
import logging
import os

from ..events import KafkaEventBus
from ..orchestrator import Orchestrator

logger = logging.getLogger(__name__)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    event_bus = KafkaEventBus(bootstrap_servers=kafka_servers)
    await event_bus.start_producer()
    
    orchestrator = Orchestrator(event_bus)
    
    try:
        await orchestrator.run()
    finally:
        await event_bus.close()


if __name__ == "__main__":
    asyncio.run(main())