import asyncio
import os

os.environ['DATABASE_URL'] = "postgresql://sketchtocad:sketchtocad_dev@localhost:5433/saga_state"

from app.events import KafkaEventBus
from app.orchestrator import Orchestrator

async def test_orchestrator():
    # Initialize
    event_bus = KafkaEventBus(bootstrap_servers='localhost:9093')
    await event_bus.start_producer()
    
    orchestrator = Orchestrator(event_bus)
    
    try:
        # Start a workflow
        print("Starting workflow...")
        saga_id = await orchestrator.start_workflow(
            session_id="test-session-001",
            image_filename="test-garden.jpg"
        )
        print(f"✅ Workflow started: {saga_id}")
        
        # Give it a moment
        await asyncio.sleep(2)
        
        print("\n✅ Orchestrator is working!")
        print(f"Check Kafka UI to see the WorkflowStarted and ImageProcessingRequested events")
        print(f"Check database: SELECT * FROM sagas WHERE id = '{saga_id}';")
        
    finally:
        await event_bus.close()

if __name__ == "__main__":
    asyncio.run(test_orchestrator())