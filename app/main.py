from fastapi import FastAPI, HTTPException, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import uvicorn
from contextlib import asynccontextmanager
import logging
import os

from .models import HealthResponse
from .events import KafkaEventBus
from .orchestrator import Orchestrator
from .database.saga_repository import SagaRepository
from .database.init_db import SessionLocal
from .observability.tracing import setup_tracing, instrument_app
from .observability.metrics import setup_metrics
from .observability.logging import setup_logging

# Setup observability
logger = setup_logging()
tracer = setup_tracing("workflow-orchestrator")
metrics = setup_metrics()

# Global instances
event_bus: KafkaEventBus = None
orchestrator: Orchestrator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global event_bus, orchestrator
    
    logger.info("Workflow Orchestrator Service starting up")
    
    # Initialize event bus
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    event_bus = KafkaEventBus(bootstrap_servers=kafka_servers)
    await event_bus.start_producer()
    
    # Initialize orchestrator
    orchestrator = Orchestrator(event_bus)
    
    logger.info("Event-driven orchestrator initialized ✅")
    
    yield
    
    # Shutdown
    logger.info("Workflow Orchestrator Service shutting down")
    await event_bus.close()


app = FastAPI(
    title="SketchToCAD Workflow Orchestrator",
    description="Event-driven saga orchestrator for image processing pipeline",
    version="2.0.0",
    lifespan=lifespan
)

instrument_app(app)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics
Instrumentator().instrument(app).expose(app)


@app.post("/workflow/start")
async def start_workflow(file: UploadFile = File(...)):
    """
    Start a new image-to-CAD workflow
    
    Returns immediately with saga_id. Use GET /workflow/{saga_id} to check status.
    """
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    
    try:
        # For now, we'll create a session_id from the filename
        # In real implementation, this would come from image-processing service
        import uuid
        session_id = f"sess_{uuid.uuid4().hex[:8]}"
        
        # Start the saga
        saga_id = await orchestrator.start_workflow(
            session_id=session_id,
            image_filename=file.filename
        )
        
        logger.info(f"Started workflow: saga_id={saga_id}, file={file.filename}")
        
        return {
            "saga_id": saga_id,
            "session_id": session_id,
            "status": "started",
            "message": "Workflow initiated. Use GET /workflow/{saga_id} to check status."
        }
        
    except Exception as e:
        logger.error(f"Failed to start workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/workflow/{saga_id}")
async def get_workflow_status(saga_id: str):
    """Get current status of a workflow"""
    repo = SagaRepository(SessionLocal())
    
    try:
        saga = repo.get_saga(saga_id)
        
        if not saga:
            raise HTTPException(status_code=404, detail=f"Saga not found: {saga_id}")
        
        steps = repo.get_saga_steps(saga_id)
        
        return {
            "saga_id": saga.id,
            "status": saga.status,
            "current_step": saga.current_step,
            "session_id": saga.session_id,
            "created_at": saga.created_at.isoformat() if saga.created_at else None,
            "updated_at": saga.updated_at.isoformat() if saga.updated_at else None,
            "completed_at": saga.completed_at.isoformat() if saga.completed_at else None,
            "total_duration_ms": saga.total_duration_ms,
            "error_message": saga.error_message,
            "result_data": saga.result_data,
            "steps": [
                {
                    "step_name": step.step_name,
                    "status": step.status,
                    "duration_ms": step.duration_ms
                }
                for step in steps
            ]
        }
        
    finally:
        repo.db.close()


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check"""
    return HealthResponse(
        status="healthy",
        service="workflow-orchestrator",
        version="2.0.0"
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8004,
        reload=True,
        log_config=None
    )