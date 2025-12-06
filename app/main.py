from fastapi import FastAPI, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel
from typing import Dict, List, Optional
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

logger = setup_logging()
tracer = setup_tracing("workflow-orchestrator")
metrics = setup_metrics()

event_bus: KafkaEventBus = None
orchestrator: Orchestrator = None


class EnhancementSelectionRequest(BaseModel):
    enhancement_method: str
    enhanced_colors: Dict[str, List[List[float]]]


class ClusteringSubmitRequest(BaseModel):
    clusters_data: Dict[str, List[int]]


class ExportRequest(BaseModel):
    export_type: str = 'detailed'


@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_bus, orchestrator
    
    logger.info("Workflow Orchestrator Service starting up")
    
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    event_bus = KafkaEventBus(bootstrap_servers=kafka_servers)
    await event_bus.start_producer()
    
    orchestrator = Orchestrator(event_bus)
    
    logger.info("Event-driven orchestrator initialized")
    
    yield
    
    logger.info("Workflow Orchestrator Service shutting down")
    await event_bus.close()


app = FastAPI(
    title="SketchToCAD Workflow Orchestrator",
    description="Event-driven saga orchestrator with human-in-the-loop support",
    version="2.0.0",
    lifespan=lifespan
)

instrument_app(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)


@app.post("/workflow/start")
async def start_workflow(session_id: str = Form(...), image_filename: str = Form(...)):
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    
    try:
        saga_id = await orchestrator.start_workflow(
            session_id=session_id,
            image_filename=image_filename
        )
        
        logger.info(f"Started workflow: saga_id={saga_id}, session={session_id}")
        
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


@app.post("/workflow/{saga_id}/enhancement")
async def submit_enhancement_selection(saga_id: str, request: EnhancementSelectionRequest):
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    
    try:
        success = await orchestrator.resume_with_enhancement(
            saga_id=saga_id,
            enhancement_method=request.enhancement_method,
            enhanced_colors=request.enhanced_colors
        )
        
        if not success:
            raise HTTPException(
                status_code=400, 
                detail="Cannot submit enhancement: saga not in correct state"
            )
        
        return {
            "saga_id": saga_id,
            "status": "enhancement_submitted",
            "message": "Enhancement selection received. Saga will proceed to clustering."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit enhancement: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/{saga_id}/clustering")
async def submit_clustering(saga_id: str, request: ClusteringSubmitRequest):
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    
    try:
        success = await orchestrator.resume_with_clustering(
            saga_id=saga_id,
            clusters_data=request.clusters_data
        )
        
        if not success:
            raise HTTPException(
                status_code=400, 
                detail="Cannot submit clustering: saga not in correct state"
            )
        
        return {
            "saga_id": saga_id,
            "status": "clustering_submitted",
            "message": "Clustering data received. Processing clusters."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to submit clustering: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/workflow/{saga_id}/export")
async def request_export(saga_id: str, request: ExportRequest):
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Orchestrator not initialized")
    
    try:
        success = await orchestrator.resume_with_export(
            saga_id=saga_id,
            export_type=request.export_type
        )
        
        if not success:
            raise HTTPException(
                status_code=400, 
                detail="Cannot request export: saga not in correct state"
            )
        
        return {
            "saga_id": saga_id,
            "status": "export_requested",
            "message": "Export requested. DXF file will be generated."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to request export: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse)
async def health_check():
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