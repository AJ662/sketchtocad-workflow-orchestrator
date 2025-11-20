from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import uvicorn
from contextlib import asynccontextmanager
import time
import httpx
import io
from fastapi.responses import StreamingResponse
from datetime import datetime, timedelta
from typing import Dict

from .service import WorkflowOrchestrator
from .models import WorkflowResponse, HealthResponse
from .observability.tracing import setup_tracing, instrument_app
from .observability.metrics import setup_metrics
from .observability.logging import setup_logging

# Setup observability
logger = setup_logging()
tracer = setup_tracing("workflow-orchestrator")
metrics = setup_metrics()

# Session tracking
class SessionTracker:
    def __init__(self):
        self.sessions: Dict[str, Dict] = {}
    
    def create_session(self, session_id: str, bed_count: int):
        self.sessions[session_id] = {
            "status": "image_processed",
            "created_at": datetime.now(),
            "bed_count": bed_count
        }
    
    def get_session(self, session_id: str):
        return self.sessions.get(session_id)

session_tracker = SessionTracker()
orchestrator = WorkflowOrchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    logger.info("Workflow Orchestrator Service starting up")
    yield
    logger.info("Workflow Orchestrator Service shutting down")


app = FastAPI(
    title="SketchToCAD Workflow Orchestrator",
    description="Orchestrates the complete image processing pipeline",
    version="1.0.0",
    lifespan=lifespan
)

instrument_app(app)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://api-gateway:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus metrics
Instrumentator().instrument(app).expose(app)


# Simple proxy endpoints
@app.post("/image-processing/process-image")
async def proxy_image_processing(file: UploadFile = File(...)):
    """Proxy image processing with orchestration"""
    async with httpx.AsyncClient(timeout=300.0) as client:
        files = {"file": (file.filename, await file.read(), file.content_type)}
        response = await client.post(f"{orchestrator.image_processing_url}/process-image", files=files)
        
        if response.status_code == 200:
            result = response.json()
            # Track session
            session_tracker.create_session(result["session_id"], result["bed_count"])
            logger.info(f"Tracked session: {result['session_id']}")
        
        return response.json()


@app.post("/clustering/create-enhanced-colors") 
async def proxy_enhanced_colors(request: dict):
    """Proxy enhanced colors"""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(f"{orchestrator.clustering_url}/create-enhanced-colors", json=request)
        return response.json()


@app.post("/clustering/process-clustering")
async def proxy_clustering(request: dict):
    """Proxy clustering"""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(f"{orchestrator.clustering_url}/process-clustering", json=request)
        return response.json()


@app.post("/dxf-export/export-dxf")
async def proxy_dxf_export(request: dict):
    """Proxy DXF export"""
    async with httpx.AsyncClient(timeout=300.0) as client:
        response = await client.post(f"{orchestrator.dxf_export_url}/export-dxf", json=request)
        
        return StreamingResponse(
            io.BytesIO(response.content),
            media_type="application/octet-stream",
            headers={"Content-Disposition": "attachment; filename=export.dxf"}
        )


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check"""
    return HealthResponse(
        status="healthy",
        service="workflow-orchestrator",
        version="1.0.0"
    )


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8004,
        reload=True,
        log_config=None
    )