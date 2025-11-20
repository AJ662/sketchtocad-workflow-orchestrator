import httpx
import asyncio
import time
from typing import Dict, Any
import logging
from opentelemetry import trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class WorkflowOrchestrator:
    """Orchestrates the complete SketchToCad processing pipeline"""
    
    def __init__(self):
        self.image_processing_url = "http://image-processing:8001"
        self.clustering_url = "http://clustering:8002"
        self.dxf_export_url = "http://dxf-export:8003"
        self.timeout = 300.0  # 5 minutes
    
    async def execute_complete_workflow(self, image_file_content: bytes, filename: str) -> Dict[str, Any]:
        """Execute the complete processing workflow"""
        workflow_start_time = time.time()
        session_id = None
        
        with tracer.start_as_current_span("complete_workflow") as span:
            try:
                # Step 1: Process Image
                logger.info(f"Starting workflow for file: {filename}")
                processing_result = await self._process_image(image_file_content, filename)
                session_id = processing_result['session_id']
                span.set_attribute("session_id", session_id)
                span.set_attribute("bed_count", processing_result['bed_count'])
                
                # Step 2: Cluster Plant Beds
                logger.info(f"Starting clustering for session: {session_id}")
                clustering_result = await self._cluster_beds(processing_result['bed_data'])
                span.set_attribute("cluster_count", len(clustering_result['processed_clusters']))
                
                # Step 3: Export to DXF
                logger.info(f"Starting DXF export for session: {session_id}")
                dxf_result = await self._export_dxf(
                    session_id, 
                    clustering_result['processed_clusters']
                )
                
                total_time = (time.time() - workflow_start_time) * 1000
                span.set_attribute("total_workflow_time_ms", total_time)
                
                logger.info(f"Workflow completed successfully for session: {session_id}")
                
                return {
                    'session_id': session_id,
                    'bed_count': processing_result['bed_count'],
                    'cluster_count': len(clustering_result['processed_clusters']),
                    'dxf_download_url': dxf_result['download_url'],
                    'processing_summary': {
                        'image_processing_ms': processing_result['processing_time_ms'],
                        'clustering_ms': clustering_result.get('processing_time_ms', 0),
                        'dxf_export_ms': dxf_result['export_time_ms'],
                        'total_workflow_ms': total_time
                    },
                    'total_processing_time_ms': total_time
                }
                
            except Exception as e:
                logger.error(f"Workflow failed: {str(e)}", exc_info=True)
                span.set_attribute("error", True)
                span.set_attribute("error.message", str(e))
                
                # Compensation: Cleanup on failure
                if session_id:
                    await self._compensate_workflow_failure(session_id)
                
                raise
    
    async def _process_image(self, image_content: bytes, filename: str) -> Dict[str, Any]:
        """Step 1: Process the uploaded image"""
        with tracer.start_as_current_span("step1_process_image") as span:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                files = {"file": (filename, image_content, "image/jpeg")}
                
                response = await client.post(
                    f"{self.image_processing_url}/process-image",
                    files=files
                )
                
                if response.status_code != 200:
                    raise Exception(f"Image processing failed: {response.status_code} - {response.text}")
                
                result = response.json()
                span.set_attribute("beds_detected", result['bed_count'])
                return result
    
    async def _cluster_beds(self, bed_data: list) -> Dict[str, Any]:
        """Step 2: Create enhanced colors and process clustering"""
        with tracer.start_as_current_span("step2_cluster_beds") as span:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                
                # First, create enhanced colors
                enhanced_colors_payload = {"bed_data": bed_data}
                colors_response = await client.post(
                    f"{self.clustering_url}/create-enhanced-colors",
                    json=enhanced_colors_payload
                )
                
                if colors_response.status_code != 200:
                    raise Exception(f"Enhanced colors creation failed: {colors_response.status_code}")
                
                enhanced_colors_result = colors_response.json()
                
                # TODO: For now, create simple clusters based on bed count
                # In a real implementation, this would come from frontend user interaction
                clusters_data = self._create_default_clusters(len(bed_data))
                
                # Process clustering
                clustering_payload = {
                    "bed_data": bed_data,
                    "enhanced_colors": enhanced_colors_result['enhanced_colors'],
                    "clusters_data": clusters_data
                }
                
                clustering_response = await client.post(
                    f"{self.clustering_url}/process-clustering",
                    json=clustering_payload
                )
                
                if clustering_response.status_code != 200:
                    raise Exception(f"Clustering failed: {clustering_response.status_code}")
                
                result = clustering_response.json()
                span.set_attribute("clusters_created", len(result['processed_clusters']))
                return result
    
    async def _export_dxf(self, session_id: str, clusters: Dict[str, list]) -> Dict[str, Any]:
        """Step 3: Export clustered beds to DXF format"""
        with tracer.start_as_current_span("step3_export_dxf") as span:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                
                export_payload = {
                    "session_id": session_id,
                    "cluster_dict": clusters,
                    "export_type": "detailed"
                }
                
                response = await client.post(
                    f"{self.dxf_export_url}/export-dxf",
                    json=export_payload
                )
                
                if response.status_code != 200:
                    raise Exception(f"DXF export failed: {response.status_code}")
                
                # For this implementation, we'll return a mock download URL
                # In reality, you'd handle the file response differently
                return {
                    'download_url': f'/api/v1/files/dxf/{session_id}/download',
                    'file_size_bytes': len(response.content) if hasattr(response, 'content') else 0,
                    'polygon_count': 0,  # Would be extracted from DXF service response
                    'export_time_ms': 0
                }
    
    def _create_default_clusters(self, bed_count: int) -> Dict[str, list]:
        """Create default clusters for demonstration"""
        # Simple clustering: every 3 beds in same cluster
        clusters = {}
        for i in range(bed_count):
            cluster_id = str(i // 3)
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(i)
        return clusters
    
    async def _compensate_workflow_failure(self, session_id: str):
        """Compensation logic for workflow failures"""
        logger.info(f"Executing compensation for failed workflow: {session_id}")
        
        try:
            # Cleanup image processing session
            async with httpx.AsyncClient(timeout=30.0) as client:
                await client.delete(f"{self.image_processing_url}/session/{session_id}")
                logger.info(f"Cleaned up session: {session_id}")
        except Exception as e:
            logger.error(f"Compensation cleanup failed: {str(e)}")
    
    async def health_check_dependencies(self) -> Dict[str, str]:
        """Check health of all dependent services"""
        services = {
            "image-processing": f"{self.image_processing_url}/health",
            "clustering": f"{self.clustering_url}/health", 
            "dxf-export": f"{self.dxf_export_url}/health"
        }
        
        health_status = {}
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            for service_name, health_url in services.items():
                try:
                    response = await client.get(health_url)
                    health_status[service_name] = "healthy" if response.status_code == 200 else "unhealthy"
                except Exception:
                    health_status[service_name] = "unreachable"
        
        return health_status