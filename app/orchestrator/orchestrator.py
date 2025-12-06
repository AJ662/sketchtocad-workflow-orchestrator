# sketchtocad-workflow-orchestrator/app/orchestrator/orchestrator.py
import logging
import uuid
from typing import Dict, Any
from datetime import datetime

from ..events import (
    KafkaEventBus,
    SagaEvent,
    EventType,
    WorkflowStarted,
    ImageProcessingRequested,
    EnhancedColorsRequested,
    ClusteringRequested,
    DXFExportRequested,
    WorkflowCompleted,
    EnhancementSelected,
    ClusteringSubmitted,
    ExportRequested,
    SagaStatus
)
from ..database.saga_repository import SagaRepository
from ..database.init_db import SessionLocal

logger = logging.getLogger(__name__)


class Orchestrator:
    """Event-driven saga orchestrator with human-in-the-loop support"""
    
    def __init__(self, event_bus: KafkaEventBus):
        self.event_bus = event_bus
        
        self.step_handlers = {
            EventType.WORKFLOW_STARTED: self._handle_workflow_started,
            EventType.IMAGE_PROCESSED: self._handle_image_processed,
            EventType.ENHANCED_COLORS_GENERATED: self._handle_enhanced_colors_generated,
            EventType.ENHANCEMENT_SELECTED: self._handle_enhancement_selected,
            EventType.CLUSTERING_SUBMITTED: self._handle_clustering_submitted,
            EventType.CLUSTERING_COMPLETED: self._handle_clustering_completed,
            EventType.EXPORT_REQUESTED: self._handle_export_requested,
            EventType.DXF_EXPORTED: self._handle_dxf_exported,
            EventType.WORKFLOW_FAILED: self._handle_workflow_failed,
        }
        
        self.step_numbers = {
            'image_processing': 1,
            'enhanced_colors': 2,
            'enhancement_selection': 3,
            'clustering': 4,
            'dxf_export': 5
        }
    
    def _get_repo(self) -> SagaRepository:
        db = SessionLocal()
        return SagaRepository(db)
    
    async def start_workflow(self, session_id: str, image_filename: str) -> str:
        saga_id = f"saga_{uuid.uuid4().hex}"
        
        repo = self._get_repo()
        try:
            repo.create_saga(
                saga_id=saga_id,
                workflow_type="image_to_cad",
                session_id=session_id
            )
            logger.info(f"Created saga {saga_id} for session {session_id}")
        finally:
            repo.db.close()
        
        event = WorkflowStarted(
            saga_id=saga_id,
            session_id=session_id,
            image_filename=image_filename
        )
        await self.event_bus.publish('saga-events', event)
        
        logger.info(f"Workflow started: saga_id={saga_id}")
        return saga_id
    
    async def resume_with_enhancement(self, saga_id: str, enhancement_method: str) -> bool:
        repo = self._get_repo()
        try:
            saga = repo.get_saga(saga_id)
            if not saga or saga.status != SagaStatus.AWAITING_ENHANCEMENT_SELECTION:
                logger.error(f"Cannot resume saga {saga_id}: invalid status {saga.status if saga else 'not found'}")
                return False
            
            event = EnhancementSelected(
                saga_id=saga_id,
                session_id=saga.session_id,
                enhancement_method=enhancement_method
            )
            await self.event_bus.publish('saga-events', event)
            return True
        finally:
            repo.db.close()
    
    async def resume_with_clustering(self, saga_id: str, clusters_data: Dict) -> bool:
        repo = self._get_repo()
        try:
            saga = repo.get_saga(saga_id)
            if not saga or saga.status != SagaStatus.AWAITING_CLUSTERING:
                logger.error(f"Cannot resume saga {saga_id}: invalid status {saga.status if saga else 'not found'}")
                return False
            
            event = ClusteringSubmitted(
                saga_id=saga_id,
                session_id=saga.session_id,
                clusters_data=clusters_data
            )
            await self.event_bus.publish('saga-events', event)
            return True
        finally:
            repo.db.close()
    
    async def resume_with_export(self, saga_id: str, export_type: str = 'detailed') -> bool:
        repo = self._get_repo()
        try:
            saga = repo.get_saga(saga_id)
            if not saga or saga.status != SagaStatus.AWAITING_EXPORT:
                logger.error(f"Cannot resume saga {saga_id}: invalid status {saga.status if saga else 'not found'}")
                return False
            
            event = ExportRequested(
                saga_id=saga_id,
                session_id=saga.session_id,
                export_type=export_type
            )
            await self.event_bus.publish('saga-events', event)
            return True
        finally:
            repo.db.close()
    
    async def handle_event(self, event: SagaEvent):
        handler = self.step_handlers.get(event.event_type)
        if handler:
            logger.info(f"Handling event: {event.event_type} [saga_id={event.saga_id}]")
            await handler(event)
        else:
            logger.debug(f"No handler for event type: {event.event_type}")
    
    async def _handle_workflow_started(self, event: SagaEvent):
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        
        repo = self._get_repo()
        try:
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.IMAGE_PROCESSING, current_step='image_processing')
            repo.log_step_started(saga_id=saga_id, step_number=self.step_numbers['image_processing'], step_name='image_processing', event_type=EventType.IMAGE_PROCESSING_REQUESTED, correlation_id=event.correlation_id, input_data={'session_id': session_id})
        finally:
            repo.db.close()
        
        cmd_event = ImageProcessingRequested(saga_id=saga_id, session_id=session_id, image_filename=event.payload.get('image_filename', 'unknown.jpg'), correlation_id=event.correlation_id)
        await self.event_bus.publish('saga-commands', cmd_event)
        logger.info(f"Saga {saga_id}: Requested image processing")
    
    async def _handle_image_processed(self, event: SagaEvent):
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        bed_data = event.payload['bed_data']
        bed_count = event.payload['bed_count']
        processing_time_ms = event.payload.get('processing_time_ms', 0)
        statistics = event.payload.get('statistics', {})
        image_shape = event.payload.get('image_shape', [])
        
        repo = self._get_repo()
        try:
            repo.log_step_completed(saga_id=saga_id, step_name='image_processing', output_data={'bed_count': bed_count, 'processing_time_ms': processing_time_ms})
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.GENERATING_ENHANCED_COLORS, current_step='enhanced_colors')
            repo.log_step_started(saga_id=saga_id, step_number=self.step_numbers['enhanced_colors'], step_name='enhanced_colors', event_type=EventType.ENHANCED_COLORS_REQUESTED, correlation_id=event.correlation_id, input_data={'bed_count': bed_count})
            repo.set_saga_result(saga_id=saga_id, result_data={'session_id': session_id, 'bed_count': bed_count, 'bed_data': bed_data, 'statistics': statistics, 'image_shape': image_shape, 'processing_time_ms': processing_time_ms})
        finally:
            repo.db.close()
        
        cmd_event = EnhancedColorsRequested(saga_id=saga_id, session_id=session_id, bed_data=bed_data, correlation_id=event.correlation_id)
        await self.event_bus.publish('saga-commands', cmd_event)
        logger.info(f"Saga {saga_id}: Image processed, requesting enhanced colors")
    
    async def _handle_enhanced_colors_generated(self, event: SagaEvent):
        saga_id = event.saga_id
        enhanced_colors = event.payload['enhanced_colors']
        enhancement_methods = event.payload['enhancement_methods']
        
        repo = self._get_repo()
        try:
            repo.log_step_completed(saga_id=saga_id, step_name='enhanced_colors', output_data={'enhancement_methods': enhancement_methods})
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.AWAITING_ENHANCEMENT_SELECTION, current_step='enhancement_selection')
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            result_data['enhanced_colors'] = enhanced_colors
            result_data['enhancement_methods'] = enhancement_methods
            result_data['awaiting'] = 'enhancement_selection'
            repo.set_saga_result(saga_id=saga_id, result_data=result_data)
        finally:
            repo.db.close()
        logger.info(f"Saga {saga_id}: Enhanced colors generated, awaiting user selection")
    
    async def _handle_enhancement_selected(self, event: SagaEvent):
        saga_id = event.saga_id
        enhancement_method = event.payload['enhancement_method']
        
        repo = self._get_repo()
        try:
            repo.log_step_started(saga_id=saga_id, step_number=self.step_numbers['enhancement_selection'], step_name='enhancement_selection', event_type=EventType.ENHANCEMENT_SELECTED, correlation_id=event.correlation_id, input_data={'enhancement_method': enhancement_method})
            repo.log_step_completed(saga_id=saga_id, step_name='enhancement_selection', output_data={'enhancement_method': enhancement_method})
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.AWAITING_CLUSTERING, current_step='clustering')
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            result_data['enhancement_method'] = enhancement_method
            result_data['awaiting'] = 'clustering'
            repo.set_saga_result(saga_id=saga_id, result_data=result_data)
        finally:
            repo.db.close()
        logger.info(f"Saga {saga_id}: Enhancement selected ({enhancement_method}), awaiting clustering")
    
    async def _handle_clustering_submitted(self, event: SagaEvent):
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        clusters_data = event.payload['clusters_data']
        
        repo = self._get_repo()
        try:
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.PROCESSING_CLUSTERING, current_step='clustering')
            repo.log_step_started(saga_id=saga_id, step_number=self.step_numbers['clustering'], step_name='clustering', event_type=EventType.CLUSTERING_REQUESTED, correlation_id=event.correlation_id, input_data={'cluster_count': len(clusters_data)})
        finally:
            repo.db.close()
        
        cmd_event = ClusteringRequested(saga_id=saga_id, session_id=session_id, bed_data=result_data.get('bed_data', []), enhanced_colors=result_data.get('enhanced_colors', {}), clusters_data=clusters_data, correlation_id=event.correlation_id)
        await self.event_bus.publish('saga-commands', cmd_event)
        logger.info(f"Saga {saga_id}: Clustering submitted to service")
    
    async def _handle_clustering_completed(self, event: SagaEvent):
        saga_id = event.saga_id
        processed_clusters = event.payload['processed_clusters']
        cluster_count = event.payload['cluster_count']
        clustering_statistics = event.payload.get('statistics', {})
        
        repo = self._get_repo()
        try:
            repo.log_step_completed(saga_id=saga_id, step_name='clustering', output_data={'cluster_count': cluster_count, 'processed_clusters': processed_clusters})
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.AWAITING_EXPORT, current_step='dxf_export')
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            result_data['processed_clusters'] = processed_clusters
            result_data['clustering_statistics'] = clustering_statistics
            result_data['awaiting'] = 'export'
            repo.set_saga_result(saga_id=saga_id, result_data=result_data)
        finally:
            repo.db.close()
        logger.info(f"Saga {saga_id}: Clustering completed, awaiting export request")
    
    async def _handle_export_requested(self, event: SagaEvent):
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        export_type = event.payload.get('export_type', 'detailed')
        
        repo = self._get_repo()
        try:
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.DXF_EXPORT, current_step='dxf_export')
            repo.log_step_started(saga_id=saga_id, step_number=self.step_numbers['dxf_export'], step_name='dxf_export', event_type=EventType.DXF_EXPORT_REQUESTED, correlation_id=event.correlation_id, input_data={'export_type': export_type})
        finally:
            repo.db.close()
        
        cmd_event = DXFExportRequested(saga_id=saga_id, session_id=session_id, cluster_dict=result_data.get('processed_clusters', {}), export_type=export_type, correlation_id=event.correlation_id)
        await self.event_bus.publish('saga-commands', cmd_event)
        logger.info(f"Saga {saga_id}: DXF export requested")
    
    async def _handle_dxf_exported(self, event: SagaEvent):
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        download_url = event.payload['download_url']
        file_size_bytes = event.payload.get('file_size_bytes', 0)
        export_time_ms = event.payload.get('export_time_ms', 0)
        
        repo = self._get_repo()
        try:
            repo.log_step_completed(saga_id=saga_id, step_name='dxf_export', output_data={'download_url': download_url, 'file_size_bytes': file_size_bytes, 'export_time_ms': export_time_ms})
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.COMPLETED, current_step=None)
            saga = repo.get_saga(saga_id)
            result_data = saga.result_data or {}
            result_data['download_url'] = download_url
            result_data['file_size_bytes'] = file_size_bytes
            result_data['export_time_ms'] = export_time_ms
            result_data['awaiting'] = None
            result_data['completed_at'] = datetime.utcnow().isoformat()
            repo.set_saga_result(saga_id=saga_id, result_data=result_data)
        finally:
            repo.db.close()
        
        completed_event = WorkflowCompleted(saga_id=saga_id, session_id=session_id, total_time_ms=0, download_url=download_url, correlation_id=event.correlation_id)
        await self.event_bus.publish('saga-events', completed_event)
        logger.info(f"Saga {saga_id}: COMPLETED successfully!")
    
    async def _handle_workflow_failed(self, event: SagaEvent):
        saga_id = event.saga_id
        failed_step = event.payload['failed_step']
        error_message = event.error_message or "Unknown error"
        
        repo = self._get_repo()
        try:
            repo.log_step_failed(saga_id=saga_id, step_name=failed_step, error_message=error_message)
            repo.update_saga_status(saga_id=saga_id, status=SagaStatus.FAILED, current_step=failed_step, error_message=error_message)
        finally:
            repo.db.close()
        logger.error(f"Saga {saga_id}: FAILED at {failed_step}: {error_message}")
    
    async def run(self):
        logger.info("Event-driven orchestrator starting...")
        async for event in self.event_bus.subscribe(topics=['saga-events'], group_id='orchestrator-group'):
            try:
                await self.handle_event(event)
            except Exception as e:
                logger.error(f"Failed to handle event {event.event_type} for saga {event.saga_id}: {e}", exc_info=True)