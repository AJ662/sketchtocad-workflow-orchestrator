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
    ImageProcessed,
    ClusteringRequested,
    ClusteringCompleted,
    DXFExportRequested,
    DXFExported,
    WorkflowCompleted,
    WorkflowFailed,
    CompensationRequested,
    SagaStatus
)
from ..database.saga_repository import SagaRepository
from ..database.init_db import SessionLocal

logger = logging.getLogger(__name__)


class Orchestrator:
    """Event-driven saga orchestrator"""
    
    def __init__(self, event_bus: KafkaEventBus):
        self.event_bus = event_bus
        
        # Step mapping
        self.step_handlers = {
            EventType.WORKFLOW_STARTED: self._handle_workflow_started,
            EventType.IMAGE_PROCESSED: self._handle_image_processed,
            EventType.CLUSTERING_COMPLETED: self._handle_clustering_completed,
            EventType.DXF_EXPORTED: self._handle_dxf_exported,
            EventType.WORKFLOW_FAILED: self._handle_workflow_failed,
        }
        
        # Step number mapping
        self.step_numbers = {
            'image_processing': 1,
            'clustering': 2,
            'dxf_export': 3
        }
    
    def _get_repo(self) -> SagaRepository:
        """Get a new repository instance with fresh DB session"""
        db = SessionLocal()
        return SagaRepository(db)
    
    async def start_workflow(self, session_id: str, image_filename: str) -> str:
        saga_id = f"saga_{uuid.uuid4().hex}"
        
        # Create saga in database
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
        
        # Publish workflow started event
        event = WorkflowStarted(
            saga_id=saga_id,
            session_id=session_id,
            image_filename=image_filename
        )
        await self.event_bus.publish('saga-events', event)
        
        logger.info(f"Workflow started: saga_id={saga_id}")
        return saga_id
    
    async def handle_event(self, event: SagaEvent):
        handler = self.step_handlers.get(event.event_type)
        
        if handler:
            logger.info(
                f"Handling event: {event.event_type} "
                f"[saga_id={event.saga_id}]"
            )
            await handler(event)
        else:
            logger.debug(f"No handler for event type: {event.event_type}")
    
    async def _handle_workflow_started(self, event: SagaEvent):
        """Handle workflow started - kick off image processing"""
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        
        repo = self._get_repo()
        try:
            # Update saga status
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.IMAGE_PROCESSING,
                current_step='image_processing'
            )
            
            # Log step started
            repo.log_step_started(
                saga_id=saga_id,
                step_number=self.step_numbers['image_processing'],
                step_name='image_processing',
                event_type=EventType.IMAGE_PROCESSING_REQUESTED,
                correlation_id=event.correlation_id,
                input_data={'session_id': session_id}
            )
        finally:
            repo.db.close()
        
        # Publish image processing command
        cmd_event = ImageProcessingRequested(
            saga_id=saga_id,
            session_id=session_id,
            image_filename=event.payload.get('image_filename', 'unknown.jpg'),
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-commands', cmd_event)
        
        logger.info(f"Saga {saga_id}: Requested image processing")
    
    async def _handle_image_processed(self, event: SagaEvent):
        """Handle image processing complete - kick off clustering"""
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        bed_data = event.payload['bed_data']
        
        repo = self._get_repo()
        try:
            # Log step completed
            repo.log_step_completed(
                saga_id=saga_id,
                step_name='image_processing',
                output_data={
                    'bed_count': event.payload['bed_count'],
                    'processing_time_ms': event.payload.get('processing_time_ms', 0)
                }
            )
            
            # Update saga status
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.CLUSTERING,
                current_step='clustering'
            )
            
            # Log next step started
            repo.log_step_started(
                saga_id=saga_id,
                step_number=self.step_numbers['clustering'],
                step_name='clustering',
                event_type=EventType.CLUSTERING_REQUESTED,
                correlation_id=event.correlation_id,
                input_data={'bed_count': len(bed_data)}
            )
        finally:
            repo.db.close()
        
        # Publish clustering command
        cmd_event = ClusteringRequested(
            saga_id=saga_id,
            session_id=session_id,
            bed_data=bed_data,
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-commands', cmd_event)
        
        logger.info(f"Saga {saga_id}: Requested clustering")
    
    async def _handle_clustering_completed(self, event: SagaEvent):
        """Handle clustering complete - kick off DXF export"""
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        processed_clusters = event.payload['processed_clusters']
        
        repo = self._get_repo()
        try:
            # Log step completed
            repo.log_step_completed(
                saga_id=saga_id,
                step_name='clustering',
                output_data={
                    'cluster_count': event.payload['cluster_count']
                }
            )
            
            # Update saga status
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.DXF_EXPORT,
                current_step='dxf_export'
            )
            
            # Log next step started
            repo.log_step_started(
                saga_id=saga_id,
                step_number=self.step_numbers['dxf_export'],
                step_name='dxf_export',
                event_type=EventType.DXF_EXPORT_REQUESTED,
                correlation_id=event.correlation_id,
                input_data={'cluster_count': len(processed_clusters)}
            )
        finally:
            repo.db.close()
        
        # Publish DXF export command
        cmd_event = DXFExportRequested(
            saga_id=saga_id,
            session_id=session_id,
            cluster_dict=processed_clusters,
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-commands', cmd_event)
        
        logger.info(f"Saga {saga_id}: Requested DXF export")
    
    async def _handle_dxf_exported(self, event: SagaEvent):
        """Handle DXF export complete - workflow done!"""
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        download_url = event.payload['download_url']
        
        repo = self._get_repo()
        try:
            # Log step completed
            repo.log_step_completed(
                saga_id=saga_id,
                step_name='dxf_export',
                output_data={
                    'download_url': download_url,
                    'file_size_bytes': event.payload.get('file_size_bytes', 0),
                    'export_time_ms': event.payload.get('export_time_ms', 0)
                }
            )
            
            # Update saga status to completed
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.COMPLETED,
                current_step=None
            )
            
            # Store final result
            repo.set_saga_result(
                saga_id=saga_id,
                result_data={
                    'session_id': session_id,
                    'download_url': download_url,
                    'completed_at': datetime.utcnow().isoformat()
                }
            )
        finally:
            repo.db.close()
        
        # Publish workflow completed event
        completed_event = WorkflowCompleted(
            saga_id=saga_id,
            session_id=session_id,
            total_time_ms=0,  # Will be calculated from saga record
            download_url=download_url,
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-events', completed_event)
        
        logger.info(f"Saga {saga_id}: COMPLETED successfully! ✅")
    
    async def _handle_workflow_failed(self, event: SagaEvent):
        """Handle workflow failure - trigger compensation"""
        saga_id = event.saga_id
        failed_step = event.payload['failed_step']
        error_message = event.error_message or "Unknown error"
        
        repo = self._get_repo()
        try:
            # Log step failed
            repo.log_step_failed(
                saga_id=saga_id,
                step_name=failed_step,
                error_message=error_message
            )
            
            # Update saga status to failed
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.FAILED,
                current_step=failed_step,
                error_message=error_message
            )
            
            # Get completed steps for compensation
            completed_steps = repo.get_completed_steps(saga_id)
            
            if completed_steps:
                # Update to compensating status
                repo.update_saga_status(
                    saga_id=saga_id,
                    status=SagaStatus.COMPENSATING
                )
                
                # Publish compensation request
                comp_event = CompensationRequested(
                    saga_id=saga_id,
                    session_id=event.payload['session_id'],
                    completed_steps=completed_steps,
                    correlation_id=event.correlation_id
                )
                await self.event_bus.publish('saga-commands', comp_event)
                
                logger.warning(
                    f"Saga {saga_id}: FAILED at {failed_step}. "
                    f"Triggering compensation for {len(completed_steps)} steps."
                )
            else:
                logger.error(
                    f"Saga {saga_id}: FAILED at {failed_step}. "
                    f"No steps to compensate."
                )
        finally:
            repo.db.close()
    
    async def run(self):
        logger.info("Event-driven orchestrator starting...")
        
        async for event in self.event_bus.subscribe(
            topics=['saga-events'],
            group_id='orchestrator-group'
        ):
            try:
                await self.handle_event(event)
            except Exception as e:
                logger.error(
                    f"Failed to handle event {event.event_type} "
                    f"for saga {event.saga_id}: {e}",
                    exc_info=True
                )
                # Event will be reprocessed due to no commit