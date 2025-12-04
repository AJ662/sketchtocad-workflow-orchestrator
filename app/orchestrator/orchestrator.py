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
    WorkflowCompleted,
    WorkflowFailed,
    SagaStatus
)
from ..database.saga_repository import SagaRepository
from ..database.init_db import SessionLocal

logger = logging.getLogger(__name__)


class Orchestrator:
    """Event-driven saga orchestrator - stops after image processing for manual clustering"""
    
    def __init__(self, event_bus: KafkaEventBus):
        self.event_bus = event_bus
        
        self.step_handlers = {
            EventType.WORKFLOW_STARTED: self._handle_workflow_started,
            EventType.IMAGE_PROCESSED: self._handle_image_processed,
            EventType.WORKFLOW_FAILED: self._handle_workflow_failed,
        }
        
        self.step_numbers = {
            'image_processing': 1,
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
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        
        repo = self._get_repo()
        try:
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.IMAGE_PROCESSING,
                current_step='image_processing'
            )
            
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
        
        cmd_event = ImageProcessingRequested(
            saga_id=saga_id,
            session_id=session_id,
            image_filename=event.payload.get('image_filename', 'unknown.jpg'),
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-commands', cmd_event)
        
        logger.info(f"Saga {saga_id}: Requested image processing")
    
    async def _handle_image_processed(self, event: SagaEvent):
        """Handle image processing complete - workflow done, return bed_data to frontend"""
        saga_id = event.saga_id
        session_id = event.payload['session_id']
        bed_data = event.payload['bed_data']
        bed_count = event.payload['bed_count']
        processing_time_ms = event.payload.get('processing_time_ms', 0)
        
        repo = self._get_repo()
        try:
            repo.log_step_completed(
                saga_id=saga_id,
                step_name='image_processing',
                output_data={
                    'bed_count': bed_count,
                    'processing_time_ms': processing_time_ms
                }
            )
            
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.COMPLETED,
                current_step=None
            )
            
            repo.set_saga_result(
                saga_id=saga_id,
                result_data={
                    'session_id': session_id,
                    'bed_count': bed_count,
                    'bed_data': bed_data,
                    'processing_time_ms': processing_time_ms,
                    'completed_at': datetime.utcnow().isoformat()
                }
            )
        finally:
            repo.db.close()
        
        completed_event = WorkflowCompleted(
            saga_id=saga_id,
            session_id=session_id,
            total_time_ms=processing_time_ms,
            download_url='',
            correlation_id=event.correlation_id
        )
        await self.event_bus.publish('saga-events', completed_event)
        
        logger.info(f"Saga {saga_id}: COMPLETED - {bed_count} beds detected")
    
    async def _handle_workflow_failed(self, event: SagaEvent):
        saga_id = event.saga_id
        failed_step = event.payload['failed_step']
        error_message = event.error_message or "Unknown error"
        
        repo = self._get_repo()
        try:
            repo.log_step_failed(
                saga_id=saga_id,
                step_name=failed_step,
                error_message=error_message
            )
            
            repo.update_saga_status(
                saga_id=saga_id,
                status=SagaStatus.FAILED,
                current_step=failed_step,
                error_message=error_message
            )
        finally:
            repo.db.close()
        
        logger.error(f"Saga {saga_id}: FAILED at {failed_step}: {error_message}")
    
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