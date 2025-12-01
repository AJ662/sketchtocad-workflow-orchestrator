from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime
import logging

from .saga_models import Saga, SagaStepLog, SagaCompensation
from ..events.types import SagaStatus

logger = logging.getLogger(__name__)


class SagaRepository:
    """Repository for saga state management"""
    
    def __init__(self, db: Session):
        self.db = db
    
    # Saga CRUD
    
    def create_saga(
        self,
        saga_id: str,
        workflow_type: str,
        session_id: str
    ) -> Saga:
        """Create a new saga"""
        saga = Saga(
            id=saga_id,
            workflow_type=workflow_type,
            session_id=session_id,
            status=SagaStatus.STARTED,
            created_at=datetime.utcnow()
        )
        self.db.add(saga)
        self.db.commit()
        self.db.refresh(saga)
        
        logger.info(f"Created saga: {saga_id} [workflow={workflow_type}]")
        return saga
    
    def get_saga(self, saga_id: str) -> Optional[Saga]:
        """Get saga by ID"""
        return self.db.query(Saga).filter(Saga.id == saga_id).first()
    
    def update_saga_status(
        self,
        saga_id: str,
        status: SagaStatus,
        current_step: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> Saga:
        """Update saga status"""
        saga = self.get_saga(saga_id)
        if not saga:
            raise ValueError(f"Saga not found: {saga_id}")
        
        saga.status = status
        saga.updated_at = datetime.utcnow()
        
        if current_step:
            saga.current_step = current_step
        
        if error_message:
            saga.error_message = error_message
        
        if status in [SagaStatus.COMPLETED, SagaStatus.FAILED, SagaStatus.COMPENSATED]:
            saga.completed_at = datetime.utcnow()
            if saga.created_at:
                saga.total_duration_ms = int(
                    (saga.completed_at - saga.created_at).total_seconds() * 1000
                )
        
        self.db.commit()
        self.db.refresh(saga)
        
        logger.info(f"Updated saga {saga_id}: status={status}, step={current_step}")
        return saga
    
    def set_saga_result(self, saga_id: str, result_data: dict) -> Saga:
        """Set final result data"""
        saga = self.get_saga(saga_id)
        if not saga:
            raise ValueError(f"Saga not found: {saga_id}")
        
        saga.result_data = result_data
        self.db.commit()
        self.db.refresh(saga)
        
        return saga
    
    def get_sagas_by_status(self, status: SagaStatus, limit: int = 100) -> List[Saga]:
        """Get sagas by status"""
        return self.db.query(Saga).filter(Saga.status == status).limit(limit).all()
    
    def get_sagas_by_session(self, session_id: str) -> List[Saga]:
        """Get all sagas for a session"""
        return self.db.query(Saga).filter(Saga.session_id == session_id).all()
    
    # Step Logs
    
    def log_step_started(
        self,
        saga_id: str,
        step_number: int,
        step_name: str,
        event_type: str,
        correlation_id: str,
        input_data: Optional[dict] = None
    ) -> SagaStepLog:
        """Log that a step has started"""
        step_log = SagaStepLog(
            saga_id=saga_id,
            step_number=step_number,
            step_name=step_name,
            status='started',
            event_type=event_type,
            correlation_id=correlation_id,
            input_data=input_data,
            started_at=datetime.utcnow()
        )
        self.db.add(step_log)
        self.db.commit()
        self.db.refresh(step_log)
        
        logger.info(f"Saga {saga_id}: Step {step_number} ({step_name}) started")
        return step_log
    
    def log_step_completed(
        self,
        saga_id: str,
        step_name: str,
        output_data: Optional[dict] = None
    ) -> SagaStepLog:
        """Log that a step completed successfully"""
        step_log = self.db.query(SagaStepLog).filter(
            SagaStepLog.saga_id == saga_id,
            SagaStepLog.step_name == step_name,
            SagaStepLog.status == 'started'
        ).order_by(SagaStepLog.id.desc()).first()
        
        if not step_log:
            raise ValueError(f"No started step found: {saga_id}/{step_name}")
        
        step_log.status = 'completed'
        step_log.output_data = output_data
        step_log.completed_at = datetime.utcnow()
        
        if step_log.started_at:
            step_log.duration_ms = int(
                (step_log.completed_at - step_log.started_at).total_seconds() * 1000
            )
        
        self.db.commit()
        self.db.refresh(step_log)
        
        logger.info(f"Saga {saga_id}: Step {step_name} completed in {step_log.duration_ms}ms")
        return step_log
    
    def log_step_failed(
        self,
        saga_id: str,
        step_name: str,
        error_message: str
    ) -> SagaStepLog:
        """Log that a step failed"""
        step_log = self.db.query(SagaStepLog).filter(
            SagaStepLog.saga_id == saga_id,
            SagaStepLog.step_name == step_name,
            SagaStepLog.status == 'started'
        ).order_by(SagaStepLog.id.desc()).first()
        
        if not step_log:
            # Create a new log entry for the failure
            step_log = SagaStepLog(
                saga_id=saga_id,
                step_number=0,
                step_name=step_name,
                status='failed',
                error_message=error_message,
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow()
            )
            self.db.add(step_log)
        else:
            step_log.status = 'failed'
            step_log.error_message = error_message
            step_log.completed_at = datetime.utcnow()
            
            if step_log.started_at:
                step_log.duration_ms = int(
                    (step_log.completed_at - step_log.started_at).total_seconds() * 1000
                )
        
        self.db.commit()
        self.db.refresh(step_log)
        
        logger.error(f"Saga {saga_id}: Step {step_name} failed: {error_message}")
        return step_log
    
    def get_saga_steps(self, saga_id: str) -> List[SagaStepLog]:
        """Get all steps for a saga"""
        return self.db.query(SagaStepLog).filter(
            SagaStepLog.saga_id == saga_id
        ).order_by(SagaStepLog.step_number).all()
    
    def get_completed_steps(self, saga_id: str) -> List[str]:
        """Get list of completed step names"""
        steps = self.db.query(SagaStepLog.step_name).filter(
            SagaStepLog.saga_id == saga_id,
            SagaStepLog.status == 'completed'
        ).all()
        return [step[0] for step in steps]
    
    # Compensation
    
    def log_compensation(
        self,
        saga_id: str,
        step_name: str,
        compensation_action: str,
        status: str = 'completed',
        error_message: Optional[str] = None
    ) -> SagaCompensation:
        """Log a compensation action"""
        compensation = SagaCompensation(
            saga_id=saga_id,
            step_name=step_name,
            compensation_action=compensation_action,
            status=status,
            error_message=error_message,
            executed_at=datetime.utcnow()
        )
        self.db.add(compensation)
        self.db.commit()
        self.db.refresh(compensation)
        
        logger.info(f"Saga {saga_id}: Compensation for {step_name} - {compensation_action}")
        return compensation
    
    def get_compensations(self, saga_id: str) -> List[SagaCompensation]:
        """Get all compensation actions for a saga"""
        return self.db.query(SagaCompensation).filter(
            SagaCompensation.saga_id == saga_id
        ).all()