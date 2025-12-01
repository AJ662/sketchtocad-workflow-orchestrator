from sqlalchemy import Column, String, Integer, DateTime, Text, JSON, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import enum

from ..events.types import SagaStatus

Base = declarative_base()


class Saga(Base):
    """Main saga execution record"""
    __tablename__ = 'sagas'
    
    id = Column(String(64), primary_key=True)  # saga_id
    workflow_type = Column(String(50), nullable=False)  # e.g., "image_to_cad"
    status = Column(SQLEnum(SagaStatus), nullable=False, default=SagaStatus.STARTED)
    current_step = Column(String(50), nullable=True)  # Current step name
    session_id = Column(String(64), nullable=False, index=True)  # Links to image processing session
    
    # Timestamps
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    
    # Result data
    result_data = Column(JSON, nullable=True)  # Final workflow result
    error_message = Column(Text, nullable=True)  # Error if failed
    
    # Metrics
    total_duration_ms = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<Saga(id={self.id}, status={self.status}, workflow_type={self.workflow_type})>"


class SagaStepLog(Base):
    """Log of each step in the saga"""
    __tablename__ = 'saga_step_logs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    saga_id = Column(String(64), nullable=False, index=True)
    
    step_number = Column(Integer, nullable=False)  # Sequence: 1, 2, 3...
    step_name = Column(String(50), nullable=False)  # e.g., "image_processing"
    status = Column(String(20), nullable=False)  # started, completed, failed
    
    event_type = Column(String(50), nullable=True)  # Event that triggered this step
    correlation_id = Column(String(64), nullable=True)  # For tracing
    
    # Step data
    input_data = Column(JSON, nullable=True)  # Input payload
    output_data = Column(JSON, nullable=True)  # Output payload
    error_message = Column(Text, nullable=True)
    
    # Timestamps
    started_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    
    def __repr__(self):
        return f"<SagaStepLog(saga_id={self.saga_id}, step={self.step_name}, status={self.status})>"


class SagaCompensation(Base):
    """Track compensation actions"""
    __tablename__ = 'saga_compensations'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    saga_id = Column(String(64), nullable=False, index=True)
    
    step_name = Column(String(50), nullable=False)  # Step being compensated
    compensation_action = Column(String(100), nullable=False)  # What was done
    status = Column(String(20), nullable=False)  # completed, failed
    
    error_message = Column(Text, nullable=True)
    
    executed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<SagaCompensation(saga_id={self.saga_id}, step={self.step_name}, status={self.status})>"