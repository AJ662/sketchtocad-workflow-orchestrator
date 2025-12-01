from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, Any
import uuid

from .types import EventType


class SagaEvent(BaseModel):    
    saga_id: str = Field(..., description="Unique saga identifier")
    event_type: EventType = Field(..., description="Type of event")
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="For distributed tracing")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="When event was created")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event-specific data")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata (user_id, etc)")
    
    # Retry and error handling
    retry_count: int = Field(default=0, description="Number of times this event has been retried")
    error_message: Optional[str] = Field(default=None, description="Error message if event failed")
    
    class Config:
        use_enum_values = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }