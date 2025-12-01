from enum import Enum

class EventType(str, Enum):
    # Command events (requests to do work)
    IMAGE_PROCESSING_REQUESTED = "image_processing_requested"
    CLUSTERING_REQUESTED = "clustering_requested"
    DXF_EXPORT_REQUESTED = "dxf_export_requested"
    
    # Success events
    IMAGE_PROCESSED = "image_processed"
    CLUSTERING_COMPLETED = "clustering_completed"
    DXF_EXPORTED = "dxf_exported"
    
    # Workflow control
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    WORKFLOW_FAILED = "workflow_failed"
    
    # Compensation events
    COMPENSATION_REQUESTED = "compensation_requested"
    COMPENSATION_COMPLETED = "compensation_completed"


class SagaStatus(str, Enum):
    STARTED = "started"
    IMAGE_PROCESSING = "image_processing"
    CLUSTERING = "clustering"
    DXF_EXPORT = "dxf_export"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"