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
    
    # User input events (resume saga with user data)
    ENHANCEMENT_SELECTED = "enhancement_selected"
    CLUSTERING_SUBMITTED = "clustering_submitted"
    EXPORT_REQUESTED = "export_requested"
    
    # Compensation events
    COMPENSATION_REQUESTED = "compensation_requested"
    COMPENSATION_COMPLETED = "compensation_completed"


class SagaStatus(str, Enum):
    STARTED = "started"
    IMAGE_PROCESSING = "image_processing"
    AWAITING_ENHANCEMENT_SELECTION = "awaiting_enhancement_selection"
    GENERATING_ENHANCED_COLORS = "generating_enhanced_colors"
    AWAITING_CLUSTERING = "awaiting_clustering"
    PROCESSING_CLUSTERING = "processing_clustering"
    AWAITING_EXPORT = "awaiting_export"
    DXF_EXPORT = "dxf_export"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"