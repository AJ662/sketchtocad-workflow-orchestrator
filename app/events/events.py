from typing import List, Dict
from .base import SagaEvent
from .types import EventType


# Command Events (Requests)

class ImageProcessingRequested(SagaEvent):
    """Request to process an uploaded image"""
    
    def __init__(self, saga_id: str, session_id: str, image_filename: str, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.IMAGE_PROCESSING_REQUESTED,
            payload={
                "session_id": session_id,
                "image_filename": image_filename
            },
            **kwargs
        )


class ClusteringRequested(SagaEvent):
    """Request to cluster plant beds"""
    
    def __init__(self, saga_id: str, session_id: str, bed_data: List[Dict], **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.CLUSTERING_REQUESTED,
            payload={
                "session_id": session_id,
                "bed_data": bed_data
            },
            **kwargs
        )


class DXFExportRequested(SagaEvent):
    """Request to export DXF file"""
    
    def __init__(self, saga_id: str, session_id: str, cluster_dict: Dict, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.DXF_EXPORT_REQUESTED,
            payload={
                "session_id": session_id,
                "cluster_dict": cluster_dict
            },
            **kwargs
        )


# Success Events

class ImageProcessed(SagaEvent):
    """Image processing completed successfully"""
    
    def __init__(self, saga_id: str, session_id: str, bed_count: int, bed_data: List[Dict], 
                 processing_time_ms: float, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.IMAGE_PROCESSED,
            payload={
                "session_id": session_id,
                "bed_count": bed_count,
                "bed_data": bed_data,
                "processing_time_ms": processing_time_ms
            },
            **kwargs
        )


class ClusteringCompleted(SagaEvent):
    """Clustering completed successfully"""
    
    def __init__(self, saga_id: str, session_id: str, processed_clusters: Dict, 
                 cluster_count: int, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.CLUSTERING_COMPLETED,
            payload={
                "session_id": session_id,
                "processed_clusters": processed_clusters,
                "cluster_count": cluster_count
            },
            **kwargs
        )


class DXFExported(SagaEvent):
    """DXF export completed successfully"""
    
    def __init__(self, saga_id: str, session_id: str, download_url: str, 
                 file_size_bytes: int, export_time_ms: float, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.DXF_EXPORTED,
            payload={
                "session_id": session_id,
                "download_url": download_url,
                "file_size_bytes": file_size_bytes,
                "export_time_ms": export_time_ms
            },
            **kwargs
        )


# Workflow Control Events

class WorkflowStarted(SagaEvent):
    """Workflow has been initiated"""
    
    def __init__(self, saga_id: str, session_id: str, image_filename: str, 
                 workflow_type: str = "image_to_cad", **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.WORKFLOW_STARTED,
            payload={
                "session_id": session_id,
                "workflow_type": workflow_type,
                "image_filename": image_filename
            },
            **kwargs
        )


class WorkflowCompleted(SagaEvent):
    """Entire workflow completed successfully"""
    
    def __init__(self, saga_id: str, session_id: str, total_time_ms: float, 
                 download_url: str, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.WORKFLOW_COMPLETED,
            payload={
                "session_id": session_id,
                "total_time_ms": total_time_ms,
                "download_url": download_url
            },
            **kwargs
        )


class WorkflowFailed(SagaEvent):
    """Workflow failed at some step"""
    
    def __init__(self, saga_id: str, session_id: str, failed_step: str, 
                 error_message: str, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.WORKFLOW_FAILED,
            payload={
                "session_id": session_id,
                "failed_step": failed_step
            },
            error_message=error_message,
            **kwargs
        )


# Compensation Events

class CompensationRequested(SagaEvent):
    """Request to compensate/rollback completed steps"""
    
    def __init__(self, saga_id: str, session_id: str, completed_steps: List[str], **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.COMPENSATION_REQUESTED,
            payload={
                "session_id": session_id,
                "completed_steps": completed_steps
            },
            **kwargs
        )


class CompensationCompleted(SagaEvent):
    """Compensation completed"""
    
    def __init__(self, saga_id: str, session_id: str, compensated_steps: List[str], **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.COMPENSATION_COMPLETED,
            payload={
                "session_id": session_id,
                "compensated_steps": compensated_steps
            },
            **kwargs
        )