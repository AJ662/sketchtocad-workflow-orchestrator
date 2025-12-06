from typing import List, Dict
from .base import SagaEvent
from .types import EventType


# Command Events (Requests)

class ImageProcessingRequested(SagaEvent):
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
    def __init__(self, saga_id: str, session_id: str, bed_data: List[Dict], 
                 enhanced_colors: Dict, clusters_data: Dict, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.CLUSTERING_REQUESTED,
            payload={
                "session_id": session_id,
                "bed_data": bed_data,
                "enhanced_colors": enhanced_colors,
                "clusters_data": clusters_data
            },
            **kwargs
        )


class DXFExportRequested(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, cluster_dict: Dict, 
                 export_type: str = 'detailed', **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.DXF_EXPORT_REQUESTED,
            payload={
                "session_id": session_id,
                "cluster_dict": cluster_dict,
                "export_type": export_type
            },
            **kwargs
        )


# Success Events

class ImageProcessed(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, bed_count: int, bed_data: List[Dict], 
                 processing_time_ms: float, statistics: Dict = None, image_shape: List[int] = None, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.IMAGE_PROCESSED,
            payload={
                "session_id": session_id,
                "bed_count": bed_count,
                "bed_data": bed_data,
                "processing_time_ms": processing_time_ms,
                "statistics": statistics or {},
                "image_shape": image_shape or []
            },
            **kwargs
        )


class ClusteringCompleted(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, processed_clusters: Dict, 
                 cluster_count: int, statistics: Dict = None, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.CLUSTERING_COMPLETED,
            payload={
                "session_id": session_id,
                "processed_clusters": processed_clusters,
                "cluster_count": cluster_count,
                "statistics": statistics or {}
            },
            **kwargs
        )


class DXFExported(SagaEvent):
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


# User Input Events (resume saga)

class EnhancementSelected(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, enhancement_method: str, 
                 enhanced_colors: Dict, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.ENHANCEMENT_SELECTED,
            payload={
                "session_id": session_id,
                "enhancement_method": enhancement_method,
                "enhanced_colors": enhanced_colors
            },
            **kwargs
        )


class ClusteringSubmitted(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, clusters_data: Dict, **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.CLUSTERING_SUBMITTED,
            payload={
                "session_id": session_id,
                "clusters_data": clusters_data
            },
            **kwargs
        )


class ExportRequested(SagaEvent):
    def __init__(self, saga_id: str, session_id: str, export_type: str = 'detailed', **kwargs):
        super().__init__(
            saga_id=saga_id,
            event_type=EventType.EXPORT_REQUESTED,
            payload={
                "session_id": session_id,
                "export_type": export_type
            },
            **kwargs
        )


# Workflow Control Events

class WorkflowStarted(SagaEvent):
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