from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional


class BedData(BaseModel):
    bed_id: int
    area: int
    rgb_median: List[int]
    rgb_mean: List[int]
    clean_pixel_count: int


class WorkflowRequest(BaseModel):
    # File will be handled as UploadFile in the endpoint
    pass


class ProcessingResult(BaseModel):
    session_id: str
    bed_count: int
    bed_data: List[BedData]
    statistics: Dict[str, Any]
    image_shape: List[int]
    processing_time_ms: float


class ClusteringResult(BaseModel):
    final_labels: List[int]
    processed_clusters: Dict[str, List[int]]
    statistics: Dict[str, Any]


class DXFExportResult(BaseModel):
    download_url: str
    file_size_bytes: int
    polygon_count: int
    export_time_ms: float


class WorkflowResponse(BaseModel):
    session_id: str
    bed_count: int
    cluster_count: int
    dxf_download_url: str
    processing_summary: Dict[str, Any]
    total_processing_time_ms: float


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str
    dependencies: Dict[str, str] = {}