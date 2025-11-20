from prometheus_client import Counter, Histogram, Gauge


class Metrics:
    """Custom Prometheus metrics for workflow orchestrator"""
    
    def __init__(self):
        # Counters
        self.workflows_total = Counter(
            'workflows_total',
            'Total number of workflows processed',
            ['status']  # success, failure
        )
        
        self.workflow_steps_total = Counter(
            'workflow_steps_total', 
            'Total number of workflow steps executed',
            ['step', 'status']  # step: image_processing/clustering/dxf_export, status: success/failure
        )
        
        # Histograms
        self.workflow_duration_seconds = Histogram(
            'workflow_duration_seconds',
            'Time spent processing complete workflows',
            buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        )
        
        self.workflow_step_duration_seconds = Histogram(
            'workflow_step_duration_seconds',
            'Time spent on individual workflow steps',
            ['step'],
            buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        )
        
        # Gauges
        self.active_workflows = Gauge(
            'active_workflows',
            'Number of workflows currently being processed'
        )


def setup_metrics():
    """Setup Prometheus metrics"""
    return Metrics()