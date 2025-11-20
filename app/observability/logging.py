import logging
import sys
from pythonjsonlogger import jsonlogger


def setup_logging():
    """Setup structured JSON logging"""
    logHandler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        fmt="%(timestamp)s %(service)s %(level)s %(name)s %(message)s"
    )
    logHandler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.addHandler(logHandler)
    logger.setLevel(logging.INFO)
    
    # Add service context to all log records
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.service = "workflow-orchestrator"
        record.timestamp = record.created
        return record
    logging.setLogRecordFactory(record_factory)
    
    return logger