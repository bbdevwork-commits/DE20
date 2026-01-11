import logging
import os
from datetime import datetime


def setup_logger(name, log_dir="/Users/bhavanamxavier/Applications/DE20/logs"):
    """Setup and return a logger with file and console handlers."""
    
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger
    
    # Create formatters
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler - daily log file
    log_filename = f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name):
    """Get or create a logger by name."""
    return setup_logger(name)


class PipelineLogger:
    """Pipeline execution logger with structured logging."""
    
    def __init__(self, pipeline_name="DataPipeline"):
        self.logger = setup_logger(pipeline_name)
        self.start_time = None
        self.step_times = {}
    
    def start_pipeline(self, config_path):
        """Log pipeline start."""
        self.start_time = datetime.now()
        self.logger.info("=" * 60)
        self.logger.info(f"PIPELINE STARTED")
        self.logger.info(f"Config: {config_path}")
        self.logger.info(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 60)
    
    def end_pipeline(self, status="SUCCESS"):
        """Log pipeline end with duration."""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        self.logger.info("=" * 60)
        self.logger.info(f"PIPELINE {status}")
        self.logger.info(f"End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Total Duration: {duration:.2f} seconds")
        self.logger.info("=" * 60)
    
    def start_step(self, step_name):
        """Log step start."""
        self.step_times[step_name] = datetime.now()
        self.logger.info("-" * 40)
        self.logger.info(f"STEP START: {step_name}")
    
    def end_step(self, step_name, records=None, status="SUCCESS"):
        """Log step end with duration."""
        end_time = datetime.now()
        start_time = self.step_times.get(step_name, end_time)
        duration = (end_time - start_time).total_seconds()
        
        msg = f"STEP END: {step_name} | Status: {status} | Duration: {duration:.2f}s"
        if records is not None:
            msg += f" | Records: {records}"
        
        self.logger.info(msg)
    
    def log_info(self, message):
        """Log info message."""
        self.logger.info(message)
    
    def log_warning(self, message):
        """Log warning message."""
        self.logger.warning(message)
    
    def log_error(self, message):
        """Log error message."""
        self.logger.error(message)
    
    def log_debug(self, message):
        """Log debug message."""
        self.logger.debug(message)
    
    def log_record_count(self, stage, count):
        """Log record count at a stage."""
        self.logger.info(f"[{stage}] Records: {count}")
    
    def log_output(self, output_path):
        """Log output file path."""
        self.logger.info(f"Output: {output_path}")
