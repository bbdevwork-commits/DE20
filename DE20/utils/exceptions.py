"""Custom exceptions for the data pipeline."""


class PipelineError(Exception):
    """Base exception for pipeline errors."""
    def __init__(self, message, component=None, details=None):
        self.message = message
        self.component = component
        self.details = details
        super().__init__(self.message)

    def __str__(self):
        if self.component:
            return f"[{self.component}] {self.message}"
        return self.message


class IngestionError(PipelineError):
    """Exception raised during data ingestion."""
    def __init__(self, message, source=None, details=None):
        super().__init__(message, component="INGESTION", details=details)
        self.source = source


class DQCheckError(PipelineError):
    """Exception raised during data quality checks."""
    def __init__(self, message, rule=None, details=None):
        super().__init__(message, component="DQ_CHECK", details=details)
        self.rule = rule


class TransformationError(PipelineError):
    """Exception raised during data transformation."""
    def __init__(self, message, transformation=None, details=None):
        super().__init__(message, component="TRANSFORMATION", details=details)
        self.transformation = transformation


class RouterError(PipelineError):
    """Exception raised during data routing."""
    def __init__(self, message, destination=None, details=None):
        super().__init__(message, component="ROUTER", details=details)
        self.destination = destination


class ConfigError(PipelineError):
    """Exception raised for configuration errors."""
    def __init__(self, message, config_file=None, details=None):
        super().__init__(message, component="CONFIG", details=details)
        self.config_file = config_file


class DataValidationError(PipelineError):
    """Exception raised for data validation errors."""
    def __init__(self, message, column=None, details=None):
        super().__init__(message, component="VALIDATION", details=details)
        self.column = column
