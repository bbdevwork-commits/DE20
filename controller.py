import sys
import yaml
import os

# Add wrapper folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'wrapper'))

from wrapper4actions import (
    trigger_ingestion_handler,
    trigger_dq_check_handler,
    trigger_transformations_handler,
    trigger_router_handler
)


def load_config(config_path):
    """Load YAML configuration file."""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file) or {}


def execute_component(component_type, config_file_path):
    """Execute handler based on component type via wrapper."""
    component_handlers = {
        "INGESTOR": trigger_ingestion_handler,
        "DQ-CHECKER": trigger_dq_check_handler,
        "TRANSFORMER": trigger_transformations_handler,
        "ROUTER": trigger_router_handler
    }

    handler = component_handlers.get(component_type)
    if handler:
        print(f"\nExecuting component: {component_type}")
        handler(config_file_path)
    else:
        print(f"Unknown component type: {component_type}")


def main(datapipeline_config_path):
    """Main controller function that orchestrates the data pipeline."""
    print(f"Loading Data Pipeline Config: {datapipeline_config_path}")

    pipeline_config = load_config(datapipeline_config_path)

    print("Starting Data Pipeline Execution...")
    print("=" * 60)

    # Get components from execution_pipeline
    execution_pipeline = pipeline_config.get('execution_pipeline', {})
    components = execution_pipeline.get('components', [])

    # Execute each component based on component_type
    for component in components:
        component_type = component.get('component_type')
        config_file_path = component.get('config_file_name')
        execute_component(component_type, config_file_path)

    print("=" * 60)
    print("Data Pipeline Execution Complete.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python controller.py <path_to_DATAPIPELINE_CONFIG.yaml>")
        sys.exit(1)

    datapipeline_config_path = sys.argv[1]

    if not os.path.exists(datapipeline_config_path):
        print(f"Error: Config file not found: {datapipeline_config_path}")
        sys.exit(1)

    main(datapipeline_config_path)
