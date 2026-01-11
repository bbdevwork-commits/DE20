import sys
import os

# Add handler folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'handler'))

from ingestion_handler import print_yaml_content as ingestion_print
from dq_check_handler import print_yaml_content as dq_check_print
from transformations_handler import print_yaml_content as transformations_print
from router_handler import print_yaml_content as router_print


def trigger_ingestion_handler(yaml_file_path):
    """Trigger ingestion handler."""
    print("Triggering Ingestion Handler...")
    return ingestion_print(yaml_file_path)


def trigger_dq_check_handler(yaml_file_path):
    """Trigger DQ check handler."""
    print("Triggering DQ Check Handler...")
    return dq_check_print(yaml_file_path)


def trigger_transformations_handler(yaml_file_path):
    """Trigger transformations handler."""
    print("Triggering Transformations Handler...")
    return transformations_print(yaml_file_path)


def trigger_router_handler(yaml_file_path):
    """Trigger router handler."""
    print("Triggering Router Handler...")
    return router_print(yaml_file_path)


def trigger_all_handlers(config_dir):
    """Trigger all handlers with their respective config files."""
    print("=" * 60)
    print("TRIGGERING ALL HANDLERS")
    print("=" * 60)

    ingestion_config = os.path.join(config_dir, 'INGESTER_CONFIG.yaml')
    dq_config = os.path.join(config_dir, 'DQ_CHECKS_CONFIG.yaml')
    transformations_config = os.path.join(config_dir, 'TRANSFORMATIONS_CONFIG.yaml')
    router_config = os.path.join(config_dir, 'ROUTER_CONFIG.yaml')

    trigger_ingestion_handler(ingestion_config)
    print()
    trigger_dq_check_handler(dq_config)
    print()
    trigger_transformations_handler(transformations_config)
    print()
    trigger_router_handler(router_config)

    print("=" * 60)
    print("ALL HANDLERS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python wrapper4actions.py <path_to_config_directory>")
        print("Example: python wrapper4actions.py /path/to/DE20/config")
        sys.exit(1)

    config_dir = sys.argv[1]

    if not os.path.isdir(config_dir):
        print(f"Error: Config directory not found: {config_dir}")
        sys.exit(1)

    trigger_all_handlers(config_dir)
