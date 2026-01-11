import sys
import os

# Add handler folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'handler'))

from ingestion_handler import process_ingestion, print_yaml_content as ingestion_print
from dq_check_handler import process_dq_checks, print_yaml_content as dq_check_print
from transformations_handler import process_transformations, print_yaml_content as transformations_print
from router_handler import process_router, print_yaml_content as router_print


def trigger_ingestion_handler(yaml_file_path, input_data=None):
    """Trigger ingestion handler and return data."""
    print("Triggering Ingestion Handler...")
    result = process_ingestion(yaml_file_path)
    return result


def trigger_dq_check_handler(yaml_file_path, input_data=None):
    """Trigger DQ check handler with input data."""
    print("Triggering DQ Check Handler...")
    result = process_dq_checks(yaml_file_path, input_data)
    return result


def trigger_transformations_handler(yaml_file_path, input_data=None):
    """Trigger transformations handler with input data."""
    print("Triggering Transformations Handler...")
    result = process_transformations(yaml_file_path, input_data)
    return result


def trigger_router_handler(yaml_file_path, input_data=None):
    """Trigger router handler with input data and write final_dataset."""
    print("Triggering Router Handler...")
    result = process_router(yaml_file_path, input_data)
    return result


def trigger_all_handlers(config_dir):
    """Trigger all handlers with data passing between them."""
    print("=" * 60)
    print("TRIGGERING ALL HANDLERS WITH DATA PIPELINE")
    print("=" * 60)

    ingestion_config = os.path.join(config_dir, 'INGESTER_CONFIG.yaml')
    dq_config = os.path.join(config_dir, 'DQ_CHECKS_CONFIG.yaml')
    transformations_config = os.path.join(config_dir, 'TRANSFORMATIONS_CONFIG.yaml')
    router_config = os.path.join(config_dir, 'ROUTER_CONFIG.yaml')

    # Step 1: Ingestion - returns data
    print("\n" + "=" * 60)
    print("STEP 1: INGESTION")
    print("=" * 60)
    ingestion_result = trigger_ingestion_handler(ingestion_config)

    # Step 2: DQ Checks - receives data from ingestion
    print("\n" + "=" * 60)
    print("STEP 2: DATA QUALITY CHECKS")
    print("=" * 60)
    dq_result = trigger_dq_check_handler(dq_config, ingestion_result)

    # Step 3: Transformations - receives data from DQ
    print("\n" + "=" * 60)
    print("STEP 3: TRANSFORMATIONS")
    print("=" * 60)
    transform_result = trigger_transformations_handler(transformations_config, dq_result)

    # Step 4: Router - receives data from transformations
    print("\n" + "=" * 60)
    print("STEP 4: ROUTING")
    print("=" * 60)
    router_result = trigger_router_handler(router_config, transform_result)

    print("\n" + "=" * 60)
    print("ALL HANDLERS COMPLETED")
    print("=" * 60)

    return {
        'ingestion': ingestion_result,
        'dq_check': dq_result,
        'transformations': transform_result,
        'router': router_result
    }


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
