import yaml
import os
import sys
from datetime import datetime

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from logger import get_logger
from exceptions import RouterError, ConfigError

# Initialize logger
logger = get_logger("RouterHandler")


def load_config(yaml_file_path):
    """Load YAML configuration file."""
    try:
        if not os.path.exists(yaml_file_path):
            raise ConfigError(f"Config file not found: {yaml_file_path}", config_file=yaml_file_path)

        with open(yaml_file_path, 'r') as file:
            config = yaml.safe_load(file)

        if not config:
            raise ConfigError(f"Empty or invalid config file: {yaml_file_path}", config_file=yaml_file_path)

        return config

    except yaml.YAMLError as e:
        logger.error(f"YAML parsing error: {str(e)}")
        raise ConfigError(f"Invalid YAML format: {str(e)}", config_file=yaml_file_path)


def route_to_destination(data, destination, output_dir):
    """Route data to a specific destination."""
    dest_name = destination.get('name')
    dest_type = destination.get('type')
    enabled = destination.get('enabled', True)

    if not enabled:
        print(f"  ⊘ {dest_name} ({dest_type}) - DISABLED, skipping")
        return None

    print(f"  → Routing to {dest_name} ({dest_type})")

    # Simulate routing to different destination types
    if dest_type == 's3':
        bucket = destination.get('connection', {}).get('bucket', 'unknown')
        path = destination.get('connection', {}).get('path', '')
        print(f"    S3: s3://{bucket}/{path}")

    elif dest_type == 'redshift':
        host = destination.get('connection', {}).get('host', 'unknown')
        table = destination.get('connection', {}).get('table', 'unknown')
        print(f"    Redshift: {host} -> {table}")

    elif dest_type == 'kinesis':
        stream = destination.get('connection', {}).get('stream_name', 'unknown')
        print(f"    Kinesis: {stream}")

    print(f"    Records routed: {len(data)}")
    return dest_name


def write_final_dataset(data, output_path, delimiter="~|~"):
    """Write final dataset to output file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if not data:
        print("No data to write.")
        return output_path

    headers = list(data[0].keys())

    with open(output_path, 'w', encoding='utf-8') as file:
        file.write(delimiter.join(headers) + '\n')
        for row in data:
            values = [str(row.get(h, '')) for h in headers]
            file.write(delimiter.join(values) + '\n')

    print(f"\n{'=' * 60}")
    print(f"FINAL DATASET WRITTEN: {output_path}")
    print(f"Total records: {len(data)}")
    print(f"Total columns: {len(headers)}")
    print(f"{'=' * 60}")

    return output_path


def process_router(config_path, input_data=None):
    """Main function to process routing."""
    try:
        logger.info("Starting Router Handler...")
        print(f"Starting Router Handler...")

        if input_data is None:
            logger.warning("No input data provided. Skipping routing.")
            print("No input data provided. Skipping routing.")
            return {
                'data': [],
                'headers': [],
                'output_path': None,
                'record_count': 0,
                'destinations_routed': [],
                'status': 'SKIPPED',
                'error': 'No input data provided'
            }

        data = input_data.get('data', [])
        if not data:
            logger.warning("Empty dataset. Skipping routing.")
            print("Empty dataset. Skipping routing.")
            return {
                'data': [],
                'headers': [],
                'output_path': None,
                'record_count': 0,
                'destinations_routed': [],
                'status': 'SKIPPED',
                'error': 'Empty dataset'
            }

        logger.info(f"Loading Router config: {config_path}")
        print(f"\nLoading Router config: {config_path}")
        config = load_config(config_path)

        destinations = config.get('destinations', [])
        routing_rules = config.get('routing_rules', [])
        settings = config.get('settings', {})

        logger.info(f"Processing {len(data)} records for routing")
        logger.info(f"Destinations configured: {len(destinations)}")
        print(f"Processing {len(data)} records...")
        print("-" * 60)

        # Process routing rules and destinations
        print("\nRouting to configured destinations:")
        routed_destinations = []

        for destination in destinations:
            result = route_to_destination(data, destination, os.path.dirname(config_path))
            if result:
                routed_destinations.append(result)
                logger.info(f"Routed to destination: {result}")

        print("-" * 60)
        print(f"Routing Summary: Routed to {len(routed_destinations)} destinations")
        logger.info(f"Routing Summary: Routed to {len(routed_destinations)} destinations")

        # Create output directory and write final dataset
        output_dir = os.path.dirname(config_path).replace('/config', '/output')
        final_output_path = os.path.join(output_dir, 'final_dataset.txt')

        # Write final dataset
        write_final_dataset(data, final_output_path)
        logger.info(f"Final dataset written to: {final_output_path}")

        # Send notification if configured
        if settings.get('notification_on_complete'):
            email = settings.get('notification_email', 'N/A')
            logger.info(f"Notification sent to: {email}")
            print(f"\nNotification sent to: {email}")

        # Return results
        return {
            'data': data,
            'headers': list(data[0].keys()) if data else [],
            'output_path': final_output_path,
            'record_count': len(data),
            'destinations_routed': routed_destinations,
            'status': 'SUCCESS'
        }

    except (RouterError, ConfigError) as e:
        logger.error(f"Router failed: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'output_path': None,
            'record_count': 0,
            'destinations_routed': [],
            'status': 'FAILED',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during routing: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'output_path': None,
            'record_count': 0,
            'destinations_routed': [],
            'status': 'FAILED',
            'error': f"Unexpected error: {str(e)}"
        }


def print_yaml_content(yaml_file_path):
    """Load and print YAML file content."""
    with open(yaml_file_path, 'r') as file:
        content = yaml.safe_load(file)

    print(f"Contents of {yaml_file_path}:")
    print("-" * 50)
    if content:
        print(yaml.dump(content, default_flow_style=False))
    else:
        print("(empty file)")
    print("-" * 50)

    return content


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python router_handler.py <path_to_ROUTER_CONFIG.yaml>")
        sys.exit(1)

    print_yaml_content(sys.argv[1])
