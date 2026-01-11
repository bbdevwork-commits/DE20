import yaml
import os
import sys

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from logger import get_logger
from exceptions import IngestionError, ConfigError

# Initialize logger
logger = get_logger("IngestionHandler")


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


def read_delimited_file(file_path, delimiter, has_header=True):
    """Read a delimited file and return list of dictionaries."""
    try:
        if not os.path.exists(file_path):
            raise IngestionError(f"Source file not found: {file_path}", source=file_path)

        data = []
        headers = []

        with open(file_path, 'r', encoding='utf-8') as file:
            for i, line in enumerate(file):
                line = line.strip()
                if not line:
                    continue

                fields = line.split(delimiter)

                if i == 0 and has_header:
                    headers = fields
                else:
                    if headers:
                        row = {headers[j]: fields[j] if j < len(fields) else None
                               for j in range(len(headers))}
                        data.append(row)

        if not data:
            logger.warning(f"No data found in file: {file_path}")

        return data, headers

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error reading file {file_path}: {str(e)}")
        raise IngestionError(f"File encoding error: {str(e)}", source=file_path)
    except PermissionError as e:
        logger.error(f"Permission denied reading file {file_path}: {str(e)}")
        raise IngestionError(f"Permission denied: {str(e)}", source=file_path)


def join_datasets(parent_data, child_data, join_key, join_type="left"):
    """Join two datasets based on join key."""
    try:
        if not parent_data:
            raise IngestionError("Parent dataset is empty", details="Cannot join with empty parent dataset")

        # Create lookup from child data (group by join_key for one-to-many)
        child_lookup = {}
        for row in child_data:
            key = row.get(join_key)
            if key not in child_lookup:
                child_lookup[key] = []
            child_lookup[key].append(row)

        # Perform join
        result = []
        for parent_row in parent_data:
            key = parent_row.get(join_key)
            child_rows = child_lookup.get(key, [])

            if child_rows:
                for child_row in child_rows:
                    merged = {**parent_row}
                    for k, v in child_row.items():
                        if k != join_key:  # Avoid duplicate join key
                            merged[f"order_{k}" if k in parent_row else k] = v
                    result.append(merged)
            elif join_type == "left":
                result.append(parent_row)

        return result

    except Exception as e:
        logger.error(f"Error joining datasets: {str(e)}")
        raise IngestionError(f"Join operation failed: {str(e)}")


def write_output_file(data, output_path, delimiter="~|~"):
    """Write joined data to output file."""
    try:
        if not data:
            logger.warning("No data to write.")
            print("No data to write.")
            return

        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        headers = list(data[0].keys())

        with open(output_path, 'w', encoding='utf-8') as file:
            # Write header
            file.write(delimiter.join(headers) + '\n')

            # Write data rows
            for row in data:
                values = [str(row.get(h, '')) for h in headers]
                file.write(delimiter.join(values) + '\n')

        logger.info(f"Output file created: {output_path}")
        print(f"Output file created: {output_path}")
        print(f"Total records: {len(data)}")

    except PermissionError as e:
        logger.error(f"Permission denied writing to {output_path}: {str(e)}")
        raise IngestionError(f"Cannot write output file: {str(e)}")
    except IOError as e:
        logger.error(f"IO error writing to {output_path}: {str(e)}")
        raise IngestionError(f"IO error writing output: {str(e)}")


def process_ingestion(config_path):
    """Main function to process ingestion based on config."""
    try:
        logger.info(f"Loading config: {config_path}")
        print(f"Loading config: {config_path}")
        config = load_config(config_path)

        sources = config.get('sources', [])
        relationships = config.get('relationships', [])

        if not sources:
            raise ConfigError("No sources defined in config", config_file=config_path)

        logger.info(f"Sources to load: {len(sources)}")
        logger.info(f"Relationships to process: {len(relationships)}")

        # Load all source datasets
        datasets = {}
        for source in sources:
            name = source.get('name')
            file_path = source.get('path')
            delimiter = source.get('delimiter', ',')
            has_header = source.get('has_header', True)

            if not name or not file_path:
                raise ConfigError(f"Invalid source configuration: missing name or path")

            logger.info(f"Reading source: {name}")
            logger.debug(f"  Path: {file_path}, Delimiter: {delimiter}")
            print(f"\nReading source: {name}")
            print(f"  Path: {file_path}")
            print(f"  Delimiter: {delimiter}")

            data, headers = read_delimited_file(file_path, delimiter, has_header)
            datasets[name] = {
                'data': data,
                'headers': headers,
                'config': source
            }
            logger.info(f"  Records loaded: {len(data)}")
            print(f"  Records loaded: {len(data)}")

        # Process relationships and join datasets
        for rel in relationships:
            rel_name = rel.get('name')
            parent_name = rel.get('parent')
            child_name = rel.get('child')
            join_key = rel.get('join_key')
            join_type = rel.get('join_type', 'left')

            if not all([rel_name, parent_name, child_name, join_key]):
                raise ConfigError(f"Invalid relationship configuration: {rel}")

            if parent_name not in datasets:
                raise IngestionError(f"Parent dataset '{parent_name}' not found")
            if child_name not in datasets:
                raise IngestionError(f"Child dataset '{child_name}' not found")

            print(f"\nProcessing relationship: {rel_name}")
            print(f"  Joining {parent_name} with {child_name} on {join_key}")
            logger.info(f"Joining {parent_name} with {child_name} on {join_key}")

            parent_data = datasets.get(parent_name, {}).get('data', [])
            child_data = datasets.get(child_name, {}).get('data', [])

            joined_data = join_datasets(parent_data, child_data, join_key, join_type)
            print(f"  Joined records: {len(joined_data)}")
            logger.info(f"  Joined records: {len(joined_data)}")

            # Store joined result
            datasets[rel_name] = {
                'data': joined_data,
                'headers': list(joined_data[0].keys()) if joined_data else []
            }

        # Create output directory
        output_dir = os.path.dirname(config_path).replace('/config', '/output')
        os.makedirs(output_dir, exist_ok=True)

        # Write output file for the joined data
        output_path = os.path.join(output_dir, 'customer_complete_info.txt')

        # Get the last relationship result (joined data)
        output_data = []
        if relationships:
            last_rel = relationships[-1].get('name')
            output_data = datasets.get(last_rel, {}).get('data', [])
            write_output_file(output_data, output_path, delimiter="~|~")

        logger.info(f"Ingestion completed successfully. Records: {len(output_data)}")

        # Return joined data for downstream processing
        return {
            'data': output_data,
            'headers': list(output_data[0].keys()) if output_data else [],
            'record_count': len(output_data),
            'output_path': output_path,
            'status': 'SUCCESS'
        }

    except (IngestionError, ConfigError) as e:
        logger.error(f"Ingestion failed: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'record_count': 0,
            'output_path': None,
            'status': 'FAILED',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during ingestion: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'record_count': 0,
            'output_path': None,
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
        print("Usage: python ingestion_handler.py <path_to_INGESTER_CONFIG.yaml>")
        sys.exit(1)

    config_path = sys.argv[1]

    # Process ingestion
    result = process_ingestion(config_path)
    if result.get('status') == 'FAILED':
        print(f"Error: {result.get('error')}")
        sys.exit(1)
