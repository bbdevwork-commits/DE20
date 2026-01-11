import yaml
import os
import sys
from datetime import datetime

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from logger import get_logger
from exceptions import TransformationError, ConfigError

# Initialize logger
logger = get_logger("TransformHandler")


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


def apply_date_format(data, source_col, target_col, date_format):
    """Apply date formatting transformation."""
    for row in data:
        value = row.get(source_col, '')
        if value:
            row[target_col] = value  # Keep as-is for now (would parse/format in real impl)
    return data


def apply_derived_column(data, expression, target_col):
    """Apply derived column transformation."""
    for row in data:
        try:
            # Simple expression evaluation (for demo purposes)
            # In real impl, would use safe expression parser
            local_vars = {k: float(v) if v and str(v).replace('.', '').replace('-', '').isdigit() else 0
                         for k, v in row.items()}
            result = eval(expression, {"__builtins__": {}}, local_vars)
            row[target_col] = round(result, 2)
        except:
            row[target_col] = 0
    return data


def apply_string_transform(data, source_col, target_col, operation):
    """Apply string transformation."""
    for row in data:
        value = str(row.get(source_col, ''))
        if operation == 'uppercase':
            row[target_col] = value.upper()
        elif operation == 'lowercase':
            row[target_col] = value.lower()
        elif operation == 'trim':
            row[target_col] = value.strip()
    return data


def apply_date_extract(data, source_col, target_col, extract_part):
    """Extract part from date."""
    for row in data:
        value = row.get(source_col, '')
        if value:
            try:
                # Parse date string
                dt = datetime.strptime(str(value)[:19], '%Y-%m-%d %H:%M:%S')
                if extract_part == 'year':
                    row[target_col] = dt.year
                elif extract_part == 'month':
                    row[target_col] = dt.month
                elif extract_part == 'day':
                    row[target_col] = dt.day
            except:
                row[target_col] = ''
    return data


def run_transformations(data, config_path):
    """Run all transformations based on config."""
    print(f"\nLoading Transformations config: {config_path}")
    config = load_config(config_path)

    transformations = config.get('transformations', [])
    settings = config.get('settings', {})

    print(f"Running transformations on {len(data)} records...")
    print("-" * 60)

    transformed_count = 0

    for transform in transformations:
        name = transform.get('name')
        transform_type = transform.get('type')

        print(f"\nApplying transformation: {name} ({transform_type})")

        if transform_type == 'date_format':
            source_col = transform.get('source_column')
            target_col = transform.get('target_column')
            date_format = transform.get('format')
            data = apply_date_format(data, source_col, target_col, date_format)
            transformed_count += 1

        elif transform_type == 'derived_column':
            expression = transform.get('expression')
            target_col = transform.get('target_column')
            data = apply_derived_column(data, expression, target_col)
            transformed_count += 1

        elif transform_type == 'string_transform':
            source_col = transform.get('source_column')
            target_col = transform.get('target_column')
            operation = transform.get('operation')
            data = apply_string_transform(data, source_col, target_col, operation)
            transformed_count += 1

        elif transform_type == 'date_extract':
            source_col = transform.get('source_column')
            target_col = transform.get('target_column')
            extract_part = transform.get('extract')
            data = apply_date_extract(data, source_col, target_col, extract_part)
            transformed_count += 1

        print(f"  ✓ {name} applied successfully")

    print("-" * 60)
    print(f"Transformations Summary: {transformed_count} transformations applied")

    return data


def write_output_file(data, output_path, delimiter="~|~"):
    """Write transformed data to output file."""
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

    print(f"Transformed output created: {output_path}")
    print(f"Total records: {len(data)}")

    return output_path


def process_transformations(config_path, input_data=None):
    """Main function to process transformations."""
    try:
        logger.info("Starting Transformations Handler...")
        print(f"Starting Transformations Handler...")

        if input_data is None:
            logger.warning("No input data provided. Skipping transformations.")
            print("No input data provided. Skipping transformations.")
            return {
                'data': [],
                'headers': [],
                'output_path': None,
                'record_count': 0,
                'status': 'SKIPPED',
                'error': 'No input data provided'
            }

        data = input_data.get('data', [])
        if not data:
            logger.warning("Empty dataset. Skipping transformations.")
            print("Empty dataset. Skipping transformations.")
            return {
                'data': [],
                'headers': [],
                'output_path': None,
                'record_count': 0,
                'status': 'SKIPPED',
                'error': 'Empty dataset'
            }

        logger.info(f"Processing {len(data)} records for transformations")

        # Run transformations
        transformed_data = run_transformations(data, config_path)

        logger.info(f"Transformations complete. Output columns: {len(transformed_data[0].keys()) if transformed_data else 0}")

        # Create output directory
        output_dir = os.path.dirname(config_path).replace('/config', '/output')
        output_path = os.path.join(output_dir, 'transformed_output.txt')

        # Write output
        write_output_file(transformed_data, output_path)

        logger.info(f"Transformed output written to: {output_path}")

        # Return results for downstream processing
        return {
            'data': transformed_data,
            'headers': list(transformed_data[0].keys()) if transformed_data else [],
            'output_path': output_path,
            'record_count': len(transformed_data),
            'status': 'SUCCESS'
        }

    except (TransformationError, ConfigError) as e:
        logger.error(f"Transformation failed: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'output_path': None,
            'record_count': 0,
            'status': 'FAILED',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during transformations: {str(e)}")
        return {
            'data': [],
            'headers': [],
            'output_path': None,
            'record_count': 0,
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
        print("Usage: python transformations_handler.py <path_to_TRANSFORMATIONS_CONFIG.yaml>")
        sys.exit(1)

    print_yaml_content(sys.argv[1])
