import yaml
import os
import re
import sys
from datetime import datetime

# Add utils to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))
from logger import get_logger
from exceptions import DQCheckError, ConfigError

# Initialize logger
logger = get_logger("DQCheckHandler")


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


def check_null_values(data, columns):
    """Check for null/empty values in specified columns."""
    failed_records = []
    for i, row in enumerate(data):
        for col in columns:
            value = row.get(col, '')
            if value is None or str(value).strip() == '':
                failed_records.append({
                    'record_index': i,
                    'column': col,
                    'value': value,
                    'issue': 'NULL or empty value'
                })
    return failed_records


def check_duplicates(data, columns):
    """Check for duplicate values in specified columns."""
    seen = {}
    failed_records = []
    for i, row in enumerate(data):
        key = tuple(str(row.get(col, '')) for col in columns)
        if key in seen:
            failed_records.append({
                'record_index': i,
                'column': ', '.join(columns),
                'value': str(key),
                'issue': f'Duplicate found (first occurrence at record {seen[key]})'
            })
        else:
            seen[key] = i
    return failed_records


def check_range(data, column, min_value=None, max_value=None):
    """Check if values are within specified range."""
    failed_records = []
    for i, row in enumerate(data):
        value = row.get(column, '')
        try:
            num_value = float(value) if value else None
            if num_value is not None:
                if min_value is not None and num_value < min_value:
                    failed_records.append({
                        'record_index': i,
                        'column': column,
                        'value': value,
                        'issue': f'Value {num_value} is below minimum {min_value}'
                    })
                if max_value is not None and num_value > max_value:
                    failed_records.append({
                        'record_index': i,
                        'column': column,
                        'value': value,
                        'issue': f'Value {num_value} exceeds maximum {max_value}'
                    })
        except (ValueError, TypeError):
            failed_records.append({
                'record_index': i,
                'column': column,
                'value': value,
                'issue': 'Invalid numeric value'
            })
    return failed_records


def check_pattern(data, column, pattern):
    """Check if values match specified regex pattern."""
    failed_records = []
    regex = re.compile(pattern)
    for i, row in enumerate(data):
        value = str(row.get(column, ''))
        if value and not regex.match(value):
            failed_records.append({
                'record_index': i,
                'column': column,
                'value': value,
                'issue': f'Value does not match pattern {pattern}'
            })
    return failed_records


def run_dq_checks(data, config_path):
    """Run all DQ checks based on config."""
    print(f"\nLoading DQ config: {config_path}")
    config = load_config(config_path)

    dq_rules = config.get('dq_rules', [])
    settings = config.get('settings', {})

    results = {
        'total_records': len(data),
        'rules_executed': 0,
        'total_failures': 0,
        'total_warnings': 0,
        'rule_results': [],
        'failed_records': []
    }

    print(f"Running DQ checks on {len(data)} records...")
    print("-" * 60)

    for rule in dq_rules:
        rule_name = rule.get('rule_name')
        action = rule.get('action', 'warn')
        failures = []

        print(f"\nExecuting rule: {rule_name}")

        if rule_name == 'null_check':
            columns = rule.get('columns', [])
            failures = check_null_values(data, columns)

        elif rule_name == 'duplicate_check':
            columns = rule.get('columns', [])
            failures = check_duplicates(data, columns)

        elif rule_name == 'range_check':
            column = rule.get('column')
            min_val = rule.get('min_value')
            max_val = rule.get('max_value')
            failures = check_range(data, column, min_val, max_val)

        elif rule_name == 'format_check':
            column = rule.get('column')
            pattern = rule.get('pattern')
            failures = check_pattern(data, column, pattern)

        # Record results
        rule_result = {
            'rule_name': rule_name,
            'action': action,
            'failure_count': len(failures),
            'status': 'PASS' if len(failures) == 0 else ('FAIL' if action == 'fail' else 'WARN')
        }
        results['rule_results'].append(rule_result)
        results['rules_executed'] += 1

        if action == 'fail':
            results['total_failures'] += len(failures)
        else:
            results['total_warnings'] += len(failures)

        for f in failures:
            f['rule_name'] = rule_name
            f['action'] = action
            results['failed_records'].append(f)

        status_icon = "✓" if len(failures) == 0 else ("✗" if action == 'fail' else "⚠")
        print(f"  {status_icon} {rule_name}: {len(failures)} issues found [{rule_result['status']}]")

    print("-" * 60)
    print(f"DQ Summary: {results['rules_executed']} rules executed")
    print(f"  Failures: {results['total_failures']}")
    print(f"  Warnings: {results['total_warnings']}")

    return results


def write_dq_output(data, dq_results, output_path, delimiter="~|~"):
    """Write DQ results and cleaned data to output file."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write DQ report
    report_path = output_path.replace('.txt', '_report.txt')
    with open(report_path, 'w', encoding='utf-8') as file:
        file.write("DATA QUALITY CHECK REPORT\n")
        file.write("=" * 60 + "\n")
        file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file.write(f"Total Records Checked: {dq_results['total_records']}\n")
        file.write(f"Rules Executed: {dq_results['rules_executed']}\n")
        file.write(f"Total Failures: {dq_results['total_failures']}\n")
        file.write(f"Total Warnings: {dq_results['total_warnings']}\n")
        file.write("=" * 60 + "\n\n")

        file.write("RULE RESULTS:\n")
        file.write("-" * 60 + "\n")
        for rule in dq_results['rule_results']:
            file.write(f"  {rule['rule_name']}: {rule['status']} ({rule['failure_count']} issues)\n")

        if dq_results['failed_records']:
            file.write("\n\nFAILED RECORDS DETAIL:\n")
            file.write("-" * 60 + "\n")
            for record in dq_results['failed_records'][:50]:  # Limit to first 50
                file.write(f"  Record {record['record_index']}: [{record['rule_name']}] "
                          f"{record['column']} = '{record['value']}' - {record['issue']}\n")

    print(f"DQ Report created: {report_path}")

    # Write data with DQ status
    if data:
        headers = list(data[0].keys()) + ['dq_status', 'dq_issues']

        # Create lookup of failed record indices
        failed_indices = {}
        for f in dq_results['failed_records']:
            idx = f['record_index']
            if idx not in failed_indices:
                failed_indices[idx] = []
            failed_indices[idx].append(f"{f['rule_name']}:{f['column']}")

        with open(output_path, 'w', encoding='utf-8') as file:
            file.write(delimiter.join(headers) + '\n')
            for i, row in enumerate(data):
                issues = failed_indices.get(i, [])
                dq_status = 'FAIL' if issues else 'PASS'
                dq_issues = '; '.join(issues) if issues else ''

                values = [str(row.get(h, '')) for h in list(data[0].keys())]
                values.extend([dq_status, dq_issues])
                file.write(delimiter.join(values) + '\n')

        print(f"DQ Output created: {output_path}")
        print(f"Total records: {len(data)}")

    return output_path


def process_dq_checks(config_path, input_data=None):
    """Main function to process DQ checks."""
    try:
        logger.info("Starting DQ Check Handler...")
        print(f"Starting DQ Check Handler...")

        if input_data is None:
            logger.warning("No input data provided. Skipping DQ checks.")
            print("No input data provided. Skipping DQ checks.")
            return {
                'data': [],
                'dq_results': None,
                'output_path': None,
                'record_count': 0,
                'passed': False,
                'status': 'SKIPPED',
                'error': 'No input data provided'
            }

        data = input_data.get('data', [])
        if not data:
            logger.warning("Empty dataset. Skipping DQ checks.")
            print("Empty dataset. Skipping DQ checks.")
            return {
                'data': [],
                'dq_results': None,
                'output_path': None,
                'record_count': 0,
                'passed': False,
                'status': 'SKIPPED',
                'error': 'Empty dataset'
            }

        logger.info(f"Processing {len(data)} records for DQ checks")

        # Run DQ checks
        dq_results = run_dq_checks(data, config_path)

        logger.info(f"DQ Results - Failures: {dq_results['total_failures']}, Warnings: {dq_results['total_warnings']}")

        # Create output directory
        output_dir = os.path.dirname(config_path).replace('/config', '/output')
        output_path = os.path.join(output_dir, 'dq_output.txt')

        # Write DQ output
        write_dq_output(data, dq_results, output_path)

        logger.info(f"DQ output written to: {output_path}")

        # Return results for downstream processing
        return {
            'data': data,
            'dq_results': dq_results,
            'output_path': output_path,
            'record_count': len(data),
            'passed': dq_results['total_failures'] == 0,
            'status': 'SUCCESS'
        }

    except (DQCheckError, ConfigError) as e:
        logger.error(f"DQ check failed: {str(e)}")
        return {
            'data': [],
            'dq_results': None,
            'output_path': None,
            'record_count': 0,
            'passed': False,
            'status': 'FAILED',
            'error': str(e)
        }
    except Exception as e:
        logger.error(f"Unexpected error during DQ checks: {str(e)}")
        return {
            'data': [],
            'dq_results': None,
            'output_path': None,
            'record_count': 0,
            'passed': False,
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
        print("Usage: python dq_check_handler.py <path_to_DQ_CHECKS_CONFIG.yaml>")
        sys.exit(1)

    print_yaml_content(sys.argv[1])
