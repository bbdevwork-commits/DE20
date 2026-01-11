import sys
import yaml
import os

# Add folders to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'wrapper'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'utils'))

from wrapper4actions import (
    trigger_ingestion_handler,
    trigger_dq_check_handler,
    trigger_transformations_handler,
    trigger_router_handler
)
from logger import PipelineLogger
from exceptions import PipelineError, ConfigError


def load_config(config_path):
    """Load YAML configuration file."""
    try:
        if not os.path.exists(config_path):
            raise ConfigError(f"Config file not found: {config_path}", config_file=config_path)

        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)

        if not config:
            raise ConfigError(f"Empty or invalid config file: {config_path}", config_file=config_path)

        return config

    except yaml.YAMLError as e:
        raise ConfigError(f"Invalid YAML format: {str(e)}", config_file=config_path)


def execute_component(component_type, config_file_path, input_data=None, logger=None):
    """Execute handler based on component type via wrapper and return result."""
    component_handlers = {
        "INGESTOR": trigger_ingestion_handler,
        "DQ-CHECKER": trigger_dq_check_handler,
        "TRANSFORMER": trigger_transformations_handler,
        "ROUTER": trigger_router_handler
    }

    handler = component_handlers.get(component_type)
    if handler:
        if logger:
            logger.start_step(component_type)
            logger.log_debug(f"Config file: {config_file_path}")

        print(f"\n{'=' * 60}")
        print(f"Executing component: {component_type}")
        print(f"{'=' * 60}")

        try:
            result = handler(config_file_path, input_data)

            # Check handler result status
            if result and result.get('status') == 'FAILED':
                error_msg = result.get('error', 'Unknown error')
                if logger:
                    logger.log_error(f"{component_type} failed: {error_msg}")
                    logger.end_step(component_type, status="FAILED")
                raise PipelineError(f"{component_type} failed: {error_msg}", component=component_type)

            if logger and result:
                status = result.get('status', 'SUCCESS')
                logger.end_step(component_type,
                              records=result.get('record_count'),
                              status=status)
                logger.log_output(result.get('output_path', 'N/A'))

            return result

        except PipelineError:
            raise
        except Exception as e:
            if logger:
                logger.log_error(f"Error in {component_type}: {str(e)}")
                logger.end_step(component_type, status="FAILED")
            raise PipelineError(f"Error in {component_type}: {str(e)}", component=component_type)
    else:
        if logger:
            logger.log_warning(f"Unknown component type: {component_type}")
        print(f"Unknown component type: {component_type}")
        return input_data


def main(datapipeline_config_path):
    """Main controller function that orchestrates the data pipeline."""

    # Initialize logger
    logger = PipelineLogger("DataPipeline")

    try:
        logger.start_pipeline(datapipeline_config_path)

        logger.log_info(f"Loading Data Pipeline Config: {datapipeline_config_path}")
        print(f"Loading Data Pipeline Config: {datapipeline_config_path}")

        pipeline_config = load_config(datapipeline_config_path)

        print("\n" + "=" * 60)
        print("STARTING DATA PIPELINE EXECUTION")
        print("=" * 60)

        # Get components from execution_pipeline
        execution_pipeline = pipeline_config.get('execution_pipeline', {})
        components = execution_pipeline.get('components', [])

        if not components:
            raise ConfigError("No components defined in execution_pipeline", config_file=datapipeline_config_path)

        logger.log_info(f"Components to execute: {len(components)}")

        # Execute each component and pass data to the next
        pipeline_data = None
        results = {}
        all_success = True
        failed_component = None

        for i, component in enumerate(components):
            component_type = component.get('component_type')
            config_file_path = component.get('config_file_name')

            if not component_type or not config_file_path:
                raise ConfigError(f"Invalid component configuration at index {i}: missing component_type or config_file_name")

            print(f"\n[Step {i + 1}/{len(components)}]")
            logger.log_info(f"Step {i + 1}/{len(components)}: {component_type}")

            try:
                # Execute component and capture result
                result = execute_component(component_type, config_file_path, pipeline_data, logger)

                # Store result and pass to next component
                results[component_type] = result
                pipeline_data = result

                # Print summary
                if result:
                    record_count = result.get('record_count', 'N/A')
                    output_path = result.get('output_path', 'N/A')
                    print(f"  -> Records processed: {record_count}")
                    print(f"  -> Output: {output_path}")

            except PipelineError as e:
                logger.log_error(f"Pipeline failed at {component_type}: {str(e)}")
                results[component_type] = {'status': 'FAILED', 'error': str(e)}
                all_success = False
                failed_component = component_type
                break
            except Exception as e:
                logger.log_error(f"Unexpected error at {component_type}: {str(e)}")
                results[component_type] = {'status': 'FAILED', 'error': f"Unexpected error: {str(e)}"}
                all_success = False
                failed_component = component_type
                break

        print("\n" + "=" * 60)
        if all_success:
            print("DATA PIPELINE EXECUTION COMPLETE")
        else:
            print(f"DATA PIPELINE FAILED AT: {failed_component}")
        print("=" * 60)

        # Print final summary
        print("\nPipeline Summary:")
        logger.log_info("Pipeline Summary:")

        for comp_type, result in results.items():
            if result:
                status = result.get('status', 'SUCCESS')
                if comp_type == "DQ-CHECKER" and result.get('passed') is False:
                    status = "COMPLETED WITH WARNINGS"
                print(f"  {comp_type}: {status}")
                logger.log_info(f"  {comp_type}: {status}")

        # End pipeline logging
        logger.end_pipeline("SUCCESS" if all_success else "FAILED")

        return {
            'results': results,
            'status': 'SUCCESS' if all_success else 'FAILED',
            'failed_component': failed_component
        }

    except ConfigError as e:
        logger.log_error(f"Configuration error: {str(e)}")
        logger.end_pipeline("FAILED")
        print(f"\nPIPELINE FAILED: Configuration error - {str(e)}")
        return {
            'results': {},
            'status': 'FAILED',
            'error': str(e)
        }
    except Exception as e:
        logger.log_error(f"Unexpected pipeline error: {str(e)}")
        logger.end_pipeline("FAILED")
        print(f"\nPIPELINE FAILED: Unexpected error - {str(e)}")
        return {
            'results': {},
            'status': 'FAILED',
            'error': f"Unexpected error: {str(e)}"
        }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python controller.py <path_to_DATAPIPELINE_CONFIG.yaml>")
        sys.exit(1)

    datapipeline_config_path = sys.argv[1]

    if not os.path.exists(datapipeline_config_path):
        print(f"Error: Config file not found: {datapipeline_config_path}")
        sys.exit(1)

    main(datapipeline_config_path)
