import yaml


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
        print("Usage: python router_handler.py <path_to_yaml_file>")
        sys.exit(1)

    print_yaml_content(sys.argv[1])
