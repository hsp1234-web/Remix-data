import yaml

def load_config(config_path: str = "config_v16.yaml") -> dict:
    """
    Loads configuration from a YAML file.

    Args:
        config_path: Path to the YAML configuration file.
                     Defaults to "config_v16.yaml" in the project root.

    Returns:
        A dictionary containing the configuration.

    Raises:
        FileNotFoundError: If the configuration file is not found.
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

if __name__ == '__main__':
    # Example usage (optional, for direct testing of this script)
    try:
        config_data = load_config()
        print("Config loaded successfully:")
        print(config_data)
    except FileNotFoundError as e:
        print(e)

    try:
        config_data_custom = load_config("non_existent_config.yaml")
        print(config_data_custom)
    except FileNotFoundError as e:
        print(e)
