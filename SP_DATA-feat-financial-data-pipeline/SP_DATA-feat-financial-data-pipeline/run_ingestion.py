import argparse
import os
from src.sp_data_v16.ingestion.pipeline import IngestionPipeline
from src.sp_data_v16.core.config import load_config # For initial validation

CONFIG_FILE_PATH = "config_v16.yaml"

def main():
    """
    Main function to initialize and run the ingestion pipeline.
    It expects 'config_v16.yaml' to be in the same directory or a path
    to be provided via an argument.
    """
    parser = argparse.ArgumentParser(description="Run the SP Data v16 Ingestion Pipeline.")
    parser.add_argument(
        "--config",
        type=str,
        default=CONFIG_FILE_PATH,
        help=f"Path to the configuration file (default: {CONFIG_FILE_PATH})"
    )
    args = parser.parse_args()

    config_path = args.config

    print(f"Using configuration file: {config_path}")

    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found.")
        print("Please ensure the configuration file exists or specify its path using --config.")
        # Try to load default config to see if it's in a standard location if user provided a bad path
        try:
            default_config_test = load_config(CONFIG_FILE_PATH) # Test default
            if default_config_test:
                 print(f"Hint: A default '{CONFIG_FILE_PATH}' was found in the working directory.")
        except FileNotFoundError:
            pass # Default also not found
        return

    try:
        # Initialize and run the pipeline
        pipeline = IngestionPipeline(config_path=config_path)
        pipeline.run()
    except FileNotFoundError as e:
        # This might be raised by load_config if the file is still not found
        # or if the input_directory specified in the config is not found by the scanner
        print(f"Error: A required file or directory was not found: {e}")
    except ValueError as ve:
        # Raised by IngestionPipeline or load_config for config issues
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        # For debugging, you might want to print the full traceback
        # import traceback
        # traceback.print_exc()

if __name__ == "__main__":
    main()
