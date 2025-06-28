import yaml
from pathlib import Path
from connectors.nyfed_connector import NYFedConnectorDiagnosis # Adjusted import
import logging

# Setup basic logging for the test script itself
logger = logging.getLogger("NYFedTestDriver")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_config(config_path_str: str) -> dict:
    config_path = Path(config_path_str)
    logger.info(f"Loading configuration from: {config_path.resolve()}")
    if not config_path.exists():
        logger.error(f"Configuration file {config_path} not found.")
        return {}
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading YAML configuration from {config_path}: {e}")
        return {}

def main():
    logger.info("--- Starting NYFed Connector Diagnosis ---")

    # Path assumes script is run from project root where diagnose_nyfed.sh creates src/
    config_file = "src/configs/project_config.yaml"
    config = load_config(config_file)

    if not config:
        logger.error("Failed to load configuration. Exiting diagnosis.")
        return

    # Ensure 'nyfed_primary_dealer_urls' and 'requests_config' are present
    if 'nyfed_primary_dealer_urls' not in config or 'requests_config' not in config:
        logger.error("Essential configuration ('nyfed_primary_dealer_urls' or 'requests_config') missing. Exiting.")
        return

    try:
        connector = NYFedConnectorDiagnosis(config=config)
        connector.fetch_data()
    except Exception as e:
        logger.error(f"An error occurred during NYFedConnectorDiagnosis execution: {e}", exc_info=True)

    logger.info("--- NYFed Connector Diagnosis Finished ---")

if __name__ == "__main__":
    main()
