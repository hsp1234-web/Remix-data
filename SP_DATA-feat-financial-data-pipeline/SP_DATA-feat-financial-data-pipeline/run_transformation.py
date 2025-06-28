import logging
from src.sp_data_v16.transformation.pipeline import TransformationPipeline

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

if __name__ == "__main__":
    logging.info("Starting the transformation process.")
    config_path = "config_v16.yaml"

    try:
        pipeline = TransformationPipeline(config_path)
        pipeline.run()
    except Exception as e:
        logging.error(f"An error occurred during the transformation process: {e}")
    finally:
        logging.info("Transformation process finished.")
