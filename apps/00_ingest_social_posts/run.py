import os
import pandas as pd
import argparse
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_INPUT_FILE = "data/input/social_posts/善甲狼藏金閣V3-Threads版.xlsx - 工作表2.csv"
DEFAULT_OUTPUT_FILE = "data/bronze/social_posts/threads_posts.parquet"

def process_social_csv_to_parquet(csv_filepath, parquet_filepath):
    """
    Reads a CSV file (potentially from Excel export), converts it to Parquet, and saves it.
    """
    try:
        # Ensure output directory exists
        output_dir = os.path.dirname(parquet_filepath)
        os.makedirs(output_dir, exist_ok=True)

        # Read CSV. Excel exports can sometimes have mixed types or strange formatting,
        # so being robust here is good.
        # Common encodings for Excel-generated CSVs might be utf-8 or utf-8-sig (with BOM)
        try:
            df = pd.read_csv(csv_filepath, encoding='utf-8')
        except UnicodeDecodeError:
            logging.warning(f"UTF-8 decoding failed for {csv_filepath}. Trying 'utf-8-sig'...")
            try:
                df = pd.read_csv(csv_filepath, encoding='utf-8-sig')
            except UnicodeDecodeError:
                logging.error(f"UTF-8-SIG decoding also failed for {csv_filepath}. Please check file encoding.")
                return False
        except Exception as e: # Catch other pd.read_csv errors
            logging.error(f"Pandas error reading CSV '{csv_filepath}': {e}")
            return False


        # Basic data cleaning example: remove fully empty rows if any
        df.dropna(how='all', inplace=True)

        df.to_parquet(parquet_filepath, index=False)
        logging.info(f"Successfully converted '{csv_filepath}' to '{parquet_filepath}'")
        return True
    except Exception as e:
        logging.error(f"Error processing file '{csv_filepath}' to '{parquet_filepath}': {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Ingest social media CSV file and convert to Parquet.")
    parser.add_argument("--input-file", type=str, default=DEFAULT_INPUT_FILE,
                        help=f"Input CSV file from social media data. Default: {DEFAULT_INPUT_FILE}")
    parser.add_argument("--output-file", type=str, default=DEFAULT_OUTPUT_FILE,
                        help=f"Output Parquet file path. Default: {DEFAULT_OUTPUT_FILE}")

    args = parser.parse_args()

    input_file = args.input_file
    output_file = args.output_file

    logging.info(f"Starting social media post ingestion from '{input_file}' to '{output_file}'")

    if not os.path.isfile(input_file):
        logging.error(f"Input file '{input_file}' does not exist or is not a file. Exiting.")
        return

    if process_social_csv_to_parquet(input_file, output_file):
        logging.info(f"Social media post ingestion finished successfully.")
    else:
        logging.error(f"Social media post ingestion failed.")

if __name__ == "__main__":
    main()
