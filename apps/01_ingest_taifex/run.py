import os
import pandas as pd
import argparse
import logging
import glob

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration (can be overridden by args, but runner.py won't use args for these) ---
DEFAULT_INPUT_DIR = "data/input/taifex/unzipped"
DEFAULT_OUTPUT_DIR = "data/bronze/taifex"

def process_csv_to_parquet(csv_filepath, output_dir):
    """
    Reads a CSV file, converts it to Parquet, and saves it to the output directory.
    The Parquet filename will be the same as the CSV's basename, with .parquet extension.
    """
    try:
        # Attempt to read with utf-8, then try with 'big5' or 'cp950' for common Taiwanese CSV encodings
        try:
            df = pd.read_csv(csv_filepath, encoding='utf-8')
        except UnicodeDecodeError:
            logging.warning(f"UTF-8 decoding failed for {csv_filepath}. Trying 'cp950' (Big5)...")
            try:
                df = pd.read_csv(csv_filepath, encoding='cp950')
            except UnicodeDecodeError:
                logging.error(f"cp950 (Big5) decoding also failed for {csv_filepath}. Skipping this file.")
                return False
        except Exception as e: # Catch other pd.read_csv errors
            logging.error(f"Pandas error reading CSV '{csv_filepath}': {e}")
            return False


        base_filename = os.path.basename(csv_filepath)
        parquet_filename = os.path.splitext(base_filename)[0] + ".parquet"
        output_filepath = os.path.join(output_dir, parquet_filename)

        os.makedirs(output_dir, exist_ok=True)
        df.to_parquet(output_filepath, index=False)
        logging.info(f"Successfully converted '{csv_filepath}' to '{output_filepath}'")
        return True
    except Exception as e:
        logging.error(f"Error processing file '{csv_filepath}': {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Ingest Taifex CSV files and convert them to Parquet.")
    parser.add_argument("--input-dir", type=str, default=DEFAULT_INPUT_DIR,
                        help=f"Directory containing raw Taifex CSV files. Default: {DEFAULT_INPUT_DIR}")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"Directory to save converted Parquet files. Default: {DEFAULT_OUTPUT_DIR}")

    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir

    logging.info(f"Starting Taifex ingestion from '{input_dir}' to '{output_dir}'")

    if not os.path.isdir(input_dir):
        logging.error(f"Input directory '{input_dir}' does not exist or is not a directory. Exiting.")
        return

    # Find all CSV files in the input directory (non-recursive)
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    # Alternative for recursive search: glob.glob(os.path.join(input_dir, "**/*.csv"), recursive=True)

    if not csv_files:
        logging.warning(f"No CSV files found in '{input_dir}'. Ingestion complete.")
        return

    success_count = 0
    failure_count = 0

    for csv_file in csv_files:
        if process_csv_to_parquet(csv_file, output_dir):
            success_count += 1
        else:
            failure_count += 1

    logging.info(f"Taifex ingestion finished. Successfully processed: {success_count}, Failed: {failure_count}")

if __name__ == "__main__":
    main()
