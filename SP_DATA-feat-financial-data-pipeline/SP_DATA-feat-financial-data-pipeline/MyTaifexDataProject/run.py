# MyTaifexDataProject/run.py
import argparse
import logging
from pathlib import Path

from taifex_pipeline.core.config_loader import ConfigLoader
from taifex_pipeline.core.logger_setup import get_logger
from taifex_pipeline.ingestion.pipeline import IngestionPipeline
from taifex_pipeline.transformation.pipeline import TransformationPipeline
# 1. 匯入 MetadataScanner
from taifex_pipeline.scripts.metadata_scanner import MetadataScanner

def main():
    """主執行函數"""
    parser = argparse.ArgumentParser(description="Taifex Data Pipeline")
    subparsers = parser.add_subparsers(dest="action", help="Action to perform")
    subparsers.required = True

    # Ingestion pipeline parser
    ingest_parser = subparsers.add_parser(
        "ingest", help="Run the ingestion pipeline"
    )
    ingest_parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to the config file",
    )

    # Transformation pipeline parser
    transform_parser = subparsers.add_parser(
        "transform", help="Run the transformation pipeline"
    )
    transform_parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to the config file",
    )

    # 2. 為 'scan_metadata' 新增一個子解析器
    scan_parser = subparsers.add_parser(
        "scan_metadata", help="Scan processed files and populate metadata database"
    )
    scan_parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to the config file",
    )
    # 允許透過 CLI 覆蓋設定檔中的路徑
    scan_parser.add_argument(
        "--parquet-dir",
        type=str,
        help="Directory of processed parquet files to scan (overrides config)",
    )
    scan_parser.add_argument(
        "--db-path",
        type=str,
        help="Path to the metadata database (overrides config)",
    )


    args = parser.parse_args()
    config = ConfigLoader.load(args.config)
    logger = get_logger(__name__, log_level=config.get("logging", {}).get("level", "INFO"))

    try:
        if args.action == "ingest":
            logger.info("Starting ingestion pipeline...")
            pipeline = IngestionPipeline(config)
            pipeline.run()
            logger.info("Ingestion pipeline finished.")
        elif args.action == "transform":
            logger.info("Starting transformation pipeline...")
            pipeline = TransformationPipeline(config)
            pipeline.run()
            logger.info("Transformation pipeline finished.")
        # 3. 新增執行 MetadataScanner 的邏輯
        elif args.action == "scan_metadata":
            logger.info("Starting metadata scan...")

            # 從設定檔讀取預設值
            scanner_config = config.get("metadata_scanner", {})

            # 如果提供了 CLI 參數，則使用它；否則，使用設定檔的值
            parquet_dir = args.parquet_dir or scanner_config.get("parquet_dir")
            db_path = args.db_path or scanner_config.get("db_path")

            if not parquet_dir or not db_path:
                raise ValueError("Parquet directory and DB path must be configured in config.yaml or provided via arguments.")

            logger.info(f"Scanning Parquet files in: {Path(parquet_dir).resolve()}")
            logger.info(f"Using metadata database at: {Path(db_path).resolve()}")

            scanner = MetadataScanner(
                parquet_dir=parquet_dir,
                db_path=db_path
            )
            scanner.run_scan()
            logger.info("Metadata scan finished successfully.")

    except Exception as e:
        logger.error(f"An error occurred during pipeline execution: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
