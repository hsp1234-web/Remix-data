from data_pipeline.db_utils import get_db_connection, create_price_data_table, create_weekly_price_data_table, create_fmp_profiles_table, create_fmp_financial_statements_table
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def initialize_all_tables(conn_obj, symbol_for_price_tables=None):
    logger.info("Initializing core tables...")
    create_fmp_profiles_table(conn_obj)
    create_fmp_financial_statements_table(conn_obj)
    if symbol_for_price_tables:
        # Price tables are symbol-specific, so only create if a symbol is contextually relevant
        # or create them on-demand when saving data.
        # For a generic init, we might skip symbol specific tables or create for a default/test symbol.
        logger.info(
            f"Ensuring price tables for symbol: {symbol_for_price_tables}")
        create_price_data_table(conn_obj, symbol_for_price_tables)  # Daily
        create_weekly_price_data_table(
            conn_obj, symbol_for_price_tables)  # Weekly
    logger.info("Core table initialization process completed.")


if __name__ == '__main__':
    db_conn = None
    try:
        db_conn = get_db_connection()
        # Pass a default/example symbol if you want to ensure these tables are created during init_db stage
        # Otherwise, these tables will be created when data for a specific symbol is first saved.
        initialize_all_tables(db_conn, "AAPL")
    finally:
        if db_conn:
            db_conn.close()
