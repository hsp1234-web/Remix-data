import yaml
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import logging
import json
import sys
from pathlib import Path

# --- Early Path Setup & Pre-Init Logger ---
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s (main-pre-init)',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
pre_init_logger = logging.getLogger("MainPreInit")

PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
# Ensure the log file is created in the project root, next to main.py
DETAILED_LOG_FILENAME = os.path.join(PROJECT_ROOT, "market_briefing_log.txt")

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
    pre_init_logger.info(f"Inserted PROJECT_ROOT ({PROJECT_ROOT}) into sys.path.")

pre_init_logger.info(f"INITIAL: __file__ is {__file__}")
pre_init_logger.info(f"INITIAL: PROJECT_ROOT: {PROJECT_ROOT}")
pre_init_logger.info(f"sys.path for module import: {sys.path}")

# --- Module Imports ---
global_log = None
init_global_log_function = None
global_log_file_path_imported = None
get_taipei_time_func_imported = None

try:
    from connectors.base import BaseConnector
    # from src.connectors.fred_connector import FredConnector # FredConnector definition not found
    from connectors.nyfed_connector import NYFedConnector
    from connectors.yfinance_connector import YFinanceConnector
    # from src.database.database_manager import DatabaseManager # DatabaseManager definition not found
    from engine.indicator_engine import IndicatorEngine

    # Import from the updated initialize_global_log V3
    from scripts.initialize_global_log import log_message, get_taipei_time, LOG_FILE_PATH, initialize_log_file
    global_log = log_message
    init_global_log_function = initialize_log_file
    global_log_file_path_imported = LOG_FILE_PATH
    get_taipei_time_func_imported = get_taipei_time

    # Explicitly initialize global logger from main.py
    # Pass project_root to ensure log directory is created correctly relative to project.
    if init_global_log_function is not None: # Check if function was imported
        try:
            log_dir_main = Path(PROJECT_ROOT) / "api_test_logs" # Consistent with initialize_global_log's default
            actual_log_file = init_global_log_function(
                log_dir_override=str(log_dir_main),
                force_reinit=True, # Force reinit to ensure main's config takes precedence if called first
                project_root_path=Path(PROJECT_ROOT)
            )
            global_log(f"main.py: Global logger explicitly initialized/re-initialized by main. Log file: {actual_log_file}", "INFO", logger_name="MainApp.Setup")
        except Exception as e_log_init_main:
            pre_init_logger.error(f"main.py: Failed to explicitly initialize global logger: {e_log_init_main}", exc_info=True)
            if global_log is None:
                 global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), msg)
            global_log("main.py: Using pre_init_logger due to global_log explicit init failure.", "WARNING", logger_name="MainApp.Setup")
    else:
        pre_init_logger.error("main.py: init_global_log_function was not imported. Global logging will be compromised.")
        if global_log is None:
            global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), msg)


except ImportError as e:
    pre_init_logger.error(f"Failed to import custom modules: {e}. Current sys.path: {sys.path}", exc_info=True)
    if global_log is None: print(f"CRITICAL IMPORT ERROR (main.py, global_log unavailable): {e}.")
    else: global_log(f"CRITICAL: Failed to import custom modules in main.py: {e}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1)
except Exception as e_general_import:
    pre_init_logger.error(f"General error during import phase: {e_general_import}", exc_info=True)
    if global_log is None: print(f"CRITICAL GENERAL IMPORT ERROR (main.py, global_log unavailable): {e_general_import}.")
    else: global_log(f"CRITICAL: General error during import phase in main.py: {e_general_import}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1)


def load_config(path="configs/project_config.yaml") -> Dict[str,Any]:
    global_log(f"Loading project configuration from: {path}", "INFO", logger_name="MainApp.ConfigLoader")
    try:
        with open(Path(PROJECT_ROOT) / path, 'r', encoding='utf-8') as f: # Ensure path is relative to PROJECT_ROOT
            config = yaml.safe_load(f)
        global_log("Project configuration loaded successfully.", "INFO", logger_name="MainApp.ConfigLoader")
        return config
    except FileNotFoundError:
        global_log(f"Config file not found: {Path(PROJECT_ROOT) / path}. Exiting.", "ERROR", logger_name="MainApp.ConfigLoader")
        raise
    except Exception as e:
        global_log(f"Error loading config from {Path(PROJECT_ROOT) / path}: {e}", "ERROR", logger_name="MainApp.ConfigLoader", exc_info=True)
        raise

def main():
    # --- Setup detailed file logger for this run ---
    root_logger = logging.getLogger() # Get the root logger
    file_handler = logging.FileHandler(DETAILED_LOG_FILENAME, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.DEBUG) # Capture all levels
    root_logger.addHandler(file_handler)
    # Also ensure console output still happens for project_logger namespace if it was set up by initialize_global_log
    # If initialize_global_log already added a StreamHandler to 'project_logger', this might be redundant
    # but better to ensure it if we are manipulating the root_logger directly.
    if global_log: # Check if global_log was successfully initialized
        global_log(f"Detailed logging for this run will be saved to: {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Setup")
    else: # Fallback if global_log is not available (e.g. import error)
        pre_init_logger.info(f"Detailed logging for this run will be saved to: {DETAILED_LOG_FILENAME}")


    global_log("--- 開始執行端到端金融數據處理原型 (V2.5 with Market Briefing) ---", "INFO", logger_name="MainApp.main")

    config = load_config() # Path is relative to where main.py is (PROJECT_ROOT)
    start_date_cfg = config.get('data_fetch_range', {}).get('start_date', "2020-01-01")
    end_date_cfg = config.get('data_fetch_range', {}).get('end_date')

    current_date_for_end = ""
    try:
        current_date_for_end = get_taipei_time_func_imported().strftime('%Y-%m-%d') if get_taipei_time_func_imported else datetime.now(timezone.utc).strftime('%Y-%m-%d')
    except Exception as e_time:
        current_date_for_end = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        global_log(f"Using UTC for 'today' as get_taipei_time is unavailable/failed: {e_time}", "WARNING", logger_name="MainApp.main")

    if not end_date_cfg:
        end_date = current_date_for_end
        global_log(f"Config 'end_date' is empty, using current date: {end_date}", "INFO", logger_name="MainApp.main")
    else:
        end_date = end_date_cfg
    
    fred_api_key_env = config.get('api_endpoints', {}).get('fred', {}).get('api_key_env')
    if fred_api_key_env:
        user_provided_fred_key = "78ea51fb13b546d89f1a683cb4ba26f5"
        os.environ[fred_api_key_env] = user_provided_fred_key
        global_log(f"Temporarily set env var {fred_api_key_env} for FRED API.", "DEBUG", logger_name="MainApp.main")
    else:
        global_log("FRED API key env name not in config.", "WARNING", logger_name="MainApp.main")

    db_logger = logging.getLogger("project_logger.DatabaseManager")
    fred_logger = logging.getLogger("project_logger.FredConnector")
    nyfed_logger = logging.getLogger("project_logger.NYFedConnector")
    yf_logger = logging.getLogger("project_logger.YFinanceConnector")
    engine_logger = logging.getLogger("project_logger.IndicatorEngine")

    # Pass full config to DatabaseManager, which will extract its specific part
    # db_manager = DatabaseManager(config, logger_instance=db_logger) # Definition not found

    try:
        # db_manager.connect() # Definition not found
        data_fetch_flags = {'fred': False, 'nyfed': False, 'yfinance_move': False}

        global_log("\n--- 階段 1: 數據獲取 ---", "INFO", logger_name="MainApp.main")

        # fred_conn = FredConnector(config, logger_instance=fred_logger) # Definition not found
        # fred_ids = config.get('target_metrics', {}).get('fred_series_ids', [])
        # fred_df, fred_err = fred_conn.fetch_data(series_ids=fred_ids, start_date=start_date_cfg, end_date=end_date)
        # if fred_err: global_log(f"FRED Error: {fred_err}", "ERROR", logger_name="MainApp.main"); data_fetch_flags['fred'] = False
        # elif fred_df is not None:
        #     global_log(f"Fetched {len(fred_df)} FRED records.", "INFO", logger_name="MainApp.main")
        #     if not fred_df.empty: db_manager.bulk_insert_or_replace('fact_macro_economic_data', fred_df) # db_manager not defined
        #     data_fetch_flags['fred'] = True
        # else: global_log("FRED Connector returned None, no error.", "WARNING", logger_name="MainApp.main"); data_fetch_flags['fred'] = False
        global_log("FRED data fetching skipped as FredConnector definition not found.", "WARNING", logger_name="MainApp.main")
        data_fetch_flags['fred'] = False


        nyfed_conn = NYFedConnector(config, logger_instance=nyfed_logger)
        nyfed_df, nyfed_err = nyfed_conn.fetch_data() # NYFedConnector processes all its URLs internally
        if nyfed_err: global_log(f"NYFed Error: {nyfed_err}", "ERROR", logger_name="MainApp.main"); data_fetch_flags['nyfed'] = False
        elif nyfed_df is not None:
            global_log(f"Fetched {len(nyfed_df)} NYFed records.", "INFO", logger_name="MainApp.main")
            # if not nyfed_df.empty: db_manager.bulk_insert_or_replace('fact_macro_economic_data', nyfed_df) # db_manager not defined
            if not nyfed_df.empty: global_log("NYFed data fetched but DB insert skipped (DatabaseManager not defined).", "INFO", logger_name="MainApp.main")
            data_fetch_flags['nyfed'] = True
        else: global_log("NYFed Connector returned None, no error.", "WARNING", logger_name="MainApp.main"); data_fetch_flags['nyfed'] = False

        yf_conn = YFinanceConnector(config, logger_instance=yf_logger)
        yf_ids = config.get('target_metrics', {}).get('yfinance_tickers', [])
        yf_df, yf_err = yf_conn.fetch_data(tickers=yf_ids, start_date=start_date_cfg, end_date=end_date)
        if yf_err: global_log(f"yfinance Error: {yf_err}", "ERROR", logger_name="MainApp.main"); data_fetch_flags['yfinance_move'] = False
        elif yf_df is not None:
            global_log(f"Fetched {len(yf_df)} yfinance records for {yf_ids}.", "INFO", logger_name="MainApp.main")
            # if not yf_df.empty: db_manager.bulk_insert_or_replace('fact_stock_price', yf_df) # db_manager not defined
            if not yf_df.empty: global_log("YFinance data fetched but DB insert skipped (DatabaseManager not defined).", "INFO", logger_name="MainApp.main")
            data_fetch_flags['yfinance_move'] = True
        else: global_log(f"yfinance for {yf_ids} returned None, no error.", "WARNING", logger_name="MainApp.main"); data_fetch_flags['yfinance_move'] = False

        global_log("\n--- 階段 2 & 3: 指標計算與市場簡報 ---", "INFO", logger_name="MainApp.main")
        if not all(data_fetch_flags.get(k, False) for k in ['fred', 'nyfed', 'yfinance_move']):
             global_log("One or more critical data sources may have failed. Stress index might be incomplete or skipped.", "WARNING", logger_name="MainApp.main")

        # macro_data = db_manager.fetch_all_for_engine('fact_macro_economic_data', start_date_cfg, end_date, date_column='metric_date') # db_manager not defined
        # stock_data = db_manager.fetch_all_for_engine('fact_stock_price', start_date_cfg, end_date, date_column='price_date') # db_manager not defined
        global_log("Skipping data fetch from DB for IndicatorEngine as DatabaseManager is not defined.", "WARNING", logger_name="MainApp.main")
        macro_data = pd.DataFrame() # Provide empty DataFrame
        stock_data = pd.DataFrame() # Provide empty DataFrame


        if any(d is None or d.empty for d in [macro_data, stock_data]): # Check if any are None or empty
            global_log("Insufficient data (due to missing DB manager) for IndicatorEngine. Skipping stress index.", "ERROR", logger_name="MainApp.main")
        else:
            # This else block will likely not be reached if macro_data and stock_data are empty DFs
            move_data_eng = stock_data[stock_data['security_id'] == '^MOVE'] if 'security_id' in stock_data.columns else pd.DataFrame()
            if move_data_eng.empty : global_log("^MOVE data not in DB for IndicatorEngine. Will proceed; calculations may be affected.", "WARNING", logger_name="MainApp.main")

            engine_input = {'macro': macro_data, 'move': move_data_eng}
            engine_cfg = config.get('indicator_engine_params', {})

            indicator_eng = IndicatorEngine(engine_input, params=engine_cfg, logger_instance=engine_logger)
            stress_df = indicator_eng.calculate_dealer_stress_index()

            if stress_df is None or stress_df.empty:
                global_log("Stress index calculation resulted in no data/all NaN.", "ERROR", logger_name="MainApp.main")
            else:
                global_log(f"Stress Index calculated. Shape: {stress_df.shape}. Latest {stress_df.index[-1].strftime('%Y-%m-%d') if not stress_df.empty else 'N/A'}:", "INFO", logger_name="MainApp.main")
                global_log(f"\n{stress_df.tail().to_string()}", "INFO", logger_name="MainApp.main") # Log DataFrame to file

                briefing_dt = stress_df.index[-1]
                briefing_dt_str = briefing_dt.strftime('%Y-%m-%d')
                latest_stress = stress_df['DealerStressIndex'].iloc[-1]

                s_desc = "N/A"
                if pd.notna(latest_stress):
                    th_m = engine_cfg.get('stress_threshold_moderate',40); th_h = engine_cfg.get('stress_threshold_high',60); th_e = engine_cfg.get('stress_threshold_extreme',80)
                    if latest_stress >= th_e: s_desc = f"{latest_stress:.2f} (極度緊張)"
                    elif latest_stress >= th_h: s_desc = f"{latest_stress:.2f} (高度緊張)"
                    elif latest_stress >= th_m: s_desc = f"{latest_stress:.2f} (中度緊張)"
                    else: s_desc = f"{latest_stress:.2f} (正常)"

                trend_str = "N/A"
                if len(stress_df) >= 2:
                    chg = stress_df['DealerStressIndex'].diff().iloc[-1]
                    if pd.notna(chg): trend_str = "上升" if chg > 0.1 else ("下降" if chg < -0.1 else "穩定")

                engine_prep_df = indicator_eng.df_prepared
                latest_brief_data = None
                if engine_prep_df is not None and not engine_prep_df.empty:
                    # Ensure briefing_dt is in a comparable format to the index
                    # If engine_prep_df.index is DatetimeIndex, briefing_dt (which is a date from stress_df.index) should work.
                    # If engine_prep_df.index is just date objects, it should also work.
                    if briefing_dt in engine_prep_df.index:
                        latest_brief_data = engine_prep_df.loc[briefing_dt]
                    else: # Fallback if exact date match fails (e.g. due to time component if not careful)
                        try: # Try to match by string date
                           latest_brief_data = engine_prep_df.loc[briefing_dt_str]
                        except KeyError:
                           global_log(f"Could not find briefing_date {briefing_dt_str} or {briefing_dt} in engine_prepared_df.index. Using last available row.", "WARNING", logger_name="MainApp.main")
                           if not engine_prep_df.empty: latest_brief_data = engine_prep_df.iloc[-1]


                def get_fmt_val(srs, key, fmt="{:.2f}", N_A="N/A"):
                    if srs is not None and key in srs.index and pd.notna(srs[key]):
                        val = srs[key]
                        return fmt.format(val) if isinstance(val,(int,float)) and fmt and pd.notna(val) else str(val)
                    return N_A

                mv_str = get_fmt_val(latest_brief_data, '^MOVE')
                spread_val_raw = latest_brief_data['spread_10y2y'] if latest_brief_data is not None and 'spread_10y2y' in latest_brief_data else None
                sp_str = f"{(spread_val_raw * 100):.2f} bps" if pd.notna(spread_val_raw) else "N/A"
                pos_str = get_fmt_val(latest_brief_data, 'NYFED/PRIMARY_DEALER_NET_POSITION', fmt="{:,.0f}")
                vx_str = get_fmt_val(latest_brief_data, 'FRED/VIXCLS')
                sfr_dev_str = get_fmt_val(latest_brief_data, 'FRED/SOFR_Dev')

                market_briefing = {
                    "briefing_date": briefing_dt_str, "data_window_end": briefing_dt_str,
                    "dealer_stress_index": {"current_value_desc": s_desc, "trend_approx": trend_str},
                    "key_components_latest": [
                        {"name": "MOVE Index", "value_str": mv_str},
                        {"name": "10Y-2Y Spread", "value_str": sp_str},
                        {"name": "Primary Dealer Positions (Millions USD)", "value_str": pos_str}],
                    "market_context_latest": {"vix": vx_str, "sofr_deviation": sfr_dev_str},
                    "summary_narrative": f"摘要：市場壓力指數為 {s_desc}，主要由債券市場波動率(MOVE={mv_str})和利差(10Y-2Y={sp_str})驅動。一級交易商倉位 ({pos_str} 百萬美元)。"
                }
                global_log("\n--- 市場簡報 (JSON) ---", "INFO", logger_name="MainApp.main")
                print("\n--- 市場簡報 (JSON) ---")
                print(json.dumps(market_briefing, indent=2, ensure_ascii=False))
                global_log(json.dumps(market_briefing, indent=2, ensure_ascii=False), "INFO", logger_name="MainApp.main")

    except Exception as e_main_flow:
        global_log(f"主流程 main() 發生嚴重錯誤: {e_main_flow}", "CRITICAL", logger_name="MainApp.main", exc_info=True)
        print(f"主流程 main() 發生嚴重錯誤: {e_main_flow}")
    finally:
        # if 'db_manager' in locals() and hasattr(db_manager, 'conn') and db_manager.conn is not None and not db_manager.conn.isclosed(): # Check if db_manager and conn exist
        #     db_manager.disconnect() # db_manager not defined
        # else:
        #     global_log("DB Manager not fully initialized or connection already closed/not established.", "DEBUG", logger_name="MainApp.main")
        global_log("Skipping DB disconnect as DatabaseManager was not used.", "DEBUG", logger_name="MainApp.main")
        global_log("\n--- 端到端原型執行完畢 ---", "INFO", logger_name="MainApp.main")

        # --- Clean up detailed file logger ---
        if 'file_handler' in locals() and file_handler is not None:
            global_log(f"Removing detailed file handler. Log saved to {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Cleanup")
            root_logger.removeHandler(file_handler)
            file_handler.close()

if __name__ == "__main__":
    if global_log is None:
        pre_init_logger.critical("global_log function was not imported. Logging will be severely limited.")
        global_log = lambda msg, level="INFO", **kwargs: print(f"ULTRA_FALLBACK_LOG [{level}] {msg}")

    try:
        if init_global_log_function and (global_log_file_path_imported is None): # Check if it was imported AND if the path var is None (meaning not run by itself)
            log_dir_main_run = Path(PROJECT_ROOT) / LOG_DIR_NAME # Use LOG_DIR_NAME from initialize_global_log
            pre_init_logger.info(f"main.py (__main__): Attempting to explicitly initialize global logger in {log_dir_main_run}")
            init_global_log_function(log_dir_override=str(log_dir_main_run), force_reinit=True, project_root_path=Path(PROJECT_ROOT))
            global_log("main.py (__main__): Global logger explicitly re-initialized by main.", "DEBUG", logger_name="MainApp.Setup")
    except Exception as e_log_re_init_main:
         global_log(f"Error during explicit re-init of global_log in main __main__: {e_log_re_init_main}", "ERROR", logger_name="MainApp.Setup", exc_info=True)

    main()
