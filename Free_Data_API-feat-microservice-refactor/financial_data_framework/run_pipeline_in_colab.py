# run_pipeline_in_colab.py

import argparse
import logging
import os
from datetime import datetime, timedelta
from commander import Commander

# --- 配置日誌 ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Colab 環境的 API 金鑰管理 ---
def setup_api_keys_from_colab():
    """
    如果在 Colab 環境中，嘗試從 Secrets 讀取金鑰。
    注意：此框架目前主要使用 yfinance，不需要 API 金鑰。
    此函數作為未來擴展的佔位符。
    """
    try:
        from google.colab import userdata
        # 假設未來需要 'FMP_API_KEY'
        # if 'FMP_API_KEY' in userdata.get_all():
        #     os.environ['FMP_API_KEY'] = userdata.get('FMP_API_KEY')
        #     logging.info("已從 Colab Secrets 加載 FMP_API_KEY。")
        logging.info("在 Colab 環境中運行。未來可於此處加載 API 金鑰。")
    except ImportError:
        logging.info("不在 Colab 環境中，跳過從 Secrets 加載金鑰。")
    except Exception as e:
        logging.warning(f"從 Colab Secrets 加載金鑰時出錯: {e}")

# --- 主執行函數 ---
def main():
    # 設置 API 金鑰
    setup_api_keys_from_colab()

    # --- 命令列參數解析 ---
    parser = argparse.ArgumentParser(description="金融數據管道執行器")
    parser.add_argument(
        '--task',
        type=str,
        required=True,
        choices=['fetch'],
        help="要執行的任務。目前支持 'fetch'。"
    )
    parser.add_argument(
        '--symbols',
        type=str,
        help="要獲取的股票代號，以逗號分隔 (e.g., 'AAPL,GOOG,TSLA,^TWII')。僅在 task='fetch' 時需要。"
    )
    parser.add_argument(
        '--start_date',
        type=str,
        default=(datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d'),
        help="數據開始日期 (YYYY-MM-DD)。默認為5年前的今天。"
    )
    parser.add_argument(
        '--end_date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help="數據結束日期 (YYYY-MM-DD)。默認為今天。"
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.yaml',
        help="組態檔的路徑。"
    )

    args = parser.parse_args()

    # --- 任務分派 ---
    if not os.path.exists(args.config):
        logging.error(f"組態檔不存在: {args.config}")
        return

    commander_instance = None
    try:
        # 實例化 Commander
        commander_instance = Commander(config_path=args.config)

        if args.task == 'fetch':
            if not args.symbols:
                logging.error("'fetch' 任務需要 --symbols 參數。")
                return

            symbols_list = [s.strip().upper() for s in args.symbols.split(',')]
            logging.info(f"開始執行 'fetch' 任務，目標代號: {symbols_list}")
            commander_instance.fetch_and_store_symbols(
                symbols=symbols_list,
                start_date=args.start_date,
                end_date=args.end_date
            )
        else:
            logging.error(f"未知的任務: {args.task}")

    except Exception as e:
        logging.critical(f"管道執行過程中發生嚴重錯誤: {e}", exc_info=True)
    finally:
        # 確保資源被釋放
        if commander_instance:
            commander_instance.close()
        logging.info("管道執行流程結束。")


if __name__ == "__main__":
    main()
