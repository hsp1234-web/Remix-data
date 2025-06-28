import os
import sys
import json
from datetime import datetime, date, timedelta, timezone

# ---路徑修正---
current_script_dir = os.path.dirname(os.path.abspath(__file__)) # .../src/reports
src_dir = os.path.dirname(current_script_dir) # .../src
project_root_dir = os.path.dirname(src_dir) # AI_Assisted_Historical_Backtesting
# For imports like AI_Assisted_Historical_Backtesting.src... the parent of project_root_dir needs to be in sys.path
project_root_parent_dir = os.path.dirname(project_root_dir)
if project_root_parent_dir not in sys.path:
    sys.path.insert(0, project_root_parent_dir)
# ---路徑修正結束---

from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME, get_logger
from AI_Assisted_Historical_Backtesting.src.database.db_manager import DatabaseManager
from AI_Assisted_Historical_Backtesting.src.ai_logic.llama_agent import LlamaOllamaAgent

# setup_logger(PROJECT_LOGGER_NAME, level="INFO") # Setup should be done once in main entry point
logger = get_logger(__name__)

DB_FILEPATH_REPORTER = os.path.join(project_root_dir, "data", "project_data.sqlite")
DEFAULT_REPORTER_MODEL_NAME = "llama3:8b-instruct-q4_K_M"

class DailyReporter:
    def __init__(self, db_manager: DatabaseManager, llama_agent: LlamaOllamaAgent):
        self.db_manager = db_manager
        self.llama_agent = llama_agent

    def _fetch_daily_judgments_and_features(self, report_target_date: date) -> tuple[list[dict], list[dict]]:
        judgments_for_day = []
        features_for_day = []
        start_of_day_utc = datetime(report_target_date.year, report_target_date.month, report_target_date.day, 0, 0, 0, tzinfo=timezone.utc)
        end_of_day_utc = start_of_day_utc + timedelta(days=1) - timedelta(microseconds=1) # 修正: timedelta.microseconds(1) -> timedelta(microseconds=1)
        start_iso = start_of_day_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        end_iso = end_of_day_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        logger.info(f"獲取日期 {report_target_date.isoformat()} 的 AI 判斷 (UTC範圍: {start_iso} 到 {end_iso})")
        query_judgments = """
        SELECT id, judgment_timestamp_utc, market_briefing_json, ai_decision_category,
               ai_recommended_strategy, ai_rationale_text, confidence_score,
               key_warnings_json, feature_period_start_utc, symbol_judged
        FROM ai_historical_judgments
        WHERE judgment_timestamp_utc >= ? AND judgment_timestamp_utc <= ?
        ORDER BY judgment_timestamp_utc ASC;
        """
        raw_judgments = self.db_manager.execute_query(query_judgments, [start_iso, end_iso])

        if raw_judgments:
            feature_periods_to_fetch = set()
            for row in raw_judgments:
                judgment = {
                    "id": row[0], "judgment_timestamp_utc": row[1],
                    "market_briefing_json_str": row[2],
                    "ai_decision_category": row[3],
                    "ai_recommended_strategy_str": row[4],
                    "ai_rationale_text": row[5], "confidence_score": row[6],
                    "key_warnings_json_str": row[7],
                    "feature_period_start_utc": row[8], "symbol_judged": row[9]
                }
                try: judgment["market_briefing"] = json.loads(row[2]) if row[2] else None
                except: judgment["market_briefing"] = {"error": "failed to parse market_briefing_json"}
                try: judgment["ai_recommended_strategy"] = json.loads(row[4]) if row[4] else None # Assuming it's JSON string
                except: # If not JSON, treat as raw text
                    if isinstance(row[4], str):
                        judgment["ai_recommended_strategy"] = {"name": row[4][:50], "description": row[4]} # Simple fallback
                    else:
                        judgment["ai_recommended_strategy"] = {"raw_text": str(row[4])}

                try: judgment["key_warnings"] = json.loads(row[7]) if row[7] else []
                except: judgment["key_warnings"] = [{"error": "failed to parse key_warnings_json", "raw": str(row[7])}]
                judgments_for_day.append(judgment)
                if row[8] and row[9]: feature_periods_to_fetch.add((row[8], row[9]))

            logger.info(f"找到 {len(judgments_for_day)} 條 AI 判斷。")
            if feature_periods_to_fetch:
                conditions = []
                params_features = []
                for fp_start, fp_sym in feature_periods_to_fetch:
                    conditions.append("(timestamp_period_start_utc = ? AND symbol = ?)")
                    params_features.extend([fp_start, fp_sym])
                if conditions:
                    query_features = f"SELECT * FROM processed_features_hourly WHERE {' OR '.join(conditions)} ORDER BY timestamp_period_start_utc ASC, symbol ASC;"
                    raw_features = self.db_manager.execute_query(query_features, params_features)
                    if raw_features:
                        logger.info(f"為相關判斷找到了 {len(raw_features)} 條已處理特徵記錄。(詳細提取待實現)")
                        # TODO: Convert raw_features (list of tuples) to list of dicts for features_for_day
                        # conn = self.db_manager.get_connection(read_only=True)
                        # temp_cursor = conn.cursor()
                        # temp_cursor.execute(f"SELECT * FROM processed_features_hourly LIMIT 0;")
                        # feature_col_names = [desc[0] for desc in temp_cursor.description]
                        # temp_cursor.close()
                        # for row_feat in raw_features:
                        #     features_for_day.append(dict(zip(feature_col_names, row_feat)))
        return judgments_for_day, features_for_day

    def _format_judgments_for_prompt(self, judgments: list[dict]) -> str:
        if not judgments: return "當日無 AI 歷史決策記錄。\n"
        output = "當日 AI 歷史決策摘要：\n"
        for i, j in enumerate(judgments):
            output += f"  決策 {i+1} ({j.get('symbol_judged', 'N/A')} @ {j.get('judgment_timestamp_utc')}):\n"
            output += f"    - 分類: {j.get('ai_decision_category', '未提供')}\n"
            confidence = j.get('confidence_score', 'N/A')
            confidence_str = f"{confidence:.2f}" if isinstance(confidence, float) else str(confidence)
            output += f"    - 信心: {confidence_str}\n"
            strategy = j.get('ai_recommended_strategy')
            strat_name = "未提供"
            if isinstance(strategy, dict): strat_name = strategy.get('name', '未提供')
            elif isinstance(strategy, str): strat_name = strategy[:50]
            output += f"    - 策略建議: {strat_name}\n"
            output += f"    - 核心理由 (節選): {j.get('ai_rationale_text', '')[:150]}...\n"
        return output + "\n"

    def _format_features_for_prompt(self, features: list[dict]) -> str:
        if not features: return "當日關鍵量化特徵數據 (摘要)：\n  (詳細特徵數據提取和格式化待實現。AI應參考原始判斷中的market_briefing_json獲取特徵。)\n"
        # TODO: Implement detailed feature formatting if features_for_day is populated meaningfully
        output = "當日部分相關量化特徵數據 (摘要)：\n"
        for i, f_dict in enumerate(features[:3]): # Display max 3 feature sets for brevity
             output += f"  特徵組 {i+1} (時間: {f_dict.get('timestamp_period_start_utc')}, 標的: {f_dict.get('symbol')}):\n"
             output += f"      收盤價: {f_dict.get('price_close', 'N/A')}, RSI(14): {f_dict.get('rsi_14', 'N/A')}\n" # Example
        return output + "\n"


    def generate_daily_report_prompt(self, report_target_date: date, judgments: list[dict], features: list[dict]) -> str:
        date_str = report_target_date.isoformat()
        prompt = f"""你是一位資深的金融市場分析師，負責撰寫每日市場回顧報告。今天的日期是 {date_str} (UTC)。
請基於以下提供的當日 AI 歷史決策摘要和相關市場特徵數據，生成一份簡潔、專業、結構化的每日市場報告 (Markdown 格式)。

報告應包含以下部分：
1.  **市場總體概述**: 對當日市場主要趨勢、情緒和重要事件的簡要總結。
2.  **關鍵 AI 決策點評**: 選擇1-2個當日最重要的 AI 決策進行點評，分析其合理性、潛在影響以及從中可以學到的東西。引用 AI 的核心理由。
3.  **主要量化特徵表現**: 提及當日幾個關鍵量化指標的表現或顯著變化，及其可能的市場含義。(如果下方特徵摘要不完整，請基於AI決策中的市場簡報自行判斷)
4.  **風險與展望**: 總結當日浮現的主要風險因素，並對下一交易日或短期未來進行簡要展望。

請確保報告客觀、基於數據，並具有洞察力。避免使用過於情緒化或投機性的語言。

--- 當日數據摘要開始 ---

{self._format_judgments_for_prompt(judgments)}
{self._format_features_for_prompt(features)}

--- 當日數據摘要結束 ---

請現在生成你的每日市場報告 (Markdown 格式):
"""
        return prompt

    def generate_report(self, report_target_date: date, symbol_for_context: str = None) -> str | None:
        logger.info(f"開始為日期 {report_target_date.isoformat()} 生成每日報告...")
        judgments, features = self._fetch_daily_judgments_and_features(report_target_date)
        if not judgments:
            logger.warning(f"日期 {report_target_date.isoformat()} 沒有找到 AI 判斷數據，無法生成報告。")
            return None

        prompt_for_report = self.generate_daily_report_prompt(report_target_date, judgments, features)
        logger.debug(f"每日報告 Prompt (前500字符):\n{prompt_for_report[:500]}...")

        report_generation_options = {"temperature": 0.5, "num_predict": 1024} # num_predict or other param for length
        logger.info(f"向 LLM 發送每日報告生成 Prompt (日期: {report_target_date.isoformat()})...")
        llm_output = self.llama_agent.send_prompt_to_ollama(prompt_for_report, generation_options=report_generation_options)

        markdown_report = None
        if llm_output:
            if "error" in llm_output and "raw_response_field" in llm_output:
                markdown_report = llm_output["raw_response_field"]
                logger.info(f"成功從 LLM 獲取到每日報告的原始文本 (日期: {report_target_date.isoformat()})。")
            # Check if llm_output itself is the response if not an error dict from LlamaOllamaAgent
            elif isinstance(llm_output, dict) and "response" in llm_output and isinstance(llm_output["response"], str):
                 markdown_report = llm_output["response"]
                 logger.info(f"成功從 LLM 獲取到每日報告文本 (日期: {report_target_date.isoformat()}) (來自 response 字段)。")
            elif isinstance(llm_output, str): # If LlamaOllamaAgent was changed to return string directly
                 markdown_report = llm_output
                 logger.info(f"成功從 LLM 獲取到每日報告文本 (日期: {report_target_date.isoformat()}) (直接字符串)。")
            else:
                logger.error(f"LLM 返回的數據格式非預期，無法提取報告文本。Output: {str(llm_output)[:500]}")
        else:
            logger.error(f"LLM 未能為日期 {report_target_date.isoformat()} 生成報告。")
            return None

        if markdown_report:
            logger.debug(f"生成的每日報告 (Markdown):\n{markdown_report[:1000]}...")
            self._save_report_to_db(report_target_date, markdown_report, judgments)
            return markdown_report
        return None

    def _save_report_to_db(self, report_date_obj: date, report_text: str, source_judgments: list[dict]):
        date_str = report_date_obj.isoformat()
        judgment_ids = [j["id"] for j in source_judgments if "id" in j]

        sql = """
        INSERT OR REPLACE INTO daily_reports_log
        (report_date, report_text_markdown, source_judgment_ids_json, source_feature_day_utc, ai_model_name_reporter)
        VALUES (?, ?, ?, ?, ?);
        """
        params = [
            date_str, report_text,
            json.dumps(judgment_ids) if judgment_ids else None,
            date_str, self.llama_agent.model_name
        ]
        if self.db_manager.execute_modification(sql, params):
            logger.info(f"日期 {date_str} 的每日報告已成功存儲到數據庫。")
        else:
            logger.error(f"存儲日期 {date_str} 的每日報告到數據庫失敗。")

if __name__ == "__main__":
    import logging # Ensure logging is imported for __main__
    from unittest.mock import MagicMock # For mocking in __main__

    # Setup a minimal logger for __main__ if not already configured by a higher-level script
    if not logging.getLogger(PROJECT_LOGGER_NAME).hasHandlers(): # Check if already configured
        setup_logger(PROJECT_LOGGER_NAME, level="DEBUG")

    logger.info("--- DailyReporter (__main__) 測試開始 (使用 mock) ---")

    mock_db_manager = MagicMock(spec=DatabaseManager)
    mock_llama_agent = MagicMock(spec=LlamaOllamaAgent)
    mock_llama_agent.model_name = "mock_reporter_model_main"

    mock_judgment_row_main = (
        101, "2023-11-01T10:00:00.000Z", json.dumps({"info": "main_brief"}), "強力看多",
        json.dumps({"strat_name": "buy_calls"}), "Main Rationale", 0.9, json.dumps(["main_risk"]),
        "2023-11-01T00:00:00.000Z", "TSLA"
    )
    mock_db_manager.execute_query.side_effect = [
        [mock_judgment_row_main],
        []
    ]
    mock_db_manager.execute_modification.return_value = True

    main_mock_report_text = "# 2023-11-01 TSLA 市場報告\n一切都很好。"
    main_mock_ollama_response = {
        "error": "LLM output is not valid JSON",
        "raw_response_field": main_mock_report_text,
    }
    mock_llama_agent.send_prompt_to_ollama.return_value = main_mock_ollama_response

    reporter_main = DailyReporter(db_manager=mock_db_manager, llama_agent=mock_llama_agent)
    report_date_main = date(2023, 11, 1)

    final_report = reporter_main.generate_report(report_date_main)

    if final_report:
        print("\n--- __main__ 生成的每日報告 ---")
        print(final_report)
        assert final_report == main_mock_report_text
        mock_db_manager.execute_modification.assert_called_once()
        print("DailyReporter __main__ 測試基本通過。")
    else:
        print("DailyReporter __main__ 測試生成報告失敗。")

    logger.info("--- DailyReporter (__main__) 測試結束 ---")
