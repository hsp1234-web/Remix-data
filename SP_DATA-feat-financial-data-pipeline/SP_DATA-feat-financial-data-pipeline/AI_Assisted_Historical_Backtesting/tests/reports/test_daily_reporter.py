import unittest
import os
import sys
import json
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

# --- Path Correction for imports ---
current_script_dir_test = os.path.dirname(os.path.abspath(__file__)) # .../tests/reports
tests_dir = os.path.dirname(current_script_dir_test) # .../tests
project_root_dir_test = os.path.dirname(tests_dir) # AI_Assisted_Historical_Backtesting
project_root_parent_dir_test = os.path.dirname(project_root_dir_test) # Parent of AI_Assisted_Historical_Backtesting

if project_root_parent_dir_test not in sys.path:
    sys.path.insert(0, project_root_parent_dir_test)
# --- Path Correction End ---

from AI_Assisted_Historical_Backtesting.src.reports.daily_reporter import DailyReporter
from AI_Assisted_Historical_Backtesting.src.database.db_manager import DatabaseManager # For type hinting and mock spec
from AI_Assisted_Historical_Backtesting.src.ai_logic.llama_agent import LlamaOllamaAgent # For type hinting and mock spec
# from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME

# setup_logger(PROJECT_LOGGER_NAME, level="DEBUG") # Optional: for verbose test logging

class TestDailyReporter(unittest.TestCase):

    def setUp(self):
        self.mock_db_manager = MagicMock(spec=DatabaseManager)
        self.mock_llama_agent = MagicMock(spec=LlamaOllamaAgent)
        self.mock_llama_agent.model_name = "test_reporter_llm" # Set a model name for the mock

        self.reporter = DailyReporter(db_manager=self.mock_db_manager, llama_agent=self.mock_llama_agent)
        self.test_date = date(2023, 10, 26)

    def test_fetch_daily_judgments_and_features_no_data(self):
        self.mock_db_manager.execute_query.return_value = [] # Simulate no judgments found

        judgments, features = self.reporter._fetch_daily_judgments_and_features(self.test_date)

        self.assertEqual(judgments, [])
        self.assertEqual(features, []) # Should also be empty if no judgments led to feature lookups
        self.mock_db_manager.execute_query.assert_called_once() # Should be called for judgments

    def test_fetch_daily_judgments_and_features_with_data(self):
        mock_judgment_row1 = (
            1, "2023-10-26T08:00:00.000Z", json.dumps({"info": "brief1"}), "看多",
            json.dumps({"strat": "buy"}), "Rationale 1", 0.8, json.dumps(["risk1"]),
            "2023-10-26T00:00:00.000Z", "AAPL"
        )
        mock_judgment_row2 = (
            2, "2023-10-26T20:00:00.000Z", json.dumps({"info": "brief2"}), "中性",
            "Raw strategy text", "Rationale 2", 0.5, "[\"risk2\"]", # Test slightly different JSON str
            "2023-10-26T12:00:00.000Z", "AAPL"
        )
        self.mock_db_manager.execute_query.side_effect = [
            [mock_judgment_row1, mock_judgment_row2], # First call for judgments
            [] # Second call for features (simulating no specific features found or simplified logic for now)
            # TODO: If _fetch_daily_judgments_and_features is updated to return detailed features,
            #       this mock for the second call needs to be updated to return mock feature rows.
        ]

        judgments, features = self.reporter._fetch_daily_judgments_and_features(self.test_date)

        self.assertEqual(len(judgments), 2)
        self.assertEqual(judgments[0]["id"], 1)
        self.assertEqual(judgments[1]["ai_decision_category"], "中性")
        self.assertIsInstance(judgments[0]["market_briefing"], dict)
        self.assertIsInstance(judgments[0]["ai_recommended_strategy"], dict) # Parsed from JSON
        self.assertIsInstance(judgments[1]["ai_recommended_strategy"], dict) # Parsed from raw text to {"raw_text": ...}
        self.assertIsInstance(judgments[0]["key_warnings"], list)

        # Check that execute_query was called twice (once for judgments, once for features)
        # The feature query might not happen if no feature_periods_to_fetch are found,
        # or if the logic is simple.
        # Based on current _fetch_daily_judgments_and_features, if feature_periods_to_fetch is populated,
        # a second query is made.
        self.assertEqual(self.mock_db_manager.execute_query.call_count, 2)
        # (If no features were queried, this would be 1. We assume it queries features)


    def test_format_judgments_for_prompt(self):
        mock_judgments = [
            {"symbol_judged": "AAPL", "judgment_timestamp_utc": "2023-10-26T08:00Z",
             "ai_decision_category": "看多", "confidence_score": 0.8,
             "ai_recommended_strategy": {"name": "買入"}, "ai_rationale_text": "價格突破"},
            {"symbol_judged": "MSFT", "judgment_timestamp_utc": "2023-10-26T10:00Z",
             "ai_decision_category": "看空", "confidence_score": 0.65,
             "ai_recommended_strategy": "賣出現貨", "ai_rationale_text": "跌破支撐位"}
        ]
        formatted_text = self.reporter._format_judgments_for_prompt(mock_judgments)
        self.assertIn("AAPL @ 2023-10-26T08:00Z", formatted_text)
        self.assertIn("分類: 看多", formatted_text)
        self.assertIn("信心: 0.80", formatted_text)
        self.assertIn("策略建議: 買入", formatted_text)
        self.assertIn("MSFT @ 2023-10-26T10:00Z", formatted_text)
        self.assertIn("分類: 看空", formatted_text)
        self.assertIn("策略建議: 賣出現貨", formatted_text) # Test raw string strategy

    def test_format_features_for_prompt_placeholder(self):
        # Current _format_features_for_prompt returns a placeholder if features list is empty
        formatted_text_empty = self.reporter._format_features_for_prompt([])
        expected_placeholder_text = "當日關鍵量化特徵數據 (摘要)：\n  (詳細特徵數據提取和格式化待實現。AI應參考原始判斷中的market_briefing_json獲取特徵。)\n"
        self.assertEqual(formatted_text_empty, expected_placeholder_text)

        # If it were to format actual features (future enhancement)
        mock_features = [{"symbol":"AAPL", "timestamp_period_start_utc":"2023-10-26T00Z", "price_close":150, "rsi_14":55}]
        formatted_text_with_data = self.reporter._format_features_for_prompt(mock_features)
        self.assertIn("標的: AAPL", formatted_text_with_data)
        self.assertIn("時間: 2023-10-26T00Z", formatted_text_with_data)
        self.assertIn("收盤價: 150", formatted_text_with_data)
        self.assertIn("RSI(14): 55", formatted_text_with_data)


    def test_generate_daily_report_prompt(self):
        prompt = self.reporter.generate_daily_report_prompt(self.test_date, [], [])
        self.assertIn(self.test_date.isoformat(), prompt)
        self.assertIn("市場總體概述", prompt)
        self.assertIn("關鍵 AI 決策點評", prompt)
        self.assertIn("Markdown 格式", prompt)

    @patch.object(DailyReporter, '_fetch_daily_judgments_and_features')
    @patch.object(DailyReporter, '_save_report_to_db')
    def test_generate_report_success(self, mock_save_db, mock_fetch_data):
        mock_judgments = [{"id": 1, "ai_rationale_text": "Test Rationale"}]
        mock_features = [{"feature": "value"}] # Simplified
        mock_fetch_data.return_value = (mock_judgments, mock_features)

        expected_report_markdown = "# 每日報告\n市場良好。"
        # Simulate LlamaOllamaAgent.send_prompt_to_ollama returning the expected structure
        # when the LLM output is not JSON (as per DailyReporter's expectation for markdown)
        self.mock_llama_agent.send_prompt_to_ollama.return_value = {
            "error": "LLM output is not valid JSON", # This is expected by DailyReporter
            "raw_response_field": expected_report_markdown,
            "ollama_full_response": {"response": expected_report_markdown, "done": True}
        }

        report = self.reporter.generate_report(self.test_date)

        self.assertEqual(report, expected_report_markdown)
        mock_fetch_data.assert_called_once_with(self.test_date)
        self.mock_llama_agent.send_prompt_to_ollama.assert_called_once()
        mock_save_db.assert_called_once_with(self.test_date, expected_report_markdown, mock_judgments)

    @patch.object(DailyReporter, '_fetch_daily_judgments_and_features')
    def test_generate_report_no_judgments(self, mock_fetch_data):
        mock_fetch_data.return_value = ([], []) # No judgments
        report = self.reporter.generate_report(self.test_date)
        self.assertIsNone(report)
        self.mock_llama_agent.send_prompt_to_ollama.assert_not_called()

    @patch.object(DailyReporter, '_fetch_daily_judgments_and_features')
    def test_generate_report_llm_fails(self, mock_fetch_data):
        mock_fetch_data.return_value = ([{"id": 1}], [])
        self.mock_llama_agent.send_prompt_to_ollama.return_value = None # LLM call fails

        report = self.reporter.generate_report(self.test_date)
        self.assertIsNone(report)

    def test_save_report_to_db(self):
        report_text = "# Test Report"
        mock_judgments = [{"id": 1}, {"id": 2}]
        expected_judgment_ids_json = json.dumps([1, 2])

        self.mock_db_manager.execute_modification.return_value = True # Simulate successful DB write

        self.reporter._save_report_to_db(self.test_date, report_text, mock_judgments)

        self.mock_db_manager.execute_modification.assert_called_once()
        call_args = self.mock_db_manager.execute_modification.call_args[0]
        self.assertIn("INSERT OR REPLACE INTO daily_reports_log", call_args[0]) # Check query
        # Check params: (report_date, report_text_markdown, source_judgment_ids_json, source_feature_day_utc, ai_model_name_reporter)
        self.assertEqual(call_args[1][0], self.test_date.isoformat())
        self.assertEqual(call_args[1][1], report_text)
        self.assertEqual(call_args[1][2], expected_judgment_ids_json)
        self.assertEqual(call_args[1][3], self.test_date.isoformat()) # source_feature_day_utc
        self.assertEqual(call_args[1][4], self.mock_llama_agent.model_name)


if __name__ == '__main__':
    unittest.main(verbosity=2)
