import unittest
import os
import pandas as pd
import shutil
import sys
import duckdb
import json
from datetime import date, datetime

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from apps.x10_create_weekly_context.run import main as create_context_main
from apps.x10_create_weekly_context.run import (
    SILVER_TAIFEX_TABLE,
    GOLD_WEEKLY_SUMMARY_TABLE
)

class TestCreateWeeklyContext(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")
        os.makedirs(self.base_test_output_dir, exist_ok=True)

        self.test_duckdb_file = os.path.join(self.base_test_output_dir, "test_financial_data_10.duckdb")
        self.temp_social_posts_bronze_file = os.path.join(self.base_test_output_dir, "temp_threads_posts_10.parquet")
        self.temp_analysis_packages_dir = os.path.join(self.base_test_output_dir, "silver", "analysis_packages_10")
        self.temp_event_queue_dir = os.path.join(self.base_test_output_dir, "event_bus", "queue_10")

        os.makedirs(self.temp_analysis_packages_dir, exist_ok=True)
        os.makedirs(self.temp_event_queue_dir, exist_ok=True)

        con = None
        try:
            con = duckdb.connect(database=self.test_duckdb_file, read_only=False)
            con.execute(f"DROP TABLE IF EXISTS {SILVER_TAIFEX_TABLE}")
            con.execute(f"""
                CREATE TABLE {SILVER_TAIFEX_TABLE} (
                    trade_date DATE, contract VARCHAR, expiry_month_week VARCHAR,
                    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT
                )
            """)
            silver_data = [
                (date(2023, 1, 9), 'TX', '202302', 15200.0, 15250.0, 15180.0, 15220.0, 48000),
                (date(2023, 1, 10), 'TX', '202302', 15230.0, 15300.0, 15210.0, 15280.0, 51000),
            ]
            silver_df = pd.DataFrame(silver_data, columns=["trade_date", "contract", "expiry_month_week", "open", "high", "low", "close", "volume"])
            con.append(SILVER_TAIFEX_TABLE, silver_df)

            con.execute(f"DROP TABLE IF EXISTS {GOLD_WEEKLY_SUMMARY_TABLE}")
            con.execute(f"""
                CREATE TABLE {GOLD_WEEKLY_SUMMARY_TABLE} (
                    week_id VARCHAR PRIMARY KEY, contract_group VARCHAR,
                    weekly_open DOUBLE, weekly_high DOUBLE, weekly_low DOUBLE, weekly_close DOUBLE,
                    total_weekly_volume BIGINT, avg_pc_ratio DOUBLE
                )
            """)
            # Provide enough gold data to cover the 9-week window around 2023-W02
            # Target: 2023-W02. Window: 2022-W51, 2022-W52, 2023-W01, [2023-W02], 2023-W03, 2023-W04, 2023-W05, 2023-W06, 2023-W07
            # For simplicity, I'll ensure at least the direct neighbors are there.
            gold_data = [
                ('2022-W51', 'TX', 14000.0, 14100.0, 13900.0, 14050.0, 100000, 0.7),
                ('2022-W52', 'TX', 14050.0, 14150.0, 13950.0, 14100.0, 110000, 0.75),
                ('2023-W01', 'TX', 15000.0, 15180.0, 14950.0, 15100.0, 157000, 0.85),
                ('2023-W03', 'TX', 15300.0, 15400.0, 15250.0, 15350.0, 160000, 0.90),
                ('2023-W04', 'TX', 15350.0, 15450.0, 15300.0, 15400.0, 120000, 0.8),
                ('2023-W05', 'TX', 15400.0, 15500.0, 15350.0, 15450.0, 130000, 0.82),
            ]
            gold_df = pd.DataFrame(gold_data, columns=["week_id", "contract_group", "weekly_open", "weekly_high", "weekly_low", "weekly_close", "total_weekly_volume", "avg_pc_ratio"])
            con.append(GOLD_WEEKLY_SUMMARY_TABLE, gold_df)

        except Exception as e:
            self.fail(f"setUp failed to prepare test DuckDB: {e}")
        finally:
            if con:
                con.close()

        social_posts_data = [
            {'post_date': datetime(2023, 1, 9, 10, 0, 0), 'author': 'UserA', 'content': '目標週第一則貼文，看多。'}, # 2023-W02
            {'post_date': datetime(2023, 1, 10, 14, 0, 0), 'author': 'UserB', 'content': '目標週第二則，市場震盪。'}, # 2023-W02
            {'post_date': datetime(2023, 1, 2, 11, 0, 0), 'author': 'UserC', 'content': '背景週W01貼文，新年快樂！'}, # 2023-W01
            {'post_date': datetime(2023, 1, 3, 15, 0, 0), 'author': 'UserD', 'content': '背景週W01，有點意思。'}, # 2023-W01
            {'post_date': datetime(2023, 1, 16, 9, 0, 0), 'author': 'UserE', 'content': '背景週W03，觀察看看。'}, # 2023-W03
        ]
        social_df = pd.DataFrame(social_posts_data)
        try:
            social_df.to_parquet(self.temp_social_posts_bronze_file, index=False)
        except Exception as e:
            self.fail(f"setUp failed to create temp social posts Parquet: {e}")


    def tearDown(self):
        if os.path.exists(self.test_duckdb_file):
            try: os.remove(self.test_duckdb_file)
            except OSError: pass
        if os.path.exists(self.temp_social_posts_bronze_file):
            try: os.remove(self.temp_social_posts_bronze_file)
            except OSError: pass
        if os.path.exists(self.temp_analysis_packages_dir):
            try: shutil.rmtree(self.temp_analysis_packages_dir)
            except OSError: pass
        if os.path.exists(self.temp_event_queue_dir):
            try: shutil.rmtree(self.temp_event_queue_dir)
            except OSError: pass
        if os.path.exists(self.base_test_output_dir) and not os.listdir(self.base_test_output_dir):
            try: shutil.rmtree(self.base_test_output_dir)
            except OSError: pass


    def test_create_context_main_execution(self):
        target_week = "2023-W02"

        original_argv = sys.argv
        sys.argv = [
            "apps/10_create_weekly_context/run.py",
            "--target-week-id", target_week,
            "--duckdb-file", self.test_duckdb_file,
            "--social-posts-bronze-file", self.temp_social_posts_bronze_file,
            "--analysis-packages-dir", self.temp_analysis_packages_dir,
            "--event-queue-dir", self.temp_event_queue_dir
        ]

        try:
            create_context_main()
        except SystemExit as e:
            self.fail(f"Create context script exited prematurely: {e}")
        finally:
            sys.argv = original_argv

        expected_package_filename = f"{target_week}_AnalysisPackage.json"
        expected_package_path = os.path.join(self.temp_analysis_packages_dir, expected_package_filename)
        self.assertTrue(os.path.exists(expected_package_path),
                        f"Analysis package JSON should be created at {expected_package_path}")

        with open(expected_package_path, 'r', encoding='utf-8') as f:
            package_data = json.load(f)

        self.assertEqual(package_data["target_week_id"], target_week)
        self.assertIn("analysis_window", package_data)

        # Check that 9 weeks are in the window_ids (or whatever the script actually generates based on data)
        # The script's get_analysis_window_weeks should always return 9 weeks if date logic is correct.
        self.assertEqual(len(package_data["analysis_window"]["week_ids_in_window"]), 9,
                        f"Expected 9 weeks in analysis window, got {len(package_data['analysis_window']['week_ids_in_window'])}")

        self.assertIn("context_window_summary", package_data)
        # We expect summaries for background weeks. Our gold data covers W01, W03, W04, W05, W51, W52.
        # The script iterates through all 8 background weeks; if no gold data, it still creates an entry.
        self.assertEqual(len(package_data["context_window_summary"]["weekly_summaries"]), 8,
                         f"Expected 8 background week summaries, got {len(package_data['context_window_summary']['weekly_summaries'])}")

        summary_w01 = next((s for s in package_data["context_window_summary"]["weekly_summaries"] if s["week_id"] == "2023-W01"), None)
        self.assertIsNotNone(summary_w01, "Summary for 2023-W01 should exist")
        self.assertEqual(summary_w01["close_price"], 15100.0)
        self.assertEqual(summary_w01["post_count"], 2) # Two posts in 2023-W01
        self.assertIsNotNone(summary_w01["sentiment_score"])
        self.assertTrue(len(summary_w01["top_keywords"]) > 0)


        self.assertIn("target_week_detail", package_data)
        target_detail = package_data["target_week_detail"]
        self.assertEqual(len(target_detail["daily_market_data"]), 2)
        self.assertIn("cite_id", target_detail["daily_market_data"][0])

        self.assertEqual(len(target_detail["full_text_posts"]), 2)
        self.assertIn("cite_id", target_detail["full_text_posts"][0])
        self.assertIn("看多", target_detail["full_text_posts"][0]["content"])

        queued_files = os.listdir(self.temp_event_queue_dir)
        self.assertEqual(len(queued_files), 1)

        next_task_path = os.path.join(self.temp_event_queue_dir, queued_files[0])
        with open(next_task_path, 'r', encoding='utf-8') as f:
            next_task_data = json.load(f)

        self.assertEqual(next_task_data["app_name"], "11_analyze_weekly_context")
        self.assertEqual(next_task_data["params"]["package_path"], expected_package_path)

if __name__ == '__main__':
    unittest.main()
