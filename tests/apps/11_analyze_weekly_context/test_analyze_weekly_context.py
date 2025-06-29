import unittest
from unittest.mock import patch, mock_open
import os
import shutil
import sys
import json

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from apps.x11_analyze_weekly_context.run import main as analyze_main
from apps.x11_analyze_weekly_context.run import DEFAULT_REPORTS_DIR, PROMPT_TEMPLATE

# Mocked Gemini Response object
class MockGeminiPart:
    def __init__(self, text):
        self.text = text

class MockGeminiResponse:
    def __init__(self, text_content, blocked=False, block_reason=None):
        if blocked:
            self.parts = [] # No parts if blocked by safety
            self.prompt_feedback = unittest.mock.Mock()
            self.prompt_feedback.block_reason = block_reason
            # Simulate safety_ratings if needed for more detailed testing
            self.prompt_feedback.safety_ratings = []
        else:
            self.parts = [MockGeminiPart(text_content)]
            self.prompt_feedback = unittest.mock.Mock()
            self.prompt_feedback.block_reason = None # Explicitly None if not blocked

    # The SUT script uses response.parts directly if response.text is not available or empty
    # and checks prompt_feedback.block_reason. This mock structure supports that.


class TestAnalyzeWeeklyContext(unittest.TestCase):

    def setUp(self):
        self.fixture_dir = os.path.join(PROJECT_ROOT, "tests", "fixtures")
        self.base_test_output_dir = os.path.join(PROJECT_ROOT, "tests", "test_outputs")
        os.makedirs(self.base_test_output_dir, exist_ok=True)

        self.test_reports_dir = os.path.join(self.base_test_output_dir, "gold", "analysis_reports_11")
        os.makedirs(self.test_reports_dir, exist_ok=True)

        self.sample_package_path = os.path.join(self.fixture_dir, "sample_analysis_package.json")

        if not os.path.exists(self.sample_package_path):
            self.fail(f"Fixture sample_analysis_package.json not found at {self.sample_package_path}")

        self.original_gemini_api_key = os.environ.get("GEMINI_API_KEY")
        os.environ["GEMINI_API_KEY"] = "test_api_key_for_mocking"

    def tearDown(self):
        if os.path.exists(self.test_reports_dir):
            shutil.rmtree(self.test_reports_dir)

        # More careful cleanup of base_test_output_dir
        # Only remove if it's empty AFTER specific test dirs are gone.
        # This can be tricky if tests run in parallel or leave unexpected files.
        # A common approach is for each test class to manage its own unique subdirectories within base_test_output_dir.
        sub_dirs_to_check = [
            os.path.join(self.base_test_output_dir, "bronze"),
            os.path.join(self.base_test_output_dir, "silver"),
            os.path.join(self.base_test_output_dir, "gold"),
            os.path.join(self.base_test_output_dir, "event_bus")
        ]
        all_empty = True
        for sub_dir_outer in sub_dirs_to_check:
            if os.path.exists(sub_dir_outer):
                # Check if subdirectories like 'taifex_01', 'analysis_packages_10' are empty or non-existent
                inner_cleaned = True
                for item in os.listdir(sub_dir_outer): # e.g., item = 'taifex_01'
                    item_path = os.path.join(sub_dir_outer, item)
                    if os.path.isdir(item_path) and item.endswith(('_00', '_01', '_02', '_03', '_10', '_11')): # Specific test dirs
                        if len(os.listdir(item_path)) == 0: # If specific test output dir is empty
                            try: shutil.rmtree(item_path)
                            except OSError: pass # ignore if fails
                        else: # Not empty, so base is not fully cleaned by this test
                            inner_cleaned = False
                            all_empty = False
                            break
                    elif os.path.isfile(item_path) and item.startswith('test_financial_data_'): # Specific test DBs
                        # These should have been removed by their respective test classes' tearDown
                        pass # If they exist, it's a leak from another test or this one.
                    else: # Unrecognized item, assume base is not clean
                        all_empty = False
                        break
                if not inner_cleaned:
                    break
            # If sub_dir_outer itself is now empty after cleaning its specific test subdirs
            if os.path.exists(sub_dir_outer) and not os.listdir(sub_dir_outer):
                try: shutil.rmtree(sub_dir_outer)
                except OSError: pass
            elif os.path.exists(sub_dir_outer): # Still contains items
                all_empty = False


        if os.path.exists(self.base_test_output_dir) and all_empty and not os.listdir(self.base_test_output_dir) :
            try:
                shutil.rmtree(self.base_test_output_dir)
            except OSError:
                pass # Ignore if it fails (e.g. other test files still there)

        if self.original_gemini_api_key is None:
            if "GEMINI_API_KEY" in os.environ:
                del os.environ["GEMINI_API_KEY"]
        else:
            os.environ["GEMINI_API_KEY"] = self.original_gemini_api_key


    @patch('apps.x11_analyze_weekly_context.run.genai.GenerativeModel')
    def test_analyze_main_execution_success(self, mock_generative_model):
        mock_model_instance = mock_generative_model.return_value
        mock_model_instance.generate_content.return_value = MockGeminiResponse("Mocked AI Report Content.")

        original_argv = sys.argv
        sys.argv = [
            "apps/11_analyze_weekly_context/run.py",
            "--package-path", self.sample_package_path,
            "--reports-dir", self.test_reports_dir,
            "--gemini-model", "mocked-gemini-model"
        ]

        try:
            analyze_main()
        except SystemExit as e:
            self.fail(f"Analyze script exited prematurely: {e}")
        finally:
            sys.argv = original_argv

        self.assertTrue(mock_model_instance.generate_content.called)
        called_prompt = mock_model_instance.generate_content.call_args[0][0]
        self.assertIn("您是一位頂尖的金融市場分析師。", called_prompt)
        self.assertIn("目標週: 2023-W10", called_prompt)
        self.assertIn("Content: 目標週看起來不錯，繼續持有。\n[cite: post_tw_1]", called_prompt)

        package_data = json.load(open(self.sample_package_path, 'r'))
        target_week_id = package_data.get("target_week_id")
        expected_report_filename = f"{target_week_id}_AnalysisReport.txt"
        expected_report_path = os.path.join(self.test_reports_dir, expected_report_filename)

        self.assertTrue(os.path.exists(expected_report_path))
        with open(expected_report_path, 'r', encoding='utf-8') as f:
            report_content = f.read()
        self.assertEqual(report_content, "Mocked AI Report Content.")


    @patch('apps.x11_analyze_weekly_context.run.genai.GenerativeModel')
    def test_analyze_api_key_missing(self, mock_generative_model):
        if "GEMINI_API_KEY" in os.environ:
            del os.environ["GEMINI_API_KEY"]

        original_argv = sys.argv
        sys.argv = [
            "apps/11_analyze_weekly_context/run.py",
            "--package-path", self.sample_package_path,
            "--reports-dir", self.test_reports_dir
        ]

        analyze_main()

        self.assertFalse(mock_generative_model.return_value.generate_content.called)

        package_data = json.load(open(self.sample_package_path, 'r'))
        target_week_id = package_data.get("target_week_id")
        expected_report_filename = f"{target_week_id}_AnalysisReport.txt"
        expected_report_path = os.path.join(self.test_reports_dir, expected_report_filename)
        # The script should not create a report if API key is missing, as it returns early.
        self.assertFalse(os.path.exists(expected_report_path))


    @patch('apps.x11_analyze_weekly_context.run.genai.GenerativeModel')
    def test_analyze_api_call_blocked(self, mock_generative_model):
        mock_model_instance = mock_generative_model.return_value
        mock_model_instance.generate_content.return_value = MockGeminiResponse(
            text_content=None, # No text content when blocked
            blocked=True,
            block_reason="SAFETY"
        )

        original_argv = sys.argv
        sys.argv = [
            "apps/11_analyze_weekly_context/run.py",
            "--package-path", self.sample_package_path,
            "--reports-dir", self.test_reports_dir
        ]

        analyze_main()

        package_data = json.load(open(self.sample_package_path, 'r'))
        target_week_id = package_data.get("target_week_id")
        expected_report_filename = f"{target_week_id}_AnalysisReport.txt"
        expected_report_path = os.path.join(self.test_reports_dir, expected_report_filename)

        self.assertTrue(os.path.exists(expected_report_path))
        with open(expected_report_path, 'r', encoding='utf-8') as f:
            report_content = f.read()
        self.assertIn("Error: Gemini API call blocked. Reason: SAFETY", report_content)

if __name__ == '__main__':
    unittest.main()
