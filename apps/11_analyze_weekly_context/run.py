import os
import json
import argparse
import logging
from datetime import datetime
# Import the Gemini library
try:
    import google.generativeai as genai
except ImportError:
    logging.warning("google.generativeai library not found. Please install it: pip install google-generativeai")
    genai = None # Set to None if import fails

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_REPORTS_DIR = "data/gold/analysis_reports"
# It's better to get API key from environment variables or a secure config
# For Colab, use Colab Secrets to set GEMINI_API_KEY
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
DEFAULT_GEMINI_MODEL = "gemini-1.5-flash-latest" # Or "gemini-1.5-pro-latest"

PROMPT_TEMPLATE = """\
您是一位頂尖的金融市場分析師。您的任務是針對一個【特定目標週】進行深度分析，同時參考其【前後一個月的市場背景】來提供更宏觀的視角。

您的回答必須具有可追溯性。當您引用任何具體的貼文內容或市場數據時，必須在句子末尾附上其來源的引用標記，例如 [cite: post_tw_1] 或 [cite: market_2022-07-27]。這是一項強制要求。

請使用【背景情境摘要】來理解長期趨勢，並使用【目標週詳細數據】來進行精細的日級分析，這是您分析的重點。

---
### 【背景情境摘要 (前後一個月)】
分析時間窗口: {analysis_window_start_date} 到 {analysis_window_end_date}
市場趨勢概覽 (每週摘要):
{context_window_summary_formatted}

---
### 【目標週詳細數據 (分析核心)】
**目標週**: {target_week_id}

**每日市場數據**:
{target_week_daily_market_data_formatted}

**當週社群完整貼文**:
{target_week_full_text_posts_formatted}
---

### 【深度分析報告】

**1. 宏觀背景分析 (Macro Context):**
   - 根據【背景情境摘要】，在進入【目標週】之前，市場的整體趨勢和情緒是怎樣的？

**2. 目標週深度複盤 (Target Week Deep Dive):**
   - **逐日分析**: 結合【每日市場數據】和【當週社群完整貼文】，詳細分析目標週內每一天的關鍵走勢。例如，社群在某日的觀點 [cite: post_cite_id_example] 是如何與當時的市場價格 [cite: market_cite_id_example] 互動的？
   - **關鍵事件**: 找出本週最重要的市場事件或社群觀點，並評估其影響。

**3. 策略與洞察 (Strategy & Insights):**
   - 站在【目標週】結束的時間點，你會得出什麼交易策略或市場洞察？社群中討論的策略（如某觀點 [cite: post_cite_id_example]）是否成功？
   - 這次分析提供了什麼可供未來參考的教訓？
"""

def load_analysis_package(package_path):
    """Loads the JSON analysis package."""
    try:
        with open(package_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Analysis package not found: {package_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from package: {package_path}")
        raise

def format_context_summary(weekly_summaries):
    formatted_lines = []
    if not weekly_summaries:
        return "無背景市場趨勢數據。"
    for week_summary in weekly_summaries:
        parts = [f"- Week {week_summary.get('week_id', 'N/A')}"]
        if week_summary.get('close_price') is not None:
            parts.append(f"Close={week_summary['close_price']}")
        if week_summary.get('sentiment_score') is not None:
            parts.append(f"Sentiment={week_summary['sentiment_score']:.2f}")
        if week_summary.get('post_count') is not None:
            parts.append(f"Posts={week_summary['post_count']}")
        if week_summary.get('top_keywords'):
            parts.append(f"Keywords=[{', '.join(week_summary['top_keywords'])}]")
        formatted_lines.append(": ".join(parts))
    return "\n".join(formatted_lines) if formatted_lines else "無背景市場趨勢數據。"


def format_daily_market_data(daily_data_list):
    formatted_lines = []
    if not daily_data_list:
        return "無目標週每日市場數據。"
    for day_data in daily_data_list:
        parts = [f"- {day_data.get('date', 'N/A')}"]
        if day_data.get('close') is not None:
            parts.append(f"Close={day_data['close']}")
        if day_data.get('volume') is not None:
            parts.append(f"Volume={day_data['volume']}")
        # Add cite_id if present
        cite_id = day_data.get('cite_id')
        line = ": ".join(parts)
        if cite_id:
            line += f" [cite: {cite_id}]"
        formatted_lines.append(line)
    return "\n".join(formatted_lines) if formatted_lines else "無目標週每日市場數據。"


def format_full_text_posts(posts_list):
    formatted_posts = []
    if not posts_list:
        return "無目標週社群貼文。"
    for post in posts_list:
        post_str = "---\n"
        if post.get('post_date'):
            post_str += f"Date: {post.get('post_date')}\n"
        if post.get('author'):
            post_str += f"Author: {post.get('author')}\n"
        post_str += f"Content: {post.get('content', 'N/A')}"

        cite_id = post.get('cite_id')
        if cite_id:
            post_str += f"\n[cite: {cite_id}]\n---"
        else:
            post_str += "\n---"
        formatted_posts.append(post_str)
    return "\n".join(formatted_posts) if formatted_posts else "無目標週社群貼文。"


def call_gemini_api(prompt_text, api_key, model_name):
    """Calls the Gemini API with the provided prompt."""
    if not genai:
        logging.error("Gemini library (google.generativeai) is not available.")
        raise ImportError("Gemini library not installed.")
    if not api_key:
        logging.error("Gemini API key not provided or found in environment.")
        raise ValueError("Missing Gemini API Key.")

    genai.configure(api_key=api_key)

    generation_config = {
        "temperature": 0.5, # As per spec
        "top_p": 1.0, # Default
        "top_k": 32,  # Default
        "max_output_tokens": 8192, # Increased from 1024 as final report can be long
    }
    safety_settings = [ # Adjust as needed
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
    ]

    model = genai.GenerativeModel(model_name=model_name,
                                  generation_config=generation_config,
                                  safety_settings=safety_settings)
    try:
        logging.info(f"Calling Gemini API with model {model_name}...")
        response = model.generate_content(prompt_text)
        logging.info("Gemini API call successful.")
        # Ensure all parts are concatenated if response is chunked (though for text it's usually one part)
        full_response_text = "".join(part.text for part in response.parts) if response.parts else ""

        if not full_response_text and response.prompt_feedback and response.prompt_feedback.block_reason:
            logging.error(f"Gemini API call blocked. Reason: {response.prompt_feedback.block_reason}")
            if response.prompt_feedback.safety_ratings:
                 for rating in response.prompt_feedback.safety_ratings:
                    logging.error(f"Safety Rating: Category={rating.category}, Probability={rating.probability}")
            return f"Error: Gemini API call blocked. Reason: {response.prompt_feedback.block_reason}"

        return full_response_text

    except Exception as e:
        logging.error(f"Error calling Gemini API: {e}")
        # You might want to inspect `e` further if it's a specific Google API error type
        return f"Error: Exception during Gemini API call - {str(e)}"


def main():
    parser = argparse.ArgumentParser(description="Analyze a weekly context package using Gemini AI.")
    parser.add_argument("--package-path", type=str, required=True,
                        help="Path to the Target Week Analysis Package JSON file.")
    parser.add_argument("--reports-dir", type=str, default=DEFAULT_REPORTS_DIR,
                        help=f"Directory to save the generated analysis report. Default: {DEFAULT_REPORTS_DIR}")
    parser.add_argument("--gemini-model", type=str, default=DEFAULT_GEMINI_MODEL,
                        help=f"Gemini model to use. Default: {DEFAULT_GEMINI_MODEL}")

    args = parser.parse_args()

    logging.info(f"Starting AI analysis for package: {args.package_path}")

    if not GEMINI_API_KEY:
        logging.error("GEMINI_API_KEY environment variable not set. Cannot proceed with AI analysis.")
        # Potentially write a placeholder error report or simply exit
        # For now, let's exit if key is missing.
        return

    try:
        package_data = load_analysis_package(args.package_path)
    except Exception:
        logging.error("Failed to load analysis package. Exiting.")
        return # Or handle by creating an error report file

    # Prepare content for the prompt
    analysis_window = package_data.get("analysis_window", {})
    context_summary = package_data.get("context_window_summary", {}).get("weekly_summaries", [])
    target_week_detail = package_data.get("target_week_detail", {})

    prompt_fill_data = {
        "analysis_window_start_date": analysis_window.get("start_date", "N/A"),
        "analysis_window_end_date": analysis_window.get("end_date", "N/A"),
        "context_window_summary_formatted": format_context_summary(context_summary),
        "target_week_id": package_data.get("target_week_id", "N/A"),
        "target_week_daily_market_data_formatted": format_daily_market_data(target_week_detail.get("daily_market_data", [])),
        "target_week_full_text_posts_formatted": format_full_text_posts(target_week_detail.get("full_text_posts", []))
    }

    final_prompt = PROMPT_TEMPLATE.format(**prompt_fill_data)

    # For debugging, you might want to save the generated prompt
    # with open(os.path.join(args.reports_dir, f"{package_data.get('target_week_id', 'unknown_week')}_prompt.txt"), 'w', encoding='utf-8') as pf:
    #    pf.write(final_prompt)
    # logging.info("Saved generated prompt for debugging.")

    ai_report_content = call_gemini_api(final_prompt, GEMINI_API_KEY, args.gemini_model)

    # Save the report
    os.makedirs(args.reports_dir, exist_ok=True)
    report_filename = f"{package_data.get('target_week_id', 'unknown_week')}_AnalysisReport.txt"
    report_filepath = os.path.join(args.reports_dir, report_filename)

    try:
        with open(report_filepath, 'w', encoding='utf-8') as f:
            f.write(ai_report_content)
        logging.info(f"Successfully saved AI analysis report to: {report_filepath}")
    except Exception as e:
        logging.error(f"Error saving AI analysis report to {report_filepath}: {e}")

    logging.info(f"Finished AI analysis for package: {args.package_path}")


if __name__ == "__main__":
    # Example: python apps/11_analyze_weekly_context/run.py --package-path data/silver/analysis_packages/2023-W40_AnalysisPackage.json
    # Ensure GEMINI_API_KEY is set in your environment or Colab Secrets
    if not genai:
        print("google.generativeai library is not installed. This script requires it.")
        print("Please run: pip install google-generativeai")
    elif not GEMINI_API_KEY:
        print("GEMINI_API_KEY environment variable is not set.")
        print("Please set it before running the script, e.g., in Colab Secrets or your shell environment.")
    else:
        main()
