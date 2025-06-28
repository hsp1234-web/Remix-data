import duckdb
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
from wordcloud import WordCloud # type: ignore
import yaml
from pathlib import Path
import logging
import argparse
import json
from collections import Counter
from datetime import datetime
from typing import Optional, List

# Configure logger for this module
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Set default Plotly template
pio.templates.default = "plotly_white"

class InsightReporter:
    def __init__(self, config_path: Path, db_path_override: Optional[Path] = None):
        self.config = self._load_config(config_path)
        if db_path_override:
            self.db_path = db_path_override
        else:
            # The database path in config is relative to the project root.
            # The script is expected to be run from the project root.
            db_relative_path = self.config.get('database', {}).get('path', 'data/financial_data.duckdb')
            # Ensure this path is resolved relative to the project root, not just CWD of reporter.py if it's different.
            # Assuming reporter.py is run from project root, Path(db_relative_path) is fine.
            # For robustness, one might pass project_root to InsightReporter.
            # For now, we rely on CWD = project_root.
            self.db_path = Path(db_relative_path).resolve() # Resolve to get absolute path immediately

        logger.info(f"InsightReporter initialized. Using ABSOLUTE database path: {self.db_path}") # Log absolute path
        self.conn = None

    def _load_config(self, config_path: Path) -> dict:
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading configuration from {config_path}: {e}")
            raise

    def _connect_db(self):
        if self.conn is None: # If no connection object exists, create one
            try:
                logger.info(f"Connecting to database: {self.db_path}")
                self.conn = duckdb.connect(database=str(self.db_path), read_only=True)
            except Exception as e:
                logger.error(f"Failed to connect to database {self.db_path}: {e}")
                raise
        # If self.conn exists, we assume it's open. DuckDB will error on operations if closed.
        else:
            logger.info("Database connection object exists.")


    def _close_db(self):
        if self.conn:
            self.conn.close()
            self.conn = None # Set to None after closing
            logger.info("Database connection closed and object set to None.")

    def _fetch_data(self, query: str) -> pd.DataFrame:
        self._connect_db()
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"Error fetching data with query '{query}': {e}")
            return pd.DataFrame()

    def get_stress_index_data(self) -> pd.DataFrame:
        # Option 1: Try to get it from log_ai_decision first, as it's directly linked to decisions
        query_log_ai = "SELECT decision_date, stress_index_value FROM log_ai_decision ORDER BY decision_date;"
        df_log_ai = self._fetch_data(query_log_ai)

        if not df_log_ai.empty and 'stress_index_value' in df_log_ai.columns and df_log_ai['stress_index_value'].notna().any():
            df_log_ai.rename(columns={'decision_date': 'date', 'stress_index_value': 'DealerStressIndex'}, inplace=True)
            df_log_ai['date'] = pd.to_datetime(df_log_ai['date'])
            logger.info(f"Fetched stress index data from 'log_ai_decision' table. Rows: {len(df_log_ai)}")
            return df_log_ai.set_index('date')

        # Option 2: Fallback to fact_macro_economic_data if not in log_ai_decision or log_ai_decision is empty
        # This assumes 'DealerStressIndex' might be stored there by some other process, which is not current design.
        # For this project, stress index is calculated by IndicatorEngine and then used for AI decision.
        # A better approach might be to re-calculate it if needed, or ensure it's always logged.
        # For now, we rely on it being in log_ai_decision. If not, we might have an issue.
        logger.warning("Could not find comprehensive stress_index_value in 'log_ai_decision'. Report might be incomplete for stress index.")
        return pd.DataFrame(columns=['DealerStressIndex']) # Return empty DF with expected column

    def get_ai_decisions(self) -> pd.DataFrame:
        query = "SELECT decision_date, strategy_summary, key_factors, confidence_score FROM log_ai_decision ORDER BY decision_date;"
        df = self._fetch_data(query)
        if not df.empty:
            df['decision_date'] = pd.to_datetime(df['decision_date'])
            # Safely parse key_factors JSON string
            def parse_key_factors(json_str):
                try:
                    return json.loads(json_str)
                except (json.JSONDecodeError, TypeError):
                    return [] # Return empty list on error
            df['key_factors'] = df['key_factors'].apply(parse_key_factors)
            logger.info(f"Fetched AI decision data. Rows: {len(df)}")
        else:
            logger.warning("'log_ai_decision' table is empty or query failed. AI-related plots will be empty.")
        return df

    def generate_stress_index_timeseries_plot(self, stress_data: pd.DataFrame) -> Optional[go.Figure]:
        if stress_data.empty or 'DealerStressIndex' not in stress_data.columns:
            logger.warning("Stress index data is empty or missing 'DealerStressIndex' column. Cannot generate timeseries plot.")
            return None

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=stress_data.index, y=stress_data['DealerStressIndex'], mode='lines+markers', name='Dealer Stress Index'))

        fig.update_layout(
            title_text='每日市場壓力指標 (Dealer Stress Index) 時間序列圖',
            xaxis_title='日期',
            yaxis_title='壓力指數值',
            hovermode="x unified"
        )
        return fig

    def generate_ai_strategy_distribution_pie_chart(self, ai_decisions: pd.DataFrame) -> Optional[go.Figure]:
        if ai_decisions.empty or 'strategy_summary' not in ai_decisions.columns:
            logger.warning("AI decision data is empty or missing 'strategy_summary'. Cannot generate pie chart.")
            return None

        strategy_counts = ai_decisions['strategy_summary'].value_counts()

        fig = go.Figure(data=[go.Pie(labels=strategy_counts.index, values=strategy_counts.values, hole=.3)])
        fig.update_layout(
            title_text='AI 策略分佈餅圖 (Strategy Distribution)'
        )
        return fig

    def generate_key_factors_wordcloud_image(self, ai_decisions: pd.DataFrame, image_path: Path) -> bool:
        if ai_decisions.empty or 'key_factors' not in ai_decisions.columns:
            logger.warning("AI decision data is empty or missing 'key_factors'. Cannot generate word cloud.")
            return False

        all_factors: List[str] = []
        for factors_list in ai_decisions['key_factors']:
            if isinstance(factors_list, list): # Ensure it's a list (parsed from JSON)
                all_factors.extend([str(factor) for factor in factors_list]) # Ensure factors are strings

        if not all_factors:
            logger.warning("No key factors found after processing AI decisions. Word cloud will be empty.")
            # Create an empty image or skip? For now, skip.
            return False

        text = " ".join(all_factors)

        try:
            # Ensure the directory for the image exists
            image_path.parent.mkdir(parents=True, exist_ok=True)

            # Using a common Chinese font path, adjust if necessary or make configurable
            # This assumes the environment has such a font.
            # Common paths:
            # Windows: 'C:/Windows/Fonts/msyh.ttc' (Microsoft YaHei) or 'simhei.ttf'
            # Linux: '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc' or similar
            # macOS: '/System/Library/Fonts/PingFang.ttc'
            # If no specific font is found, WordCloud might default to a font that doesn't support Chinese.
            # For simplicity in this environment, try a generic name that might exist or let it default.
            # A more robust solution would involve bundling a font or having a font config.
            font_path = None
            # Example: font_path = 'msyh.ttc' # If locally available and WordCloud can find it.
            # If font_path is None, it uses WordCloud's default font.

            wordcloud = WordCloud(
                width=800, height=400,
                background_color='white',
                font_path=font_path, # Specify a font that supports Chinese characters
                collocations=False # Avoid showing bigrams
            ).generate(text)

            wordcloud.to_file(str(image_path))
            logger.info(f"Word cloud image saved to {image_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to generate or save word cloud image: {e}", exc_info=True)
            logger.warning("Wordcloud generation might require a font that supports Chinese characters. "
                           "If the wordcloud is missing text or looks incorrect, please check font availability and configuration.")
            return False

    def generate_report(self, output_dir: Path = Path("."), report_filename_base: str = "ai_insights_report"):
        """Generates an HTML report with various insight plots."""
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- Debug: Query log_ai_decision table ---
        self._connect_db() # Ensure connection
        if self.conn:
            try:
                debug_query = "SELECT COUNT(*) as count, SUM(CASE WHEN stress_index_value IS NOT NULL THEN 1 ELSE 0 END) as non_null_stress, SUM(CASE WHEN strategy_summary != 'AI_CALL_SKIPPED_OR_FAILED' THEN 1 ELSE 0 END) as valid_strategies FROM log_ai_decision;"
                debug_df = self.conn.execute(debug_query).fetchdf()
                logger.info(f"DEBUG log_ai_decision stats: {debug_df.to_dict('records')}")

                debug_sample_query = "SELECT decision_date, stress_index_value, strategy_summary FROM log_ai_decision LIMIT 5;"
                debug_sample_df = self.conn.execute(debug_sample_query).fetchdf()
                logger.info(f"DEBUG log_ai_decision sample:\n{debug_sample_df}")

            except Exception as e_debug:
                logger.error(f"DEBUG query failed: {e_debug}")
            # self._close_db() # Don't close yet, other functions need it. Will be closed at end of generate_report
        # --- End Debug ---

        stress_data = self.get_stress_index_data()
        ai_decisions = self.get_ai_decisions()

        fig_stress_ts = self.generate_stress_index_timeseries_plot(stress_data)
        fig_strategy_pie = self.generate_ai_strategy_distribution_pie_chart(ai_decisions)

        wordcloud_image_filename = f"{report_filename_base}_wordcloud.png"
        wordcloud_image_path = output_dir / wordcloud_image_filename
        wordcloud_success = self.generate_key_factors_wordcloud_image(ai_decisions, wordcloud_image_path)

        # Generate HTML report
        report_html_path = output_dir / f"{report_filename_base}.html"

        with open(report_html_path, 'w', encoding='utf-8') as f:
            f.write(f"<html><head><title>AI 洞察報告 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})</title>")
            f.write("<style>body { font-family: sans-serif; margin: 20px; } h1, h2 { color: #333; } .plot { margin-bottom: 40px; } img { max-width: 100%; height: auto; display: block; margin-left: auto; margin-right: auto; } </style>")
            f.write("</head><body>")
            f.write(f"<h1>AI 洞察報告 - {datetime.now().strftime('%Y-%m-%d')}</h1>")

            if fig_stress_ts:
                f.write("<h2>市場壓力指標時間序列</h2><div class='plot'>")
                f.write(pio.to_html(fig_stress_ts, full_html=False, include_plotlyjs='cdn'))
                f.write("</div>")
            else:
                f.write("<h2>市場壓力指標時間序列</h2><p>無法生成壓力指標時間序列圖 (數據不足或錯誤)。</p>")

            if fig_strategy_pie:
                f.write("<h2>AI 策略分佈</h2><div class='plot'>")
                f.write(pio.to_html(fig_strategy_pie, full_html=False, include_plotlyjs='cdn'))
                f.write("</div>")
            else:
                f.write("<h2>AI 策略分佈</h2><p>無法生成 AI 策略分佈餅圖 (數據不足或錯誤)。</p>")

            f.write("<h2>AI 決策關鍵因子詞雲圖</h2><div class='plot'>")
            if wordcloud_success:
                # Use relative path for image if HTML and image are in the same directory
                f.write(f"<img src='{wordcloud_image_filename}' alt='關鍵因子詞雲圖'>")
            else:
                f.write("<p>無法生成關鍵因子詞雲圖 (數據不足、字型問題或錯誤)。</p>")
            f.write("</div>")

            f.write("</body></html>")

        logger.info(f"HTML report generated at: {report_html_path.resolve()}")
        if wordcloud_success:
            logger.info(f"Word cloud image for the report is at: {wordcloud_image_path.resolve()}")

        self._close_db()


def main():
    parser = argparse.ArgumentParser(description="生成 AI 洞察報告。")
    parser.add_argument(
        "--config", type=str, default="src/configs/project_config.yaml",
        help="專案設定檔的路徑 (相對於專案根目錄)。"
    )
    parser.add_argument(
        "--output-dir", type=str, default="reports",
        help="儲存報告的目錄 (相對於專案根目錄)。"
    )
    parser.add_argument(
        "--db-path", type=str, default=None,
        help="可選：直接指定 DuckDB 資料庫檔案的路徑，覆蓋設定檔中的路徑。"
    )
    args = parser.parse_args()

    # Assuming this script is run from the project root
    project_root = Path(".").resolve()
    config_file_path = project_root / args.config
    output_directory = project_root / args.output_dir

    db_override_path = Path(args.db_path) if args.db_path else None

    if not config_file_path.exists():
        logger.error(f"設定檔 '{config_file_path}' 不存在。請提供正確的路徑。")
        return

    try:
        reporter = InsightReporter(config_path=config_file_path, db_path_override=db_override_path)
        reporter.generate_report(output_dir=output_directory)
    except Exception as e:
        logger.critical(f"生成報告時發生未預期的錯誤: {e}", exc_info=True)

if __name__ == "__main__":
    # Example: python src/analytics/reporter.py --output-dir reports_output
    main()
