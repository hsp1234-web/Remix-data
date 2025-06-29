import os
import json
import pandas as pd
import duckdb
import argparse
import logging
from datetime import datetime, timedelta
from sklearn.feature_extraction.text import TfidfVectorizer
from snownlp import SnowNLP # Assuming snownlp for Chinese sentiment

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Default Configuration ---
DEFAULT_DUCKDB_FILE = "data/financial_data.duckdb"
DEFAULT_SOCIAL_POSTS_BRONZE_FILE = "data/bronze/social_posts/threads_posts.parquet"
DEFAULT_ANALYSIS_PACKAGES_DIR = "data/silver/analysis_packages"
DEFAULT_EVENT_QUEUE_DIR = "event_bus/queue"

SILVER_TAIFEX_TABLE = "silver_fact_taifex_quotes"
GOLD_WEEKLY_SUMMARY_TABLE = "gold_weekly_market_summary"


def get_iso_week_dates(year, iso_week):
    """Returns the start (Monday) and end (Sunday) dates for a given ISO year and week."""
    start_of_week = datetime.strptime(f'{year}-W{iso_week}-1', "%Y-W%W-%w")
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week, end_of_week

def get_analysis_window_weeks(target_week_id):
    """
    Calculates the 9-week analysis window: target week, 4 prior, 4 following.
    Returns a list of week_ids (YYYY-Www).
    """
    year, week_num_str = target_week_id.split('-W')
    year = int(year)
    week_num = int(week_num_str)

    analysis_weeks = []
    for i in range(-4, 5): # -4, -3, -2, -1, 0, 1, 2, 3, 4
        current_date = datetime.strptime(f'{year}-W{week_num}-1', "%Y-W%W-%w") + timedelta(weeks=i)
        current_year, current_iso_week = current_date.isocalendar()[0], current_date.isocalendar()[1]
        analysis_weeks.append(f"{current_year}-W{str(current_iso_week).zfill(2)}")
    return sorted(list(set(analysis_weeks))) # sorted and unique

def fetch_target_week_daily_market_data(con, target_week_id):
    logging.info(f"Fetching target week ({target_week_id}) daily market data...")
    year, week_num_str = target_week_id.split('-W')
    start_date_dt, end_date_dt = get_iso_week_dates(int(year), week_num_str.zfill(2))

    query = f"""
    SELECT
        CAST(trade_date AS VARCHAR) AS date, -- Store date as string for JSON
        "open", "high", "low", "close", "volume"
    FROM {SILVER_TAIFEX_TABLE}
    WHERE trade_date >= ? AND trade_date <= ?
    AND contract LIKE 'TX%' -- Example: Focusing on main futures contract TX, adjust if needed
    ORDER BY trade_date ASC;
    """
    # Note: The 'contract LIKE TX%' might need to be more dynamic or configurable
    # if we need to analyze different contract groups for the target week.
    # For now, assuming 'TX' is the primary focus for daily details.
    try:
        df = con.execute(query, [start_date_dt.strftime('%Y-%m-%d'), end_date_dt.strftime('%Y-%m-%d')]).fetchdf()
        logging.info(f"Fetched {len(df)} rows for target week daily market data.")
        # Add citation IDs
        df['cite_id'] = [f"market_{row['date']}" for idx, row in df.iterrows()]
        return df.to_dict('records')
    except Exception as e:
        logging.error(f"Error fetching target week daily market data: {e}")
        return []


def fetch_target_week_social_posts(social_posts_df, target_week_id):
    logging.info(f"Fetching target week ({target_week_id}) social posts...")
    year, week_num_str = target_week_id.split('-W')
    start_date_dt, end_date_dt = get_iso_week_dates(int(year), week_num_str.zfill(2))

    # Assuming social_posts_df has a 'post_date' column that can be converted to datetime
    try:
        # Ensure 'post_date' is datetime
        if not pd.api.types.is_datetime64_any_dtype(social_posts_df['post_date']):
             social_posts_df['post_date'] = pd.to_datetime(social_posts_df['post_date'], errors='coerce')

        target_week_posts_df = social_posts_df[
            (social_posts_df['post_date'] >= start_date_dt) &
            (social_posts_df['post_date'] <= end_date_dt)
        ].copy() # Use .copy() to avoid SettingWithCopyWarning

        if target_week_posts_df.empty:
            return []

        # Add citation IDs
        target_week_posts_df['cite_id'] = [f"post_tw_{i+1}" for i in range(len(target_week_posts_df))]
        # Convert post_date to string for JSON
        target_week_posts_df['post_date'] = target_week_posts_df['post_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Select relevant columns (adjust if your CSV has different names)
        # Assuming 'author' and 'content' are the column names in your CSV/Parquet
        relevant_cols = ['post_date', 'author', 'content', 'cite_id']
        # Filter out to only include columns that exist in the DataFrame
        existing_cols = [col for col in relevant_cols if col in target_week_posts_df.columns]

        logging.info(f"Fetched {len(target_week_posts_df)} posts for target week.")
        return target_week_posts_df[existing_cols].to_dict('records')
    except Exception as e:
        logging.error(f"Error fetching target week social posts: {e}")
        return []


def fetch_background_weekly_market_summary(con, background_week_ids):
    logging.info(f"Fetching background weeks market summary for: {background_week_ids}")
    if not background_week_ids:
        return []
    placeholders = ', '.join(['?'] * len(background_week_ids))
    query = f"""
    SELECT
        week_id,
        weekly_close AS close_price, -- Renaming for consistency with spec
        total_weekly_volume,
        avg_pc_ratio
        -- Add other relevant fields from gold_weekly_market_summary as needed
    FROM {GOLD_WEEKLY_SUMMARY_TABLE}
    WHERE week_id IN ({placeholders})
    AND contract_group = 'TX' -- Example: Focusing on main futures contract TX, adjust if needed
    ORDER BY week_id ASC;
    """
    # Note: The 'contract_group = TX' might need to be more dynamic or configurable.
    try:
        df = con.execute(query, background_week_ids).fetchdf()
        logging.info(f"Fetched {len(df)} rows for background weeks market summary.")
        return df.to_dict('records') # This will be merged with qualitative data later
    except Exception as e:
        logging.error(f"Error fetching background market summary: {e}")
        return []

def analyze_background_week_posts(social_posts_df, week_id):
    year, week_num_str = week_id.split('-W')
    start_date_dt, end_date_dt = get_iso_week_dates(int(year), week_num_str.zfill(2))

    if not pd.api.types.is_datetime64_any_dtype(social_posts_df['post_date']):
        social_posts_df['post_date'] = pd.to_datetime(social_posts_df['post_date'], errors='coerce')

    week_posts_df = social_posts_df[
        (social_posts_df['post_date'] >= start_date_dt) &
        (social_posts_df['post_date'] <= end_date_dt)
    ]

    if week_posts_df.empty or 'content' not in week_posts_df.columns:
        return {"post_count": 0, "sentiment_score": None, "top_keywords": []}

    post_count = len(week_posts_df)

    # Sentiment analysis
    sentiments = [SnowNLP(text).sentiments for text in week_posts_df['content'].astype(str) if pd.notna(text)]
    avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else None

    # Keyword extraction (TF-IDF)
    # Ensure content is string and not NaN
    corpus = week_posts_df['content'].astype(str).dropna().tolist()
    top_keywords = []
    if corpus:
        try:
            vectorizer = TfidfVectorizer(max_features=100, stop_words=None) # Consider Chinese stop words
            tfidf_matrix = vectorizer.fit_transform(corpus)
            # Sum TF-IDF scores for each term across all documents in the week
            sum_tfidf = tfidf_matrix.sum(axis=0)
            words = vectorizer.get_feature_names_out()
            word_scores = {word: score for word, score in zip(words, sum_tfidf.tolist()[0])}
            # Get top 3-5 keywords
            sorted_keywords = sorted(word_scores.items(), key=lambda item: item[1], reverse=True)
            top_keywords = [kw[0] for kw in sorted_keywords[:5]]
        except Exception as e:
            logging.warning(f"TF-IDF keyword extraction failed for week {week_id}: {e}")
            top_keywords = []

    return {
        "post_count": post_count,
        "sentiment_score": round(avg_sentiment, 2) if avg_sentiment is not None else None,
        "top_keywords": top_keywords
    }

def main():
    parser = argparse.ArgumentParser(description="Generate a Target Week Analysis Package.")
    parser.add_argument("--target-week-id", type=str, required=True,
                        help="Target week in YYYY-Www format (e.g., 2022-W30).")
    parser.add_argument("--duckdb-file", type=str, default=DEFAULT_DUCKDB_FILE)
    parser.add_argument("--social-posts-bronze-file", type=str, default=DEFAULT_SOCIAL_POSTS_BRONZE_FILE)
    parser.add_argument("--analysis-packages-dir", type=str, default=DEFAULT_ANALYSIS_PACKAGES_DIR)
    parser.add_argument("--event-queue-dir", type=str, default=DEFAULT_EVENT_QUEUE_DIR)

    args = parser.parse_args()
    logging.info(f"Generating analysis package for target week: {args.target_week_id}")

    # --- 1. Calculate Analysis Window ---
    all_9_week_ids = get_analysis_window_weeks(args.target_week_id)
    if not all_9_week_ids or args.target_week_id not in all_9_week_ids:
        logging.error(f"Could not determine a valid 9-week window for {args.target_week_id}")
        return

    target_week_idx = all_9_week_ids.index(args.target_week_id)
    window_start_week = all_9_week_ids[0]
    window_end_week = all_9_week_ids[-1]

    # Get actual start and end dates of the 9-week window
    window_overall_start_date, _ = get_iso_week_dates(int(window_start_week.split('-W')[0]), window_start_week.split('-W')[1].zfill(2))
    _, window_overall_end_date = get_iso_week_dates(int(window_end_week.split('-W')[0]), window_end_week.split('-W')[1].zfill(2))

    background_week_ids = [wid for wid in all_9_week_ids if wid != args.target_week_id]

    analysis_package = {
        "target_week_id": args.target_week_id,
        "analysis_window": {
            "start_date": window_overall_start_date.strftime('%Y-%m-%d'),
            "end_date": window_overall_end_date.strftime('%Y-%m-%d'),
            "week_ids_in_window": all_9_week_ids
        },
        "context_window_summary": {"weekly_summaries": []},
        "target_week_detail": {}
    }

    con = None
    social_posts_df_all = None

    try:
        # --- 2. Connect to DB and Load Social Posts ---
        con = duckdb.connect(database=args.duckdb_file, read_only=True)
        if os.path.exists(args.social_posts_bronze_file):
            social_posts_df_all = pd.read_parquet(args.social_posts_bronze_file)
            if 'post_date' in social_posts_df_all.columns:
                 social_posts_df_all['post_date'] = pd.to_datetime(social_posts_df_all['post_date'], errors='coerce')
            else:
                logging.error(f"'post_date' column not found in {args.social_posts_bronze_file}. Social context will be limited.")
                social_posts_df_all = pd.DataFrame() # Empty df
        else:
            logging.warning(f"Social posts bronze file not found: {args.social_posts_bronze_file}. Social context will be empty.")
            social_posts_df_all = pd.DataFrame()


        # --- 3. Fetch Target Week Detailed Data ---
        analysis_package["target_week_detail"]["daily_market_data"] = fetch_target_week_daily_market_data(con, args.target_week_id)
        if not social_posts_df_all.empty:
            analysis_package["target_week_detail"]["full_text_posts"] = fetch_target_week_social_posts(social_posts_df_all, args.target_week_id)
        else:
            analysis_package["target_week_detail"]["full_text_posts"] = []

        # --- 4. Fetch and Process Background Window Summaries ---
        # Quantitative part from gold_weekly_market_summary
        bg_market_summaries_list = fetch_background_weekly_market_summary(con, background_week_ids)

        # Qualitative part (post count, sentiment, keywords) for each background week
        merged_bg_summaries = []
        for week_id in background_week_ids:
            # Find corresponding market summary
            market_sum = next((item for item in bg_market_summaries_list if item["week_id"] == week_id), None)
            if market_sum is None: # If no market data for this bg week, create a placeholder
                market_sum = {"week_id": week_id, "close_price": None, "total_weekly_volume": None, "avg_pc_ratio": None}

            if not social_posts_df_all.empty:
                posts_analysis = analyze_background_week_posts(social_posts_df_all, week_id)
                market_sum.update(posts_analysis) # Merge posts analysis into market_sum
            else: # No social posts data, fill with defaults
                market_sum.update({"post_count": 0, "sentiment_score": None, "top_keywords": []})

            merged_bg_summaries.append(market_sum)

        analysis_package["context_window_summary"]["weekly_summaries"] = sorted(merged_bg_summaries, key=lambda x: x['week_id'])


        # --- 5. Save Package ---
        os.makedirs(args.analysis_packages_dir, exist_ok=True)
        package_filename = f"{args.target_week_id}_AnalysisPackage.json"
        package_filepath = os.path.join(args.analysis_packages_dir, package_filename)
        with open(package_filepath, 'w', encoding='utf-8') as f:
            json.dump(analysis_package, f, ensure_ascii=False, indent=2)
        logging.info(f"Successfully generated and saved analysis package: {package_filepath}")

        # --- 6. Trigger Next Task (apps/11_analyze_weekly_context) ---
        next_task_payload = {
            "app_name": "11_analyze_weekly_context",
            "params": {
                "package_path": package_filepath
            },
            "triggered_by": f"task_10_create_weekly_context_for_{args.target_week_id}" # Example ID
        }

        # Create a unique filename for the task in the queue
        # Using datetime to ensure uniqueness for multiple runs of the same target_week_id (if needed for reprocessing)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        next_task_filename = f"task_11_analyze_{args.target_week_id}_{timestamp}.json"
        next_task_filepath = os.path.join(args.event_queue_dir, next_task_filename)

        os.makedirs(args.event_queue_dir, exist_ok=True)
        with open(next_task_filepath, 'w', encoding='utf-8') as f:
            json.dump(next_task_payload, f, indent=2)
        logging.info(f"Successfully queued next task for AI analysis: {next_task_filepath}")

    except Exception as e:
        logging.error(f"Error generating analysis package for {args.target_week_id}: {e}", exc_info=True)
    finally:
        if con:
            con.close()

    logging.info(f"Finished processing for target week: {args.target_week_id}")

if __name__ == "__main__":
    # Example usage:
    # python apps/10_create_weekly_context/run.py --target-week-id 2023-W40
    main()
