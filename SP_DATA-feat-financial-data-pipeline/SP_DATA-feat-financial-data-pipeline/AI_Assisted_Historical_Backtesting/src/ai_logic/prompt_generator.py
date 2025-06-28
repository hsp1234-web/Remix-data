import json
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger

logger = get_logger(__name__)

class PromptGenerator:
    """
    負責生成提交給大型語言模型 (LLM) 的 Prompt。
    主要任務是將結構化的市場數據和可能的質化信息轉換為 LLM 可理解的格式。
    """

    def __init__(self, ai_model_name="Llama3-8B-Instruct"): # 可以根據實際模型調整
        self.ai_model_name = ai_model_name
        # 可以在這裡加載一些通用的 prompt 模板或指令片段

    def _format_feature_section(self, processed_features: dict) -> str:
        """
        格式化處理後的特徵數據部分，使其易於閱讀。
        Args:
            processed_features (dict): 一個週期的 processed_features_hourly 數據。
                                       例如: {"price_open": 100, "sma_20": 98.5, ...}
        Returns:
            str: 格式化後的特徵字符串。
        """
        if not processed_features:
            return "量化特徵數據缺失。\n"

        formatted_str = "核心量化特徵：\n"
        # 可以按類別組織特徵以提高可讀性
        price_features = {k: v for k, v in processed_features.items() if k.startswith("price_")}
        volume_features = {k: v for k, v in processed_features.items() if k.startswith("volume_")}
        indicator_features = {k: v for k, v in processed_features.items() if k not in price_features and k not in volume_features and "sentiment" not in k and "fred" not in k and "event" not in k} # 簡化分類
        sentiment_features = {k:v for k,v in processed_features.items() if "sentiment" in k}
        macro_features = {k:v for k,v in processed_features.items() if "fred_" in k or "event" in k}


        if price_features:
            formatted_str += "  價格相關：\n"
            for key, value in price_features.items():
                formatted_str += f"    - {key}: {value:.2f if isinstance(value, float) else value}\n"

        if volume_features:
            formatted_str += "  成交量相關：\n"
            for key, value in volume_features.items():
                formatted_str += f"    - {key}: {value}\n" # Volume 通常是整數

        if indicator_features:
            formatted_str += "  技術指標：\n"
            for key, value in indicator_features.items():
                formatted_str += f"    - {key}: {value:.3f if isinstance(value, float) else value}\n"

        if sentiment_features:
            formatted_str += "  市場情緒指標：\n"
            for key, value in sentiment_features.items():
                 formatted_str += f"    - {key}: {value:.3f if isinstance(value, float) else value}\n"

        if macro_features:
            formatted_str += "  宏觀經濟指標：\n"
            for key, value in macro_features.items():
                 formatted_str += f"    - {key}: {value}\n"

        return formatted_str

    def generate_market_briefing_json_and_prompt(
        self,
        current_timestamp_utc: str,      # 當前回溯到的時間點 (T 時刻)
        symbol: str,                     # 當前分析的標的 (例如 "AAPL", "^GSPC")
        processed_features_current_period: dict, # 當前週期的 `processed_features_hourly` 數據
        qualitative_info: dict = None    # 質化信息，例如 {"news_summary": "...", "analyst_opinions_summary": "..."}
    ) -> tuple[str, str]:
        """
        生成結構化的市場簡報 JSON 和用於 LLM 的完整 Prompt。

        Args:
            current_timestamp_utc (str): 當前分析週期的開始時間 (ISO 8601 UTC)。
            symbol (str): 分析的金融產品代碼。
            processed_features_current_period (dict):
                從 `processed_features_hourly` 表中獲取的、對應當前週期的特徵數據。
                例如: {"price_open": 100.0, "price_close": 102.5, "sma_20": 98.5, ...}
            qualitative_info (dict, optional):
                包含質化信息的字典。鍵可能是 "news_summary", "social_media_buzz", "analyst_rating_change" 等。
                值是文本描述。

        Returns:
            tuple[str, str]:
                - market_briefing_json_str (str): 市場簡報的 JSON 字符串表示，用於存儲到 `ai_historical_judgments` 表。
                - full_prompt_for_llm (str): 構建完成的、可以直接提交給 LLM 的 Prompt 文本。
        """

        # 1. 構建市場簡報字典 (用於存儲和作為 prompt 的一部分)
        market_briefing_dict = {
            "report_time_utc": current_timestamp_utc,
            "symbol": symbol,
            "quantitative_features": processed_features_current_period if processed_features_current_period else {},
            "qualitative_elements": qualitative_info if qualitative_info else {}
        }

        try:
            market_briefing_json_str = json.dumps(market_briefing_dict, indent=2, ensure_ascii=False)
        except TypeError as e:
            logger.error(f"序列化市場簡報為 JSON 時出錯: {e}. 原始字典: {market_briefing_dict}", exc_info=True)
            # 嘗試一個更安全的序列化，將無法序列化的部分轉為字符串
            # (這部分邏輯可以更完善，例如遞歸處理)
            safe_dict = {k: (str(v) if not isinstance(v, (dict, list, str, int, float, bool, type(None))) else v)
                         for k, v in market_briefing_dict.items()}
            try:
                market_briefing_json_str = json.dumps(safe_dict, indent=2, ensure_ascii=False)
                logger.warning("市場簡報 JSON 序列化時使用了類型轉換。")
            except TypeError: # 如果還是失敗
                 market_briefing_json_str = json.dumps({"error": "Failed to serialize market briefing", "original_briefing_partial": str(market_briefing_dict)[:1000]})


        # 2. 構建 LLM Prompt
        # 基本指令/角色設定
        # (這部分可以做得非常複雜和可配置，例如從模板文件加載)
        prompt_header = f"""你是一位經驗豐富的量化交易策略師和市場分析AI。你的任務是基於以下提供的市場簡報，為金融產品 '{symbol}' 在時間點 '{current_timestamp_utc}' (UTC) 之後的下一個交易週期（例如未來12-24小時）提供分析和交易決策建議。

請嚴格按照以下格式輸出你的回應，不要添加任何額外的解釋或對話開頭/結尾。所有輸出必須是有效的 JSON 格式。

輸出格式要求 (JSON):
{{
  "decision_category": "string (例如：強力看多, 看多, 中性看漲, 中性, 中性看跌, 看空, 強力看空, 高度波動預期, 區間震盪預期, 規避風險/觀望)",
  "confidence_score": "float (0.0 到 1.0 之間，表示你對決策的信心)",
  "recommended_strategy": {{
    "name": "string (策略名稱，例如：買入看漲期權, 賣出看跌期權價差, 建立期貨多單, 觀望)",
    "description": "string (策略的簡要描述和執行細節，例如：在價格 X 附近買入 Y 月份 Z 履約價的看漲期權)",
    "parameters": {{
      "entry_price_level": "float or null (建議的入場價格水平，如果適用)",
      "stop_loss_level": "float or null (建議的止損價格水平，如果適用)",
      "take_profit_level": "float or null (建議的止盈價格水平，如果適用)",
      "option_details": {{
          "type": "string or null (Call/Put)",
          "strike": "float or null",
          "expiration_months": "integer or null (距離到期月數)"
      }},
      "other_conditions": "string or null (其他執行條件或注意事項)"
    }}
  }},
  "rationale": "string (詳細的決策推理過程，解釋你為什麼做出這個判斷和推薦這個策略，必須引用簡報中的關鍵數據點來支持你的分析。例如：基於RSI指標低於30且成交量放大，同時新聞情緒轉為正面...)",
  "key_risk_factors": [
    "string (列出此決策/策略面臨的主要潛在風險，至少一個，如果沒有明顯風險則說明)",
    "string (更多風險...)"
  ]
}}

--- 市場簡報開始 ---
"""
        # 格式化市場簡報內容
        briefing_content_for_prompt = f"時間點 (UTC): {current_timestamp_utc}\n"
        briefing_content_for_prompt += f"分析標的: {symbol}\n\n"

        briefing_content_for_prompt += self._format_feature_section(
            market_briefing_dict["quantitative_features"]
        )

        if market_briefing_dict["qualitative_elements"]:
            briefing_content_for_prompt += "\n補充質化信息：\n"
            for key, value in market_briefing_dict["qualitative_elements"].items():
                briefing_content_for_prompt += f"  - {key}: {value}\n"
        else:
            briefing_content_for_prompt += "\n無補充質化信息。\n"

        prompt_footer = "--- 市場簡報結束 ---\n\n請嚴格按照上述 JSON 格式提供你的分析和決策："

        full_prompt_for_llm = prompt_header + briefing_content_for_prompt + prompt_footer

        logger.debug(f"生成的市場簡報 JSON (前500字符): {market_briefing_json_str[:500]}...")
        logger.debug(f"生成的完整 LLM Prompt (前500字符): {full_prompt_for_llm[:500]}...")

        return market_briefing_json_str, full_prompt_for_llm


if __name__ == "__main__":
    import logging
    # 假設 utils.logger 可以在 sys.path 中找到
    # 為了直接運行此文件，可能需要調整 sys.path
    import sys, os
    current_dir_pg = os.path.dirname(os.path.abspath(__file__)) # .../ai_logic
    project_src_dir_pg = os.path.abspath(os.path.join(current_dir_pg, '..')) # .../src
    project_root_pg = os.path.abspath(os.path.join(project_src_dir_pg, '..')) # AI_Assisted_Historical_Backtesting
    if project_root_pg not in sys.path:
        sys.path.insert(0, project_root_pg)

    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME
    setup_logger(PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- PromptGenerator (__main__) 測試開始 ---")

    generator = PromptGenerator()

    sample_features = {
        "price_open": 150.20, "price_high": 152.50, "price_low": 149.80, "price_close": 151.75,
        "price_change_pct": 0.015,
        "volume_total": 12000000,
        "sma_20": 148.50, "rsi_14": 65.2,
        "news_sentiment_score": 0.65,
        "fred_interest_rate": 0.0525
    }
    sample_qual_info = {
        "news_summary": "市場對科技股財報預期樂觀，帶動指數上揚。",
        "upcoming_event": "下週將公佈 CPI 數據。"
    }

    timestamp = "2023-10-27T12:00:00.000Z"
    symbol = "AAPL"

    briefing_json, full_prompt = generator.generate_market_briefing_json_and_prompt(
        timestamp, symbol, sample_features, sample_qual_info
    )

    logger.info(f"\n--- 生成的市場簡報 JSON ---:\n{briefing_json}")
    logger.info(f"\n--- 生成的完整 LLM Prompt ---:\n{full_prompt}")

    assert symbol in full_prompt
    assert "news_sentiment_score" in full_prompt
    assert "市場對科技股財報預期樂觀" in full_prompt
    assert "decision_category" in full_prompt # 確保 JSON 格式指令在 prompt 中

    # 測試無質化信息
    briefing_json_no_qual, full_prompt_no_qual = generator.generate_market_briefing_json_and_prompt(
        timestamp, symbol, sample_features, None
    )
    assert "無補充質化信息" in full_prompt_no_qual
    logger.info("\n--- 生成的 Prompt (無質化信息) 測試通過 ---")

    # 測試無量化特徵 (雖然不太可能，但要處理)
    briefing_json_no_quant, full_prompt_no_quant = generator.generate_market_briefing_json_and_prompt(
        timestamp, symbol, None, sample_qual_info
    )
    assert "量化特徵數據缺失" in full_prompt_no_quant
    logger.info("\n--- 生成的 Prompt (無量化特徵) 測試通過 ---")

    logger.info("--- PromptGenerator (__main__) 測試結束 ---")
