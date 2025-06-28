import urllib.request
import urllib.parse # 雖然 Ollama API 通常是 POST JSON，但以防萬一
import json
import time # 用於可能的超時或簡單延遲
from AI_Assisted_Historical_Backtesting.src.utils.logger import get_logger
from AI_Assisted_Historical_Backtesting.src.utils.error_handler import retry_with_exponential_backoff # 可能用於包裝請求

logger = get_logger(__name__)

# Ollama API 的預設端點
DEFAULT_OLLAMA_API_BASE_URL = "http://localhost:11434" # 通常 Ollama 在本地運行於此端口
DEFAULT_OLLAMA_GENERATE_ENDPOINT = "/api/generate"
DEFAULT_OLLAMA_CHAT_ENDPOINT = "/api/chat" # 如果使用聊天 API

# 預設 User-Agent
DEFAULT_OLLAMA_USER_AGENT = "AI_Backtester_LlamaAgent/1.0"

class LlamaOllamaAgent:
    """
    通過 Ollama API 與本地運行的 Llama 模型進行交互的代理。
    優先使用 urllib.request 實現零依賴（除了標準庫）。
    """
    def __init__(self,
                 model_name: str,
                 ollama_api_base_url: str = DEFAULT_OLLAMA_API_BASE_URL,
                 user_agent: str = DEFAULT_OLLAMA_USER_AGENT,
                 request_timeout: int = 180): # 請求超時時間（秒），LLM 生成可能較慢
        """
        初始化 LlamaOllamaAgent。

        Args:
            model_name (str): 要在 Ollama 中使用的模型名稱 (例如 "llama3:8b-instruct-q4_K_M")。
            ollama_api_base_url (str, optional): Ollama API 的基礎 URL。
            user_agent (str, optional): HTTP 請求的 User-Agent。
            request_timeout (int, optional): API 請求的超時時間（秒）。
        """
        self.model_name = model_name
        self.api_base_url = ollama_api_base_url.rstrip('/')
        self.user_agent = user_agent
        self.request_timeout = request_timeout

        logger.info(f"LlamaOllamaAgent 初始化完成。模型: {self.model_name}, Ollama API: {self.api_base_url}, 超時: {self.request_timeout}s")

    def _prepare_request_data_generate(self, prompt: str, stream: bool = False, options: dict = None) -> bytes:
        """
        準備用於 Ollama /api/generate 端點的請求體 (JSON)。
        """
        data = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": stream, # False 表示等待完整響應，True 表示流式響應
            # "format": "json", # 如果希望 Ollama 盡力返回 JSON (需要模型支持和正確的 prompt)
                              # 我們的 prompt 已經要求 JSON，這裡可以不加，或者加上作為雙重保險
        }
        if options: # 例如 temperature, top_k, top_p 等 Ollama 支持的參數
            data["options"] = options

        return json.dumps(data).encode('utf-8')

    # 可以選擇實現 _prepare_request_data_chat 如果需要聊天端點

    @retry_with_exponential_backoff(max_retries=2, initial_delay=5, backoff_factor=2,
                                    allowed_exceptions=(urllib.error.URLError, TimeoutError, ConnectionResetError))
    def send_prompt_to_ollama(self, prompt: str, use_chat_endpoint: bool = False, stream_response: bool = False, generation_options: dict = None) -> dict | None:
        """
        將 Prompt 發送給 Ollama Llama 模型並獲取其響應。

        Args:
            prompt (str): 要發送給模型的完整 Prompt。
            use_chat_endpoint (bool): 是否使用 /api/chat 端點 (未完全實現)。預設 False，使用 /api/generate。
            stream_response (bool): 是否請求流式響應。預設 False。
                                   如果為 True，此方法需要修改以處理流。目前主要支持非流式。
            generation_options (dict, optional): 傳遞給 Ollama API 的 options 參數，
                                                 例如 {"temperature": 0.7, "num_predict": 512}。

        Returns:
            dict or None: 解析後的 LLM JSON 響應 (如果成功且非流式)。
                          對於 /api/generate (非流式)，響應通常包含 "response" (生成的文本),
                          "done", "total_duration", "context" 等字段。
                          如果發生錯誤，返回 None。
        """
        if stream_response:
            logger.error("流式響應目前在此方法中未完全支持，請將 stream_response 設為 False。")
            # 流式處理需要迭代讀取響應行，每行是一個 JSON 對象，然後拼接或處理
            raise NotImplementedError("流式響應處理尚未實現。")

        if use_chat_endpoint:
            # request_url = self.api_base_url + DEFAULT_OLLAMA_CHAT_ENDPOINT
            # request_body_bytes = self._prepare_request_data_chat(...) # 需要實現
            logger.error("聊天端點 (/api/chat) 交互尚未完全實現。請使用 /api/generate。")
            raise NotImplementedError("/api/chat 交互尚未實現。")
        else:
            request_url = self.api_base_url + DEFAULT_OLLAMA_GENERATE_ENDPOINT
            request_body_bytes = self._prepare_request_data_generate(prompt, stream=False, options=generation_options)

        headers = {
            "Content-Type": "application/json",
            "User-Agent": self.user_agent
        }

        req = urllib.request.Request(request_url, data=request_body_bytes, headers=headers, method="POST")

        logger.info(f"向 Ollama ({self.model_name}) 發送 Prompt (長度: {len(prompt)}字符)...")
        logger.debug(f"Ollama請求 URL: {request_url}, 請求體 (前200字符): {request_body_bytes[:200].decode('utf-8', 'ignore')}...")

        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as response:
                status_code = response.getcode()
                response_body_str = response.read().decode('utf-8')
                logger.debug(f"Ollama API 響應狀態碼: {status_code}")

                if status_code == 200:
                    try:
                        response_data = json.loads(response_body_str)
                        logger.info(f"成功從 Ollama ({self.model_name}) 收到響應。")
                        logger.debug(f"Ollama 原始響應 (部分): {str(response_data)[:500]}...")

                        # /api/generate (非流式) 的響應中，模型生成的文本在 "response" 字段
                        # 如果我們在 prompt 中要求 JSON，那麼這個 "response" 字段的內容應該是 JSON 字符串
                        # 我們需要再次解析 response_data.get("response")
                        generated_text = response_data.get("response")
                        if generated_text:
                            try:
                                # 假設 LLM 完全遵循了我們的 JSON 格式指令
                                parsed_llm_output = json.loads(generated_text)
                                logger.info("LLM 輸出的 JSON 內容已成功解析。")
                                return parsed_llm_output # 返回解析後的業務 JSON
                            except json.JSONDecodeError as json_err:
                                logger.error(f"LLM 的輸出不是有效的 JSON 字符串: {json_err}", exc_info=True)
                                logger.warning(f"LLM 原始輸出 (response 字段): {generated_text[:1000]}...")
                                # 返回包含原始文本的字典，以便上層處理或記錄
                                return {"error": "LLM output is not valid JSON", "raw_response_field": generated_text, "ollama_full_response": response_data}
                        else:
                            logger.error("Ollama 響應成功，但 'response' 字段為空或不存在。")
                            return {"error": "Ollama response missing 'response' field", "ollama_full_response": response_data}

                    except json.JSONDecodeError as e:
                        logger.error(f"無法解析 Ollama API 的主 JSON 響應: {e}", exc_info=True)
                        logger.debug(f"Ollama 原始響應體 (無法解析): {response_body_str[:1000]}")
                        return {"error": "Failed to parse Ollama's main JSON response", "raw_body": response_body_str}
                else:
                    logger.error(f"Ollama API 請求失敗，狀態碼: {status_code}, 響應: {response_body_str[:500]}")
                    # 嘗試解析可能的錯誤信息
                    try:
                        error_data = json.loads(response_body_str)
                        error_message = error_data.get("error", "未知 Ollama API 錯誤")
                        logger.error(f"Ollama API 錯誤詳情: {error_message}")
                    except json.JSONDecodeError:
                        pass # 如果錯誤響應不是 JSON
                    return None # 指示請求失敗

        except urllib.error.HTTPError as e:
            error_body_content = ""
            try:
                # 嘗試讀取錯誤響應體。HTTPError 對象的 read() 方法可以被調用。
                error_body_content = e.read().decode('utf-8', 'ignore')
            except Exception as read_decode_err:
                logger.debug(f"讀取或解碼 HTTPError 的響應體失敗: {read_decode_err}。將使用 e.reason。")
                # 如果 read() 或 decode() 失敗，error_body_content 保持為 "" 或使用 e.reason
                error_body_content = f"(Failed to read/decode full error body. Original reason: {e.reason})"
            logger.error(f"Ollama API HTTP 錯誤: {e.code} {e.reason}. Body (部分): {error_body_content[:500]}", exc_info=True) # logging exc_info=True is important
            raise # 重新拋出原始的 HTTPError，讓 retry 機制處理
        except urllib.error.URLError as e:
            logger.error(f"Ollama API URL 錯誤 (Ollama 服務可能未運行或網絡問題): {e.reason}", exc_info=True)
            raise
        except TimeoutError:
            logger.error(f"Ollama API 請求超時 (已等待 {self.request_timeout} 秒)。模型生成可能過慢或服務無響應。", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"與 Ollama API 交互時發生未知錯誤: {e}", exc_info=True)
            raise # 或者返回一個錯誤字典

        return None # 如果所有嘗試都失敗 (理論上 retry 會拋異常)


    def parse_llm_response_to_judgment_fields(self, llm_output_dict: dict) -> dict:
        """
        將 LLM 返回的 (已解析的業務 JSON) 字典映射/驗證到 ai_historical_judgments 表的字段。
        並進行信心評估。

        Args:
            llm_output_dict (dict): 從 LLM 的 "response" 字段解析出來的業務 JSON 字典。
                                   或者一個包含 "error" 鍵的字典，如果 LLM 輸出不是有效 JSON。

        Returns:
            dict: 包含 judgment 各字段的字典。如果解析失敗，某些字段可能為 None 或包含錯誤信息。
                  會額外添加 "parsing_success": True/False 字段。
        """
        judgment_fields = {
            "ai_decision_category": None,
            "ai_recommended_strategy": None, # 存儲為 JSON 字符串
            "ai_rationale_text": None,
            "confidence_score": None,
            "key_warnings_json": None, # 存儲為 JSON 字符串 (列表)
            "raw_llm_response_text": None, # 存儲 LLM "response" 字段的原始文本
            "parsing_success": False,
            "parsing_error_message": None
        }

        if llm_output_dict is None:
            judgment_fields["parsing_error_message"] = "LLM output_dict is None."
            return judgment_fields

        # 如果 LLM 輸出本身就是一個錯誤指示 (例如，不是有效的 JSON)
        if "error" in llm_output_dict:
            judgment_fields["parsing_error_message"] = llm_output_dict["error"]
            judgment_fields["raw_llm_response_text"] = llm_output_dict.get("raw_response_field") or str(llm_output_dict.get("ollama_full_response"))
            return judgment_fields

        # LLM 原始 "response" 字段的內容 (應該是 JSON 字符串，已被解析為 llm_output_dict)
        # 我們將 llm_output_dict (解析後的業務 JSON) 序列化回字符串存儲
        try:
            judgment_fields["raw_llm_response_text"] = json.dumps(llm_output_dict, ensure_ascii=False, indent=2)
        except TypeError: # pragma: no cover
            logger.debug(f"無法將 llm_output_dict 標準序列化為 JSON (將使用 str()): {llm_output_dict}", exc_info=False)
            judgment_fields["raw_llm_response_text"] = str(llm_output_dict) # Fallback to string representation


        # 提取核心字段
        judgment_fields["ai_decision_category"] = llm_output_dict.get("decision_category")
        judgment_fields["ai_rationale_text"] = llm_output_dict.get("rationale")

        recommended_strategy_dict = llm_output_dict.get("recommended_strategy")
        if isinstance(recommended_strategy_dict, dict):
            try:
                judgment_fields["ai_recommended_strategy"] = json.dumps(recommended_strategy_dict, ensure_ascii=False, indent=2)
            except TypeError: # pragma: no cover
                 logger.debug(f"無法將 recommended_strategy_dict 標準序列化為 JSON (將使用 str()): {recommended_strategy_dict}", exc_info=False)
                 judgment_fields["ai_recommended_strategy"] = str(recommended_strategy_dict)
        elif recommended_strategy_dict is not None:
            judgment_fields["ai_recommended_strategy"] = str(recommended_strategy_dict)

        key_warnings_list = llm_output_dict.get("key_risk_factors")
        if isinstance(key_warnings_list, list):
            try:
                judgment_fields["key_warnings_json"] = json.dumps(key_warnings_list, ensure_ascii=False, indent=2)
            except TypeError: # pragma: no cover
                logger.debug(f"無法將 key_warnings_list 標準序列化為 JSON (將使用 str()): {key_warnings_list}", exc_info=False)
                judgment_fields["key_warnings_json"] = str(key_warnings_list)
        elif key_warnings_list is not None:
            judgment_fields["key_warnings_json"] = str(key_warnings_list)


        # 信心評估 (啟發式)
        confidence = llm_output_dict.get("confidence_score")
        if isinstance(confidence, (float, int)) and 0.0 <= confidence <= 1.0:
            judgment_fields["confidence_score"] = float(confidence)
        else:
            # 如果模型沒有直接提供合法的信心分數，我們可以嘗試從 rationale 文本中推斷
            # 這是一個簡化的示例，實際中可能需要更複雜的 NLP
            text_to_analyze = (judgment_fields["ai_rationale_text"] or "") + \
                              (judgment_fields["ai_decision_category"] or "")
            text_to_analyze = text_to_analyze.lower()

            score = 0.5 # 基礎分
            if any(s in text_to_analyze for s in ["強烈", "高度確信", "極有可能", "明確顯示"]): score += 0.3
            elif any(s in text_to_analyze for s in ["建議", "認為", "傾向於", "可能"]): score += 0.1
            if any(s in text_to_analyze for s in ["不確定", "或許", "潛在的", "謹慎", "觀望"]): score -= 0.2
            if any(s in text_to_analyze for s in ["必須", "確認", "顯著"]): score +=0.15

            judgment_fields["confidence_score"] = max(0.0, min(1.0, round(score, 3)))
            logger.debug(f"啟發式評估信心分數: {judgment_fields['confidence_score']} (基於文本)")

        # 檢查是否有關鍵字段缺失 (可以根據需要定義哪些是“關鍵”的)
        if not all([judgment_fields["ai_decision_category"], judgment_fields["ai_rationale_text"]]):
            judgment_fields["parsing_error_message"] = "LLM 響應中缺少關鍵字段 (decision_category 或 rationale)。"
            # parsing_success 仍然可以是 True，表示 JSON 結構體是符合的，只是內容不全
        else:
            judgment_fields["parsing_success"] = True # 至少基本結構符合且關鍵字段存在

        return judgment_fields


if __name__ == "__main__":
    import logging
    import sys, os
    current_dir_la = os.path.dirname(os.path.abspath(__file__))
    project_src_dir_la = os.path.abspath(os.path.join(current_dir_la, '..'))
    project_root_la = os.path.abspath(os.path.join(project_src_dir_la, '..'))
    if project_root_la not in sys.path:
        sys.path.insert(0, project_root_la)

    from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME
    setup_logger(PROJECT_LOGGER_NAME, level=logging.DEBUG)

    logger.info("--- LlamaOllamaAgent (__main__) 測試開始 ---")

    # 假設 Ollama 服務正在本地運行，並且有名為 "test-model" (或 llama3) 的模型
    # 為了 __main__ 測試，我們可能需要 mock urllib.request.urlopen
    # 或者，我們可以嘗試連接到一個真實的 (如果開發環境有) Ollama 服務

    # 測試1: 初始化
    agent = LlamaOllamaAgent(model_name="mock-model") # 使用一個 mock 模型名
    logger.info(f"Agent 初始化: {agent.model_name}, {agent.api_base_url}")

    # 測試2: 準備請求體
    prompt_example = "市場前景如何？"
    req_body = agent._prepare_request_data_generate(prompt_example)
    logger.info(f"生成的請求體: {req_body.decode('utf-8')}")
    req_body_str = req_body.decode('utf-8')
    assert '"model":"mock-model"' in req_body_str
    assert '"prompt":"市場前景如何？"' in req_body_str

    # 測試3: 解析 LLM 響應 (成功案例)
    mock_llm_json_output = {
        "decision_category": "看多",
        "confidence_score": 0.85, # 模型直接提供
        "recommended_strategy": {
            "name": "買入現貨", "description": "在當前價格買入",
            "parameters": {"entry_price_level": 100.0}
        },
        "rationale": "各項指標均顯示上漲趨勢。",
        "key_risk_factors": ["宏觀經濟風險"]
    }
    parsed_fields = agent.parse_llm_response_to_judgment_fields(mock_llm_json_output)
    logger.info(f"解析後的 Judgment 字段 (成功案例): {json.dumps(parsed_fields, indent=2, ensure_ascii=False)}")
    assert parsed_fields["parsing_success"] is True
    assert parsed_fields["ai_decision_category"] == "看多"
    assert parsed_fields["confidence_score"] == 0.85
    assert isinstance(json.loads(parsed_fields["ai_recommended_strategy"]), dict) # 應為 JSON 字符串

    # 測試4: 解析 LLM 響應 (信心啟發式評估)
    mock_llm_json_output_no_score = {
        "decision_category": "強力看空",
        "recommended_strategy": {"name": "賣出期貨"},
        "rationale": "市場出現明確的強力下跌信號，建議立即做空。",
        "key_risk_factors": ["軋空風險"]
    }
    parsed_fields_heuristic_score = agent.parse_llm_response_to_judgment_fields(mock_llm_json_output_no_score)
    logger.info(f"解析後的 Judgment 字段 (啟發式信心): {json.dumps(parsed_fields_heuristic_score, indent=2, ensure_ascii=False)}")
    assert parsed_fields_heuristic_score["parsing_success"] is True
    assert parsed_fields_heuristic_score["confidence_score"] > 0.5 # 啟發式評估應給出一個分數

    # 測試5: 解析 LLM 響應 (LLM 輸出非 JSON)
    mock_llm_output_not_json = {
        "error": "LLM output is not valid JSON",
        "raw_response_field": "這不是JSON，這是一段普通文本。",
        "ollama_full_response": {"response": "這不是JSON，這是一段普通文本。", "done": True}
    }
    parsed_fields_not_json = agent.parse_llm_response_to_judgment_fields(mock_llm_output_not_json)
    logger.info(f"解析後的 Judgment 字段 (LLM非JSON): {json.dumps(parsed_fields_not_json, indent=2, ensure_ascii=False)}")
    assert parsed_fields_not_json["parsing_success"] is False
    assert "LLM output is not valid JSON" in parsed_fields_not_json["parsing_error_message"]
    assert parsed_fields_not_json["raw_llm_response_text"] == "這不是JSON，這是一段普通文本。"

    # 測試6: (可選，如果想實際調用本地 Ollama)
    # 需要本地 Ollama 運行並有名為 "llama3:8b-instruct-q4_K_M" (或您配置的) 模型
    # RUN_REAL_OLLAMA_TEST = False # 設為 True 以運行真實調用測試
    # if RUN_REAL_OLLAMA_TEST:
    #     logger.info("\n--- 嘗試真實 Ollama API 調用 (需要本地服務和模型) ---")
    #     real_agent = LlamaOllamaAgent(model_name="llama3:8b-instruct-q4_K_M") # 或其他你有的模型
    #     test_prompt_ollama = "{\"instruction\": \"你是一個天氣預報員。基於數據：溫度25C，濕度60%，風速5km/h。請預測天氣並以JSON格式返回，包含'forecast' (string) 和 'confidence' (float)字段。\"}"
    #     # 這裡的 prompt 需要符合 Llama3 instruct 的格式，或者是一個簡單的提問
    #     # 為了測試 JSON 解析，讓它返回 JSON
    #     # 更好的做法是使用 PromptGenerator 生成的 prompt
    #     from AI_Assisted_Historical_Backtesting.src.ai_logic.prompt_generator import PromptGenerator
    #     pg = PromptGenerator()
    #     _, test_prompt_ollama_from_pg = pg.generate_market_briefing_json_and_prompt(
    #         "2024-01-01T00:00:00Z", "TEST", {"price_close": 100}, {"news":"good news"}
    #     )

    #     logger.info(f"發送給真實 Ollama 的 Prompt:\n{test_prompt_ollama_from_pg}")
    #     ollama_response = real_agent.send_prompt_to_ollama(test_prompt_ollama_from_pg)

    #     if ollama_response:
    #         logger.info(f"從真實 Ollama 收到的已解析業務 JSON: {json.dumps(ollama_response, indent=2, ensure_ascii=False)}")
    #         parsed_judgment = real_agent.parse_llm_response_to_judgment_fields(ollama_response)
    #         logger.info(f"真實 Ollama 響應解析為 Judgment: {json.dumps(parsed_judgment, indent=2, ensure_ascii=False)}")
    #     else:
    #         logger.error("真實 Ollama API 調用失敗或未返回有效數據。")

    logger.info("--- LlamaOllamaAgent (__main__) 測試結束 ---")
