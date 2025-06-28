import os
import requests
import time
import logging
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, ValidationError, field_validator
import json # Ensure json is imported

# Configure logger for this module
logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

class AIDecisionModel(BaseModel):
    """
    Pydantic model to validate the structure of the AI's JSON response.
    """
    strategy_summary: str  # e.g., "看多", "看空", "中立"
    key_factors: List[str] # List of strings representing key factors considered
    confidence_score: float # A score between 0.0 and 1.0

    @field_validator('confidence_score')
    def score_must_be_between_0_and_1(cls, value: float) -> float:
        if not (0.0 <= value <= 1.0):
            raise ValueError("Confidence score must be between 0.0 and 1.0")
        return value

class RemoteAIAgent:
    """
    Agent to interact with a remote Large Language Model (LLM) service.
    """
    def __init__(self, api_key: str, api_endpoint: str, default_model: str = "claude-3-opus-20240229", max_retries: int = 3, retry_delay_seconds: int = 5, api_call_delay_seconds: float = 1.0):
        """
        Initializes the RemoteAIAgent.

        Args:
            api_key (str): The API key for the LLM service.
            api_endpoint (str): The endpoint URL for the LLM service.
            default_model (str): The default model to use for the AI service.
            max_retries (int): Maximum number of retries for API calls upon failure.
            retry_delay_seconds (int): Delay in seconds between retries.
            api_call_delay_seconds (float): Minimum delay between consecutive API calls to respect rate limits.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")
        if not api_endpoint:
            raise ValueError("API endpoint cannot be empty.")

        self.api_key = api_key
        self.api_endpoint = api_endpoint
        self.default_model = default_model
        self.max_retries = max_retries
        self.retry_delay_seconds = retry_delay_seconds
        self.api_call_delay_seconds = api_call_delay_seconds
        self.last_api_call_time = 0

    def _ensure_api_call_delay(self):
        """Ensures a minimum delay between API calls."""
        current_time = time.time()
        elapsed_time = current_time - self.last_api_call_time
        if elapsed_time < self.api_call_delay_seconds:
            time.sleep(self.api_call_delay_seconds - elapsed_time)
        self.last_api_call_time = time.time()

    def _construct_prompt(self, market_data: Dict[str, Any]) -> str:
        """
        Constructs a detailed prompt for the LLM based on market data.
        The prompt instructs the AI to return a JSON object adhering to AIDecisionModel.
        """
        market_data_str = json.dumps(market_data, indent=2, ensure_ascii=False)
        json_schema_description = """
        {
          "strategy_summary": "string (e.g., '看多', '看空', '中立', '謹慎看多', '積極看空')",
          "key_factors": ["string", "string", "..."],
          "confidence_score": "float (between 0.0 and 1.0)"
        }
        """
        prompt = f"""
        您是一位專業的金融市場分析師。請根據以下提供的市場數據，分析當前的市場狀況，並以嚴格的 JSON 格式提供您的投資策略摘要、關鍵影響因子以及信心指數。

        市場數據：
        ```json
        {market_data_str}
        ```

        您的回應必須是一個 JSON 物件，且嚴格符合以下結構：
        ```json
        {json_schema_description}
        ```

        請確保：
        1. `strategy_summary` 欄位用簡潔的中文描述您的總體策略（例如：看多、看空、中立）。
        2. `key_factors` 欄位是一個包含至少三個字串的陣列，列出您做出此判斷所依賴的最關鍵市場因素。
        3. `confidence_score` 欄位是一個介於 0.0 到 1.0 之間的浮點數，代表您對此策略的信心程度。
        4. 您的回應中，除了這個 JSON 物件之外，不要包含任何其他文字、解釋或註解。JSON 物件本身必須是頂級元素。

        JSON 回應：
        """
        return prompt.strip()

    def get_decision(self, market_data: Dict[str, Any], model_override: Optional[str] = None) -> tuple[Optional[AIDecisionModel], Optional[str]]:
        prompt = self._construct_prompt(market_data)
        selected_model = model_override if model_override else self.default_model

        headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "Anthropic-Version": "2023-06-01"
        }

        payload = {
            "model": selected_model,
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}]
        }

        raw_ai_response_content = None

        for attempt in range(self.max_retries):
            self._ensure_api_call_delay()
            logger.info(f"Attempt {attempt + 1} of {self.max_retries} to call AI service (model: {selected_model}).")
            try:
                response = requests.post(self.api_endpoint, headers=headers, json=payload, timeout=60)
                response.raise_for_status()
                raw_ai_response_content = response.text

                try:
                    response_json = response.json()
                    ai_output_str = ""
                    if 'content' in response_json and isinstance(response_json['content'], list) and response_json['content']:
                        if 'text' in response_json['content'][0]:
                           ai_output_str = response_json['content'][0]['text']
                        else:
                            logger.error(f"AI response JSON does not contain 'text' in content[0]: {response_json}")
                            ai_output_str = str(response_json)
                    else:
                        logger.warning(f"Unexpected AI response JSON structure: {response_json}. Attempting to use the whole response as JSON.")
                        ai_output_str = json.dumps(response_json)
                except json.JSONDecodeError:
                    logger.warning(f"AI response was not valid JSON. Raw response: {raw_ai_response_content}")
                    ai_output_str = raw_ai_response_content

                try:
                    json_start_index = ai_output_str.find('{')
                    json_end_index = ai_output_str.rfind('}') + 1
                    if json_start_index != -1 and json_end_index != -1 and json_start_index < json_end_index:
                        potential_json_str = ai_output_str[json_start_index:json_end_index]
                        parsed_json = json.loads(potential_json_str)
                        decision_data = AIDecisionModel(**parsed_json)
                        logger.info(f"AI decision successfully parsed and validated: {decision_data.model_dump_json(indent=2)}")
                        return decision_data, raw_ai_response_content
                    else:
                        raise ValueError("No JSON object found in AI output.")
                except (json.JSONDecodeError, ValidationError, ValueError) as e_parse: # Corrected variable name e
                    logger.error(f"Failed to parse or validate AI's JSON output. Error: {e_parse}. Raw output: '{ai_output_str}'") # Corrected variable name e

            except requests.exceptions.HTTPError as e_http:
                logger.error(f"HTTP error occurred: {e_http}. Response: {e_http.response.text if e_http.response else 'No response text'}")
                raw_ai_response_content = e_http.response.text if e_http.response else f"HTTP Error: {e_http.response.status_code if e_http.response else 'Unknown'}"
                if e_http.response and 500 <= e_http.response.status_code < 600:
                    logger.info(f"Server error, retrying in {self.retry_delay_seconds}s...")
                else:
                    logger.error("Client-side HTTP error, not retrying for this reason.")
                    break
            except requests.exceptions.RequestException as e_req:
                logger.error(f"Request exception occurred: {e_req}")
                raw_ai_response_content = f"Request Exception: {str(e_req)}"

            if attempt < self.max_retries - 1:
                time.sleep(self.retry_delay_seconds)
            else:
                logger.error(f"Max retries ({self.max_retries}) reached. Failed to get valid decision from AI.")
                return None, raw_ai_response_content
        return None, raw_ai_response_content

if __name__ == '__main__':
    print("Running ai_agent.py example...")
    api_key_env_var = "ANTHROPIC_API_KEY"
    api_endpoint_env_var = "ANTHROPIC_ENDPOINT"
    api_key = os.getenv(api_key_env_var)
    api_endpoint = os.getenv(api_endpoint_env_var, "https://api.anthropic.com/v1/messages")

    if not api_key:
        print(f"Error: API key environment variable '{api_key_env_var}' not set.")
        print("Skipping RemoteAIAgent example.")
    else:
        print(f"Using API Key from env var: {api_key_env_var}")
        print(f"Using API Endpoint: {api_endpoint}")
        agent = RemoteAIAgent(api_key=api_key, api_endpoint=api_endpoint)
        sample_market_data = {
            "current_date": "2023-10-26",
            "dealer_stress_index": {"current_value": 65.7, "trend": "上升", "level_description": "高度緊張"},
            "key_indicators": {
                "MOVE_Index": 130.5, "10Y_2Y_Spread_bps": -35.2,
                "Primary_Dealer_Net_Positions_USD_Millions": -25000.0, "VIX_Index": 22.1,
                "SOFR_Rate_Annualized_Pct": 5.31
            },
            "recent_news_summary": [
                "聯準會主席暗示可能需要進一步升息以對抗通膨。",
                "大型科技公司財報優於預期，但對未來展望保守。",
                "地緣政治緊張局勢加劇，油價上漲。"
            ]
        }
        decision_model_instance, raw_response = agent.get_decision(sample_market_data)
        if decision_model_instance:
            print("\n--- AI Decision Received ---")
            print(f"Strategy Summary: {decision_model_instance.strategy_summary}")
            print(f"Key Factors: {decision_model_instance.key_factors}")
            print(f"Confidence Score: {decision_model_instance.confidence_score}")
            print(f"Raw AI Response snippet: {(raw_response[:200] + '...') if raw_response and len(raw_response) > 200 else raw_response}")
        else:
            print("\n--- Failed to get AI Decision ---")
            print(f"Raw AI Response (if any): {raw_response}")

    print("\n--- Testing AIDecisionModel validation ---")
    valid_data = {"strategy_summary": "看多", "key_factors": ["財報佳", "技術面突破"], "confidence_score": 0.8}
    try:
        model = AIDecisionModel(**valid_data)
        print(f"Valid model created: {model.model_dump_json(indent=2)}")
    except ValidationError as e:
        print(f"Validation error for valid_data: {e}")

    invalid_data_score = {"strategy_summary": "看空", "key_factors": ["市場恐慌"], "confidence_score": 1.5}
    try:
        model = AIDecisionModel(**invalid_data_score)
        print(f"Invalid model (score) created: {model.model_dump_json(indent=2)}")
    except ValidationError as e:
        print(f"Successfully caught validation error for invalid_data_score: {e}")

    invalid_data_type = {"strategy_summary": 123, "key_factors": "not a list", "confidence_score": 0.5}
    try:
        model = AIDecisionModel(**invalid_data_type)
        print(f"Invalid model (type) created: {model.model_dump_json(indent=2)}")
    except ValidationError as e:
        print(f"Successfully caught validation error for invalid_data_type: {e}")

    if 'agent' in locals() and api_key: # Check if agent was initialized
        print("\n--- Testing Prompt Construction ---")
        prompt_generated = agent._construct_prompt(sample_market_data)
        print(prompt_generated)
    else:
        print("Skipping prompt construction test as agent was not initialized (API key missing).")

# Ensure json is imported if running this file directly for the __main__ block
# import json # Already imported at the top of the file
