#!/bin/bash

# =================================================================
# 原子化執行腳本 V2：修正、驗證並記錄 FinMind Connector
# 目標：解決 FinMindConnector 的數據獲取問題，並將全過程記錄存檔。
# 執行內容：
# 1. 建立隔離的專案環境。
# 2. 編寫重構後的 finmind_connector.py。
# 3. 編寫對應的單元測試 test_finmind_connector.py。
# 4. 使用 poetry 安裝依賴並執行 pytest。
# 5. [新] 使用 tee 命令將所有標準輸出複製到日誌檔案 finmind_fix_log.txt。
# =================================================================

# 使用 tee 將所有後續輸出重定向到日誌檔案和標準輸出
exec &> >(tee -a "finmind_fix_log.txt")

echo "================================================="
echo "執行開始時間: $(date)"
echo "================================================="

# --- 第一步：環境建構 ---
echo "[1/5] 正在建立專案目錄結構..."
mkdir -p project_finmind_fix/connectors
touch project_finmind_fix/connectors/__init__.py
mkdir -p project_finmind_fix/tests
touch project_finmind_fix/tests/__init__.py
cd project_finmind_fix

# --- 第二步：定義依賴 (pyproject.toml) ---
echo "[2/5] 正在建立 pyproject.toml..."
cat << 'EOF' > pyproject.toml
[tool.poetry]
name = "finmind-fix-validation"
version = "0.1.0"
description = "A project to fix and validate the FinMind connector."
authors = ["Jules <ai-engineer@example.com>"]
packages = [{include = "connectors", from = "."}]

[tool.poetry.dependencies]
python = ">=3.9,<3.12"
pandas = "^2.2.0"
finmind = "^1.8.1"
pytest = "^8.0.0"
tqdm = "^4.66.0"

[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"
EOF

# --- 第三步：生成重構後的 Connector 程式碼 ---
echo "[3/5] 正在生成重構後的 finmind_connector.py..."
cat << 'EOF' > connectors/finmind_connector.py
import pandas as pd
from FinMind.data import FinMindApi
import logging

# 設定基礎日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FinMindConnector:
    """
    用於從 FinMind API 獲取台股深度數據的連接器。
    V2: 根據 FinMind SDK 1.8.1 的變更，修正了獲取財報與籌碼數據的方法。
    """
    def __init__(self, api_token: str):
        self.api = FinMindApi()
        try:
            self.api.login(token=api_token)
            logging.info("FinMind API 登入成功。")
        except Exception as e:
            logging.error(f"FinMind API 登入失敗: {e}")
            raise

    def get_financial_statements(self, stock_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        獲取指定股票的綜合損益表。
        修正：使用 api.get_stock_financial_statement 替換舊的泛用方法。
        """
        logging.info(f"正在獲取 {stock_id} 從 {start_date} 到 {end_date} 的財報...")
        try:
            df = self.api.get_stock_financial_statement(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            logging.info(f"成功獲取 {len(df)} 筆財報數據。")
            return df
        except Exception as e:
            logging.error(f"從 FinMind 獲取財報時出錯: {e}")
            return pd.DataFrame() # 返回空的 DataFrame 以確保穩健性

    def get_institutional_investors(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        獲取三大法人的買賣超資訊。
        修正：使用 api.get_stock_chip_institutional_investors 替換舊的泛用方法。
        """
        logging.info(f"正在獲取從 {start_date} 到 {end_date} 的法人籌碼數據...")
        try:
            df = self.api.get_stock_chip_institutional_investors(
                start_date=start_date,
                end_date=end_date,
            )
            logging.info(f"成功獲取 {len(df)} 筆法人籌碼數據。")
            return df
        except Exception as e:
            logging.error(f"從 FinMind 獲取法人籌碼時出錯: {e}")
            return pd.DataFrame() # 返回空的 DataFrame
EOF

# --- 第四步：生成單元測試程式碼 ---
echo "[4/5] 正在生成單元測試 test_finmind_connector.py..."
cat << 'EOF' > tests/test_finmind_connector.py
import os
import pandas as pd
import pytest
from connectors.finmind_connector import FinMindConnector

# 從環境變數讀取 API Token
API_TOKEN = os.getenv("FINMIND_API_TOKEN")

@pytest.fixture(scope="module")
def connector():
    """提供一個 FinMindConnector 的實例。如果沒有 Token，則跳過所有測試。"""
    if not API_TOKEN:
        pytest.skip("FINMIND_API_TOKEN 未設定，跳過所有 FinMind 整合測試。")
    return FinMindConnector(api_token=API_TOKEN)

def test_get_financial_statements(connector):
    """測試獲取財報數據的功能 (台積電 2330)。"""
    print("\n執行測試: test_get_financial_statements")
    df = connector.get_financial_statements(stock_id="2330", start_date="2023-01-01", end_date="2023-03-31")
    assert isinstance(df, pd.DataFrame), "返回值應為 DataFrame"
    assert not df.empty, "返回的財報 DataFrame 不應為空"
    # 增加一個欄位檢查，確保獲取的是財報數據
    assert 'revenue' in df.columns, "財報數據中應包含 'revenue' 欄位"
    print("測試成功: 財報數據獲取成功，且格式正確。")

def test_get_institutional_investors(connector):
    """測試獲取三大法人籌碼數據的功能。"""
    print("\n執行測試: test_get_institutional_investors")
    df = connector.get_institutional_investors(start_date="2024-01-02", end_date="2024-01-02")
    assert isinstance(df, pd.DataFrame), "返回值應為 DataFrame"
    assert not df.empty, "返回的籌碼 DataFrame 不應為空"
    # 增加一個欄位檢查，確保獲取的是法人數據
    assert 'buy' in df.columns and 'sell' in df.columns, "法人數據中應包含 'buy' 和 'sell' 欄位"
    print("測試成功: 法人籌碼數據獲取成功，且格式正確。")
EOF

# --- 第五步：安裝依賴並執行測試 ---
echo "[5/5] 正在安裝依賴並執行測試..."

# 檢查 Poetry 是否已安裝
if ! command -v poetry &> /dev/null
then
    echo "Poetry 未安裝，正在使用 pip 安裝..."
    pip install poetry
fi

# 使用 poetry 安裝專案依賴
poetry install

# 執行 pytest
# 再次檢查環境變數，以決定是否執行測試
if [ -z "$FINMIND_API_TOKEN" ]; then
    echo "警告：FINMIND_API_TOKEN 未設定。pytest 將自動跳過需要 API 金鑰的測試。"
    poetry run pytest -v
else
    echo "偵測到 FINMIND_API_TOKEN，開始執行完整測試..."
    poetry run pytest -v
fi

echo ""
echo "================================================="
echo "執行結束時間: $(date)"
echo "日誌已儲存至: finmind_fix_log.txt"
echo "================================================="
