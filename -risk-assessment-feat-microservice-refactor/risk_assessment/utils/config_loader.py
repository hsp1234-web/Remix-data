# utils/config_loader.py
# ----------------------------------------------------
import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

def load_config():
    """載入並回傳全域設定檔"""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

# 讓設定檔成為一個可以全域引用的單例
config = load_config()
