# 全景市場分析儀 (Panoramic Market Analyzer)

本專案旨在建立一個基於微服務架構的、穩健的金融數據處理管道。

## 架構核心思想

我們遵循「職責分離」原則，將系統解構成一系列獨立的命令列工具。

## 品質保證 (Quality Assurance)

我們採用分層的品質保證策略，確保程式碼的健壯性。

### 步驟一：靜態程式碼分析 (Linting)

在提交任何程式碼前，請先執行 `flake8` 來檢查語法及風格錯誤。

```bash
bash run_lint.sh
```

### 步驟二：單元測試 (Unit Testing)

我們使用 `pytest` 進行單元測試，確保每個服務的邏輯正確。

```bash
bash run_tests.sh
```

### 建議流程：執行完整的品質檢查

為了方便起見，您應該在日常開發中使用主控腳本，它會自動依序執行 `linting` 和 `testing`。

```bash
bash run_quality_checks.sh
```

只有當這個指令成功執行，才代表您的程式碼達到了可以提交的品質標準。

## 如何管理持續失敗的測試

在開發過程中，有時會遇到暫時無法修復或預期之內會失敗的測試。為了避免這些測試阻礙開發進程，您可以使用 `pytest` 的標記 (markers) 來管理它們。

**1. 暫時跳過測試 (Skip)**

如果您正在重構某個功能，導致相關測試暫時無法通過，您可以標記它為 `skip`。

**範例：**

```python
import pytest

@pytest.mark.skip(reason="正在重構數據處理邏輯，此測試暫時停用")
def test_complex_feature_calculation():
    # ... 測試程式碼 ...
    pass
```

執行測試時，這個測試會被直接跳過，並在報告中標示為 `SKIPPED`，這樣您就不會忘記未來需要回來修復它。

**2. 標示為預期失敗 (XFAIL - Expected Failure)**

如果您正在修復一個 bug，並為此編寫了一個測試，但 bug 本身還沒修好，您可以將該測試標記為 `xfail`。

**範例：**

```python
import pytest

@pytest.mark.xfail(reason="已知 bug #123 尚未修復")
def test_edge_case_handling():
    # 這個測試目前會因為 bug 而失敗
    assert some_function_with_bug() == False
```

執行測試時，`pytest` 會運行此測試：

  * 如果它如預期般**失敗**了，整個測試套件會被視為**通過**，並在報告中標示為 `XFAIL`。
  * 如果它出乎意料地**通過**了（可能代表 bug 被意外修好了），報告會標示為 `XPASS`，提醒您移除 `@pytest.mark.xfail` 標記。

**請在 `tests/` 目錄下的測試檔案中，靈活運用這些標記來管理您的測試狀態。**

## 如何執行完整數據管道

```bash
bash run_pipeline.sh
```
