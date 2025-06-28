import threading
import psutil
import time
from .logger import Logger


class HardwareMonitor:
    """硬體資源監控器。

    這個類別提供了一個背景執行緒來定期監控 CPU、記憶體和磁碟使用情況，
    並將狀態輸出到控制台。它需要一個 Logger 物件來記錄啟動和停止訊息。

    :ivar logger: 用於記錄日誌的 Logger 物件執行個體。
    :vartype logger: data_pipeline_v15.utils.logger.Logger
    :ivar _interval: 監控狀態的更新間隔（秒）。
    :vartype _interval: int
    :ivar _thread: 背景監控執行緒的 Thread 物件。
    :vartype _thread: threading.Thread
    :ivar _stop_event: 用於通知監控執行緒停止的 Event 物件。
    :vartype _stop_event: threading.Event

    使用範例:
        >>> from data_pipeline_v15.utils.logger import Logger # 修正導入路徑
        >>> logger_instance = Logger('monitor_test.log')
        >>> monitor = HardwareMonitor(logger=logger_instance, interval=5)
        >>> monitor.start()
        >>> # ... 執行一些操作 ...
        >>> monitor.stop()
    """

    def __init__(self, logger, interval=2):
        """初始化 HardwareMonitor 物件。

        :param logger: 用於記錄啟動和停止訊息的 Logger 物件。
        :type logger: data_pipeline_v15.utils.logger.Logger
        :param interval: 監控資訊更新的間隔時間（秒），預設為 2 秒。
        :type interval: int, optional
        """
        self.logger = logger
        self._interval = interval
        self._thread = None
        self._stop_event = threading.Event()

    def _monitor(self):
        """內部監控迴圈，定期獲取並顯示硬體狀態。

        這個方法會在一個獨立的執行緒中執行，直到 _stop_event 被設定。
        它會捕捉所有預期的例外，以避免執行緒意外終止。
        """
        # """Internal method to monitor hardware resources.""" # 此為英文 docstring，但已由上方中文 docstring 取代
        while not self._stop_event.is_set():
            try:
                cpu = psutil.cpu_percent()
                mem = psutil.virtual_memory()
                disk = psutil.disk_usage("/")
                # 確保狀態訊息打印在單行然後被清除
                # \r (歸位字元) 將游標移至行首
                status = f"⏱️  中央處理器: {cpu:5.1f}% | 記憶體: {mem.percent:5.1f}% ({mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB) | 磁碟: {disk.percent:5.1f}% \r"
                print(status, end="")
                # 強制刷新 stdout 以確保即時打印，尤其在某些執行環境中
                # sys.stdout.flush() # 需要 import sys，根據 logger 行為考慮是否必要

                # 等待指定的時間間隔或直到停止事件被設定
                self._stop_event.wait(timeout=self._interval)
            except Exception as e:
                # 可選：如果 logger 可用且期望記錄，則記錄例外
                # self.logger.log(f"Error in monitor thread: {e}", "error")
                pass  # 靜默繼續或處理錯誤

    def start(self):
        """啟動背景硬體監控執行緒。

        如果監控執行緒尚未啟動或已停止，此方法會初始化並啟動一個新的 daemon 執行緒
        來執行 _monitor 方法。會使用 Logger 記錄啟動訊息。
        """
        # """Starts the hardware monitoring thread.""" # 此為英文 docstring，但已由上方中文 docstring 取代
        if (
            not self._thread or not self._thread.is_alive()
        ):  # 同時檢查執行緒是否存活
            self.logger.log("啟動高頻率即時硬體監控...", "info")
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._monitor, daemon=True)
            self._thread.start()
        else:
            self.logger.log("硬體監控已經在運行中。", "info")

    def stop(self):
        """停止背景硬體監控執行緒。

        如果監控執行緒正在執行，此方法會設定停止事件，並等待執行緒結束。
        執行緒結束後，會清除控制台的最後一行狀態輸出，並使用 Logger 記錄停止訊息。
        """
        """Stops the hardware monitoring thread."""
        if self._thread and self._thread.is_alive():
            self._stop_event.set()
            self._thread.join(
                timeout=self._interval + 0.5
            )  # 給予執行緒多一點時間來結束
            # 清除先前打印狀態訊息的該行
            # 確保長度足以覆蓋先前的狀態訊息
            print(
                "\r" + " " * 100 + "\r", end=""
            )  # 歸位字元、空格、再次歸位字元
            # sys.stdout.flush() # 如果在 _monitor 中使用了 sys.stdout.flush()
            self.logger.log("停止高頻率即時硬體監控...", "info")
            self._thread = None
        else:
            self.logger.log("硬體監控尚未啟動或已停止。", "info")

def get_hardware_usage(context_message: str = "") -> str:
    """
    Retrieves current hardware usage (CPU, Memory, Disk) as a formatted string.

    Args:
        context_message (str, optional): A message to prepend to the usage string. Defaults to "".

    Returns:
        str: A formatted string detailing current hardware usage.
    """
    try:
        cpu = psutil.cpu_percent(interval=0.1) # Short interval for a quick reading
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("/")

        usage_str = (
            f"CPU: {cpu:5.1f}% | "
            f"Memory: {mem.percent:5.1f}% ({mem.used/1024**3:.2f}/{mem.total/1024**3:.2f} GB) | "
            f"Disk: {disk.percent:5.1f}%"
        )
        if context_message:
            return f"{context_message} - {usage_str}"
        return usage_str
    except Exception as e:
        # In case of any error fetching psutil info, return a placeholder or error message
        error_msg = f"Error fetching hardware usage: {e}"
        if context_message:
            return f"{context_message} - {error_msg}"
        return error_msg
