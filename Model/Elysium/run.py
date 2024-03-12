import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
import threading
temp_path = "/Users/rivachol/Desktop/Rivachol_v2"
sys.path.append(temp_path)
from Elysium.model.model_urban import ModelUrban
from Elysium.model.execution_postmodern import ExecPostmodern
import contek_timbersaw as timbersaw
timbersaw.setup()

class AlgoTrade:
    logger = logging.getLogger(__name__)

    def __init__(self, interval: int, symbol: str, timeframe: str) -> None:
        self.model = ModelUrban(symbol, timeframe)
        self.execution = ExecPostmodern(symbol)
        self.interval = interval
        self.model_thread = threading.Thread(target=self._calc_signal_position)
        self.execution_thread = threading.Thread(target=self._execute_task)

    def _calc_signal_position(self) -> None:
        while True:
            previous_signal_position = self.model.signal_position if self.model.signal_position is not None else 0
            self.model.market.update_CKlines()
            self.model.merging_signal()
            if self.model.signal_position != previous_signal_position:
                change = self.model.signal_position - previous_signal_position
                self.model.logger.warning(
                    f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --"
                )
            time.sleep(self.interval)

    def _execute_task(self) -> None:
        while True:
            if self.model.signal_position is not None:
                complete = self.execution.task(self.model.signal_position)
                if complete:
                    time.sleep(self.interval)
                else:
                    time.sleep(self.interval / 5)
            else:
                self.execution.logger.warning(
                    f"signal_position is not calculated!\n-- -- -- -- -- -- -- -- --"
                )
                time.sleep(self.interval)

    def run(self) -> None:
        self.model_thread.start()
        self.execution_thread.start()

if __name__ == "__main__":
    algo = AlgoTrade(15, "BTCUSDT", "5m")
    algo.run()
