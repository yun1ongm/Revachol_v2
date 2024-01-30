import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from model.model_urban import ModelUrban
from model.execution_postmodern import ExecPostmodern
import threading
import contek_timbersaw as timbersaw
timbersaw.setup()

class AlgoTrade:
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.model = ModelUrban(15)
        self.execution = ExecPostmodern(10)
        self.signal_position = None  # Placeholder for position signal

    def _alpha_loop(self) -> None:
        while True:
            try:
                self.model.market5m.update_CKlines()
                previous_signal_position = self.signal_position
                self.signal_position = self.model.merging_signal()
                if self.signal_position != previous_signal_position:
                    change = self.signal_position - previous_signal_position
                    self.logger.warning(
                        f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --"
                    )
                time.sleep(self.model.interval)
            except Exception as e:
                self.logger.exception(e)
                time.sleep(self.model.interval / 5)

    def _execution_loop(self):
        """execute orders based on the signal"""
        while True:
            try:
                if self.signal_position is not None:
                    self.execution.task(self.signal_position)
                    time.sleep(self.execution.interval)
            except Exception as e:
                self.logger.exception(e)
                time.sleep(self.execution.interval / 5)

    def run(self):
        alpha_thread = threading.Thread(target=self._alpha_loop)
        execution_thread = threading.Thread(target=self._execution_loop)

        alpha_thread.start()
        execution_thread.start()

        alpha_thread.join()
        execution_thread.join()


if __name__ == "__main__":
    AlgoTrade().run()
