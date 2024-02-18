import logging
import warnings
warnings.filterwarnings("ignore")
import time
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from model.model_urban import ModelUrban
from model.execution_postmodern import ExecPostmodern
import contek_timbersaw as timbersaw
timbersaw.setup()

class AlgoTrade:
    logger = logging.getLogger(__name__)

    def __init__(self, interval: int = 10):
        self.model = ModelUrban()
        self.execution = ExecPostmodern()
        self.interval = interval

    def _calc_signal_position(self) -> None:
        previous_signal_position = self.model.signal_position if self.model.signal_position is not None else 0
        self.model.market5m.update_CKlines()
        self.model.merging_signal()
        if self.model.signal_position != previous_signal_position:
            change = self.model.signal_position - previous_signal_position
            self.model.logger.warning(
                f"Signal Position Change:{change}\n-- -- -- -- -- -- -- -- --"
            )

    def run(self) -> None:
        while True:
            try:
                self._calc_signal_position()
                if self.model.signal_position:
                    complete = self.execution.task(self.model.signal_position)
                    if complete:
                        time.sleep(self.interval)
                    else:
                        time.sleep(self.interval / 3)
            except Exception as e:
                self.logger.exception(e)
                time.sleep(self.interval / 3)


if __name__ == "__main__":
    algo = AlgoTrade(15)
    algo.run()
