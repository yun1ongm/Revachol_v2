import warnings
warnings.filterwarnings("ignore")
import time
import sys
temp_path = "/Users/rivachol/Desktop/Rivachol_v2/Elysium"
sys.path.append(temp_path)
from model.model_urban import ModelUrban
from model.execution_postmodern import ExecPostmodern
import threading


class AlgoTrade:
    def __init__(self):
        self.model = ModelUrban(15)
        self.execution = ExecPostmodern(10)
        self.signal_position = None  # Placeholder for position signal

    def alpha_loop(self) -> None:
        while True:
            try:
                self.model.market5m.update_CKlines()
                self.signal_position = round(self.model.merging_signal(), 2)
                time.sleep(self.model.interval)
            except Exception as e:
                print(e)
                time.sleep(self.model.interval / 5)

    def execution_loop(self):
        """execute orders based on the signal"""
        while True:
            try:
                self.execution.task(self.signal_position)
                time.sleep(self.execution.interval)
            except Exception as e:
                print(e)
                time.sleep(self.execution.interval / 5)

    def run(self):
        alpha_thread = threading.Thread(target=self.alpha_loop)
        execution_thread = threading.Thread(target=self.execution_loop)

        alpha_thread.start()
        execution_thread.start()

        alpha_thread.join()
        execution_thread.join()


if __name__ == "__main__":
    AlgoTrade().run()
