import unittest
from unittest.mock import patch
from model.run import AlgoTrade


class TestAlgoTrade(unittest.TestCase):
    @patch("model.run.ModelUrban")
    @patch("model.run.ExecPostmodern")
    def test_init(self, mock_exec_postmodern, mock_model_urban):
        algo_trade = AlgoTrade()

        mock_model_urban.assert_called_with(15)
        mock_exec_postmodern.assert_called_with(10)
        self.assertIsNone(algo_trade.signal_position)

    @patch("model.run.ModelUrban")
    @patch("model.run.ExecPostmodern")
    @patch("model.run.time.sleep")
    def test_alpha_loop(self, mock_sleep, mock_exec_postmodern, mock_model_urban):
        algo_trade = AlgoTrade()
        algo_trade.model.market5m.update_CKlines.return_value = None
        algo_trade.model.merging_signal.return_value = "BUY"

        algo_trade.alpha_loop()

        algo_trade.model.market5m.update_CKlines.assert_called_once()
        algo_trade.model.merging_signal.assert_called_once()
        mock_sleep.assert_called_with(algo_trade.model.interval)

    @patch("model.run.ExecPostmodern")
    @patch("model.run.time.sleep")
    def test_execution_loop(self, mock_sleep, mock_exec_postmodern):
        algo_trade = AlgoTrade()
        algo_trade.signal_position = "SELL"

        algo_trade.execution_loop()

        algo_trade.execution.task.assert_called_with("SELL")
        mock_sleep.assert_called_with(algo_trade.execution.interval)

    @patch("model.run.threading.Thread")
    def test_run(self, mock_thread):
        algo_trade = AlgoTrade()

        algo_trade.run()

        self.assertEqual(mock_thread.call_count, 2)
        mock_thread.assert_any_call(target=algo_trade.alpha_loop)
        mock_thread.assert_any_call(target=algo_trade.execution_loop)
        mock_thread.return_value.start.assert_called()
        mock_thread.return_value.join.assert_called()


if __name__ == "__main__":
    unittest.main()
