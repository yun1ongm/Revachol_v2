import pandas as pd
import pandas_ta as pta
import numpy as np


class PatternIdnetifier:

    def __init__(self, kdf) -> None:
        self.kdf = kdf

    def identify_hilo(self):
        candle_hilos = self.kdf[["high", "low", "close"]]
        candle_hilos["atr"] = pta.atr(
            candle_hilos["high"],
            candle_hilos["low"],
            candle_hilos["close"],
            length=self.atr_len,
            mamode="EMA",
        )
        candle_hilos["highs"] = candle_hilos["high"]
        candle_hilos["lows"] = candle_hilos["low"]
        highs_count = 0
        lows_count = 0

        for i in range(1, len(candle_hilos) - 1):
            candle_hilos.at[candle_hilos.index[i], "highs"] = candle_hilos.at[
                candle_hilos.index[i - 1], "highs"
            ]
            candle_hilos.at[candle_hilos.index[i], "lows"] = candle_hilos.at[
                candle_hilos.index[i - 1], "lows"
            ]

            if (
                candle_hilos.at[candle_hilos.index[i], "high"]
                > candle_hilos.at[candle_hilos.index[i - 1], "high"]
                and candle_hilos.at[candle_hilos.index[i], "high"]
                > candle_hilos.at[candle_hilos.index[i + 1], "high"]
            ):
                if (
                    candle_hilos.at[candle_hilos.index[i], "highs"]
                    >= candle_hilos.at[candle_hilos.index[i], "high"]
                ):
                    highs_count += 1
                else:  # 更新高点
                    highs_count = 0
                    candle_hilos.at[candle_hilos.index[i], "highs"] = candle_hilos.at[
                        candle_hilos.index[i], "high"
                    ]
            else:
                highs_count += 1

            if (
                candle_hilos.at[candle_hilos.index[i], "low"]
                < candle_hilos.at[candle_hilos.index[i - 1], "low"]
                and candle_hilos.at[candle_hilos.index[i], "low"]
                < candle_hilos.at[candle_hilos.index[i + 1], "low"]
            ):
                if (
                    candle_hilos.at[candle_hilos.index[i], "lows"]
                    <= candle_hilos.at[candle_hilos.index[i], "low"]
                ):
                    lows_count += 1
                else:  # 更新低点
                    lows_count = 0
                    candle_hilos.at[candle_hilos.index[i], "lows"] = candle_hilos.at[
                        candle_hilos.index[i], "low"
                    ]
            else:
                lows_count += 1

            candle_hilos.at[candle_hilos.index[i], "highs_count"] = highs_count
            candle_hilos.at[candle_hilos.index[i], "lows_count"] = lows_count
        return candle_hilos[
            [
                "highs",
                "lows",
                "highs_count",
                "lows_count",
                "atr",
                "close",
                "high",
                "low",
            ]
        ]

    def identify_pin_bar(self):
        pinbar = self.kdf[["open", "close", "high", "low"]]
        pinbar["body"] = pinbar["open"] - pinbar["close"]
        pinbar["down_wick_ratio"] = (pinbar["open"] - pinbar["low"]) / (pinbar["body"])
        pinbar["up_wick_ratio"] = (pinbar["high"] - pinbar["close"]) / (pinbar["body"])

        pinbar["pin_bar"] = np.where(
            (pinbar["up_wick_ratio"] > 2) & (pinbar["down_wick_ratio"] < 1),
            1,
            np.where(
                (pinbar["down_wick_ratio"] > 2) & (pinbar["up_wick_ratio"] < 1), -1, 0
            ),
        )
        return pinbar["pin_bar"]

    def identify_engulfing(self):
        engulfing = self.kdf[["open", "close", "high", "low"]]
        engulfing["engulfing"] = np.where(
            (engulfing["low"] < engulfing["low"].shift(1))
            & (engulfing["close"] > engulfing["high"].shift(1))
            & (engulfing["close"].shift(1) < engulfing["open"].shift(1)),
            1,
            np.where(
                (engulfing["high"] > engulfing["high"].shift(1))
                & (engulfing["close"] < engulfing["low"].shift(1))
                & (engulfing["close"].shift(1) > engulfing["open"].shift(1)),
                -1,
                0,
            ),
        )
        return engulfing["engulfing"]

    def identify_inside_bar(self):
        inside_bar = self.kdf[["open", "close", "high", "low"]]
        inside_bar["inside_bar"] = np.where(
            (inside_bar["high"] <= inside_bar["high"].shift(1))
            & (inside_bar["low"] >= inside_bar["low"].shift(1)),
            1,
            0,
        )
        return inside_bar["inside_bar"]

    def indentify_outside_bar(self):
        outside_bar = self.kdf[["open", "close", "high", "low"]]
        outside_bar["outside_bar"] = np.where(
            (outside_bar["high"] > outside_bar["high"].shift(1))
            & (outside_bar["low"] < outside_bar["low"].shift(1)),
            1,
            0,
        )
        return outside_bar["outside_bar"]

    def indentify_oneside_bar(self):
        oneside_bar = self.kdf[["open", "close", "high", "low"]]
        oneside_bar["oneside_bar"] = np.where(
            (oneside_bar["high"] > oneside_bar["high"].shift(1))
            & (oneside_bar["low"] >= oneside_bar["low"].shift(1)),
            1,
            np.where(
                (oneside_bar["low"] < oneside_bar["low"].shift(1))
                & (oneside_bar["high"] <= oneside_bar["high"].shift(1)),
                -1,
                0,
            ),
        )
        return oneside_bar["oneside_bar"]
