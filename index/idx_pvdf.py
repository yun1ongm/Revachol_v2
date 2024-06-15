import pandas as pd
import pandas_ta as pta
import numpy as np


class IdxPvdf:

    index_name = "idx_pvdf"

    def __init__(self, kdf) -> None:
        self.kdf = kdf

    def pvdf(self) -> pd.DataFrame:
        pvdf = self.kdf[["close", "volume_U"]]
        pvdf["em10"] = pta.ema(pvdf["close"], length=10)
        pvdf["ema30"] = pta.ema(pvdf["close"], length=30)
        pvdf["ema60"] = pta.ema(pvdf["close"], length=60)
        pvdf["ema100"] = pta.ema(pvdf["close"], length=100)
        pvdf["ema200"] = pta.ema(pvdf["close"], length=200)
        pvdf.fillna(method="bfill", inplace=True)
        # 计算5条ema均线在截面上的标准差
        pvdf["std"] = pvdf[["em10", "ema30", "ema60", "ema100", "ema200"]].std(axis=1)
        # 对截面std进行1+std后取log
        pvdf["pdf"] = np.log(1 + pvdf["std"])
        # 计算成交量的均值
        pvdf["vol5"] = pvdf["volume_U"].rolling(window=5).mean()
        pvdf["vol10"] = pvdf["volume_U"].rolling(window=10).mean()
        pvdf["vol20"] = pvdf["volume_U"].rolling(window=20).mean()
        pvdf["vol60"] = pvdf["volume_U"].rolling(window=60).mean()
        pvdf.fillna(method="bfill", inplace=True)
        # 计算5条成交量均线的标准差
        pvdf["vol_std"] = pvdf[["volume_U", "vol5", "vol10", "vol20", "vol60"]].std(
            axis=1
        )
        # 对成交量std进行1+std后取log
        pvdf["vdf"] = np.log(1 + pvdf["vol_std"])
        # 将两个指标进行标准化加总
        pvdf["pvdf"] = pvdf["pdf"] * pvdf["vdf"]
        pvdf["pvdf_diff"] = pvdf["pvdf"] - pvdf["pvdf"].shift(1)

        return pvdf[["pvdf", "pvdf_diff"]]
