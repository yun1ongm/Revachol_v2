from Elysium.research.strategy.cta_backtest import Btframe,Indicators
from datetime import datetime
import time
import pandas as pd
import pandas_ta as ta
import numpy as np
import logging
import sys 
import warnings
warnings.filterwarnings("ignore")
import optuna
import optuna.storages as storages

def main(btf,kdf,t,b,s,atrk,wlr):
    kdf_signal = gen_signal(kdf,t,b,s)
    result = btf.run_stretegy(kdf_signal,atrk,wlr)
    perf = btf.calc_perf(result) 
    return perf

def gen_signal(kdf,t,b,s):
    score = Indicators.buy_scoring(kdf)
    score['signal'] = np.zeros(len(score))
    score['atr'] = ta.atr(score['high'],score['low'],score['close'],length=10)
    score.loc[(score["abs_strength"]>=t) & (score["rel_strength"]>=b), "signal"] = 1
    score.loc[(score["abs_strength"]<=(1-t)) & (score["rel_strength"]<=-s), "signal"] = -1
    return score

btf= Btframe()
kdf = btf.gen_data(start = datetime(2023,10,1),
                      window_days = 30)

performance_results = main(btf,kdf,t=0.7,b=100,s=100,atrk=3,wlr=2)

def optimization(target,btf,kdf,num_evals=100):
    opt_results = {}
    date_period = len(np.unique(pd.DatetimeIndex(kdf.index).date))
    end_date = kdf.index[-1]
    end_date = pd.to_datetime(end_date)
    def objective(trial):
        # 生成参数
        kwargs = {"btf":btf,
                  'kdf':kdf,
                  't': trial.suggest_float('t',0.1,0.9,step=0.1),
                  'b':trial.suggest_float('b',100,1000,step = 100), 
                  's':trial.suggest_float('s',100,1000,step = 100), 
                  'atrk':trial.suggest_float('atrk',2,5,step=1), 
                  'wlr':trial.suggest_float('wlr',1,2,step=0.5),
                  }
        performance = main(**kwargs)
        try:
            if performance[target] is None:
                print(f'No trade detected in {target}.')
                return -100  # or any special value you prefer
        except TypeError as te:
            print(f"A TypeError occurred: {te}")
            print(f'Type of performance: {type(performance)}')
            print(f'Value of performance: {performance}')
            print(f'Type of target: {type(target)}')
            print(f'Value of target: {target}')

        print("opt_0")
        return performance[target]

    start = time.time()
    old_date = end_date - pd.Timedelta(days=3)
    cur_study_name = f'score_1001-study-{end_date}_{date_period}_{target}'
    old_study_name = f'score_1001-study-{old_date}_{date_period}_{target}'
    storage = storages.RDBStorage("sqlite:///example.db", engine_kwargs={"connect_args": {"timeout": 5}})
    try:
        old_study = optuna.load_study(study_name=old_study_name, storage=storage) 
    except Exception as e:
        print(f"Caught exception: {e}")
        old_study = None
    if old_study is not None and len(old_study.trials) > 1:
        try:
            if old_study.best_trial.state == optuna.trial.TrialState.COMPLETE:
                best_params = old_study.best_params
                last_trial_params = old_study.trials[-1].params
                # 将最优参数和最后一次trial的参数作为新的trial添加到新的study
                new_study = optuna.create_study(study_name=cur_study_name, storage=storage, direction='maximize', load_if_exists=True)
                print(f"the old study best_params: {best_params}")
                new_study.enqueue_trial(best_params)
                new_study.enqueue_trial(last_trial_params)
            else:
                raise ValueError("No complete trials found.")
        except ValueError:
            print(f'Encountered an issue while getting the best parameters from the old study for {cur_study_name}. Initializing a new study.')
            new_study = optuna.create_study(study_name=cur_study_name, storage=storage, direction='maximize', load_if_exists=True)
    else:
        print(f'We did not find the best parameters for the old study {cur_study_name} and will initialize a new one.')
        new_study = optuna.create_study(study_name=cur_study_name, storage=storage, direction='maximize', load_if_exists=True)
    # Add stream handler of stdout to show the messages to see Optuna works expectedly.
    optuna.logging.get_logger("optuna").addHandler(logging.StreamHandler(sys.stdout))   
    print(f'Now optimizing target: {target} over {date_period} days.')
    try:
        last_trial = new_study.trials[-1]
        completed_iterations = last_trial.number
        print(f"We have already run {completed_iterations} iterations.")
        remaining_iterations = max(5, num_evals - completed_iterations) #train at least 5 times
        print(f"We are going to run {remaining_iterations} iterations.")
    except:
        remaining_iterations = num_evals
    new_study.optimize(objective, n_trials=remaining_iterations, gc_after_trial=True)

    #过滤None值，按target排序trials
    def sort_func(t):
        if t.value is None:
            return -float('inf')  
        return t.value
    trials = sorted(new_study.trials, key=sort_func, reverse=True)

    #取前N个trials
    N = 10
    best_n_trials = trials[:N]
    #从trials中提取参数
    best_params = [t.params for t in best_n_trials]
    best_values = [t.value for t in best_n_trials]
    print("Top {} trials:".format(N))
    for params, value in zip(best_params, best_values):
        print("Params:", params, "Value:", value) 
    # print("opt_1")
    # optimal_pars = new_study.best_params
    # details = new_study.best_value
    # print("opt_2")
    # opt_results[target] = (optimal_pars, details)
    # print("opt_3")
    end = time.time()
    total_time = end - start
    print(f"Cost time: {total_time}")

    return opt_results

opt_res = optimization("diysharpe",btf,kdf,num_evals=200)
print(opt_res)  