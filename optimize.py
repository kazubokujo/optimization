# optimize.py

import os
from multiprocessing import Pool
import pandas as pd
import numpy as np
import optuna
import re
import warnings

warnings.filterwarnings("ignore")
optuna.logging.set_verbosity(optuna.logging.ERROR)

# ------------------------------
# 既存の最適化関数
# ------------------------------
def knapsack(row_x, data_x, required_time_x, move_time_x, attraction_no_x):
    waiting_time_x = data_x.iloc[row_x, attraction_no_x]
    return required_time_x + waiting_time_x + move_time_x

def ride_calculation(data, ride_lst, required_times, popularity, move_time):
    total_popularity, total_time = 0, 0
    new_ride_lst, ride_time = [], []
    row, c = 0, 0
    try:
        for i in ride_lst:
            if (data.index[row]) >= 675 and i in [0,1]:
                c += 1
                continue
            if i >= 8 or i < 0:
                c += 1
                continue
            elif c > 0 and i == ride_lst[c-1]:
                c += 1
                continue
            ride_time.append(data.index[row])
            required_time = required_times[i]
            ans = knapsack(row, data, required_time, move_time, i)
            total_time += ans
            row = int(np.ceil(total_time / 15))
            total_popularity += popularity[i]
            new_ride_lst.append(i)
            c += 1
    except:
        pass
    return total_popularity, total_time, new_ride_lst, ride_time

# ------------------------------
# ファイルごとの最適化
# ------------------------------
def optimize_file(file_path):
    best_plan_lst = []
    lamb_list = np.linspace(1, 20, 10)  # lambda のリスト
    NN = 200

    data = pd.read_csv(file_path).set_index('時間')
    for j, lamb in enumerate(lamb_list):

        def objective(trial):
            required_times = [5, 7, 2, 3, 3, 2, 23, 30]
            popularity = [476, 465, 473, 472, 480, 452, 461, 478]
            move_time = 15
            n_rides = trial.suggest_int('n_rides', 10, 20)
            ride_lst = [trial.suggest_int(f'ride_{i}', 0, 7) for i in range(n_rides)]
            total_popularity, total_time, new_ride_lst, ride_time = ride_calculation(
                data, ride_lst, required_times, popularity, move_time
            )
            max_allowed_time = 675
            return total_popularity + lamb * (max_allowed_time - total_time)

        print(f"[{file_path}] Optimization iteration {j+1}/10 (lambda={lamb:.2f}) starting...", flush=True)
        study = optuna.create_study(direction="maximize")
        study.optimize(objective, n_trials=NN)
        best_trial = study.best_trial

        # best_trial のパラメータで ride_list を再計算
        n_rides = best_trial.params['n_rides']
        ride_lst = [best_trial.params[f'ride_{i}'] for i in range(n_rides)]
        total_popularity, total_time, new_ride_lst, ride_time = ride_calculation(
            data, ride_lst, [5,7,2,3,3,2,23,30], [476,465,473,472,480,452,461,478], 15
        )

        match = re.search(r'\d{4}-\d{2}-\d{2}', file_path)
        date = match.group() if match else None
        best_plan_lst.append([date, lamb, total_popularity, total_time, ride_lst, new_ride_lst, ride_time])
        print(f"[{file_path}] Iteration {j+1}/10 completed. Best value: {best_trial.value}", flush=True)

    columns = ["date", "lambda", "value", "total_time", "scheduled_plan", "optimal_plan", "time_schedule"]
    df_best = pd.DataFrame(best_plan_lst, columns=columns)
    return df_best

# ------------------------------
# 並列処理関数
# ------------------------------
def process_file(file_path, output_file):
    print(f"Processing file: {file_path}", flush=True)
    best_df = optimize_file(file_path)
    best_df.to_csv(output_file, index=False)
    print(f"File completed and saved: {output_file}", flush=True)
    return output_file
