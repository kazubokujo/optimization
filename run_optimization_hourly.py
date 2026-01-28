# run_optimization_hourly.py

import os
from datetime import datetime
from multiprocessing import Pool
from optimize import process_file

input_root = "/Users/kazubokujo/Documents/git/optimization/2years_data"
output_folder = "/Users/kazubokujo/Documents/git/optimization/automation_best_result_csv"
os.makedirs(output_folder, exist_ok=True)

# 今日の CSV ファイルを取得
all_files = []
for month in range(1, 13):
    folder_path = os.path.join(input_root, str(month))
    if os.path.exists(folder_path):
        month_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.csv')]
        all_files.extend(month_files)

def wrapper(file_path):
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    output_file = os.path.join(output_folder, f're_best_results_{base_name}_{timestamp}.csv')
    print(f"[{datetime.now()}] Processing {file_path}")
    process_file(file_path, output_file)
    print(f"[{datetime.now()}] Completed {output_file}")
    return output_file

if __name__ == "__main__":
    # 並列処理で高速化
    processes = 6
    with Pool(processes=processes) as pool:
        results = pool.map(wrapper, all_files)

    print("All files completed:")
    print(results)
