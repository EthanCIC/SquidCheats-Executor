import math
import subprocess
import concurrent.futures
import csv
import os
import random
import time

def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    stdout, _ = process.communicate()
    process.wait()
    return process.returncode, stdout.decode()

def execute_commands(all_commands, N_min, N_max, sleep_min=None, sleep_max=None):
    failed_commands = all_commands.copy()  # 初始時，所有的命令都視為失敗的命令
    retry_count = 0
    total_success_count = 0  # 初始化總成功的命令數量
    total_fail_count = 0  # 初始化總失敗的命令數量

    while all_commands:
        retry_count += 1
        total_success_count = 0  # 初始化總成功的命令數量
        total_fail_count = 0  # 初始化總失敗的命令數量
        total_commands = len(all_commands)
        N_values = []
        total_commands_initial = total_commands  # 保存初始的 total_commands 的值
        while total_commands > 0:
            N = random.randint(N_min, N_max)  # 獲得隨機的 N
            if N > total_commands:
                N = total_commands
            N_values.append(N)
            total_commands -= N
        total_batches = len(N_values)  # 根據 N_values 計算 total_batches
        total_time = total_batches * 30  # 每個 batch 最多需要 30 秒

        sleep_times = []
        if sleep_min is not None or sleep_max is not None:
            sleep_min = sleep_min if sleep_min is not None else 0
            sleep_max = sleep_max if sleep_max is not None else sleep_min
            sleep_times = [random.randint(sleep_min, sleep_max) for _ in range(total_batches)]

        total_time = total_batches * 30 + sum(sleep_times)  # 每個 batch 最多需要 30 秒，加上每個批次之間的睡眠時間


        print("\n" + "=" * 92)
        if retry_count == 1:
            print(f"qqqInitial run: There are {total_commands_initial} commands in {total_batches} batches to run, which will take at most {total_time} seconds")
        else:
            print(f"Retry {retry_count - 1}: There are {total_commands_initial} commands in {total_batches} batches to run, which will take at most {total_time} seconds")
        print("=" * 92 + "\n")
        for i, N in enumerate(N_values):
            success_count = 0  # 初始化成功的命令數量
            fail_count = 0  # 初始化失敗的命令數量
            batch = all_commands[:N]
            all_commands = all_commands[N:]
            print(f"Running {len(batch)} commands:")
            for command in batch:
                print(f"\t>> {command}")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                results = list(executor.map(run_command, batch))
            for j, (returncode, output) in enumerate(results):
                print(f"Output of command {j + 1}: {output}")  # 印出命令的輸出
                if "done" in output and returncode == 0:
                    success_count += 1  # 累加成功的命令數量
                    failed_commands.remove(batch[j])  # 將成功的命令從失敗的命令列表中移除
                else:
                    fail_count += 1  # 累加失敗的命令數量
                    if batch[j] not in failed_commands:
                        failed_commands.append(batch[j])  # 將新的失敗的命令加入到列表中


            print(f"\nBatch {i + 1} of {total_batches} completed. {success_count} commands succeeded, {fail_count} commands failed.\n")

            # 將失敗的命令寫入 CSV 檔案
            with open('failed_commands.csv', 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                for command in failed_commands:
                    writer.writerow([command])
            # 如果設定了睡眠時間範圍，則在每個批次完成後隨機睡眠一段時間
            if sleep_times:
                sleep_time = sleep_times[i]
                print(f"Sleeping for {sleep_time} seconds before next batch...\n")
                time.sleep(sleep_time)

        all_commands = [command for command in failed_commands]
        if retry_count == 1:
            print(f"\nSummary of Initial run: {total_success_count} commands succeeded, {total_fail_count} commands failed. Retrying failed commands...\n")
        else:
            print(f"\nSummary of Retry {retry_count - 1}: {total_success_count} commands succeeded, {total_fail_count} commands failed. Retrying failed commands...\n")

    print("All commands have been executed")