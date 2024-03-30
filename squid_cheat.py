from sqlalchemy import create_engine
import pandas as pd
import math
import subprocess
import concurrent.futures
import csv
import os
import random
import time
import pymysql

def run_command(command):
    # 保存當前目錄的路徑
    original_cwd = os.getcwd()
    # 指定 Node.js 項目的目錄
    node_app_relative_path = '../SquidCheats'  # 根據你的目錄結構進行調整
    try:
        # 改變當前工作目錄到 Node.js 項目目錄
        os.chdir(node_app_relative_path)
        # 在新的工作目錄中執行命令
        process = subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        process.wait()
        return process.returncode
    finally:
        # 確保最後將工作目錄改回原來的路徑
        os.chdir(original_cwd)

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
            print(f"Initial run: There are {total_commands_initial} commands in {total_batches} batches to run, which will take at most {total_time} seconds")
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
            success_count += results.count(0)  # 累加成功的命令數量
            fail_count += len(results) - results.count(0)  # 累加失敗的命令數量
            print(f"\nBatch {i + 1} of {total_batches} completed. {success_count} commands succeeded, {fail_count} commands failed.\n")
            total_success_count += success_count
            total_fail_count += fail_count
            for j, returncode in enumerate(results):
                if returncode == 0:
                    failed_commands.remove(batch[j])  # 將成功的命令從失敗的命令列表中移除
                else:
                    if batch[j] not in failed_commands:
                        failed_commands.append(batch[j])  # 將新的失敗的命令加入到列表中
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

def get_bot_list_from_squid_cheat_db(ton_threshold=0.01, sort_column='total_egg'):
    squid_db_engine = create_engine('mysql+mysqlconnector://pelith:Pup3dgVfvv7Deg@34.81.131.171:3306/squid-prod')
    all_address_query = """
        SELECT address, squid, squid_egg, reward_per_squid_paid
        FROM players
    """
    squid_db_df = pd.read_sql_query(all_address_query, squid_db_engine)

    # 獲取當前腳本的路徑
    current_script_path = os.path.dirname(os.path.realpath(__file__))

    # 構建 dev.sqlite3 的相對路徑
    sqlite_db_path = os.path.join(current_script_path, '..', 'SquidCheats', 'dev.sqlite3')

    # 創建 SQLAlchemy 引擎
    cheat_engine = create_engine(f'sqlite:///{sqlite_db_path}')

    bot_db_query = "SELECT id, address, balance/1e9 AS ton FROM accounts"
    bot_db_df = pd.read_sql_query(bot_db_query, cheat_engine)

    reward_per_squid_stored_query = "SELECT reward_per_squid_stored FROM game LIMIT 1"
    reward_per_squid_stored = pd.read_sql_query(reward_per_squid_stored_query, squid_db_engine).iloc[0, 0]
    squid_db_df['new_egg'] = (reward_per_squid_stored - squid_db_df['reward_per_squid_paid']) * squid_db_df['squid'] / 1e9
    squid_db_df['total_egg'] = squid_db_df['squid_egg'] + squid_db_df['new_egg']

    df = pd.merge(bot_db_df, squid_db_df[['address', 'total_egg']], on='address', how='left')
    df = df[df['ton'] > ton_threshold]
    df = df.sort_values(sort_column, ascending=False)
    df['total_egg'] = df['total_egg'].fillna(0).astype(int)
    
    return df

def read_and_process_file(filename):
    # 開啟檔案並讀取所有行
    with open(filename, 'r') as file:
        update_commands = file.readlines()

    # 移除每一行的換行符號、'wait' 和 '&'
    update_commands = [command.replace('\n', '').replace(' &', '') for command in update_commands if 'wait' not in command]

    return update_commands

def update_balances(N):
    print("Starting to update balances...")
    
    print("Reading and processing file 'account_balance.sh'...")
    update_commands = read_and_process_file('account_balance.sh')
    
    print("Executing commands...")
    execute_commands(update_commands, N_min=N, N_max=N)
    
    print("Getting bot list from 'squid_cheat_db'...")
    df = get_bot_list_from_squid_cheat_db()
    
    print("Saving DataFrame to 'updated_balances.csv'...")
    df.to_csv('updated_balances.csv', index=False)
    
    print("Finished updating balances.")

def get_open_orders_from_squid_db():
    engine = create_engine('mysql+pymysql://pelith:Pup3dgVfvv7Deg@34.81.131.171:3306/squid-prod')
    query = """
        SELECT seller, egg, ton, nonce, last_timestamp
        FROM sell_orders
        WHERE status = 'open'
    """
    open_order_df = pd.read_sql_query(query, engine)
    open_order_df['price'] = open_order_df['ton'] / open_order_df['egg'] / 1e9
    open_order_df['price'] = round(open_order_df['price'], 7)

    return open_order_df