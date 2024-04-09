from sqlalchemy import create_engine, text
import pandas as pd
import math
import subprocess
import concurrent.futures
import csv
import os
import random
import time
import pymysql
from random import uniform, randint
from datetime import datetime
from collections import defaultdict
from psycopg2 import connect
from tqdm import tqdm

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
        # process = subprocess.Popen(command, shell=True)
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

def execute_commands_hatch(all_commands, N_min, N_max, total_hatched_egg, total_success_count, sleep_min=None, sleep_max=None, start_time=None):
    total_success_count = 0  # 初始化總成功的命令數量
    total_fail_count = 0  # 初始化總失敗的命令數量
    total_commands = len(all_commands)
    N_values = []
    total_commands_initial = total_commands  # 保存初始的 total_commands 的值
    total_hatched_egg = 0  # 初始化總孵化量
    while total_commands > 0:
        N = random.randint(N_min, N_max)  # 獲得隨機的 N
        if N > total_commands:
            N = total_commands
        N_values.append(N)
        total_commands -= N
    total_batches = len(N_values)  # 根據 N_values 計算 total_batches

    sleep_times = []
    if sleep_min is not None or sleep_max is not None:
        sleep_min = sleep_min if sleep_min is not None else 0
        sleep_max = sleep_max if sleep_max is not None else sleep_min
        sleep_times = [random.randint(sleep_min, sleep_max) for _ in range(total_batches)]

    print("\n" + "=" * 92)
    print(f"Initial run: There are {total_commands_initial} commands in {total_batches} batches to run")
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
        for command, result in zip(batch, results):
            if result == 0:  # 如果命令成功，則從命令中獲取孵化量並累加
                total_hatched_egg += int(command.split()[-1])
        success_count += results.count(0)  # 累加成功的命令數量
        fail_count += len(results) - results.count(0)  # 累加失敗的命令數量
        print(f"\nBatch {i + 1} of {total_batches} completed. {success_count} commands succeeded, {fail_count} commands failed.\n")
        total_success_count += success_count
        total_fail_count += fail_count
        # 如果設定了睡眠時間範圍，則在每個批次完成後隨機睡眠一段時間
        if sleep_times:
            sleep_time = sleep_times[i]
            print(f"Sleeping for {sleep_time} seconds before next batch...\n")
            time.sleep(sleep_time)
        elapsed_time = time.time() - start_time  # 計算經過的時間
        # print(f"\nSummary of Initial run: {total_success_count} commands succeeded, {total_fail_count} commands failed.\n")
        print("All commands have been executed")
        avg_hatched_egg_per_day = total_hatched_egg / (elapsed_time / (24 * 60 * 60))  # 計算每天的平均孵化量
        avg_success_count_per_day = total_success_count / (elapsed_time / (24 * 60 * 60))  # 計算每天的平均成功次數
        formatted_time = format_time_delta(elapsed_time)
        print(f"\nTotal hatched egg: {total_hatched_egg}, average hatched egg per day: {avg_hatched_egg_per_day:,.0f}")
        print(f"Success count: {success_count}, average per day: {avg_success_count_per_day:,.0f}")
        print(f"Total execution time: {formatted_time}\n")
    return total_hatched_egg, total_success_count

def format_time_delta(seconds):
    days, seconds = divmod(seconds, 24*60*60)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{int(days)} days, {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds"

def execute_commands_no_retry(all_commands, N_min, N_max, sleep_min=None, sleep_max=None):
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
    print(f"Initial run: There are {total_commands_initial} commands in {total_batches} batches to run, which will take at most {total_time} seconds")
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
        # 如果設定了睡眠時間範圍，則在每個批次完成後隨機睡眠一段時間
        if sleep_times:
            sleep_time = sleep_times[i]
            print(f"Sleeping for {sleep_time} seconds before next batch...\n")
            time.sleep(sleep_time)

    print(f"\nSummary of Initial run: {total_success_count} commands succeeded, {total_fail_count} commands failed.\n")
    print("All commands have been executed")

def get_failed_commands(commands, results):
    failed_commands = []
    for i, returncode in enumerate(results):
        if returncode == 0:
            continue
        failed_commands.append(commands[i])
    
    return failed_commands

def get_failed_order_ids(part_order_ids, results):
    failed_order_ids = []
    for i, returncode in enumerate(results):
        if returncode == 0:
            continue
        failed_order_ids.append(part_order_ids[i])
    
    return failed_order_ids

def add_failed_commands_back_to_all_commands(all_commands, failed_commands):
    all_commands = failed_commands + all_commands

def add_failed_order_ids_back_to_order_ids(order_ids, failed_order_ids):
    order_ids = failed_order_ids + order_ids

def get_now_timestamp():
    return datetime.now().timestamp()

def sleep_according_to_deadline_and_commands_done(deadline, origin_commands_amount, amount_of_commands_done_this_round):
    time_left = deadline - get_now_timestamp()

    time_to_sleep = time_left/origin_commands_amount*amount_of_commands_done_this_round

    time.sleep(time_to_sleep)

def is_miss_deadline(deadline):
    return get_now_timestamp() > deadline

def record_fail_count(fail_count, failed_order_ids):
    for failed_order_id in failed_order_ids:
        fail_count[failed_order_id] += 1

def remove_order_over_try_times(fail_count, failed_commands, failed_order_ids, try_times):
    new_failed_commands = []
    new_failed_order_ids = []
    for i in range(len(failed_order_ids)):
        if fail_count[failed_order_ids[i]] < try_times:
            new_failed_commands.append(failed_commands[i])
            new_failed_order_ids.append(failed_order_ids[i])

    return new_failed_commands, new_failed_order_ids

def extend_time(finish_in_seconds, origin_commands_amount, success_count):
    return int(finish_in_seconds/origin_commands_amount*success_count)

def get_order(order_id):
    engine = create_engine('mysql+pymysql://pelith:Pup3dgVfvv7Deg@34.81.131.171:3306/squid-prod')
    query = f"""
        SELECT seller, status, buyer
        FROM sell_orders
        WHERE id = {order_id}
    """
    order_df = pd.read_sql_query(query, engine)
    
    order = order_df.iloc[0]

    return order.to_dict()

def get_buyer(buyer_id):
    sqlite_db_path = get_sqlite_db_path()
    cheat_engine = create_engine(f'sqlite:///{sqlite_db_path}')

    buyer_query = f"SELECT id, address FROM accounts WHERE id = {buyer_id}"
    buyer_df = pd.read_sql_query(buyer_query, cheat_engine)

    buyer = buyer_df.iloc[0]

    return buyer.to_dict()

def get_buyer_address_from_commands(command):
    second_part = command.split('buy')[1].strip()
    buyer_id = second_part.split(' ')[0].strip()
    buyer = get_buyer(buyer_id)

    return buyer['address']

def check_sweep_success(origin_commands, origin_order_ids):
    success_record = {}
    for i in range(len(origin_order_ids)):
        order = get_order(origin_order_ids[i])
        command = origin_commands[i]
        if order['status'] != 'complete':
            success_record[command] = False
            continue
        buyer_address = get_buyer_address_from_commands(command)
        if order['buyer'] != buyer_address:
            success_record[command] = False
            continue
        success_record[command] = True
    
    success_count = 0
    for _command, is_success in success_record.items():
        success_str = 'fail'
        if is_success:
            success_count+=1
            success_str = 'success'

        print(f'{_command}: {success_str}')
    
    print('='*80)
    commands_amout = len(origin_commands)
    success_rate = round(success_count/commands_amout, 4)*100
    print(f'Out of {commands_amout} commands, {success_count} were successful, success rate is {success_rate}%.')

def execute_commands_for_sweep(all_commands, order_ids, finish_in_minutes=0, try_times=2):
    finish_in_seconds = finish_in_minutes*60
    deadline = int(get_now_timestamp()+finish_in_seconds)
    origin_commands_amount = len(all_commands)
    origin_commands = all_commands[:]
    origin_order_ids = order_ids[:]

    success_count = 0
    fail_count = defaultdict(int)
    while all_commands:
        random_amount_of_commands = randint(15, 20)
        part_commands = all_commands[:random_amount_of_commands]
        all_commands = all_commands[random_amount_of_commands:]
        part_order_ids = order_ids[:random_amount_of_commands]
        order_ids = order_ids[random_amount_of_commands:]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(run_command, part_commands))
        
        failed_commands = get_failed_commands(part_commands, results)
        failed_order_ids = get_failed_order_ids(part_order_ids, results)
        
        record_fail_count(fail_count, failed_order_ids)

        # 超過嘗試次數的 直接捨棄掉 先交易別的
        failed_commands, failed_order_ids = remove_order_over_try_times(fail_count, failed_commands, failed_order_ids, try_times)
        
        add_failed_commands_back_to_all_commands(all_commands, failed_commands)
        add_failed_order_ids_back_to_order_ids(order_ids, failed_order_ids)

        amount_of_commands_done_this_round = results.count(0)
        success_count += amount_of_commands_done_this_round
        print(f"\nTry to run another {len(part_commands)} commands. So far {success_count} commands succeeded, and still {origin_commands_amount-success_count} commands to go.\n")

        # 將失敗的命令寫入 CSV 檔案
        with open('buy_failed_commands.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for command in failed_commands:
                writer.writerow([command])

        if is_miss_deadline(deadline):
            # 如果超時 依據目前尚未完成的command數量追加時間
            deadline = get_now_timestamp() + extend_time(finish_in_seconds, origin_commands_amount, success_count)
        sleep_according_to_deadline_and_commands_done(deadline, origin_commands_amount, amount_of_commands_done_this_round)

    print("All commands have been executed")
    print("Wait 30 seconds for the on-chain work and will provide sweep result.")
    time.sleep(30)

    check_sweep_success(origin_commands, origin_order_ids)

def get_bot_list_from_squid_cheat_db(ton_threshold=0.01, sort_column='total_egg'):
    squid_db_engine = create_engine('mysql+pymysql://pelith:Pup3dgVfvv7Deg@34.81.131.171:3306/squid-prod')
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

def get_sql_in_condition_command(field, in_list, is_in=True):
    need_not = " "
    if not is_in:
        need_not = " NOT "
    return f"{field}{need_not}IN ({','.join(in_list)})"

def get_sqlite_db_path():
    # 獲取當前腳本的路徑
    current_script_path = os.path.dirname(os.path.realpath(__file__))

    # 構建 dev.sqlite3 的相對路徑
    sqlite_db_path = os.path.join(current_script_path, '..', 'SquidCheats', 'dev.sqlite3')

    return sqlite_db_path

def get_our_seller():
    sqlite_db_path = get_sqlite_db_path()
    cheat_engine = create_engine(f'sqlite:///{sqlite_db_path}')

    bot_db_query = "SELECT id, address, balance/1e9 AS ton FROM accounts"
    bot_db_df = pd.read_sql_query(bot_db_query, cheat_engine)

    return list(bot_db_df['address'])

def get_open_orders_from_squid_db_for_sweep(only_our_order=False, ignore_addresses=[], ignore_our_order=False, order_ton_threshold=0, target_price=0):
    engine = create_engine('mysql+pymysql://pelith:Pup3dgVfvv7Deg@34.81.131.171:3306/squid-prod')
    query = """
        SELECT id, seller, egg, ton, nonce, last_timestamp
        FROM sell_orders
        WHERE status = 'open'"""
    in_seller = []
    not_in_seller = []

    if only_our_order:
        in_seller += get_our_seller()
    
    if ignore_our_order:
        not_in_seller += get_our_seller()

    if ignore_addresses:
        not_in_seller += ignore_addresses
    
    if in_seller:
        query += f" AND {get_sql_in_condition_command(field='seller', in_list=in_seller, is_in=True)}"
    
    if not_in_seller:
        query += f" AND {get_sql_in_condition_command(field='seller', in_list=not_in_seller, is_in=False)}"
    
    if order_ton_threshold > 0:
        query += f" AND ton <= {order_ton_threshold*1e9}"

    open_order_df = pd.read_sql_query(query, engine)
    open_order_df['price'] = open_order_df['ton'] / open_order_df['egg'] / 1e9
    # open_order_df['price'] = round(open_order_df['price'], 7)
    if target_price > 0:
        open_order_df = open_order_df[open_order_df['price'] < target_price]

    return open_order_df

def get_columns_of_table(table_name):
    conn = connect(
        host='35.189.161.212',
        user='postgres',
        password='squidcheat',
        port=5432,
        database='sc',
    )
    cursor = conn.cursor()
    query = f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
    cursor.execute(query)

    column_names = cursor.fetchall()

    for column in column_names:
        print(column[0])

    cursor.close()
    conn.close()

def update_latest_bot_balance():
    sqlite_db_path = get_sqlite_db_path()
    cheat_engine = create_engine(f'sqlite:///{sqlite_db_path}')

    conn = connect(
        host='35.189.161.212',
        user='postgres',
        password='squidcheat',
        port=5432,
        database='sc',
    )
    cursor = conn.cursor()
    query = """SELECT address, balance FROM players"""
    cursor.execute(query)

    addresses = []
    with cheat_engine.connect() as connection:
        for address, balance in tqdm(cursor.fetchall()):
            query = f"""
                UPDATE accounts
                SET balance={balance}
                WHERE address='{address}';"""
            connection.execute(text(query))
            addresses.append(address)
        connection.commit()

    print('同步錢包餘額完成！')
    cursor.close()
    conn.close()

def check_bot_balance_synced():
    sqlite_db_path = get_sqlite_db_path()
    cheat_engine = create_engine(f'sqlite:///{sqlite_db_path}')

    conn = connect(
        host='35.189.161.212',
        user='postgres',
        password='squidcheat',
        port=5432,
        database='sc',
    )
    cursor = conn.cursor()
    query = f"""SELECT address, balance FROM players LIMIT 1000"""
    cursor.execute(query)

    for address, balance in cursor.fetchall():
        query = f"""
            SELECT balance
            FROM accounts
            WHERE address='{address}';"""
        balance_df = pd.read_sql_query(query, cheat_engine)
        if balance != balance_df['balance'].iloc[0]:
            print(f'{address} not synced')
        # print(address, balance)


    cursor.close()
    conn.close()
    cheat_engine.dispose()

if __name__ == '__main__':
    update_latest_bot_balance()