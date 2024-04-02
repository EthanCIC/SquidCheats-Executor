import argparse
import time
import random
from squid_cheat import *

def generate_hatch_commands(N_commands, ton_threshold=1, sort_column='total_egg'):
    bot_df = get_bot_list_from_squid_cheat_db(ton_threshold=ton_threshold, sort_column=sort_column)
    
    # 如果 N_commands 大於 bot_df 的行數，就將 N_commands 設定為 bot_df 的行數
    if N_commands > len(bot_df):
        N_commands = len(bot_df)
        
    selected_bots = bot_df.sample(N_commands)
    commands = []
    for index, row in selected_bots.iterrows():
        command = f"timeout --kill-after=30 30 npm run start -- hatch {row['id']} {row['id']} {row['total_egg']}"
        commands.append(command)
    return commands

def main(N_commands, ton_threshold, N_min, N_max, sleep_min, sleep_max):
    while True:
        update_balances(N=5)
        hatch_commands = generate_hatch_commands(N_commands, ton_threshold=ton_threshold)
        execute_commands(hatch_commands, N_min=N_min, N_max=N_max, sleep_min=sleep_min, sleep_max=sleep_max)
        sleep_time = random.uniform(sleep_min, sleep_max)
        time.sleep(sleep_time)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Auto hatch script.')
    parser.add_argument('N_commands', type=int, help='Number of hatch commands to generate.')
    parser.add_argument('ton_threshold', type=float, help='TON threshold.')
    parser.add_argument('N_min', type=int, help='Minimum number of bots to hatch.')
    parser.add_argument('N_max', type=int, help='Maximum number of bots to hatch.')
    parser.add_argument('sleep_min', type=float, help='Minimum sleep time in seconds.')
    parser.add_argument('sleep_max', type=float, help='Maximum sleep time in seconds.')

    args = parser.parse_args()

    main(args.N_commands, args.ton_threshold, args.N_min, args.N_max, args.sleep_min, args.sleep_max)
