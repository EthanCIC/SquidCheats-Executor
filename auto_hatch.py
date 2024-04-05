import argparse
import time
import random
from squid_cheat import *

def generate_hatch_commands(N_commands, ton_threshold=1, sort_column='total_egg', egg_min=None, egg_max=None):
    bot_df = get_bot_list_from_squid_cheat_db(ton_threshold=ton_threshold, sort_column=sort_column)
    
    # 如果 N_commands 大於 bot_df 的行數，就將 N_commands 設定為 bot_df 的行數
    if N_commands > len(bot_df):
        N_commands = len(bot_df)
        
    if egg_min is not None:
        bot_df = bot_df[bot_df['total_egg'] >= egg_min]
    if egg_max is not None:
        bot_df = bot_df[bot_df['total_egg'] <= egg_max]
        
    selected_bots = bot_df.sample(N_commands)
    commands = []
    for index, row in selected_bots.iterrows():
        command = f"timeout --kill-after=30 30 npm run start -- hatch {row['id']} {row['id']} {row['total_egg']}"
        commands.append(command)
    return commands, sum(row['total_egg'] for _, row in selected_bots.iterrows())

def main(N_commands, ton_threshold, N_min, N_max, sleep_min, sleep_max, egg_min=None, egg_max=70000):
    while True:
        update_latest_bot_balance()
        hatch_commands, total_eggs = generate_hatch_commands(N_commands, ton_threshold=ton_threshold, egg_min=egg_min, egg_max=egg_max)
        print(f"Total eggs to hatch: {total_eggs}")
        execute_commands(hatch_commands, N_min=N_min, N_max=N_max, sleep_min=sleep_min, sleep_max=sleep_max)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='自動 hatch 機器人，會永無止盡的執行下去，直到手動停止程式。')
    parser.add_argument('N_commands', type=int, help='Number of total commands to generate for each update.')
    parser.add_argument('--ton_threshold', default=0.1, type=float, help='TON threshold. Default is 0.1')
    parser.add_argument('--N_min', type=int, default=1, help='Minimum number of bots to hatch. Default is 1')
    parser.add_argument('--N_max', type=int, default=1, help='Maximum number of bots to hatch. Default is 1')
    parser.add_argument('--sleep_min', type=float, help='Minimum sleep time in seconds.')
    parser.add_argument('--sleep_max', type=float, help='Maximum sleep time in seconds.')
    parser.add_argument('--egg_min', type=int, default=5000, help='Minimum total egg count. Default is 5000')
    parser.add_argument('--egg_max', type=int, default=70000, help='Maximum total egg count. Default is 70000')

    args = parser.parse_args()

    main(args.N_commands, args.ton_threshold, args.N_min, args.N_max, args.sleep_min, args.sleep_max, egg_min=args.egg_min, egg_max=args.egg_max)
