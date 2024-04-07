import argparse
from sell_strategies import *
from squid_cheat import *

# 建立參數解析器
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('file_number', type=int, help='the number of the file to process')

# 解析命令列參數
args = parser.parse_args()

# 使用解析出的參數來建立檔案名稱
filename = f"airdrop/airdrop{args.file_number}.csv"

commands = read_and_process_file(filename)
execute_commands(commands, N_min=1, N_max=1, sleep_min=None, sleep_max=None)