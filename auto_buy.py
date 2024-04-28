from sell_strategies import *
from squid_cheat import *

buy_sqd_continous_commands = buy_sell_sqd_continuous(
    total_orders = 1000,
    buy_ratio = 0.5,
    ton_min = 5, 
    ton_max = 15, 
    ratio=0.5)

execute_commands(buy_sqd_continous_commands, N_min=1, N_max=1, sleep_min=0, sleep_max=60)