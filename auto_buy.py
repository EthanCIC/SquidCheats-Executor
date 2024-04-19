from sell_strategies import *
from squid_cheat import *

buy_sqd_continous_commands = buy_sqd_continous(
    order_number = 1000, 
    ton_min = 15, 
    ton_max = 60, 
    ratio=0.9, 
    ton_threshold=1)

execute_commands(buy_sqd_continous_commands, N_min=1, N_max=1, sleep_min=0, sleep_max=60)