from sell_strategies import *
from squid_cheat import *

def get_open_orders_stats(target_price):
    open_order_df = get_open_orders_from_squid_db_for_sweep(only_our_order=False, ignore_addresses=[], ignore_our_order=False, order_ton_threshold=0, target_price=target_price)
    
    # 我想計算所有 open orders ton 和 egg 的總和
    total_ton = open_order_df['ton'].sum()/1e9
    total_egg = open_order_df['egg'].sum()

    # 共幾筆訂單
    total_orders = len(open_order_df)

    return total_ton, total_egg, total_orders

def swap(egg_amount):

    reserve_ton, reserve_sqg = fetch_reserves()

    spot_price = calculate_spot_price(reserve_ton, reserve_sqg)

    # 计算交换前后的储备量
    outputAmount = (reserve_ton * egg_amount)/(reserve_sqg + egg_amount)

    # 计算交换后的价格
    reserveTon_after, reserveSqg_after = reserve_ton - outputAmount, reserve_sqg + egg_amount
    spot_price_after = calculate_spot_price(reserveTon_after, reserveSqg_after)
    
    # 计算价格变化
    price_change = (spot_price_after - spot_price) / spot_price * 100

    # 計算成交均價
    avg_price = outputAmount / egg_amount

    print(f"spot_price_after: {spot_price_after}")
    # print(f"avg_price: {avg_price}")
    return outputAmount

def fetch_reserves(retries=3, delay=5):
    url = 'https://tonapi.io/v2/blockchain/accounts/EQBLDmTKujkYvOUJ5pa7xYhj2m4ro7Y5IISag-D4FWpDqEm2/methods/get_reserves'
    for i in range(retries):
        try:
            response = requests.get(url)
            data = response.json()
            reserve_ton = int(data['decoded']['reserve0']) / 1e9
            reserve_sqd = int(data['decoded']['reserve1']) / 1e9
            return reserve_ton, reserve_sqd
        except RequestException as e:
            print(f"Request failed with {e}. Retrying...")
            time.sleep(delay)
    raise Exception("API request failed after multiple retries.")

def calculate_spot_price(reserve_ton, reserve_sqd):
    return reserve_ton / reserve_sqd

from unittest.mock import Mock

# 假設 fetch_reserves 返回兩個 reserve
fetch_reserves = Mock(return_value=(3000, 3000000))

def get_amm_price():
    reserve_ton, reserve_sqd = fetch_reserves()
    return calculate_spot_price(reserve_ton, reserve_sqd)

def find_max_profit(target_price):
    left = 0
    right = target_price
    golden_ratio = 0.618
    epsilon = 0.00005
    max_profit = 0
    max_profit_price = 0

    print(f"target_price: {target_price:.6f}")

    while right - left > epsilon:
        left_inner = left + (1 - golden_ratio) * (right - left)
        right_inner = left + golden_ratio * (right - left)

        cost_ton_left, get_egg_left, total_orders_left = get_open_orders_stats(left_inner)
        returned_ton_left = swap(get_egg_left)
        profit_left = returned_ton_left - cost_ton_left

        cost_ton_right, get_egg_right, total_orders_right = get_open_orders_stats(right_inner)
        returned_ton_right = swap(get_egg_right)
        profit_right = returned_ton_right - cost_ton_right

        print(f"Left Price: {left_inner:.6f}, Cost: {cost_ton_left:.2f}, Get Egg: {get_egg_left:,}, Sell Ton: {returned_ton_left:.2f}, Profit: {profit_left:.2f}")
        print(f"Right Price: {right_inner:.6f}, Cost: {cost_ton_right:.2f}, Get Egg: {get_egg_right:,}, Sell Ton: {returned_ton_right:.2f}, Profit: {profit_right:.2f}")
        print("")

        if profit_left > profit_right:
            if profit_left > max_profit:
                max_profit = profit_left
                max_profit_price = left_inner
            right = right_inner
        else:
            if profit_right > max_profit:
                max_profit = profit_right
                max_profit_price = right_inner
            left = left_inner

    print(f"Max Profit: {max_profit:.2f} at Price: {max_profit_price:.6f}")
    return max_profit, max_profit_price

def calculate_profit(price):
    cost_ton, get_egg, total_orders = get_open_orders_stats(price)
    returned_ton = swap(get_egg)
    profit = returned_ton - cost_ton
    return profit
    
def main():
    target_price = get_amm_price()
    # print(get_open_orders_stats(0.001))
    exec_profit, exec_price = find_max_profit(target_price)

    print("........")
    print(get_open_orders_stats(0.000416))
    print(swap(1752192))
    
if __name__ == "__main__":
    main()
