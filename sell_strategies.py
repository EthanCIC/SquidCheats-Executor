import pandas as pd
import numpy as np
import math
from IPython.display import display
from squid_cheat import *
import matplotlib.pyplot as plt
from datetime import datetime
import requests
from requests.exceptions import RequestException

def sell_strategy_avg(order_number, lower_bound_price, upper_bound_price, lower_bound_egg, upper_bound_egg, ratio=0.2, ton_threshold=0.01, sort_column='total_egg'):
    # 產出 uniform 隨機 order_number 筆資料
    price = np.random.uniform(lower_bound_price, upper_bound_price, order_number)
    # 產出 order_number 個 egg 數量，用 log2 平均分佈

    egg = 2 ** np.random.uniform(np.log2(lower_bound_egg), np.log2(upper_bound_egg), order_number)

    # 先用 df 把每個 price & egg 放在一起，之後我需要根據 price 對 egg 做調整
    script_df = pd.DataFrame({'price': price, 'egg': egg})

    # 根據 price 對 egg 做調整，coefficients 是 price / lower_bound
    script_df['egg'] = (script_df['egg'] / (script_df['price'] / lower_bound_price)).astype(int)


    # 有多少比例的單要被四捨五入到十位或百位？
    ratio = 0.2

    # 隨機地 (機率 20%) 選出要調整的單並附上標記
    script_df['to_adjust'] = np.random.choice([True, False], size=order_number, p=[ratio, 1-ratio])
    # 將其中大於 1000 的單四捨五入到百位
    script_df.loc[script_df['to_adjust'] & (script_df['egg'] > 1000), 'egg'] = script_df.loc[script_df['to_adjust'] & (script_df['egg'] > 1000), 'egg'].round(-2)
    # 將其中介於 100 和 1000 之間的單四捨五入到十位
    script_df.loc[script_df['to_adjust'] & (script_df['egg'] < 1000) & (script_df['egg'] > 100), 'egg'] = script_df.loc[script_df['to_adjust'] & (script_df['egg'] < 1000) & (script_df['egg'] > 100), 'egg'].round(-1)
    # 移除標記欄位
    script_df.drop('to_adjust', axis=1, inplace=True)

    # 計算 TON 數量 = egg * price，並四捨五入到小數點第一位
    script_df['TON'] = np.round(script_df['egg'] * script_df['price'], 1)

    script_df['real_price'] = script_df['TON'] / script_df['egg']

    # 這是我要的訂單
    script_df = script_df.sort_values('egg', ascending=False)

    # 這是我要配對的地址
    squid_cheat_df = get_bot_list_from_squid_cheat_db(ton_threshold, sort_column='total_egg')

    # 開始配對
    orders = []

    for i in range(len(script_df)):
        egg = script_df.iloc[i, 1]
        price = script_df.iloc[i, 0]
        TON = script_df.iloc[i, 2]
        # 找到 squid_cheat_df 內 'total_egg' - 'egg' 差值最小（但必須是正的）的那個地址
        address = squid_cheat_df[squid_cheat_df['total_egg'] >= egg].iloc[-1, 1]
        # 找到該地址的 id
        id = squid_cheat_df[squid_cheat_df['address'] == address].iloc[0, 0]
        # 把配對的地址從 df 內刪掉
        squid_cheat_df = squid_cheat_df[squid_cheat_df['address'] != address]
        # 把配對的地址存到 orders 內
        orders.append({'id': id, 'address': address, 'egg': egg, 'TON': TON})

    # 把 orders 轉換成 DataFrame
    orders_df = pd.DataFrame(orders)
    
    # 把 orders_df 的順序全部打亂
    orders_df = orders_df.sample(frac=1)

    all_commands = []
    for index, row in orders_df.iterrows():
        id = row['id']
        address = row['address']
        amount = row['egg']
        ton = row['TON']
        command = f"npm run start -- sell {id} {id} {amount} {math.floor(ton * 1e9)}"
        all_commands.append(command)

    bins = pd.cut(script_df['price'], bins=np.arange(lower_bound_price, upper_bound_price+0.0001, 0.0001))

    # 價格區間內的掛單數
    print("價格區間內的掛單數：")
    display(script_df['price'].value_counts(bins=np.arange(lower_bound_price, upper_bound_price+0.0001, 0.0001), sort=False, normalize=False).reset_index().rename(columns={'index': 'price', 'price': 'count'}).style.hide(axis='index'))

    # 價格區間內的掛單 TON 總數
    print("\n\n價格區間內的掛單 TON 總數：")
    ton_df = script_df.groupby(bins, observed=False)['TON'].sum().reset_index()
    ton_df['TON'] = ton_df['TON'].apply(lambda x: '{:.2f}'.format(x))
    display(ton_df.style.hide(axis='index'))

    # 價格區間內的最大和最小 egg
    print("\n\n價格區間內的最大和最小 Egg：")
    display(script_df.groupby(bins, observed=False)['egg'].agg(['min', 'max']).reset_index().style.hide(axis='index'))

    # 價格區間內的最大和最小 TON
    print("\n\n價格區間內的最大和最小 TON：")
    ton_agg_df = script_df.groupby(bins, observed=False)['TON'].agg(['min', 'max']).reset_index()
    for col in ['min', 'max']:
        ton_agg_df[col] = ton_agg_df[col].apply(lambda x: '{:.2f}'.format(x))
    display(ton_agg_df.style.hide(axis='index'))    

    # Total egg
    print("Total Egg:", f"{script_df['egg'].sum():,}")

    # Total TON
    print("Total TON:", f"{script_df['TON'].sum():,.2f}")

    fig, axs = plt.subplots(1, 2, figsize=(20, 8))

    # egg 量分佈，橫軸是 egg 數量，縱軸是掛單數，x 軸要 Egg，y 軸是數量
    script_df['egg'].plot.hist(bins=np.arange(0, upper_bound_egg, (upper_bound_egg - lower_bound_egg) / 20), alpha=0.6, ax=axs[0])
    axs[0].set_xlabel('Egg')
    axs[0].set_ylabel('Count')
    axs[0].yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    # TON 量分佈，橫軸是 TON 數量，縱軸是掛單數，x 軸要 TON 軸是數量
    script_df['TON'].plot.hist(bins=np.arange(0, script_df['TON'].max(), script_df['TON'].max() / 20), alpha=0.6, ax=axs[1])
    axs[1].set_xlabel('TON')
    axs[1].set_ylabel('Count')
    axs[1].yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.tight_layout()
    plt.show()

    return all_commands

def get_nonce():
    return int(datetime.now().timestamp() // 1)

def sweep(target_price, ton_threshold=0.62, buffer=0.5, only_our_order=False, ignore_addresses=[], ignore_our_order=False, order_ton_threshold=0):
    open_order_df = get_open_orders_from_squid_db_for_sweep(only_our_order=only_our_order, ignore_addresses=ignore_addresses, ignore_our_order=ignore_our_order, order_ton_threshold=order_ton_threshold, target_price=target_price)

    # 這是我要的訂單 (從price少的開始掃)
    open_order_df = open_order_df.sort_values(by='price')

    # 這是我要配對的地址
    squid_cheat_df = get_bot_list_from_squid_cheat_db(ton_threshold, sort_column='ton')

    squid_cheat_df = squid_cheat_df.sort_values(by='ton', ascending=True)

    # 開始配對
    orders = []
    
    # command = f"timeout --kill-after=30 30 npm run start -- buy {row1['id']} {row2['egg']} {row2['ton']} {row2['seller']} {row2['nonce']}"
    for i in range(len(open_order_df)):
        egg = open_order_df['egg'].iloc[i]
        price = int(open_order_df['price'].iloc[i]*1e9)
        TON = open_order_df['ton'].iloc[i]
        seller = open_order_df['seller'].iloc[i]
        nonce = open_order_df['nonce'].iloc[i]
        order_id = open_order_df['id'].iloc[i]

        # 找到 squid_cheat_df 內 'ton'(balance) - 'ton'(order_amount) 差值最小（但必須是大於buffer的）的那個地址
        address_series = squid_cheat_df[squid_cheat_df['ton'] - TON/1e9 > buffer]['address']
        if len(address_series) == 0:
            continue
        address = address_series.iloc[0]
        # 找到該地址的 id
        id = squid_cheat_df[squid_cheat_df['address'] == address]['id'].iloc[0]
        # 把配對的地址從 df 內刪掉
        squid_cheat_df = squid_cheat_df[squid_cheat_df['address'] != address]
        # 把配對的地址存到 orders 內
        orders.append({'id': id, 'egg': egg, 'price': TON, 'seller': seller, 'nonce': nonce, 'order_id': order_id})
    
    # 把 orders 轉換成 DataFrame
    orders_df = pd.DataFrame(orders)

    #TODO 分成兩種單

    all_commands = []
    order_ids = []
    for index, row in orders_df.iterrows():
        id = row['id']
        amount = row['egg']
        price = row['price']
        seller = row['seller']
        nonce = row['nonce']
        order_id = row['order_id']

        command = f"npm run start -- buy {id} {amount} {price} {seller} {nonce}"
        all_commands.append(command)
        order_ids.append(order_id)
    
    return all_commands, order_ids

def buy_sqd_continous(order_number, ton_min, ton_max, ratio=0.9, ton_threshold=0.01):
    # 目標是組出一堆包含這些指令的 list
    # npm run start -- swap TON SQD <id> <ton_in>

    df = get_bot_list_from_god_bd(ton_threshold)
    df = df.sort_values(by='ton', ascending=True)
    
    # 篩選出 ton_min 到 ton_max 之間的地址
    df = df[(df['ton'] >= ton_min) & (df['ton'] <= ton_max)]

    if len(df) < order_number:
        print("資料不足，只有 {} 筆資料可供選擇。".format(len(df)))
        order_number = len(df)  # 將 order_number 設為可用的最大數量

    # 從 df 中隨機選出 order_number 筆資料
    df = df.sample(order_number)



    # 取出 id 和 ton
    ids = df['id'].tolist()
    ton = (df['ton'] * ratio).round().astype(int).tolist()

    # 20% 的機率讓 ton 中的每個元素能被 5 整除
    ton = [t - t % 5 if np.random.rand() < 0.2 else t for t in ton]

    # 更新 df 的 'ton' 列
    df['ton'] = ton

    all_commands = []
    for i in range(order_number):
        id = ids[i]
        ton_in = ton[i]
        command = f"npm run start -- swap TON SQD {id} {ton_in}"
        all_commands.append(command)

    # 用 print 和 display 來顯示資料統計
    print("Total TON to swap:", f"{sum(ton):,.0f}")

    # 計算假設這些資金一口氣全部進入的話會漲多少，直接用 calculate_price_change()
    spot_price_original, spot_price_after, price_change = calculate_price_change(sum(ton), 'TON')

    print("\n假設資金一口氣全部瞬間買入的情況下：") 
    print(f"Original Price: {spot_price_original:,.07f} TON")
    print(f"New Price: {spot_price_after:,.07f} TON")
    print(f"Price Change: {price_change:.2f} %")

    # display(df[['id', 'ton']].style.hide(axis='index'))
    df['ton'].plot.hist(bins=50)
    plt.xlabel('TON')
    plt.title('Histogram of TON')
    plt.show()

    return all_commands

def sell_sqd_continous(order_number, sqd_min, sqd_max, ratio=0.9, ton_threshold=0.01):
    # 目標是組出一堆包含這些指令的 list
    # npm run start -- swap SQD TON <id> <ton_in>

    df = get_bot_list_from_god_bd(ton_threshold)
    df = df.sort_values(by='sqd', ascending=True)
    
    # 篩選出 sqd_min 到 sqd_max 之間的地址
    df = df[(df['sqd'] >= sqd_min) & (df['sqd'] <= sqd_max)]

    if len(df) < order_number:
        print("資料不足，只有 {} 筆資料可供選擇。".format(len(df)))
        order_number = len(df)  # 將 order_number 設為可用的最大數量

    # 從 df 中隨機選出 order_number 筆資料
    df = df.sample(order_number)

    # 取出 id 和 sqd
    ids = df['id'].tolist()
    sqd = (df['sqd'] * ratio).round().astype(int).tolist()

    # 40% 的機率讓 sqd 中的每個元素能被 5 整除
    sqd = [t - t % 1000 if np.random.rand() < 0.4 else t for t in sqd]

    # 更新 df 的 'sqd' 列
    df['sqd'] = sqd

    all_commands = []
    for i in range(order_number):
        id = ids[i]
        sqd_in = sqd[i]
        command = f"npm run start -- swap SQD TON {id} {sqd_in}"
        all_commands.append(command)

    # 用 print 和 display 來顯示資料統計
    print("Total TON to swap:", f"{sum(sqd):,.0f}")

    # 計算假設這些資金一口氣全部進入的話會漲多少，直接用 calculate_price_change()
    spot_price_original, spot_price_after, price_change = calculate_price_change(sum(sqd), 'SQD')

    print("\n假設資金一口氣全部瞬間賣出的情況下：") 
    print(f"Original Price: {spot_price_original:,.07f} TON")
    print(f"New Price: {spot_price_after:,.07f} TON")
    print(f"Price Change: {price_change:.2f} %")

    # display(df[['id', 'ton']].style.hide(axis='index'))
    df['sqd'].plot.hist(bins=50)
    plt.xlabel('SQD')
    plt.title('Histogram of SQD')
    plt.show()

    return all_commands

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

def calculate_price_change(inputAmount, inputToken):
    inputToken = inputToken.upper()

    # 获取当前储备量
    reserve_ton, reserve_sqd = fetch_reserves()

    reserveInput, reserveOutput = (reserve_ton, reserve_sqd) if inputToken == 'TON' else (reserve_sqd, reserve_ton)

    spot_price = calculate_spot_price(reserve_ton, reserve_sqd)

    # 计算交换前后的储备量
    outputAmount = (reserveOutput * inputAmount)/(reserveInput + inputAmount)

    # 计算交换后的价格
    reserveTon_after, reserveSqd_after = (reserveInput + inputAmount, reserveOutput - outputAmount) if inputToken == 'TON' else (reserveOutput - outputAmount, reserveInput + inputAmount)
    spot_price_after = calculate_spot_price(reserveTon_after, reserveSqd_after)

    # 计算价格变化
    price_change = (spot_price_after - spot_price) / spot_price * 100

    return spot_price, spot_price_after, price_change

if __name__ == '__main__':
    all_commands, order_ids = sweep(0.0005)
    for c in all_commands:
        print(c)
