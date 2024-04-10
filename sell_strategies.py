import pandas as pd
import numpy as np
import math
from IPython.display import display
from squid_cheat import *
import matplotlib.pyplot as plt
from datetime import datetime

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
        command = f"timeout --kill-after=30 30 npm run start -- sell {id} {id} {amount} {math.floor(ton * 1e9)}"
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
        address = squid_cheat_df[squid_cheat_df['ton'] - TON/1e9 > buffer]['address'].iloc[0]
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

if __name__ == '__main__':
    all_commands, order_ids = sweep(0.0005)
    for c in all_commands:
        print(c)
