# scripts/download_kline.py
import baostock as bs
import pandas as pd
import numpy as np
import os
import json
import time
import random
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" 
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

# === 激进重连配置 ===
SLOW_THRESHOLD = 2.0   # 阈值降为 2秒
MAX_SLOW_STREAK = 3    # 连续 3次 就重连
FORCE_REFRESH_RATE = 25 # 每 25 个强制重连一次 (防止慢性劣化)

os.makedirs(OUTPUT_DIR, exist_ok=True)

class BaostockSession:
    def __init__(self):
        self.login()

    def login(self):
        try:
            bs.logout()
        except:
            pass
        
        time.sleep(random.uniform(0.5, 1.0))
        
        ret = bs.login()
        if ret.error_code != '0':
            print(f"⚠️ Login failed: {ret.error_msg}, wait 5s...")
            time.sleep(5)
            bs.login()

    def refresh(self):
        self.login()

    def close(self):
        bs.logout()

def get_kdata_final(code):
    # 1. K线
    fields_k = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
    rs = bs.query_history_k_data_plus(
        code, fields_k,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    if rs.error_code != '0': return pd.DataFrame()
    data_list = []
    while rs.next(): data_list.append(rs.get_row_data())
    if not data_list: return pd.DataFrame()
    
    df_k = pd.DataFrame(data_list, columns=rs.fields)

    # 2. 因子
    rs_fac = bs.query_adjust_factor(code=code, start_date=START_DATE, end_date="")
    data_fac = []
    while rs_fac.next(): data_fac.append(rs_fac.get_row_data())
    
    # 3. 处理
    df_k['date'] = pd.to_datetime(df_k['date'])
    if data_fac:
        df_fac = pd.DataFrame(data_fac, columns=rs_fac.fields)
        df_fac.rename(columns={'dividOperateDate': 'date'}, inplace=True)
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        df_k = pd.merge(df_k, df_fac[['date', 'adjustFactor']], on='date', how='left')
        df_k['adjustFactor'] = df_k['adjustFactor'].ffill().fillna(1.0)
    else:
        df_k['adjustFactor'] = 1.0

    df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor']
    df_k[numeric_cols] = df_k[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # 市值
    try:
        safe_turn = df_k['turn'].replace(0, np.nan)
        float_shares = df_k['volume'] / (safe_turn / 100)
        df_k['mkt_cap'] = df_k['close'] * float_shares
        df_k['mkt_cap'] = df_k['mkt_cap'].ffill().fillna(0.0)
    except:
        df_k['mkt_cap'] = 0.0

    return df_k

def main():
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    session = BaostockSession()
    
    success_count = 0
    slow_streak = 0 
    process_count = 0
    
    pbar = tqdm(stocks, desc=f"Job {TASK_INDEX}")
    
    for s in pbar:
        process_count += 1
        
        # === 策略 B: 强制保底重连 ===
        if process_count % FORCE_REFRESH_RATE == 0:
            # pbar.write(f"🔄 Scheduled refresh at {process_count}...")
            session.refresh()
            slow_streak = 0
        
        start_ts = time.time()
        
        try:
            df = get_kdata_final(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
        except Exception as e:
            time.sleep(1)
            pass
            
        duration = time.time() - start_ts
        
        # === 策略 A: 动态漏桶重连 ===
        if duration > SLOW_THRESHOLD:
            slow_streak += 1
            # 只有慢的时候才显示红灯
            pbar.set_postfix({"cost": f"{duration:.1f}s", "lag": f"{slow_streak}/{MAX_SLOW_STREAK}"})
            
            if slow_streak >= MAX_SLOW_STREAK:
                pbar.write(f"⚡ Lag detected ({slow_streak}x > {SLOW_THRESHOLD}s), refreshing...")
                session.refresh()
                slow_streak = 0
        else:
            # 漏桶逻辑：如果快了，不是直接清零，而是减一
            # 这样避免偶尔一个快请求掩盖整体慢的事实
            if slow_streak > 0:
                slow_streak -= 1
            pbar.set_postfix({"cost": f"{duration:.1f}s", "lag": f"{slow_streak}"})

    session.close()
    print(f"Job {TASK_INDEX} Done: {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
