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

# === 动态重连配置 ===
SLOW_THRESHOLD = 3.0  # 慢速阈值 (秒)
MAX_SLOW_STREAK = 5   # 连续慢几次触发重连

os.makedirs(OUTPUT_DIR, exist_ok=True)

class BaostockSession:
    """Baostock 会话管理器，负责自动重连"""
    def __init__(self):
        self.login()

    def login(self):
        try:
            bs.logout()
        except:
            pass
        
        # 避免并发撞击
        time.sleep(random.uniform(0.5, 1.5))
        
        ret = bs.login()
        if ret.error_code != '0':
            print(f"⚠️ Login failed: {ret.error_msg}, waiting...")
            time.sleep(5)
            bs.login()

    def refresh(self):
        """强制刷新连接"""
        # print("🔄 Connection is slow/dead, refreshing...")
        self.login()

    def close(self):
        bs.logout()

def get_kdata_final(code):
    """
    获取 K线 + 估值指标 + 复权因子 + 自动计算流通市值
    """
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
    
    # 3. 处理与合并
    df_k['date'] = pd.to_datetime(df_k['date'])
    
    if data_fac:
        df_fac = pd.DataFrame(data_fac, columns=rs_fac.fields)
        df_fac.rename(columns={'dividOperateDate': 'date'}, inplace=True)
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        df_k = pd.merge(df_k, df_fac[['date', 'adjustFactor']], on='date', how='left')
        df_k['adjustFactor'] = df_k['adjustFactor'].ffill().fillna(1.0)
    else:
        df_k['adjustFactor'] = 1.0

    # 4. 类型转换
    df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
    
    numeric_cols = [
        'open', 'high', 'low', 'close', 
        'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustFactor'
    ]
    df_k[numeric_cols] = df_k[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # 5. 计算流通市值
    try:
        safe_turn = df_k['turn'].replace(0, np.nan)
        float_shares = df_k['volume'] / (safe_turn / 100)
        df_k['mkt_cap'] = df_k['close'] * float_shares
        df_k['mkt_cap'] = df_k['mkt_cap'].ffill().fillna(0.0)
    except Exception:
        df_k['mkt_cap'] = 0.0

    return df_k

def main():
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    session = BaostockSession()
    
    success_count = 0
    slow_streak = 0 # 连续慢速计数器
    
    # 初始化 tqdm
    pbar = tqdm(stocks, desc=f"Job {TASK_INDEX}")
    
    for s in pbar:
        # === 计时开始 ===
        start_ts = time.time()
        
        try:
            df = get_kdata_final(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
        except Exception as e:
            # 出错视为极慢，直接触发计数
            time.sleep(1) 
            pass
            
        # === 计时结束 ===
        duration = time.time() - start_ts
        
        # 动态显示当前耗时
        pbar.set_postfix({"cost": f"{duration:.2f}s", "streak": slow_streak})

        # === 智能重连逻辑 ===
        if duration > SLOW_THRESHOLD:
            slow_streak += 1
            if slow_streak >= MAX_SLOW_STREAK:
                pbar.write(f"⚡ Detected lag ({slow_streak}x > {SLOW_THRESHOLD}s), refreshing connection...")
                session.refresh()
                slow_streak = 0 # 重置计数器
        else:
            # 只要有一次快的，说明连接是健康的，重置计数器
            slow_streak = 0

    session.close()
    print(f"Job {TASK_INDEX} Done: {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
