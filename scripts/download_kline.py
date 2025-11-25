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

os.makedirs(OUTPUT_DIR, exist_ok=True)

class BaostockSession:
    """Baostock 会话管理器，负责自动重连"""
    def __init__(self):
        self.login()

    def login(self):
        try:
            bs.logout() # 先尝试登出清理旧连接
        except:
            pass
        
        # 稍微随机等待一下，避免并发任务同时撞击登录接口
        time.sleep(random.uniform(0.5, 2.0))
        
        ret = bs.login()
        if ret.error_code != '0':
            print(f"⚠️ Login failed: {ret.error_msg}, retrying...")
            time.sleep(5)
            bs.login() # 再试一次

    def refresh(self):
        """强制刷新连接"""
        # print("🔄 Refreshing Baostock session...")
        self.login()

    def close(self):
        bs.logout()

def get_kdata_final(code):
    """获取 K线 + 因子 + 市值"""
    # 1. K线
    fields_k = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
    rs = bs.query_history_k_data_plus(
        code, fields_k,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    if rs.error_code != '0': 
        raise Exception(f"KLine Error: {rs.error_msg}")
        
    data_list = []
    while rs.next(): data_list.append(rs.get_row_data())
    if not data_list: return pd.DataFrame()
    
    df_k = pd.DataFrame(data_list, columns=rs.fields)

    # 2. 因子
    rs_fac = bs.query_adjust_factor(code=code, start_date=START_DATE, end_date="")
    if rs_fac.error_code != '0':
        raise Exception(f"Factor Error: {rs_fac.error_msg}")
        
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
    
    # 类型转换
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor']
    df_k[numeric_cols] = df_k[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # 计算市值
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
    
    # 初始化会话
    session = BaostockSession()
    
    success_count = 0
    process_count = 0
    
    # 进度条
    pbar = tqdm(stocks, desc=f"Job {TASK_INDEX}")
    
    for s in pbar:
        process_count += 1
        
        # 【优化策略】每处理 50 只股票，主动重置一次连接，防止TCP长连接老化变慢
        if process_count % 50 == 0:
            session.refresh()
            
        retry_max = 3
        for attempt in range(retry_max):
            try:
                df = get_kdata_final(s['code'])
                if not df.empty:
                    df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                    success_count += 1
                break # 成功则跳出重试
            except Exception as e:
                # 如果报错，说明连接可能坏了，立刻刷新连接并重试
                if attempt < retry_max - 1:
                    # print(f"Retrying {s['code']} due to: {e}")
                    session.refresh()
                else:
                    print(f"Failed {s['code']}: {e}")

    session.close()
    print(f"Job {TASK_INDEX} Done: {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
