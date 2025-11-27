# scripts/download_kline.py
import baostock as bs
import pandas as pd
import numpy as np
import os
import json
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "2005-01-01" 
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata_final(code):
    """
    获取 K线 + 估值指标 + 复权因子 + 自动计算流通市值
    """
    # -------------------------------------------------------
    # 1. 下载基础 K 线 (含 PE/PB)
    # -------------------------------------------------------
    # adjustflag="3": 保持交易所原始行情 (不复权)
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

    # -------------------------------------------------------
    # 2. 下载 复权因子 (adjustFactor)
    # -------------------------------------------------------
    rs_fac = bs.query_adjust_factor(code=code, start_date=START_DATE, end_date="")
    data_fac = []
    while rs_fac.next(): data_fac.append(rs_fac.get_row_data())
    
    # -------------------------------------------------------
    # 3. 数据处理与合并
    # -------------------------------------------------------
    df_k['date'] = pd.to_datetime(df_k['date'])
    
    if data_fac:
        df_fac = pd.DataFrame(data_fac, columns=rs_fac.fields)
        df_fac.rename(columns={'dividOperateDate': 'date'}, inplace=True)
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        
        # Merge: 将因子并入 K 线
        df_k = pd.merge(df_k, df_fac[['date', 'adjustFactor']], on='date', how='left')
        
        # 向下填充 (Forward Fill)
        df_k['adjustFactor'] = df_k['adjustFactor'].ffill()
        df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
    else:
        df_k['adjustFactor'] = 1.0

    # -------------------------------------------------------
    # 4. 数值转换
    # -------------------------------------------------------
    df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
    
    numeric_cols = [
        'open', 'high', 'low', 'close', 
        'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustFactor'
    ]
    
    df_k[numeric_cols] = df_k[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    # -------------------------------------------------------
    # 5. 计算流通市值 (Float Market Cap)
    # -------------------------------------------------------
    try:
        # 避免除以 0
        safe_turn = df_k['turn'].replace(0, np.nan)
        # 计算流通股本
        float_shares = df_k['volume'] / (safe_turn / 100)
        # 计算市值
        df_k['mkt_cap'] = df_k['close'] * float_shares
        # 填充空值
        df_k['mkt_cap'] = df_k['mkt_cap'].ffill().fillna(0.0)
    except Exception:
        df_k['mkt_cap'] = 0.0

    return df_k

def main():
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): 
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    # 登录一次，保持会话直到结束
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Login failed: {lg.error_msg}")
        return

    success_count = 0
    
    # 简单的遍历，没有任何复杂的重连逻辑
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        try:
            df = get_kdata_final(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
        except Exception as e:
            print(f"Error {s['code']}: {e}")
            
    bs.logout()
    print(f"Job {TASK_INDEX} Done: {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
