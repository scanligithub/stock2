# scripts/download_kline.py
import baostock as bs
import pandas as pd
import numpy as np
import os
import json
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" 
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
    # 这是“后复权因子”。
    # 计算前复权公式: price_fwd = price_raw * (adjustFactor / adjustFactor_latest)
    rs_fac = bs.query_adjust_factor(code=code, start_date=START_DATE, end_date="")
    data_fac = []
    while rs_fac.next(): data_fac.append(rs_fac.get_row_data())
    
    # -------------------------------------------------------
    # 3. 数据处理与合并
    # -------------------------------------------------------
    df_k['date'] = pd.to_datetime(df_k['date'])
    
    if data_fac:
        df_fac = pd.DataFrame(data_fac, columns=rs_fac.fields)
        # 因子数据中的日期是除权除息日
        df_fac.rename(columns={'dividOperateDate': 'date'}, inplace=True)
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        
        # Merge: 将因子并入 K 线
        df_k = pd.merge(df_k, df_fac[['date', 'adjustFactor']], on='date', how='left')
        
        # 向下填充 (Forward Fill): 除权日之后的日子沿用该因子
        df_k['adjustFactor'] = df_k['adjustFactor'].ffill()
        # 上市初期填充为 1.0
        df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
    else:
        # 无除权记录，因子默认为 1.0
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
    # 5. 【核心】计算流通市值 (Float Market Cap)
    # -------------------------------------------------------
    # 公式: 流通市值 = 收盘价 * 流通股本
    # 流通股本 = 成交量 / (换手率%)
    try:
        # 避免除以 0 (停牌日换手率为0)
        safe_turn = df_k['turn'].replace(0, np.nan)
        
        # 计算流通股本 (volume单位是股, turn是百分比)
        float_shares = df_k['volume'] / (safe_turn / 100)
        
        # 计算市值 (单位: 元)
        df_k['mkt_cap'] = df_k['close'] * float_shares
        
        # 停牌日市值填充 (沿用昨日)
        df_k['mkt_cap'] = df_k['mkt_cap'].ffill()
        
        # 极端情况填充 0
        df_k['mkt_cap'] = df_k['mkt_cap'].fillna(0.0)
        
    except Exception:
        df_k['mkt_cap'] = 0.0

    return df_k

def main():
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): 
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    lg = bs.login()
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        try:
            df = get_kdata_final(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
        except Exception as e:
            # print(f"Error {s['code']}: {e}")
            pass
            
    bs.logout()

if __name__ == "__main__":
    main()
