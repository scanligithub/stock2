# scripts/download_sector.py
import requests
import pandas as pd
import time
import random
import os

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://quote.eastmoney.com/"
}

def get_sector_list():
    sectors = []
    # 获取行业(m:90 t:2)和概念(m:90 t:3)
    for fs in ["m:90 t:2 f:!50", "m:90 t:3 f:!50"]:
        url = "http://17.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 5000, "po": 1, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f13,f14"
        }
        try:
            res = requests.get(url, params=params, headers=HEADERS).json()
            if res and res.get('data'):
                sectors.extend(res['data']['diff'])
        except Exception as e:
            print(f"List Error: {e}")
    return pd.DataFrame(sectors).rename(columns={'f12': 'code', 'f14': 'name'})

def get_history(code):
    # 尝试不同前缀
    for prefix in ["90", "93"]: 
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"{prefix}.{code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
        }
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=5).json()
            if res and res.get('data') and res['data'].get('klines'):
                klines = res['data']['klines']
                data = [x.split(',') for x in klines]
                df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
                df['code'] = code
                cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                return df
        except:
            pass
    return pd.DataFrame()

def main():
    print("下载板块列表...")
    df_list = get_sector_list()
    # 存为 Parquet 供 DuckDB 查询
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    print(f"下载 {len(df_list)} 个板块历史K线...")
    all_dfs = []
    for idx, row in df_list.iterrows():
        df = get_history(row['code'])
        if not df.empty:
            all_dfs.append(df)
        time.sleep(random.uniform(0.1, 0.2))
        
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        # 存为板块宽表
        full_df.to_parquet(f"{OUTPUT_DIR}/sector_full.parquet", index=False, compression='zstd')
        print(f"✅ 板块数据完成: {OUTPUT_DIR}/sector_full.parquet")

if __name__ == "__main__":
    main()
