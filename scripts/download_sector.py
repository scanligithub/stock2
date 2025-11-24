import requests
import pandas as pd
import time
import random
import os

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://quote.eastmoney.com/"
}

def get_sector_list():
    """获取所有行业和概念板块"""
    sectors = []
    # m:90 t:2 (行业), m:90 t:3 (概念)
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
    """一次性获取历史数据 (1990-2050)"""
    for prefix in ["90", "93"]: # 试探前缀
        url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        params = {
            "secid": f"{prefix}.{code}",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
            "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
        }
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=3).json()
            if res and res.get('data') and res['data'].get('klines'):
                klines = res['data']['klines']
                data = [x.split(',') for x in klines]
                df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
                df['code'] = code
                # 转数值
                cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                return df
        except:
            pass
    return pd.DataFrame()

def main():
    print("Step 1: 获取板块列表...")
    df_list = get_sector_list()
    # 保存板块映射表 (供选股用)
    df_list.to_parquet(f"{OUTPUT_DIR}/../sector_list.parquet", index=False) # 存在 engine 上级
    
    print(f"Step 2: 下载 {len(df_list)} 个板块的历史数据...")
    all_dfs = []
    for idx, row in df_list.iterrows():
        df = get_history(row['code'])
        if not df.empty:
            all_dfs.append(df)
        if idx % 50 == 0: print(f"  Processed {idx}")
        time.sleep(random.uniform(0.1, 0.2)) # 极速但礼貌
        
    if all_dfs:
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        # 输出到 engine 目录
        full_df.to_parquet(f"{OUTPUT_DIR}/sector_full.parquet", index=False, compression='zstd')
        print(f"✅ 板块宽表生成完毕: {OUTPUT_DIR}/sector_full.parquet")

if __name__ == "__main__":
    main()
