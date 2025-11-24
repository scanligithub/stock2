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
    """
    获取全量板块列表
    """
    sectors = []
    # 移除 f:!50 过滤，确保获取全量
    board_types = {
        "行业": "m:90 t:2",
        "概念": "m:90 t:3",
        "地域": "m:90 t:1"
    }
    
    for name, fs in board_types.items():
        print(f"正在获取 {name} 板块列表...")
        url = "http://17.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 5000, "po": 1, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f13,f14" # f12:code, f14:name
        }
        try:
            res = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                print(f"  -> 发现 {len(data)} 个 {name} 板块")
                for item in data:
                    item['type'] = name
                sectors.extend(data)
        except Exception as e:
            print(f"List Error ({name}): {e}")
            
    df = pd.DataFrame(sectors)
    # 重命名列
    return df.rename(columns={'f12': 'code', 'f14': 'name'})

def get_history(code):
    """
    一次性获取历史数据
    关键修复：代码前必须加 BK
    """
    # 【核心修复】
    # 列表返回的是 "0425"，接口需要的是 "90.BK0425"
    if not str(code).startswith('BK'):
        secid = f"90.BK{code}"
    else:
        secid = f"90.{code}"
    
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
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
            df['code'] = code # 这里保留原始代码(如0425)，不带BK，方便后续映射
            
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
    except:
        pass
    
    return pd.DataFrame()

def main():
    print("Step 1: 下载全量板块列表...")
    df_list = get_sector_list()
    
    # 去重
    df_list.drop_duplicates(subset=['code'], inplace=True)
    print(f"去重后待下载板块总数: {len(df_list)} 个")
    
    # 保存板块列表
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    print(f"Step 2: 并发下载历史数据...")
    all_dfs = []
    
    total = len(df_list)
    success_count = 0
    
    for idx, row in df_list.iterrows():
        df = get_history(row['code'])
        
        if not df.empty:
            all_dfs.append(df)
            success_count += 1
        
        if idx % 50 == 0: 
            print(f"  Processed {idx}/{total} - Success: {success_count}")
        
        time.sleep(random.uniform(0.05, 0.1))
        
    if all_dfs:
        print("正在合并板块宽表...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        
        outfile = f"{OUTPUT_DIR}/sector_full.parquet"
        full_df.to_parquet(outfile, index=False, compression='zstd')
        print(f"✅ 板块宽表生成完毕: {outfile}")
        print(f"   最终有效板块数: {full_df['code'].nunique()}")
        print(f"   总记录数: {len(full_df)}")
    else:
        print("❌ 未下载到任何板块数据")

if __name__ == "__main__":
    main()
