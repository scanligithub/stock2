# scripts/download_sector.py
import requests
import pandas as pd
import time
import random
import os
import sys

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 尝试获取 Cloudflare Worker 环境变量
CF_WORKER_URL = os.getenv("CF_WORKER_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://quote.eastmoney.com/",
    "Connection": "close"
}

def get_sector_list_raw(name, fs):
    """获取原始列表"""
    sectors = []
    page = 1
    page_size = 100
    
    # 如果有 CF Worker，走 Worker
    base_url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    
    print(f"正在获取 {name} 列表...", end="", flush=True)
    
    while True:
        params = {
            "pn": page, "pz": page_size, "po": 1, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f13,f14" 
        }
        
        try:
            if CF_WORKER_URL:
                params["target_func"] = "list"
                res = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
            else:
                res = requests.get(base_url, params=params, headers=HEADERS, timeout=10).json()

            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                for item in data:
                    item['type'] = name
                sectors.extend(data)
                if len(data) < page_size: break
                page += 1
                if not CF_WORKER_URL: time.sleep(0.5)
            else:
                break
        except Exception as e:
            print(f" [Err: {e}] ", end="")
            break
            
    print(f" -> {len(sectors)} 个")
    return sectors

def get_sector_list():
    all_sectors = []
    targets = {"行业": "m:90 t:2", "概念": "m:90 t:3", "地域": "m:90 t:1"}
    for name, fs in targets.items():
        data = get_sector_list_raw(name, fs)
        all_sectors.extend(data)
    
    df = pd.DataFrame(all_sectors)
    if df.empty: return pd.DataFrame()
    return df.rename(columns={'f12': 'code', 'f13': 'market', 'f14': 'name'})

def get_history(code, market):
    clean_code = str(code)
    # 构造 secid
    if str(market) == '90' and not clean_code.startswith('BK'):
        secid = f"{market}.BK{clean_code}"
    else:
        secid = f"{market}.{clean_code}"

    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
    }
    
    try:
        if CF_WORKER_URL:
            params["target_func"] = "kline"
            res = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
        else:
            base_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
            res = requests.get(base_url, params=params, headers=HEADERS, timeout=10).json()
        
        # 成功拿到数据
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
            df['code'] = clean_code
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
        
        # 备用方案 (处理 BK 前缀)
        if ".BK" in secid:
            params['secid'] = secid.replace(".BK", ".")
            if CF_WORKER_URL:
                res_alt = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
            else:
                res_alt = requests.get(base_url, params=params, headers=HEADERS, timeout=10).json()
            
            if res_alt and res_alt.get('data') and res_alt['data'].get('klines'):
                klines = res_alt['data']['klines']
                data = [x.split(',') for x in klines]
                df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
                df['code'] = clean_code
                cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                return df

    except Exception:
        pass
    
    return pd.DataFrame()

def main():
    if CF_WORKER_URL:
        print(f"🚀 代理模式: {CF_WORKER_URL}")
    else:
        print("🐢 直连模式 (可能会慢/不稳定)")

    # 1. 获取目标列表
    print("Step 1: 获取全市场板块列表...")
    df_list = get_sector_list()
    if df_list.empty:
        print("❌ 列表获取失败")
        return
        
    df_list.drop_duplicates(subset=['code'], inplace=True)
    total_targets = len(df_list)
    print(f"✅ 目标板块总数: {total_targets} 个")
    
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    # 2. 循环补录机制
    all_dfs = []
    downloaded_codes = set()
    
    # 最多尝试 3 轮
    MAX_ROUNDS = 3
    
    for round_num in range(1, MAX_ROUNDS + 1):
        # 找出本轮需要下载的 (总目标 - 已成功)
        pending_df = df_list[~df_list['code'].isin(downloaded_codes)]
        
        if pending_df.empty:
            print("✨ 所有板块已全部下载完成！")
            break
            
        print(f"\n🔄 第 {round_num}/{MAX_ROUNDS} 轮下载 (剩余 {len(pending_df)} 个)...")
        
        count = 0
        for _, row in pending_df.iterrows():
            df = get_history(row['code'], row['market'])
            
            if not df.empty:
                all_dfs.append(df)
                downloaded_codes.add(row['code'])
            
            count += 1
            if count % 50 == 0:
                print(f"   进度: {count}/{len(pending_df)} | 当前总成功: {len(downloaded_codes)}")
            
            # 延时策略
            if not CF_WORKER_URL:
                time.sleep(random.uniform(0.1, 0.3))
            else:
                time.sleep(0.02) # 代理模式可以很快
    
    # 3. 合并结果
    print(f"\n📊 最终统计: 目标 {total_targets} -> 成功 {len(downloaded_codes)}")
    
    if all_dfs:
        print("正在合并宽表...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        
        outfile = f"{OUTPUT_DIR}/sector_full.parquet"
        full_df.to_parquet(outfile, index=False, compression='zstd')
        print(f"✅ 文件已生成: {outfile}")
        print(f"   总记录数: {len(full_df)}")
    else:
        print("❌ 严重错误：所有轮次均未下载到数据！")

if __name__ == "__main__":
    main()
