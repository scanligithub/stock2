# scripts/download_sector.py
import requests
import pandas as pd
import time
import random
import os
import sys

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 从环境变量获取 Cloudflare Worker 地址
# 格式如: https://xxx.xxx.workers.dev
CF_WORKER_URL = os.getenv("CF_WORKER_URL")

if not CF_WORKER_URL:
    print("❌ 错误: 未设置 CF_WORKER_URL 环境变量！")
    # 为了防止你本地运行报错，这里可以写死一个方便调试，但在 GitHub 上必须用 Secrets
    # CF_WORKER_URL = "https://你的worker地址" 
    sys.exit(1)

def get_sector_list_by_type(name, fs):
    """通过 CF Worker 获取板块列表"""
    sectors = []
    page = 1
    page_size = 100 # Worker 速度快，可以尝试大一点，但东财限制单页100
    
    print(f"正在获取 {name} 列表...", end="", flush=True)
    
    while True:
        # 请求 Worker，带上 target_func=list
        params = {
            "target_func": "list",  # 告诉 Worker 我们要访问列表接口
            "pn": page, "pz": page_size, "po": 1, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f13,f14" 
        }
        
        try:
            # 直接请求 Worker，不需要复杂的 Headers，Worker 会帮我们加
            res = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
            
            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                for item in data:
                    item['type'] = name
                sectors.extend(data)
                
                if len(data) < page_size:
                    break
                page += 1
            else:
                break
        except Exception as e:
            print(f"\n❌ Error fetching {name} page {page}: {e}")
            break
            
    print(f" -> 共 {len(sectors)} 个")
    return sectors

def get_sector_list():
    all_sectors = []
    targets = {
        "行业": "m:90 t:2",
        "概念": "m:90 t:3",
        "地域": "m:90 t:1"
    }
    for name, fs in targets.items():
        data = get_sector_list_by_type(name, fs)
        all_sectors.extend(data)
        
    df = pd.DataFrame(all_sectors)
    if df.empty: return pd.DataFrame()
    return df.rename(columns={'f12': 'code', 'f13': 'market', 'f14': 'name'})

def get_history(code, market):
    """通过 CF Worker 获取历史 K 线"""
    clean_code = str(code)
    
    # 构造 secid
    if str(market) == '90' and not clean_code.startswith('BK'):
        secid = f"{market}.BK{clean_code}"
    else:
        secid = f"{market}.{clean_code}"

    # 构造 Worker 请求参数
    params = {
        "target_func": "kline", # 告诉 Worker 我们要访问K线接口
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
    }
    
    try:
        res = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
        
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
            df['code'] = clean_code
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
        else:
            # 备用方案（处理 BK 前缀问题）
            if ".BK" in secid:
                params['secid'] = secid.replace(".BK", ".")
                res_alt = requests.get(CF_WORKER_URL, params=params, timeout=20).json()
                if res_alt and res_alt.get('data') and res_alt['data'].get('klines'):
                     klines = res_alt['data']['klines']
                     data = [x.split(',') for x in klines]
                     df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
                     df['code'] = clean_code
                     cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                     df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                     return df

    except Exception as e:
        # CF Worker 可能会返回 500 或 502，如果不打印具体错误很难排查
        # print(f"Error {code}: {e}") 
        pass
    
    return pd.DataFrame()

def main():
    print(f"🚀 使用代理加速: {CF_WORKER_URL}")
    print("Step 1: 扫描全市场板块...")
    df_list = get_sector_list()
    
    if df_list.empty:
        print("❌ 列表获取失败，可能是 Worker 配置错误或额度耗尽。")
        return

    df_list.drop_duplicates(subset=['code'], inplace=True)
    print(f"✅ 待下载板块总数: {len(df_list)} 个")
    
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    print(f"Step 2: 并发下载历史数据...")
    all_dfs = []
    total = len(df_list)
    success_count = 0
    
    for idx, row in df_list.iterrows():
        df = get_history(row['code'], row['market'])
        
        if not df.empty:
            all_dfs.append(df)
            success_count += 1
        
        if idx % 50 == 0:
            print(f"  进度: {idx}/{total} | 成功: {success_count}")
        
        # Cloudflare 抗压能力极强，我们不需要 sleep 很久，0.05秒足够
        # 甚至可以尝试 0 秒，但为了保险起见保留一点点
        time.sleep(0.05)
        
    if all_dfs:
        print("正在合并...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        
        outfile = f"{OUTPUT_DIR}/sector_full.parquet"
        full_df.to_parquet(outfile, index=False, compression='zstd')
        print(f"✅ 板块宽表生成完毕: {outfile}")
        print(f"   最终有效板块数: {full_df['code'].nunique()}")
        print(f"   总记录数: {len(full_df)}")
    else:
        print("❌ 严重错误：未下载到任何板块数据！")

if __name__ == "__main__":
    main()
