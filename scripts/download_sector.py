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
    """获取原始列表 (带页级重试 + 自动翻页)"""
    sectors = []
    page = 1
    page_size = 100
    
    base_url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    
    print(f"正在获取 {name} 列表...", end="", flush=True)
    
    while True:
        # 页级重试循环
        success = False
        res_json = None
        
        for retry in range(3):
            params = {
                "pn": page, "pz": page_size, "po": 1, "np": 1, 
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
                "fields": "f12,f13,f14" 
            }
            
            try:
                if CF_WORKER_URL:
                    params["target_func"] = "list"
                    resp = requests.get(CF_WORKER_URL, params=params, timeout=30)
                else:
                    resp = requests.get(base_url, params=params, headers=HEADERS, timeout=10)
                
                res_json = resp.json()
                success = True
                break
            except Exception:
                time.sleep(1)
        
        if not success:
            print(f" [Page {page} Failed] ", end="")
            break 
            
        try:
            if res_json and res_json.get('data') and res_json['data'].get('diff'):
                data = res_json['data']['diff']
                for item in data:
                    item['type'] = name
                sectors.extend(data)
                
                print(".", end="", flush=True)
                
                if len(data) < page_size: 
                    break 
                
                page += 1
                if not CF_WORKER_URL: time.sleep(0.2)
            else:
                break
        except Exception:
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

def get_constituents(sector_code):
    """
    获取指定板块的成分股列表
    """
    stocks = []
    page = 1
    page_size = 200 # 成分股通常不多，200足够一页
    
    # 构造请求用的 code (e.g., BK0425)
    req_code = f"BK{sector_code}" if not str(sector_code).startswith('BK') else sector_code
    
    base_url = "http://4.push2.eastmoney.com/api/qt/clist/get"
    
    while True:
        params = {
            "pn": page, "pz": page_size, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3",
            "fs": f"b:{req_code}", # 关键参数：b:BKxxxx
            "fields": "f12,f14"    # f12:股票代码, f14:股票名称
        }
        
        try:
            if CF_WORKER_URL:
                params["target_func"] = "list" # 复用 list 逻辑
                res = requests.get(CF_WORKER_URL, params=params, timeout=15).json()
            else:
                res = requests.get(base_url, params=params, headers=HEADERS, timeout=10).json()

            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                stocks.extend(data)
                if len(data) < page_size: break
                page += 1
                if not CF_WORKER_URL: time.sleep(0.1)
            else:
                break
        except:
            break
            
    return stocks

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
            res = requests.get(CF_WORKER_URL, params=params, timeout=30).json()
        else:
            base_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
            res = requests.get(base_url, params=params, headers=HEADERS, timeout=10).json()
        
        # 1. 成功拿到数据
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
            df['code'] = clean_code
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
        
        # 2. 备用方案 (处理 BK 前缀不一致问题)
        if ".BK" in secid:
            params['secid'] = secid.replace(".BK", ".")
            if CF_WORKER_URL:
                res_alt = requests.get(CF_WORKER_URL, params=params, timeout=30).json()
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
    
    raw_count = len(df_list)
    
    # 【核心去重逻辑】
    df_list.drop_duplicates(subset=['code'], inplace=True)
    
    unique_count = len(df_list)
    print(f"📋 原始扫描: {raw_count} 个 -> 剔除重复: {raw_count - unique_count} 个")
    print(f"✅ 最终有效目标: {unique_count} 个")
    
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    # ==========================================
    # 2. 【新增】下载成分股映射关系
    # ==========================================
    print("Step 1.5: 下载板块成分股映射关系...")
    all_relations = []
    sector_codes = df_list['code'].unique()
    
    count = 0
    for sec_code in sector_codes:
        stocks = get_constituents(sec_code)
        for s in stocks:
            all_relations.append({
                'sector_code': str(sec_code).replace('BK', ''), 
                'stock_code': s['f12'],
                'stock_name': s['f14']
            })
        
        count += 1
        if count % 50 == 0:
            print(f"  已获取成分股: {count}/{len(sector_codes)} 个板块")
        
        # 即使是 Worker，获取成分股也建议保留微小延迟，防止并发过高
        if not CF_WORKER_URL: time.sleep(0.1)
        else: time.sleep(0.01)
            
    if all_relations:
        df_rel = pd.DataFrame(all_relations)
        rel_path = f"{OUTPUT_DIR}/sector_constituents.parquet"
        df_rel.to_parquet(rel_path, index=False, compression='zstd')
        print(f"✅ 成分股表已生成: {len(df_rel)} 行 -> {rel_path}")
    else:
        print("⚠️ 未获取到成分股关系")

    # ==========================================
    # 3. 循环补录机制下载 K 线 (Retry Loop)
    # ==========================================
    all_dfs = []
    downloaded_codes = set()
    MAX_ROUNDS = 3
    
    for round_num in range(1, MAX_ROUNDS + 1):
        pending_df = df_list[~df_list['code'].isin(downloaded_codes)]
        
        if pending_df.empty:
            print("✨ 所有板块K线已全部下载完成！")
            break
            
        print(f"\n🔄 第 {round_num}/{MAX_ROUNDS} 轮下载K线 (剩余 {len(pending_df)} 个)...")
        
        count = 0
        for _, row in pending_df.iterrows():
            df = get_history(row['code'], row['market'])
            
            if not df.empty:
                all_dfs.append(df)
                downloaded_codes.add(row['code'])
            
            count += 1
            if count % 50 == 0:
                print(f"   进度: {count}/{len(pending_df)} | 当前总成功: {len(downloaded_codes)}")
            
            if not CF_WORKER_URL: time.sleep(random.uniform(0.1, 0.3))
            else: time.sleep(0.02)
    
    # 4. 合并结果
    print(f"\n📊 最终统计: 目标 {unique_count} -> 成功 {len(downloaded_codes)}")
    
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
