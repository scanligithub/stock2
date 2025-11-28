# scripts/download_sector.py
import requests
import pandas as pd
import time
import random
import os
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 尝试获取 Cloudflare Worker 环境变量
CF_WORKER_URL = os.getenv("CF_WORKER_URL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://quote.eastmoney.com/",
    "Connection": "close"
}

def create_session():
    """创建一个高可用的 Session"""
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 104])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session

sess = create_session()

def get_sector_list_raw(name, fs):
    """获取板块列表"""
    sectors = []
    page = 1
    page_size = 100
    base_url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    
    print(f"正在获取 {name} 列表...", end="", flush=True)
    
    while True:
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
                    resp = sess.get(CF_WORKER_URL, params=params, timeout=30)
                else:
                    resp = sess.get(base_url, params=params, timeout=10)
                res_json = resp.json()
                success = True
                break
            except:
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
                if len(data) < page_size: break
                page += 1
                time.sleep(0.1)
            else:
                break
        except: break
            
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

def get_kline_history(secid, clean_code):
    """获取 K 线历史"""
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58", # 日期,开,收,高,低,量,额,换手
        "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
    }
    
    try:
        if CF_WORKER_URL:
            params["target_func"] = "kline"
            res = sess.get(CF_WORKER_URL, params=params, timeout=30).json()
        else:
            url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
            res = sess.get(url, params=params, timeout=10).json()
        
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
            df['code'] = clean_code
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
    except Exception as e:
        # print(f"Kline err {clean_code}: {e}")
        pass
    return pd.DataFrame()

def get_flow_history(secid, clean_code):
    """【新增】获取资金流历史"""
    # 字段映射：
    # f51:日期, f52:主力净流入, f53:小单, f54:中单, f55:大单, f56:超大单
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f7",
        "fields2": "f51,f52,f53,f54,f55,f56", 
        "klt": "101", "lmt": "0" # 0代表全量
    }
    
    try:
        if CF_WORKER_URL:
            params["target_func"] = "flow" # 调用 Worker 的 flow 接口
            res = sess.get(CF_WORKER_URL, params=params, timeout=30).json()
        else:
            url = "http://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get"
            res = sess.get(url, params=params, timeout=10).json()
            
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'main_net_flow', 'small_net_flow', 'medium_net_flow', 'large_net_flow', 'super_large_net_flow'])
            
            # 计算 net_flow_amount (主力 = 超大+大)
            # 东财接口里 f52 已经是主力净流入
            df.rename(columns={'main_net_flow': 'net_flow_amount'}, inplace=True) 
            # 兼容性：这里我们将 net_flow_amount 视为主力净流入，与个股保持一致
            # 个股表中 net_flow_amount 是全单净流入吗？通常主力净流入更有价值。
            # 为了统一，我们把 f52 映射为 main_net_flow
            
            df['code'] = clean_code
            cols = ['net_flow_amount', 'small_net_flow', 'medium_net_flow', 'large_net_flow', 'super_large_net_flow']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            
            # 这里额外生成一个 main_net_flow 字段，等于 net_flow_amount (东财定义f52即主力)
            df['main_net_flow'] = df['net_flow_amount']
            
            return df
    except Exception as e:
        # print(f"Flow err {clean_code}: {e}")
        pass
    return pd.DataFrame()

def process_one_sector(code, market):
    clean_code = str(code)
    # 构造 secid
    if str(market) == '90' and not clean_code.startswith('BK'):
        secid = f"{market}.BK{clean_code}"
    else:
        secid = f"{market}.{clean_code}"
        
    # 1. 下载 K 线
    df_k = get_kline_history(secid, clean_code)
    
    # 2. 备用 secid 尝试 (处理 BK 前缀不一致)
    if df_k.empty and ".BK" in secid:
        alt_secid = secid.replace(".BK", ".")
        df_k = get_kline_history(alt_secid, clean_code)
        if not df_k.empty:
            secid = alt_secid # 修正 secid 用于后续资金流下载

    if df_k.empty: return pd.DataFrame()

    # 3. 下载 资金流
    df_f = get_flow_history(secid, clean_code)
    
    # 4. 合并 (Left Join)
    if not df_f.empty:
        df_merged = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
        # 填充 NaN 为 0 (早期没有资金流数据)
        flow_cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_net_flow', 'small_net_flow']
        for c in flow_cols:
            if c in df_merged.columns:
                df_merged[c] = df_merged[c].fillna(0)
        return df_merged
    
    return df_k

def main():
    if CF_WORKER_URL:
        print(f"🚀 代理模式: {CF_WORKER_URL}")
    else:
        print("🐢 直连模式")

    # 1. 获取列表
    print("Step 1: 获取全市场板块列表...")
    df_list = get_sector_list()
    if df_list.empty:
        print("❌ 列表获取失败")
        return
    
    df_list.drop_duplicates(subset=['code'], inplace=True)
    unique_count = len(df_list)
    print(f"✅ 最终有效目标: {unique_count} 个")
    
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    # 2. 循环补录
    all_dfs = []
    downloaded_codes = set()
    MAX_ROUNDS = 3
    
    for round_num in range(1, MAX_ROUNDS + 1):
        pending_df = df_list[~df_list['code'].isin(downloaded_codes)]
        if pending_df.empty:
            print("✨ 所有板块已全部下载完成！")
            break
            
        print(f"\n🔄 第 {round_num}/{MAX_ROUNDS} 轮下载 (剩余 {len(pending_df)} 个)...")
        
        count = 0
        for _, row in pending_df.iterrows():
            # 同时下载 K线 + 资金流
            df = process_one_sector(row['code'], row['market'])
            
            if not df.empty:
                all_dfs.append(df)
                downloaded_codes.add(row['code'])
            
            count += 1
            if count % 50 == 0:
                print(f"   进度: {count}/{len(pending_df)} | 成功: {len(downloaded_codes)}")
            
            time.sleep(0.05)
    
    # 3. 合并保存
    print(f"\n📊 最终统计: 目标 {unique_count} -> 成功 {len(downloaded_codes)}")
    
    if all_dfs:
        print("正在合并宽表...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        full_df.sort_values(['code', 'date'], inplace=True)
        
        # 压缩类型
        float_cols = full_df.select_dtypes(include=['float64']).columns
        full_df[float_cols] = full_df[float_cols].astype('float32')
        
        outfile = f"{OUTPUT_DIR}/sector_full.parquet"
        full_df.to_parquet(outfile, index=False, compression='zstd')
        print(f"✅ 文件已生成: {outfile}")
        print(f"   总记录数: {len(full_df)}")
        print(f"   包含资金流列: {'net_flow_amount' in full_df.columns}")
    else:
        print("❌ 严重错误：未下载到数据！")

if __name__ == "__main__":
    main()
