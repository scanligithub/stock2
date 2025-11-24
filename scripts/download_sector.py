# scripts/download_sector.py
import requests
import pandas as pd
import time
import random
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "http://quote.eastmoney.com/",
    "Connection": "close"  # 【关键修改】主动关闭长连接，防止 RemoteDisconnected
}

def create_session():
    """创建一个高可用的 Session"""
    session = requests.Session()
    # 增加重试次数到 5 次，增加 backoff_factor (重试间隔时间)
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 104])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    return session

# 全局 session
sess = create_session()

def get_sector_list_by_type(name, fs):
    """自动翻页获取板块列表"""
    sectors = []
    page = 1
    page_size = 100
    
    print(f"正在获取 {name} 列表...", end="", flush=True)
    
    while True:
        url = "http://17.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": page, "pz": page_size, "po": 1, "np": 1, 
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3", "fs": fs,
            "fields": "f12,f13,f14" 
        }
        try:
            res = sess.get(url, params=params, timeout=15).json()
            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                for item in data:
                    item['type'] = name
                sectors.extend(data)
                
                if len(data) < page_size:
                    break
                page += 1
                time.sleep(0.5) # 翻页稍微停顿一下
            else:
                break
        except Exception as e:
            print(f"\n❌ Error fetching {name} page {page}: {e}")
            break
            
    print(f" -> 共 {len(sectors)} 个")
    return sectors

def get_sector_list():
    """获取全量板块"""
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
    # 可能有些板块获取失败导致为空，做个判断
    if df.empty:
        return pd.DataFrame(columns=['code', 'market', 'name'])
    return df.rename(columns={'f12': 'code', 'f13': 'market', 'f14': 'name'})

def get_history(code, market):
    """获取历史数据"""
    clean_code = str(code)
    
    # 构造 secid
    if str(market) == '90' and not clean_code.startswith('BK'):
        secid = f"{market}.BK{clean_code}"
    else:
        secid = f"{market}.{clean_code}"

    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101", "fqt": "1", "beg": "19900101", "end": "20500101", "lmt": "1000000"
    }
    
    try:
        # timeout 增加到 15秒
        res = sess.get(url, params=params, timeout=15).json()
        
        if res and res.get('data') and res['data'].get('klines'):
            klines = res['data']['klines']
            data = [x.split(',') for x in klines]
            df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
            df['code'] = clean_code
            cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
            df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
            return df
        else:
            # 备用方案重试
            if ".BK" in secid:
                alt_secid = secid.replace(".BK", ".")
                params['secid'] = alt_secid
                res_alt = sess.get(url, params=params, timeout=15).json()
                if res_alt and res_alt.get('data') and res_alt['data'].get('klines'):
                     klines = res_alt['data']['klines']
                     data = [x.split(',') for x in klines]
                     df = pd.DataFrame(data, columns=['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turnover'])
                     df['code'] = clean_code
                     cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                     df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                     return df

    except Exception as e:
        # 此时的 error 通常是重试多次后依然失败
        print(f"Failed {code}: {e}")
    
    return pd.DataFrame()

def main():
    print("Step 1: 扫描全市场板块 (稳健模式)...")
    df_list = get_sector_list()
    
    if df_list.empty:
        print("❌ 列表获取失败，退出。")
        return

    df_list.drop_duplicates(subset=['code'], inplace=True)
    print(f"✅ 去重后待下载板块总数: {len(df_list)} 个")
    
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    print(f"Step 2: 开始下载历史数据...")
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
        
        # 【关键】增加延迟到 0.2 - 0.4 秒，宁慢勿挂
        time.sleep(random.uniform(0.2, 0.4))
        
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
