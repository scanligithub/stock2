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

def get_sector_list_by_type(name, fs):
    """
    自动翻页获取某一类板块的全量列表
    """
    sectors = []
    page = 1
    page_size = 100 # 东财很多接口强制限制单页100，所以要翻页
    
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
            res = requests.get(url, params=params, headers=HEADERS, timeout=10).json()
            if res and res.get('data') and res['data'].get('diff'):
                data = res['data']['diff']
                # 标记类型
                for item in data:
                    item['type'] = name
                sectors.extend(data)
                
                # 如果返回的数据少于页大小，说明是最后一页
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
    """获取全量板块"""
    all_sectors = []
    
    # 行业、概念、地域
    # 增加 "风格" 板块 (m:90 t:4) 
    targets = {
        "行业": "m:90 t:2",
        "概念": "m:90 t:3",
        "地域": "m:90 t:1"
    }
    
    for name, fs in targets.items():
        data = get_sector_list_by_type(name, fs)
        all_sectors.extend(data)
        
    return pd.DataFrame(all_sectors).rename(columns={'f12': 'code', 'f14': 'name'})

def get_history(code):
    """
    尝试多种组合获取历史数据
    """
    # 清洗代码：有些代码自带 BK，有的不带
    # 我们统一把 BK 去掉，然后自己拼
    clean_code = str(code).replace("BK", "")
    
    # 暴力尝试列表：优先试 90.BKxxxx (最常见)，其次 90.xxxx
    secid_trials = [
        f"90.BK{clean_code}",  # 标准格式
        f"90.{clean_code}",    # 纯数字格式
    ]
    
    for secid in secid_trials:
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
                
                # 统一使用原始 code（不带BK，方便映射）
                df['code'] = clean_code 
                
                cols = ['open', 'close', 'high', 'low', 'volume', 'amount', 'turnover']
                df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
                
                return df
        except:
            continue
    
    return pd.DataFrame()

def main():
    print("Step 1: 扫描全市场板块...")
    df_list = get_sector_list()
    
    # 去重：同一个板块可能属于多种分类，按代码去重
    # 清洗代码列，去掉 BK 前缀以便统一
    df_list['code'] = df_list['code'].astype(str).str.replace("BK", "")
    df_list.drop_duplicates(subset=['code'], inplace=True)
    
    print(f"✅ 去重后待下载板块总数: {len(df_list)} 个")
    
    # 保存列表
    df_list.to_parquet(f"{OUTPUT_DIR}/sector_list.parquet", index=False)
    
    print(f"Step 2: 开始下载历史数据...")
    all_dfs = []
    
    total = len(df_list)
    success_count = 0
    
    for idx, row in df_list.iterrows():
        df = get_history(row['code'])
        
        if not df.empty:
            all_dfs.append(df)
            success_count += 1
        
        # 进度打印
        if idx % 50 == 0:
            print(f"  进度: {idx}/{total} | 成功: {success_count}")
            
        time.sleep(random.uniform(0.05, 0.1))
        
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
