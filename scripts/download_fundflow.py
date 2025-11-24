# scripts/download_fundflow.py
import requests
import pandas as pd
import os
import json
import time
import random
from tqdm import tqdm

OUTPUT_DIR = "temp_fundflow"
# 获取当前任务编号，默认为 0
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 伪装浏览器头
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def get_sina_flow(code):
    """
    下载新浪资金流数据
    """
    symbol = code.replace('.', '') 
    # num=5000 足够覆盖很久的历史
    url = f"https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_qsfx_lscjfb?page=1&num=5000&sort=opendate&asc=0&daima={symbol}"
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        data = r.json()
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data)
        rename_map = {
            'opendate': 'date', 
            'netamount': 'net_flow_amount',
            'r0_net': 'main_net_flow',
            'r1_net': 'super_large_net_flow',
            'r2_net': 'large_net_flow',
            'r3_net': 'medium_small_net_flow'
        }
        df = df.rename(columns=rename_map)
        
        # 转数值
        cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
        for c in cols:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        
        return df[['date'] + cols] 
    except:
        return pd.DataFrame()

def main():
    # 读取对应的任务分片文件
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    
    # 如果文件不存在（可能任务数少于分片数），直接跳过
    if not os.path.exists(task_file): 
        print(f"分片文件不存在: {task_file}，本任务跳过。")
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
        
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} FundFlow"):
        try:
            df = get_sina_flow(s['code'])
            if not df.empty:
                df['code'] = s['code'] 
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
        except Exception as e:
            print(f"Error {s['code']}: {e}")
        
        # 随机延迟，防止封IP
        time.sleep(random.uniform(0.1, 0.25)) 

if __name__ == "__main__":
    main()
