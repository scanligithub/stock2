import baostock as bs
import pandas as pd
import os
import json
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" # 下载全部历史
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    # adjustflag="3" 不复权 (复权由前端计算或使用因子)
    # 包含 peTTM, pbMRQ (宽表核心)
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
    
    rs = bs.query_history_k_data_plus(
        code, fields,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    data = []
    while rs.next():
        data.append(rs.get_row_data())
    
    if not data: return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=rs.fields)
    # 类型转换，压缩体积
    nums = ['open','high','low','close','volume','amount','turn','pctChg','peTTM','pbMRQ']
    df[nums] = df[nums].apply(pd.to_numeric, errors='coerce')
    return df

def main():
    task_file = f"tasks/task_{TASK_INDEX}.json"
    if not os.path.exists(task_file): return

    with open(task_file) as f:
        stocks = json.load(f)
    
    bs.login()
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX}"):
        df = get_kdata(s['code'])
        if not df.empty:
            df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
    bs.logout()

if __name__ == "__main__":
    main()
