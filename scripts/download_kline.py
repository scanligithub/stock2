# scripts/download_kline.py
import baostock as bs
import pandas as pd
import os
import json
from tqdm import tqdm

OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" # 下载全量历史
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    # adjustflag="3" 不复权
    # 包含 peTTM (滚动市盈率), pbMRQ (市净率) -> 宽表合并必备
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
    # 强转数值类型，减小体积
    nums = ['open','high','low','close','volume','amount','turn','pctChg','peTTM','pbMRQ']
    df[nums] = df[nums].apply(pd.to_numeric, errors='coerce')
    return df

def main():
    # 修正：读取 task_slices 目录下的文件
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): 
        print(f"任务文件不存在: {task_file}")
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    lg = bs.login()
    if lg.error_code != '0':
        print("Baostock 登录失败")
        return

    success_count = 0
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        df = get_kdata(s['code'])
        if not df.empty:
            df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
            success_count += 1
            
    bs.logout()
    print(f"Job {TASK_INDEX} 完成: 成功下载 {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
