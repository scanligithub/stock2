# scripts/download_kline.py
import baostock as bs
import pandas as pd
import os
import json
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" 
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    # adjustflag="3" 不复权
    # 包含 peTTM, pbMRQ, adjustfactor
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,adjustfactor"
    
    # 【调试】打印正在下载谁
    # print(f"Querying {code}...") 
    
    rs = bs.query_history_k_data_plus(
        code, fields,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    # 【关键调试】如果出错，打印错误信息
    if rs.error_code != '0':
        print(f"❌ Baostock Error [{code}]: {rs.error_msg}")
        return pd.DataFrame()

    data = []
    while rs.next():
        data.append(rs.get_row_data())
    
    # 【关键调试】如果没有数据，打印提示
    if not data: 
        print(f"⚠️ No data returned for [{code}] (Error Code: {rs.error_code})")
        return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=rs.fields)
    
    # 强制转换数值
    numeric_cols = [
        'open', 'high', 'low', 'close', 
        'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustfactor'
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    return df

def main():
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    if not os.path.exists(task_file): 
        print(f"任务文件不存在: {task_file}，跳过。")
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        return

    success_count = 0
    # 这里的 tqdm 会显示进度
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        try:
            df = get_kdata(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
            # 不需要 else print，因为 get_kdata 里面已经打印了
                
        except Exception as e:
            print(f"❌ Exception downloading {s['code']}: {e}")
            
    bs.logout()
    print(f"Job {TASK_INDEX} 完成: 成功下载 {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
