# scripts/download_kline.py
import baostock as bs
import pandas as pd
import os
import json
from tqdm import tqdm

# 配置
OUTPUT_DIR = "temp_kline"
START_DATE = "1990-01-01" # 下载全量历史
# 获取环境变量中的任务索引，默认为 0
TASK_INDEX = int(os.getenv("TASK_INDEX", 0))

os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_kdata(code):
    """
    下载单只股票的K线数据，包含：
    1. 基础行情 (Open/High/Low/Close/Volume...)
    2. 财务指标 (PE-TTM, PB-MRQ)
    3. 复权因子 (adjustfactor)
    """
    # adjustflag="3" 代表不复权 (下载交易所原始价格)
    # 我们通过额外下载 adjustfactor 来支持后续计算前/后复权
    fields = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,adjustfactor"
    
    rs = bs.query_history_k_data_plus(
        code, fields,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    if rs.error_code != '0':
        return pd.DataFrame()

    data = []
    while rs.next():
        data.append(rs.get_row_data())
    
    if not data: return pd.DataFrame()
    
    df = pd.DataFrame(data, columns=rs.fields)
    
    # 强制转换数值类型，减小 Parquet 体积并提升 DuckDB 读取速度
    numeric_cols = [
        'open', 'high', 'low', 'close', 
        'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustfactor'
    ]
    
    # 使用 errors='coerce' 将无效字符转为 NaN
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    
    return df

def main():
    # 读取对应的任务分片文件
    task_file = f"task_slices/task_slice_{TASK_INDEX}.json"
    
    # 如果文件不存在（可能任务数少于分片数），直接跳过
    if not os.path.exists(task_file): 
        print(f"任务文件不存在: {task_file}，跳过。")
        return

    with open(task_file, 'r', encoding='utf-8') as f:
        stocks = json.load(f)
    
    # 登录 Baostock 系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f"Baostock 登录失败: {lg.error_msg}")
        return

    success_count = 0
    # 使用 tqdm 显示进度条
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        try:
            df = get_kdata(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
        except Exception as e:
            print(f"Error downloading {s['code']}: {e}")
            
    bs.logout()
    print(f"Job {TASK_INDEX} 完成: 成功下载 {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
