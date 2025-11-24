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

def get_kdata_with_factor(code):
    """
    分两步获取数据：
    1. 获取 K 线 + PE/PB
    2. 获取 复权因子
    3. 合并
    """
    # -------------------------------------------------------
    # 1. 下载 K 线 (去掉不支持的 adjustfactor)
    # -------------------------------------------------------
    # adjustflag="3" 不复权
    fields_k = "date,code,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ"
    
    rs = bs.query_history_k_data_plus(
        code, fields_k,
        start_date=START_DATE, end_date="", 
        frequency="d", adjustflag="3"
    )
    
    if rs.error_code != '0':
        print(f"❌ KLine Error [{code}]: {rs.error_msg}")
        return pd.DataFrame()

    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    
    if not data_list: 
        return pd.DataFrame() # 没K线也就没必要下因子了
    
    df_k = pd.DataFrame(data_list, columns=rs.fields)

    # -------------------------------------------------------
    # 2. 下载 复权因子
    # -------------------------------------------------------
    # 字段：dividOperateDate(除权除息日), adjustFactor(复权因子)
    # 这里的 adjustFactor 通常是“后复权因子”
    rs_fac = bs.query_adjust_factor(code=code, start_date=START_DATE, end_date="")
    
    data_fac = []
    while rs_fac.next():
        data_fac.append(rs_fac.get_row_data())
    
    # -------------------------------------------------------
    # 3. 数据合并与填充
    # -------------------------------------------------------
    # 确保日期格式一致用于合并
    df_k['date'] = pd.to_datetime(df_k['date'])
    
    if data_fac:
        df_fac = pd.DataFrame(data_fac, columns=rs_fac.fields)
        # 将除权日重命名为 date，以便 Merge
        df_fac.rename(columns={'dividOperateDate': 'date'}, inplace=True)
        df_fac['date'] = pd.to_datetime(df_fac['date'])
        
        # 只保留需要的列
        df_fac = df_fac[['date', 'adjustFactor']]
        
        # Merge: 左连接 (以K线日期为准)
        df_k = pd.merge(df_k, df_fac, on='date', how='left')
        
        # 【关键】向下填充 (Forward Fill)
        # 因子只在除权日有记录，随后的日子应沿用最近的因子
        # 比如 1号因子是1.0，2号无记录，3号除权变成1.5。则2号也应该是1.0
        df_k['adjustFactor'] = df_k['adjustFactor'].ffill()
        
        # 填充上市初期的空值 (通常设为 1.0)
        df_k['adjustFactor'] = df_k['adjustFactor'].fillna(1.0)
    else:
        # 如果这只股票从未除权（没查到因子），则因子全为 1.0
        df_k['adjustFactor'] = 1.0

    # -------------------------------------------------------
    # 4. 类型转换
    # -------------------------------------------------------
    # 恢复 date 为字符串 (parquet兼容性更好，或者保留datetime看你需求，这里转回str保持一致)
    df_k['date'] = df_k['date'].dt.strftime('%Y-%m-%d')
    
    numeric_cols = [
        'open', 'high', 'low', 'close', 
        'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustFactor'
    ]
    # 注意 Baostock 返回的 adjustFactor 大小写可能不同，上面重命名后是 adjustFactor
    # 使用 errors='coerce' 避免空字符串报错
    for col in numeric_cols:
        if col in df_k.columns:
            df_k[col] = pd.to_numeric(df_k[col], errors='coerce')
    
    return df_k

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
    for s in tqdm(stocks, desc=f"Job {TASK_INDEX} KLine"):
        try:
            df = get_kdata_with_factor(s['code'])
            if not df.empty:
                df.to_parquet(f"{OUTPUT_DIR}/{s['code']}.parquet", index=False)
                success_count += 1
        except Exception as e:
            print(f"❌ Exception {s['code']}: {e}")
            
    bs.logout()
    print(f"Job {TASK_INDEX} 完成: 成功下载 {success_count}/{len(stocks)}")

if __name__ == "__main__":
    main()
