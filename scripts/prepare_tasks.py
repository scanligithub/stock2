import baostock as bs
import pandas as pd
import json
import os
import math

TASK_COUNT = 20  # 并行任务数
OUTPUT_DIR = "tasks"
META_DIR = "meta_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

def main():
    print("正在登录 Baostock...")
    bs.login()

    # 1. 获取全市场股票
    # 注意：这里取最近一个交易日可能不准，直接取当天的，如果没有则取昨天的
    # Baostock query_all_stock 不需要特定日期，它返回的是当前最新的列表
    rs = bs.query_all_stock(day=pd.Timestamp.now().strftime('%Y-%m-%d'))
    
    data_list = []
    while rs.next():
        data_list.append(rs.get_row_data())
    
    bs.logout()

    df = pd.DataFrame(data_list, columns=rs.fields)
    
    # 2. 过滤：只保留 股票 (排除指数等，Baostock type=1 是股票)
    # 也可以通过 code 前缀过滤: sh.6, sz.0, sz.3, bj.4, bj.8
    # 简单过滤：剔除空的 tradeStatus
    df = df[df['code'] != '']
    
    # 3. 生成前端用的精简列表 (stock_list.json)
    # 包含：code, name. 前端搜索用
    stock_list_frontend = df[['code', 'code_name']].rename(columns={'code_name': 'name'}).to_dict(orient='records')
    
    meta_path = os.path.join(META_DIR, "stock_list.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(stock_list_frontend, f, ensure_ascii=False)
    print(f"✅ 生成元数据: {meta_path} (共 {len(stock_list_frontend)} 只股票)")

    # 4. 生成并行任务分片
    stocks = df[['code', 'code_name']].to_dict(orient='records')
    total = len(stocks)
    chunk_size = math.ceil(total / TASK_COUNT)
    
    for i in range(TASK_COUNT):
        subset = stocks[i * chunk_size : (i + 1) * chunk_size]
        task_path = os.path.join(OUTPUT_DIR, f"task_{i}.json")
        with open(task_path, "w") as f:
            json.dump(subset, f)
    
    print(f"✅ 任务分片完成: 生成 {TASK_COUNT} 个文件")

if __name__ == "__main__":
    main()
