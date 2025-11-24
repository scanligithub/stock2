# scripts/prepare_tasks.py
import baostock as bs
import json
import random
import os
from datetime import datetime, timedelta
import pandas as pd

# 配置
TASK_COUNT = 20
OUTPUT_DIR = "task_slices"
META_DIR = "meta_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

def get_valid_stock_list():
    """
    智能获取股票列表：
    如果当天数据未入库（返回空），自动回溯前一天，直到获取到数据。
    """
    # 尝试回溯过去 10 天
    for i in range(0, 10):
        date_check = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        
        # 1. 先判断是不是交易日
        rs_date = bs.query_trade_dates(start_date=date_check, end_date=date_check)
        if rs_date.error_code != '0' or not rs_date.next() or rs_date.get_row_data()[1] != '1':
            continue # 不是交易日，跳过
            
        print(f"尝试获取 {date_check} 的股票列表...")
        
        # 2. 获取全市场股票
        rs_stock = bs.query_all_stock(day=date_check)
        if rs_stock.error_code != '0':
            continue
            
        data_list = []
        while rs_stock.next():
            data_list.append(rs_stock.get_row_data())
            
        if len(data_list) > 0:
            print(f"✅ 成功获取 {date_check} 的原始数据，共 {len(data_list)} 条")
            return pd.DataFrame(data_list, columns=rs_stock.fields)
        else:
            print(f"⚠️ {date_check} 是交易日但数据未入库 (空列表)，尝试回溯前一天...")
            
    raise Exception("❌ 致命错误：回溯 10 天仍未找到有效的股票列表数据！")

def main():
    print("开始初始化任务...")
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"登录失败: {lg.error_msg}")

    try:
        # 1. 智能获取有数据的日期的列表
        stock_df = get_valid_stock_list()

        stock_list = []
        for _, row in stock_df.iterrows():
            code, name = row['code'], row['code_name']
            # 过滤逻辑：只保留沪深京A股，排除ST和退市，排除空代码
            if code and code.startswith(('sh.', 'sz.', 'bj.')) and 'ST' not in name and '退' not in name:
                stock_list.append({'code': code, 'name': name})

        print(f"清洗后有效股票: {len(stock_list)} 只")
        
        if len(stock_list) == 0:
            raise Exception("❌ 错误：过滤后股票列表为空，请检查过滤逻辑！")

        # 2. 生成前端用的 stock_list.json
        meta_path = os.path.join(META_DIR, "stock_list.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(stock_list, f, ensure_ascii=False)
        print(f"✅ 前端元数据已生成: {meta_path}")

        # 3. 任务分片
        random.shuffle(stock_list)
        chunk_size = (len(stock_list) + TASK_COUNT - 1) // TASK_COUNT

        for i in range(TASK_COUNT):
            subset = stock_list[i * chunk_size: (i + 1) * chunk_size]
            path = os.path.join(OUTPUT_DIR, f"task_slice_{i}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)

        print(f"✅ 成功生成 {TASK_COUNT} 个任务分片，保存在 {OUTPUT_DIR}/")

    finally:
        bs.logout()
        print("logout success!")

if __name__ == "__main__":
    main()
