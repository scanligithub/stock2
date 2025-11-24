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
META_DIR = "meta_data"  # 新增：用于存放 stock_list.json 给前端用

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

def get_recent_trade_day():
    """获取最近的一个交易日"""
    for i in range(0, 10): # 尝试过去10天
        day = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        rs = bs.query_trade_dates(start_date=day, end_date=day)
        if rs.error_code == '0' and rs.next() and rs.get_row_data()[1] == '1':
            print(f"最近交易日: {day}")
            return day
    raise Exception("未找到最近的交易日")

def main():
    print("开始获取股票列表...")
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"登录失败: {lg.error_msg}")

    try:
        trade_day = get_recent_trade_day()
        rs = bs.query_all_stock(day=trade_day)
        
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        
        stock_df = pd.DataFrame(data_list, columns=rs.fields)

        stock_list = []
        for _, row in stock_df.iterrows():
            code, name = row['code'], row['code_name']
            # 过滤逻辑：只保留沪深京A股，排除ST和退市
            if code.startswith(('sh.', 'sz.', 'bj.')) and 'ST' not in name and '退' not in name:
                stock_list.append({'code': code, 'name': name})

        print(f"获取到 {len(stock_list)} 只有效股票")
        
        # --- 生成前端用的 stock_list.json ---
        # 包含 code 和 name，用于前端搜索
        meta_path = os.path.join(META_DIR, "stock_list.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(stock_list, f, ensure_ascii=False)
        print(f"✅ 前端元数据已生成: {meta_path}")
        # -----------------------------------

        # 打乱顺序，实现负载均衡
        random.shuffle(stock_list)
        
        # 分片生成
        chunk_size = (len(stock_list) + TASK_COUNT - 1) // TASK_COUNT

        for i in range(TASK_COUNT):
            subset = stock_list[i * chunk_size: (i + 1) * chunk_size]
            path = os.path.join(OUTPUT_DIR, f"task_slice_{i}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)

        print(f"成功生成 {TASK_COUNT} 个任务分片，保存在 {OUTPUT_DIR}/")

    finally:
        bs.logout()

if __name__ == "__main__":
    main()
