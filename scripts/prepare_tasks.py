# scripts/prepare_tasks.py
import baostock as bs
import json
import random
import os
import sys
from datetime import datetime, timedelta
import pandas as pd

# ================== 配置区域 ==================
# 【开关】True = 测试模式 (100只); False = 全量模式 (5000+只)
# 你可以在这里手动修改，或者通过环境变量 TEST_MODE=true 来覆盖
DEFAULT_TEST_MODE = True 

# 检查环境变量 (GitHub Actions 传入)
ENV_TEST_MODE = os.getenv("TEST_MODE", "").lower() == "true"
IS_TEST_MODE = ENV_TEST_MODE or DEFAULT_TEST_MODE

# 测试模式下的切片范围
TEST_RANGE = (1000, 1100) # 取第1000到1100只，共100只

# 并行任务数
TASK_COUNT = 20
OUTPUT_DIR = "task_slices"
META_DIR = "meta_data"
# ============================================

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(META_DIR, exist_ok=True)

def get_valid_stock_list():
    """智能回溯获取有效的股票列表"""
    for i in range(0, 10):
        date_check = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        
        # 1. 判断交易日
        rs_date = bs.query_trade_dates(start_date=date_check, end_date=date_check)
        if rs_date.error_code != '0' or not rs_date.next() or rs_date.get_row_data()[1] != '1':
            continue 
            
        print(f"尝试获取 {date_check} 的股票列表...")
        
        # 2. 获取全列表
        rs_stock = bs.query_all_stock(day=date_check)
        if rs_stock.error_code != '0':
            continue
            
        data_list = []
        while rs_stock.next():
            data_list.append(rs_stock.get_row_data())
            
        if len(data_list) > 0:
            print(f"✅ 成功获取 {date_check} 的数据，共 {len(data_list)} 条")
            return pd.DataFrame(data_list, columns=rs_stock.fields)
        else:
            print(f"⚠️ {date_check} 是交易日但数据未入库，回溯前一天...")
            
    raise Exception("❌ 致命错误：回溯 10 天仍未找到有效的股票列表数据！")

def main():
    mode_str = "⚡ 极速测试模式 (100只)" if IS_TEST_MODE else "🚀 全量生产模式 (全部)"
    print(f"启动任务初始化: [{mode_str}]")
    
    lg = bs.login()
    if lg.error_code != '0':
        raise Exception(f"登录失败: {lg.error_msg}")

    try:
        # 1. 获取全量原始列表
        stock_df = get_valid_stock_list()

        # 2. 清洗过滤
        stock_list = []
        for _, row in stock_df.iterrows():
            code, name = row['code'], row['code_name']
            # 过滤逻辑：只保留A股(sh/sz/bj)，排除ST，排除退市
            if code and code.startswith(('sh.', 'sz.', 'bj.')) and 'ST' not in name and '退' not in name:
                stock_list.append({'code': code, 'name': name})

        total_count = len(stock_list)
        print(f"全市场清洗后有效股票: {total_count} 只")

        # 3. 根据模式裁切
        if IS_TEST_MODE:
            start, end = TEST_RANGE
            if total_count > start:
                # 确保不超过边界
                real_end = min(total_count, end)
                stock_list = stock_list[start:real_end]
                print(f"✂️ 已裁切: 仅保留索引 {start} 到 {real_end}，共 {len(stock_list)} 只")
            else:
                print("⚠️ 警告: 股票总数不足以进行测试切片，将使用全部股票。")
        else:
            print("✅ 使用全量股票列表，不进行裁切。")

        # 4. 生成元数据 (stock_list.json)
        meta_path = os.path.join(META_DIR, "stock_list.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(stock_list, f, ensure_ascii=False)
        print(f"📄 前端元数据已生成: {meta_path}")

        # 5. 任务分片
        random.shuffle(stock_list) # 打乱顺序以实现负载均衡
        chunk_size = (len(stock_list) + TASK_COUNT - 1) // TASK_COUNT

        for i in range(TASK_COUNT):
            subset = stock_list[i * chunk_size: (i + 1) * chunk_size]
            path = os.path.join(OUTPUT_DIR, f"task_slice_{i}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(subset, f, ensure_ascii=False, indent=2)

        print(f"📦 成功生成 {TASK_COUNT} 个任务分片 (平均每片 {chunk_size} 只)")

    finally:
        bs.logout()

if __name__ == "__main__":
    main()
