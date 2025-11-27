# scripts/data_quality_check.py
import pandas as pd
import numpy as np
import os
import json
import datetime
import duckdb

ENGINE_DIR = "final_output/engine"
REPORT_DIR = "final_output/report"
os.makedirs(REPORT_DIR, exist_ok=True)

# ================= 数据字典 =================
STOCK_FIELD_DESC = {
    "date": "交易日期", "code": "股票代码",
    "close": "收盘价", "peTTM": "市盈率", "pbMRQ": "市净率",
    "adjustFactor": "后复权因子", "mkt_cap": "流通市值",
    "volume": "成交量", "turn": "换手率",
    
    # 资金流
    "net_flow_amount": "净流入额", "main_net_flow": "主力净流入",
    
    # 指标
    "ma5": "MA5", "ma20": "MA20", "ma250": "年线",
    "vol_ma5": "VMA5", "vol_ma20": "VMA20",
    "dif": "MACD_DIF", "k": "KDJ_K", "rsi6": "RSI6", "boll_up": "布林上轨",
    "cci": "CCI", "atr": "ATR"
}

SECTOR_FIELD_DESC = {
    "date": "交易日期", "code": "板块代码", "name": "板块名称",
    "type": "板块类型", "close": "收盘点位", "pctChg": "涨跌幅",
    "volume": "成交量",
    
    # 【新增】板块资金流
    "net_flow_amount": "板块净流入 (聚合)",
    "main_net_flow": "板块主力净流入 (聚合)"
}

def get_schema_info(df, desc_map):
    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if 'float' in dtype: dtype = 'float'
        elif 'int' in dtype: dtype = 'int'
        elif 'object' in dtype: dtype = 'string'
        schema.append({
            "name": col,
            "type": dtype,
            "desc": desc_map.get(col, "自定义字段")
        })
    return schema

def format_money(val):
    if pd.isna(val): return "N/A"
    abs_val = abs(val)
    if abs_val >= 10**8: return f"{val/10**8:.2f} 亿"
    elif abs_val >= 10**4: return f"{val/10**4:.2f} 万"
    else: return f"{val:.2f}"

def check_stock_data():
    # 【关键修改】读取 stock_daily 目录下的所有 parquet
    dir_path = f"{ENGINE_DIR}/stock_daily"
    print(f"🔍 检查个股数据目录: {dir_path}/*.parquet ...")
    
    if not os.path.exists(dir_path):
        return {"status": "Error", "message": "Directory not found"}

    try:
        con = duckdb.connect()
        # 1. 基础统计 (Count, Min Date, Max Date)
        base_info = con.execute(f"""
            SELECT 
                COUNT(*) as total,
                MIN(date) as min_d,
                MAX(date) as max_d,
                COUNT(DISTINCT code) as stocks
            FROM read_parquet('{dir_path}/*.parquet')
        """).fetchone()
        
        total_rows, min_date, max_date, unique_stocks = base_info
        
        # 2. 资金流统计 (SQL 聚合)
        ff_info = con.execute(f"""
            SELECT 
                COUNT(*) FILTER (WHERE net_flow_amount IS NULL OR net_flow_amount = 0) as anomalies,
                MIN(date) FILTER (WHERE net_flow_amount != 0 AND net_flow_amount IS NOT NULL) as start_d,
                COUNT(*) FILTER (WHERE net_flow_amount > 0) as pos,
                COUNT(*) FILTER (WHERE net_flow_amount < 0) as neg,
                MAX(net_flow_amount) as max_in
            FROM read_parquet('{dir_path}/*.parquet')
        """).fetchone()
        
        anomaly_count, ff_start, pos_days, neg_days, max_in = ff_info
        
        # 3. 采样获取 Schema (读第一行)
        df_sample = con.execute(f"SELECT * FROM read_parquet('{dir_path}/*.parquet') LIMIT 1").fetchdf()
        
        con.close()

        # 评分
        anomaly_rate = anomaly_count / total_rows if total_rows > 0 else 0
        score = max(0, 100 - int(anomaly_rate * 100)) # 简单算法
        
        return {
            "status": "Success",
            "global_score": score,
            "total_rows": int(total_rows),
            "stock_count": int(unique_stocks),
            "date_range": f"{min_date} ~ {max_date}",
            "fund_flow": {
                "start_date": str(ff_start),
                "anomaly_count": int(anomaly_count),
                "valid_count": int(total_rows - anomaly_count),
                "pos_days": int(pos_days),
                "neg_days": int(neg_days),
                "max_in": float(max_in) if max_in else 0
            },
            "schema": get_schema_info(df_sample, STOCK_FIELD_DESC)
        }

    except Exception as e:
        return {"status": "Error", "message": str(e)}

def check_sector_data():
    file_path = f"{ENGINE_DIR}/sector_full.parquet"
    print(f"🔍 检查板块表: {file_path} ...")
    
    if not os.path.exists(file_path):
        return {"status": "Error", "message": "File not found"}
    
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    if total_rows == 0: return {"status": "Error", "message": "Empty"}

    # 检查板块资金流覆盖率
    ff_valid = 0
    if 'net_flow_amount' in df.columns:
        ff_valid = (df['net_flow_amount'] != 0).sum()

    return {
        "status": "Success",
        "total_rows": int(total_rows),
        "sector_count": int(df['code'].nunique()),
        "date_range": f"{str(df['date'].min())[:10]} ~ {str(df['date'].max())[:10]}",
        "latest_date": str(df['date'].max())[:10],
        "ff_coverage": f"{int(ff_valid / total_rows * 100)}%", # 资金流覆盖率
        "schema": get_schema_info(df, SECTOR_FIELD_DESC)
    }

def main():
    stock_res = check_stock_data()
    sector_res = check_sector_data()
    
    report = {
        "generate_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stock_data": stock_res,
        "sector_data": sector_res
    }
    
    json_path = f"{REPORT_DIR}/quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    md_path = f"{REPORT_DIR}/summary.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"## 📊 数据质量报告\n**时间**: {report['generate_time']} (UTC)\n\n")
        
        # Stock
        s = report['stock_data']
        f.write(f"### 🚀 个股全量 (Stock Daily)\n")
        if s.get('status') == 'Success':
            f.write(f"- **总记录数**: {s['total_rows']:,} 行\n")
            f.write(f"- **股票数量**: {s['stock_count']} 只\n")
            
            ff = s.get('fund_flow')
            if ff:
                f.write(f"- **资金流有效数**: {ff['valid_count']:,} 行\n")
                f.write(f"- **覆盖起始日**: {ff['start_date']}\n")
                f.write(f"- **单日最大流入**: {format_money(ff['max_in'])}\n")
            
            f.write(f"\n#### 📋 个股字段示例 ({len(s['schema'])}个)\n`{', '.join([x['name'] for x in s['schema'][:8]])}...`\n")
        else:
            f.write(f"❌ Error: {s.get('message')}\n")
        
        f.write("\n---\n")
        
        # Sector
        sec = report['sector_data']
        f.write(f"### 🌍 板块全量 (Sector Full)\n")
        if sec.get('status') == 'Success':
            f.write(f"- **总记录数**: {sec['total_rows']:,}\n")
            f.write(f"- **板块数量**: {sec['sector_count']}\n")
            f.write(f"- **资金流覆盖率**: **{sec.get('ff_coverage')}** (聚合计算结果)\n")
            f.write(f"\n#### 📋 板块字段示例\n`{', '.join([x['name'] for x in sec['schema']])}`\n")
        else:
            f.write(f"❌ Error: {sec.get('message')}\n")

    print(f"✅ 质检完成: {json_path}")

if __name__ == "__main__":
    main()
