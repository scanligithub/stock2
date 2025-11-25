# scripts/data_quality_check.py
import pandas as pd
import numpy as np
import os
import json
import datetime

ENGINE_DIR = "final_output/engine"
REPORT_DIR = "final_output/report"
os.makedirs(REPORT_DIR, exist_ok=True)

# ================= 数据字典 =================
STOCK_FIELD_DESC = {
    "date": "交易日期 (YYYY-MM-DD)",
    "code": "股票代码",
    "close": "收盘价 (原始)",
    "peTTM": "滚动市盈率",
    "pbMRQ": "市净率",
    "adjustFactor": "后复权因子",
    "mkt_cap": "流通市值 (元)",
    "volume": "成交量",
    "turn": "换手率",
    "net_flow_amount": "净流入金额 (全单, 元)",
    "main_net_flow": "主力净流入 (超大+大单, 元)",
    "super_large_net_flow": "超大单净流入",
    "large_net_flow": "大单净流入",
    "medium_small_net_flow": "中小单净流入"
}

SECTOR_FIELD_DESC = {
    "date": "交易日期",
    "code": "板块代码",
    "name": "板块名称",
    "type": "类型",
    "close": "收盘点位",
    "pctChg": "涨跌幅"
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
    file_path = f"{ENGINE_DIR}/stock_full.parquet"
    if not os.path.exists(file_path):
        return {"status": "Error", "message": "File not found"}
    
    print(f"🔍 检查个股表: {file_path} ...")
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    
    if total_rows == 0:
        return {"status": "Error", "message": "File is empty"}

    # --- 基础指标 ---
    unique_stocks = df['code'].nunique()
    min_date = str(df['date'].min())
    max_date = str(df['date'].max())
    
    # --- 资金流向专属质检 ---
    ff_stats = {}
    if 'net_flow_amount' in df.columns:
        # 1. 异常统计
        nan_count = df['net_flow_amount'].isnull().sum()
        zero_count = (df['net_flow_amount'] == 0).sum()
        anomaly_count = nan_count + zero_count
        
        # 2. 【核心新增】探测资金流的“有效起始日期”
        # 找到第一个 net_flow_amount 不为 0 且不为 NaN 的日期
        valid_ff_df = df[df['net_flow_amount'].notna() & (df['net_flow_amount'] != 0)]
        if not valid_ff_df.empty:
            ff_start_date = str(valid_ff_df['date'].min())
        else:
            ff_start_date = "无有效数据"

        # 3. 评分逻辑 (放宽对早期的惩罚)
        # 如果异常大多集中在 ff_start_date 之前，说明是历史原因，不是数据质量问题
        anomaly_rate = anomaly_count / total_rows
        ff_score = max(0, 100 - int(anomaly_rate * 100))
        
        # 4. 详细统计
        pos_flow = (df['net_flow_amount'] > 0).sum()
        neg_flow = (df['net_flow_amount'] < 0).sum()
        max_inflow = df['net_flow_amount'].max()
        max_outflow = df['net_flow_amount'].min()
        
        ff_stats = {
            "score": ff_score,
            "total_rows": int(total_rows),
            "stock_count": int(unique_stocks),
            "date_range": f"{min_date} ~ {max_date}",
            "ff_start_date": ff_start_date, # 新增字段
            "anomaly_count": int(anomaly_count),
            "details": {
                "nan_rows": int(nan_count),
                "zero_rows": int(zero_count),
                "pos_days": int(pos_flow),
                "neg_days": int(neg_flow),
                "max_in": float(max_inflow),
                "max_out": float(max_outflow)
            }
        }
    
    # --- 全局指标 ---
    missing_factor = df['adjustFactor'].isnull().sum() if 'adjustFactor' in df.columns else total_rows
    invalid_cap = (df['mkt_cap'] <= 0).sum() if 'mkt_cap' in df.columns else 0
    
    global_score = 100
    if ff_stats.get('score', 0) < 60: global_score -= 20
    if invalid_cap / total_rows > 0.1: global_score -= 10
    
    return {
        "status": "Success",
        "global_score": global_score,
        "total_rows": int(total_rows),
        "stock_count": int(unique_stocks),
        "date_range": f"{min_date} ~ {max_date}",
        "other_metrics": {
            "missing_factor_pct": round(missing_factor / total_rows * 100, 2),
            "invalid_mkt_cap": int(invalid_cap)
        },
        "fund_flow_data": ff_stats,
        "schema": get_schema_info(df, STOCK_FIELD_DESC)
    }

def check_sector_data():
    full_path = f"{ENGINE_DIR}/sector_full.parquet"
    if not os.path.exists(full_path):
        return {"status": "Error", "message": "File not found"}
    
    print(f"🔍 检查板块表: {full_path} ...")
    df = pd.read_parquet(full_path)
    total_rows = len(df)
    
    if total_rows == 0: return {"status": "Error", "message": "Empty"}

    return {
        "status": "Success",
        "total_rows": int(total_rows),
        "sector_count": int(df['code'].nunique()),
        "date_range": f"{str(df['date'].min())[:10]} ~ {str(df['date'].max())[:10]}",
        "latest_date": str(df['date'].max())[:10],
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
        f.write(f"## 📊 数据质量报告 (Data Quality Report)\n")
        f.write(f"**生成时间**: {report['generate_time']} (UTC)\n\n")
        
        # --- Stock Section ---
        s = report['stock_data']
        f.write(f"### 🚀 个股全量表 (Stock Full)\n")
        if s.get('status') == 'Success':
            f.write(f"- **全局健康度**: {s['global_score']} / 100\n")
            
            # === 资金流向专属区块 ===
            ff = s.get('fund_flow_data', {})
            if ff:
                f.write(f"\n#### 💰 资金流向质量专区\n")
                score = ff['score']
                icon = "🟢" if score >= 90 else ("🟡" if score >= 60 else "🔴")
                
                f.write(f"- **资金流健康评分**: {icon} **{score}** / 100\n")
                f.write(f"- **K线日期范围**: {ff['date_range']}\n")
                # 【核心新增】
                f.write(f"- **资金流覆盖始于**: **{ff['ff_start_date']}** (在此之前无数据属正常)\n")
                
                anom = ff['anomaly_count']
                f.write(f"- **数据异常数**: ⚠️ {anom:,} (早期历史空值 + 近期停牌)\n")
                
                det = ff['details']
                f.write(f"\n> **统计细节**: 多头天数 {det['pos_days']:,} | 空头天数 {det['neg_days']:,} | 单日最大流入 {format_money(det['max_in'])}\n")
            
            om = s.get('other_metrics', {})
            f.write(f"\n#### 🛠 其他指标\n")
            f.write(f"- 市值异常记录(<=0): {om.get('invalid_mkt_cap')}\n")
            f.write(f"- 复权因子缺失率: {om.get('missing_factor_pct')}%\n")

            f.write(f"\n#### 📋 字段字典\n| 字段 | 类型 | 说明 |\n|---|---|---|\n")
            for field in s['schema']:
                f.write(f"| `{field['name']}` | {field['type']} | {field['desc']} |\n")
        else:
            f.write(f"❌ Error: {s.get('message')}\n")
        
        f.write("\n---\n")
        
        # --- Sector Section ---
        sec = report['sector_data']
        f.write(f"### 🌍 板块全量表 (Sector Full)\n")
        if sec.get('status') == 'Success':
            f.write(f"- **板块数量**: {sec['sector_count']}\n")
            f.write(f"- **总记录数**: {sec['total_rows']:,}\n")
            f.write(f"- **最新日期**: **{sec['latest_date']}**\n")
            f.write(f"\n#### 📋 字段字典\n| 字段 | 类型 | 说明 |\n|---|---|---|\n")
            for field in sec['schema']:
                f.write(f"| `{field['name']}` | {field['type']} | {field['desc']} |\n")
        else:
            f.write(f"❌ Error: {sec.get('message')}\n")

    print(f"✅ 质检报告已生成: {json_path}")

if __name__ == "__main__":
    main()
