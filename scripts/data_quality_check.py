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
    # 索引
    "date": "交易日期", "code": "股票代码",
    
    # 基础行情
    "open": "开盘价", "high": "最高价", "low": "最低价", "close": "收盘价",
    "volume": "成交量", "amount": "成交额", "turn": "换手率", "pctChg": "涨跌幅",
    
    # 因子与基本面
    "peTTM": "滚动市盈率", "pbMRQ": "市净率", 
    "adjustFactor": "后复权因子", "mkt_cap": "流通市值",
    
    # 资金流
    "net_flow_amount": "净流入额 (全单)", "main_net_flow": "主力净流入 (超大+大单)",
    "super_large_net_flow": "超大单净流入", "large_net_flow": "大单净流入",
    "medium_small_net_flow": "中小单净流入",
    
    # 均线
    "ma5": "5日均线", "ma10": "10日均线", "ma20": "20日均线", 
    "ma60": "60日均线", "ma120": "半年线", "ma250": "年线",
    
    # 均量
    "vol_ma5": "5日均量", "vol_ma10": "10日均量", 
    "vol_ma20": "20日均量", "vol_ma30": "30日均量",
    
    # 技术指标
    "dif": "MACD-DIF", "dea": "MACD-DEA", "macd": "MACD-柱",
    "k": "KDJ-K", "d": "KDJ-D", "j": "KDJ-J",
    "rsi6": "RSI-6", "rsi12": "RSI-12", "rsi24": "RSI-24",
    "boll_up": "布林上轨", "boll_lb": "布林下轨",
    "cci": "CCI", "atr": "ATR"
}

SECTOR_FIELD_DESC = {
    "date": "交易日期", "code": "板块代码", "name": "板块名称",
    "type": "类型", "close": "收盘点位", "pctChg": "涨跌幅",
    "volume": "成交量",
    
    # 新增板块资金流字段
    "net_flow_amount": "板块净流入(聚合)", 
    "main_net_flow": "板块主力净流入(聚合)"
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
    # 检查 stock_daily 目录
    dir_path = f"{ENGINE_DIR}/stock_daily"
    print(f"🔍 检查个股数据目录: {dir_path}/*.parquet ...")
    
    if not os.path.exists(dir_path):
        return {"status": "Error", "message": "Directory not found"}

    try:
        con = duckdb.connect()
        # 统计总行数和日期
        base_info = con.execute(f"""
            SELECT COUNT(*), MIN(date), MAX(date), COUNT(DISTINCT code)
            FROM read_parquet('{dir_path}/*.parquet')
        """).fetchone()
        total_rows, min_date, max_date, unique_stocks = base_info
        
        # 资金流统计
        ff_info = con.execute(f"""
            SELECT 
                COUNT(*) FILTER (WHERE net_flow_amount IS NULL OR net_flow_amount = 0),
                MIN(date) FILTER (WHERE net_flow_amount != 0 AND net_flow_amount IS NOT NULL),
                COUNT(*) FILTER (WHERE net_flow_amount > 0),
                COUNT(*) FILTER (WHERE net_flow_amount < 0),
                MAX(net_flow_amount)
            FROM read_parquet('{dir_path}/*.parquet')
        """).fetchone()
        anomaly_count, ff_start, pos_days, neg_days, max_in = ff_info
        
        # 读取 Schema (采样)
        df_sample = con.execute(f"SELECT * FROM read_parquet('{dir_path}/*.parquet') LIMIT 1").fetchdf()
        
        con.close()

        score = max(0, 100 - int((anomaly_count / total_rows) * 100)) if total_rows > 0 else 0
        
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
                "details": {
                    "pos_days": int(pos_days), "neg_days": int(neg_days), 
                    "max_in": float(max_in) if max_in else 0
                }
            },
            "schema": get_schema_info(df_sample, STOCK_FIELD_DESC)
        }
    except Exception as e:
        return {"status": "Error", "message": str(e)}

def check_sector_data():
    file_path = f"{ENGINE_DIR}/sector_full.parquet"
    if not os.path.exists(file_path): return {"status": "Error", "message": "File not found"}
    
    df = pd.read_parquet(file_path)
    if len(df) == 0: return {"status": "Error", "message": "Empty"}

    # 检查板块资金流是否成功聚合 (非0值占比)
    ff_valid = (df['net_flow_amount'] != 0).sum() if 'net_flow_amount' in df.columns else 0

    return {
        "status": "Success",
        "total_rows": int(len(df)),
        "sector_count": int(df['code'].nunique()),
        "latest_date": str(df['date'].max())[:10],
        "ff_coverage": f"{int(ff_valid / len(df) * 100)}%",
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
    
    with open(f"{REPORT_DIR}/quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    with open(f"{REPORT_DIR}/summary.md", "w", encoding="utf-8") as f:
        f.write(f"## 📊 数据质量报告\n**时间**: {report['generate_time']} (UTC)\n\n")
        
        s = report['stock_data']
        f.write(f"### 🚀 个股全量 (Stock Daily)\n")
        if s.get('status') == 'Success':
            f.write(f"- **K线记录总数**: **{s['total_rows']:,}** 行\n")
            
            ff = s.get('fund_flow')
            if ff:
                f.write(f"- **资金流记录数**: **{ff['valid_count']:,}** 行\n")
                f.write(f"- **资金流始于**: **{ff['start_date']}**\n")
                f.write(f"- **数据异常数**: ⚠️ {ff['anomaly_count']:,} (2010年前或停牌)\n")
            
            f.write(f"\n#### 📋 字段列表\n")
            cols = [x['name'] for x in s['schema']]
            f.write(f"`{'`, `'.join(cols)}`\n")
        else:
            f.write(f"❌ Error: {s.get('message')}\n")
        
        f.write("\n---\n")
        
        sec = report['sector_data']
        f.write(f"### 🌍 板块全量 (Sector Full)\n")
        if sec.get('status') == 'Success':
            f.write(f"- **总记录数**: {sec['total_rows']:,}\n")
            f.write(f"- **板块数量**: {sec['sector_count']}\n")
            f.write(f"- **资金流覆盖率**: **{sec.get('ff_coverage')}**\n")
            
            f.write(f"\n#### 📋 字段列表\n")
            cols = [x['name'] for x in sec['schema']]
            f.write(f"`{'`, `'.join(cols)}`\n")
        else:
            f.write(f"❌ Error: {sec.get('message')}\n")

if __name__ == "__main__":
    main()
