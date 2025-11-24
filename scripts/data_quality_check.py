# scripts/data_quality_check.py
import pandas as pd
import os
import json
import datetime

# 核心数据路径
ENGINE_DIR = "final_output/engine"
REPORT_DIR = "final_output/report"
os.makedirs(REPORT_DIR, exist_ok=True)

def check_stock_data():
    file_path = f"{ENGINE_DIR}/stock_full.parquet"
    if not os.path.exists(file_path):
        return {"error": "stock_full.parquet not found"}
    
    print(f"正在检查个股宽表: {file_path} ...")
    
    # 读取数据
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    
    if total_rows == 0:
        return {"error": "stock_full.parquet is empty"}

    # 1. 基础维度
    unique_stocks = df['code'].nunique()
    min_date = str(df['date'].min())
    max_date = str(df['date'].max())
    
    # 2. 缺失值检查 (Miss Rate)
    # 资金流缺失率 (main_net_flow 为空或为0的比例)
    # 注意：merge时如果fillna(0)了，要查0；如果是NaN查NaN。
    # 假设 merge_data.py 里没有 fillna(0)，则是 NaN。
    # 但有些脚本习惯 fillna。这里检查 NaN。
    missing_flow = df['main_net_flow'].isnull().sum()
    missing_pe = df['peTTM'].isnull().sum()
    missing_factor = df['adjustFactor'].isnull().sum()
    
    # 3. 异常值检查
    # 成交量 < 0
    neg_vol = (df['volume'] < 0).sum()
    # 收盘价 <= 0
    neg_close = (df['close'] <= 0).sum()
    
    # 4. 生成统计摘要
    summary = {
        "status": "Success",
        "total_rows": int(total_rows),
        "stock_count": int(unique_stocks),
        "date_range": f"{min_date} ~ {max_date}",
        "quality_metrics": {
            "missing_fund_flow_pct": round(missing_flow / total_rows * 100, 2),
            "missing_pe_pct": round(missing_pe / total_rows * 100, 2),
            "missing_factor_pct": round(missing_factor / total_rows * 100, 2),
            "negative_volume_count": int(neg_vol),
            "invalid_price_count": int(neg_close)
        }
    }
    
    # 5. 简单的健康评分 (0-100)
    score = 100
    if summary['quality_metrics']['missing_fund_flow_pct'] > 50: score -= 20
    if summary['quality_metrics']['invalid_price_count'] > 0: score -= 50
    summary['health_score'] = score
    
    print(f"✅ 个股数据检查完成。健康分: {score}")
    return summary

def check_sector_data():
    file_path = f"{ENGINE_DIR}/sector_full.parquet"
    if not os.path.exists(file_path):
        return {"warning": "sector_full.parquet not found"}
    
    print(f"正在检查板块宽表: {file_path} ...")
    df = pd.read_parquet(file_path)
    
    return {
        "total_rows": int(len(df)),
        "sector_count": int(df['code'].nunique()),
        "date_range": f"{str(df['date'].min())} ~ {str(df['date'].max())}"
    }

def main():
    report = {
        "generate_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stock_data": check_stock_data(),
        "sector_data": check_sector_data()
    }
    
    # 保存 JSON 报告
    json_path = f"{REPORT_DIR}/quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    # 生成 Markdown 摘要 (用于 GitHub Actions Job Summary)
    md_path = f"{REPORT_DIR}/summary.md"
    with open(md_path, "w", encoding="utf-8") as f:
        s = report['stock_data']
        q = s.get('quality_metrics', {})
        f.write(f"## 📊 数据质量报告 (Data Quality Report)\n")
        f.write(f"**生成时间**: {report['generate_time']}\n\n")
        
        f.write(f"### 🚀 个股全量表 (Stock Full)\n")
        f.write(f"- **健康评分**: {s.get('health_score', 'N/A')} / 100\n")
        f.write(f"- **总记录数**: {s.get('total_rows', 0):,}\n")
        f.write(f"- **股票数量**: {s.get('stock_count', 0)}\n")
        f.write(f"- **日期范围**: {s.get('date_range', '-')}\n")
        f.write(f"- **资金流缺失率**: {q.get('missing_fund_flow_pct', 0)}%\n")
        f.write(f"- **复权因子缺失率**: {q.get('missing_factor_pct', 0)}%\n\n")
        
        sec = report['sector_data']
        f.write(f"### 🌍 板块全量表 (Sector Full)\n")
        f.write(f"- **总记录数**: {sec.get('total_rows', 0):,}\n")
        f.write(f"- **板块数量**: {sec.get('sector_count', 0)}\n")

    print(f"✅ 质检报告已生成: {json_path}")

if __name__ == "__main__":
    main()
