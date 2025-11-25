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
        return {"status": "Error", "message": "File not found"}
    
    print(f"🔍 正在检查个股宽表: {file_path} ...")
    df = pd.read_parquet(file_path)
    total_rows = len(df)
    
    if total_rows == 0:
        return {"status": "Error", "message": "File is empty"}

    # 基础指标
    unique_stocks = df['code'].nunique()
    min_date = str(df['date'].min())
    max_date = str(df['date'].max())
    
    # 质量指标
    missing_flow = df['main_net_flow'].isnull().sum()
    missing_factor = df['adjustFactor'].isnull().sum() if 'adjustFactor' in df.columns else 0
    neg_close = (df['close'] <= 0).sum()
    
    # 评分逻辑
    score = 100
    if missing_flow / total_rows > 0.5: score -= 20
    if neg_close > 0: score -= 50
    
    return {
        "status": "Success",
        "health_score": score,
        "total_rows": int(total_rows),
        "stock_count": int(unique_stocks),
        "date_range": f"{min_date} ~ {max_date}",
        "missing_fund_flow_pct": round(missing_flow / total_rows * 100, 2),
        "missing_factor_pct": round(missing_factor / total_rows * 100, 2),
        "invalid_price_count": int(neg_close)
    }

def check_sector_data():
    full_path = f"{ENGINE_DIR}/sector_full.parquet"
    list_path = f"{ENGINE_DIR}/sector_list.parquet"
    
    if not os.path.exists(full_path):
        return {"status": "Error", "message": "sector_full.parquet not found"}
    
    print(f"🔍 正在检查板块宽表: {full_path} ...")
    df = pd.read_parquet(full_path)
    
    # 尝试加载板块元数据以进行分类统计
    df_meta = pd.DataFrame()
    if os.path.exists(list_path):
        df_meta = pd.read_parquet(list_path)
    
    total_rows = len(df)
    unique_sectors = df['code'].nunique()
    
    if total_rows == 0:
        return {"status": "Error", "message": "Sector file is empty"}

    # 1. 时效性检查 (Freshness)
    max_date = df['date'].max()
    min_date = df['date'].min()
    # 统计最新日期有多少板块更新了
    latest_count = df[df['date'] == max_date]['code'].nunique()
    miss_update = unique_sectors - latest_count
    
    # 2. 逻辑完整性 (Integrity)
    # 最高价 < 最低价 的错误行数
    logic_error = (df['high'] < df['low']).sum()
    # 成交量 < 0
    neg_vol = (df['volume'] < 0).sum()
    
    # 3. 分类统计 (如果元数据存在)
    type_stats = {}
    if not df_meta.empty:
        # 假设 sector_list 有 'type' 列 (行业/概念/地域)
        if 'type' in df_meta.columns:
            # 只统计有数据的板块
            valid_codes = df['code'].unique()
            valid_meta = df_meta[df_meta['code'].isin(valid_codes)]
            type_counts = valid_meta['type'].value_counts()
            type_stats = type_counts.to_dict()
    
    # 4. 历史长度统计
    # 每个板块的数据行数
    counts = df['code'].value_counts()
    avg_history = int(counts.mean())
    
    return {
        "status": "Success",
        "total_rows": int(total_rows),
        "sector_count": int(unique_sectors),
        "date_range": f"{str(min_date)[:10]} ~ {str(max_date)[:10]}",
        "latest_date": str(max_date)[:10],
        "latest_coverage": f"{latest_count}/{unique_sectors}",
        "miss_update_count": int(miss_update),
        "avg_history_days": avg_history,
        "logic_errors": int(logic_error + neg_vol),
        "type_breakdown": type_stats
    }

def main():
    stock_res = check_stock_data()
    sector_res = check_sector_data()
    
    report = {
        "generate_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stock_data": stock_res,
        "sector_data": sector_res
    }
    
    # 保存 JSON
    json_path = f"{REPORT_DIR}/quality_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
        
    # 生成 Markdown
    md_path = f"{REPORT_DIR}/summary.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"## 📊 数据质量报告 (Data Quality Report)\n")
        f.write(f"**生成时间**: {report['generate_time']} (UTC)\n\n")
        
        # --- 个股部分 ---
        s = report['stock_data']
        f.write(f"### 🚀 个股全量表 (Stock Full)\n")
        if s.get('status') == 'Success':
            health_color = "🟢" if s['health_score'] == 100 else "🟠"
            f.write(f"- **健康评分**: {health_color} {s['health_score']} / 100\n")
            f.write(f"- **总记录数**: {s['total_rows']:,}\n")
            f.write(f"- **股票数量**: {s['stock_count']}\n")
            f.write(f"- **日期范围**: {s['date_range']}\n")
            f.write(f"- **资金流缺失率**: {s['missing_fund_flow_pct']}%\n")
            f.write(f"- **复权因子缺失率**: {s['missing_factor_pct']}%\n")
        else:
            f.write(f"❌ Error: {s.get('message')}\n")
        
        f.write("\n---\n")
        
        # --- 板块部分 ---
        sec = report['sector_data']
        f.write(f"### 🌍 板块全量表 (Sector Full)\n")
        if sec.get('status') == 'Success':
            f.write(f"- **总记录数**: {sec['total_rows']:,}\n")
            f.write(f"- **板块数量**: {sec['sector_count']}\n")
            f.write(f"- **平均历史**: {sec['avg_history_days']} 个交易日 (约 {round(sec['avg_history_days']/250, 1)} 年)\n")
            f.write(f"- **最新日期**: **{sec['latest_date']}**\n")
            
            # 覆盖率检查
            cov_icon = "🟢" if sec['miss_update_count'] == 0 else "🟡"
            f.write(f"- **最新覆盖**: {cov_icon} {sec['latest_coverage']} (未更新: {sec['miss_update_count']})\n")
            
            # 异常检查
            err_icon = "🟢" if sec['logic_errors'] == 0 else "🔴"
            f.write(f"- **逻辑异常**: {err_icon} {sec['logic_errors']} 行 (H<L 或 Vol<0)\n")
            
            # 类型分布
            if sec.get('type_breakdown'):
                f.write(f"\n**板块分类统计**:\n")
                for k, v in sec['type_breakdown'].items():
                    f.write(f"- {k}: {v} 个\n")
        else:
            f.write(f"❌ Error: {sec.get('message')}\n")

    print(f"✅ 增强版质检报告已生成: {json_path}")

if __name__ == "__main__":
    main()
