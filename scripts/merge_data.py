# scripts/merge_data.py
import pandas as pd
import glob
import os
from tqdm import tqdm
import pandas_ta as ta
import numpy as np
import duckdb
import shutil
import datetime

# --- 配置 ---
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"
CACHE_DIR = "cache_data" 
OUTPUT_BASE = "final_output/engine"

# 输出目录结构
DIRS = {
    "daily": f"{OUTPUT_BASE}/stock_daily",
    "weekly": f"{OUTPUT_BASE}", 
    "monthly": f"{OUTPUT_BASE}"
}
for d in DIRS.values(): os.makedirs(d, exist_ok=True)

# 历史归档文件名
ARCHIVE_FILENAME = "stock_history_2005_2024.parquet"
ARCHIVE_PATH = f"{CACHE_DIR}/{ARCHIVE_FILENAME}"

BATCH_SIZE = 500

def clean_indicators(df):
    """强制清洗指标列，防止类型错误"""
    target_cols = [
        'dif', 'dea', 'macd', 'k', 'd', 'j', 'rsi6', 'rsi12', 'rsi24',
        'boll_up', 'boll_lb', 'cci', 'atr',
        'net_flow_amount', 'main_net_flow', 'super_large_net_flow', 
        'large_net_flow', 'medium_small_net_flow',
        'peTTM', 'pbMRQ', 'adjustFactor', 'mkt_cap',
        'vol_ma5', 'vol_ma10', 'vol_ma20', 'vol_ma30'
    ]
    for w in [5, 10, 20, 60, 120, 250]: target_cols.append(f'ma{w}')

    for col in target_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
    return df

def calculate_all_indicators(df):
    """计算单只股票的全套指标"""
    df = df.sort_values('date').reset_index(drop=True)
    
    # 1. MAs
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(w).mean()
    # 2. Vol MAs
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

    # 3. Technical Indicators (Pandas-TA)
    try:
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0]; df['macd'] = macd.iloc[:, 1]; df['dea'] = macd.iloc[:, 2]
    except: pass

    try:
        kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
        if kdj is not None:
            df['k'] = kdj.iloc[:, 0]; df['d'] = kdj.iloc[:, 1]; df['j'] = kdj.iloc[:, 2]
    except: pass

    try:
        df['rsi6'] = df.ta.rsi(close='close', length=6)
        df['rsi12'] = df.ta.rsi(close='close', length=12)
        df['rsi24'] = df.ta.rsi(close='close', length=24)
    except: pass

    try:
        boll = df.ta.bbands(close='close', length=20, std=2)
        if boll is not None:
            df['boll_lb'] = boll.iloc[:, 0]; df['boll_up'] = boll.iloc[:, 2]
    except: pass
    
    try:
        df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
        df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
    except: pass

    return clean_indicators(df)

def main():
    print("🚀 开始全量合并与周期生成...")
    
    # 1. 加载历史归档 (Cold Data)
    df_history = pd.DataFrame()
    if os.path.exists(ARCHIVE_PATH):
        print(f"🧊 加载历史归档: {ARCHIVE_PATH} ...")
        try:
            df_history = pd.read_parquet(ARCHIVE_PATH)
            df_history['date'] = pd.to_datetime(df_history['date'])
        except Exception as e:
            print(f"⚠️ 历史归档加载失败: {e}")

    # 2. 加载今日下载 (New Data)
    print("🔥 加载今日增量数据...")
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    f_map = {os.path.basename(f): f for f in f_files}
    
    new_dfs = []
    for k_path in tqdm(k_files, desc="Reading New"):
        try:
            df = pd.read_parquet(k_path)
            filename = os.path.basename(k_path)
            code = filename.replace('.parquet', '')
            
            # Merge Flow
            if filename in f_map:
                try:
                    df_f = pd.read_parquet(f_map[filename])
                    if not df_f.empty:
                        df['date'] = pd.to_datetime(df['date'])
                        df_f['date'] = pd.to_datetime(df_f['date'])
                        df = pd.merge(df, df_f, on=['date', 'code'], how='left')
                except: pass
            
            new_dfs.append(df)
        except: pass
    
    if new_dfs:
        df_new = pd.concat(new_dfs, ignore_index=True)
        df_new['date'] = pd.to_datetime(df_new['date'])
    else:
        df_new = pd.DataFrame()

    # 3. 拼接全量
    if df_history.empty and df_new.empty:
        print("❌ 无数据")
        return

    print("🔄 合并全量数据...")
    df_total = pd.concat([df_history, df_new])
    # 去重：保留最新的数据
    df_total.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df_total.sort_values(['code', 'date'], inplace=True)
    
    # 4. 计算个股全量指标
    print("🧮 计算个股日线指标 (MA/MACD/KDJ)...")
    # 注意：数据量大时 groupby apply 会慢，但这是最稳妥方式
    df_total = df_total.groupby('code', group_keys=False).apply(calculate_all_indicators)
    
    # 5. 切分与保存个股数据
    
    # A. 保存 2025 热数据
    print("💾 保存 2025 热数据...")
    df_2025 = df_total[df_total['date'] >= '2025-01-01'].copy()
    df_2025['date'] = df_2025['date'].dt.strftime('%Y-%m-%d')
    df_2025.to_parquet(f"{DIRS['daily']}/stock_2025.parquet", index=False, compression='zstd')
    
    # B. 保存 历史归档 (仅当本地没有归档文件时生成，用于首次初始化)
    if not os.path.exists(ARCHIVE_PATH):
        print(f"💾 生成历史归档补丁: {ARCHIVE_FILENAME}")
        df_hist_save = df_total[df_total['date'] < '2025-01-01'].copy()
        if not df_hist_save.empty:
            df_hist_save['date'] = df_hist_save['date'].dt.strftime('%Y-%m-%d')
            df_hist_save.to_parquet(f"{DIRS['daily']}/{ARCHIVE_FILENAME}", index=False, compression='zstd')

    # C. 生成周线/月线
    print("📅 生成周线/月线数据...")
    agg_dict = {
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last',
        'net_flow_amount': 'sum', 'main_net_flow': 'sum'
    }
    valid_agg = {k: v for k, v in agg_dict.items() if k in df_total.columns}
    
    # 周线
    df_weekly = df_total.set_index('date').groupby('code').resample('W-FRI').agg(valid_agg).reset_index()
    df_weekly.dropna(subset=['close'], inplace=True)
    df_weekly = df_weekly.groupby('code', group_keys=False).apply(calculate_all_indicators)
    df_weekly['date'] = df_weekly['date'].dt.strftime('%Y-%m-%d')
    df_weekly.to_parquet(f"{DIRS['weekly']}/stock_weekly.parquet", index=False, compression='zstd')
    
    # 月线
    df_monthly = df_total.set_index('date').groupby('code').resample('ME').agg(valid_agg).reset_index()
    df_monthly.dropna(subset=['close'], inplace=True)
    df_monthly = df_monthly.groupby('code', group_keys=False).apply(calculate_all_indicators)
    df_monthly['date'] = df_monthly['date'].dt.strftime('%Y-%m-%d')
    df_monthly.to_parquet(f"{DIRS['monthly']}/stock_monthly.parquet", index=False, compression='zstd')

    # ==========================================
    # 6. 【新增】板块资金流聚合计算
    # ==========================================
    print("💰 正在计算板块资金流向 (基于个股聚合)...")
    
    sector_kline_path = f"{OUTPUT_BASE}/sector_full.parquet"
    relation_path = f"{OUTPUT_BASE}/sector_constituents.parquet"
    
    if os.path.exists(sector_kline_path) and os.path.exists(relation_path):
        try:
            # 1. 准备数据：df_total 已经在内存中，包含 net_flow_amount
            # 2. 连接 DuckDB
            con = duckdb.connect()
            
            # 注册内存中的个股表
            con.register('stock_data', df_total[['date', 'code', 'net_flow_amount', 'main_net_flow']])
            
            # 读取磁盘上的板块K线和关系表
            con.execute(f"CREATE TABLE sector_kline AS SELECT * FROM read_parquet('{sector_kline_path}')")
            con.execute(f"CREATE TABLE relations AS SELECT * FROM read_parquet('{relation_path}')")
            
            # 3. 执行聚合：板块K线 Left Join (关系表 Join 个股表 Group By 板块,日期)
            print("   -> 执行 DuckDB SQL 聚合...")
            sql = """
            WITH sector_flows AS (
                SELECT 
                    r.sector_code,
                    s.date,
                    SUM(s.net_flow_amount) as net_flow_amount,
                    SUM(s.main_net_flow) as main_net_flow
                FROM stock_data s
                JOIN relations r ON s.code = r.stock_code
                GROUP BY r.sector_code, s.date
            )
            SELECT 
                k.*,
                COALESCE(f.net_flow_amount, 0) as net_flow_amount,
                COALESCE(f.main_net_flow, 0) as main_net_flow
            FROM sector_kline k
            LEFT JOIN sector_flows f ON k.code = f.sector_code AND k.date = f.date
            ORDER BY k.code, k.date
            """
            
            df_sector_final = con.execute(sql).fetchdf()
            
            # 4. 覆盖保存
            print(f"💾 更新板块文件 (含资金流): {sector_kline_path}")
            df_sector_final.to_parquet(sector_kline_path, index=False, compression='zstd')
            con.close()
            
        except Exception as e:
            print(f"❌ 板块资金流计算失败: {e}")
    else:
        print(f"⚠️ 跳过板块计算 (文件缺失): {sector_kline_path} 或 {relation_path}")

    print("✅ 所有数据处理完毕！")

if __name__ == "__main__":
    main()
