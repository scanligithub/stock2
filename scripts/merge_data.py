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

# 路径配置
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"
# 输出目录调整：建立子目录以便管理
OUTPUT_DIR = "final_output/engine/stock_daily"
TEMP_DIR = "temp_chunks"
CACHE_DIR = "cache_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

BATCH_SIZE = 500

# ... (clean_indicators 函数保持不变) ...
def clean_indicators(df):
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

# ... (process_single_stock 函数保持不变) ...
def process_single_stock(k_path, f_map):
    try:
        df = pd.read_parquet(k_path)
        if df.empty: return None
        filename = os.path.basename(k_path)
        code = filename.replace('.parquet', '')
        
        if filename in f_map:
            try:
                df_f = pd.read_parquet(f_map[filename])
                if not df_f.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df = pd.merge(df, df_f, on=['date', 'code'], how='left')
            except: pass
        
        if df['date'].dtype != 'datetime64[ns]':
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        # --- 指标计算 ---
        for w in [5, 10, 20, 60, 120, 250]:
            df[f'ma{w}'] = df['close'].rolling(w).mean()
        for w in [5, 10, 20, 30]:
            df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

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

        df = clean_indicators(df)
        return df
    except Exception as e:
        print(f"Err processing {k_path}: {e}")
        return None

def main():
    print("🚀 开始内存安全版合并流程 (全量计算 -> 冷热分离)...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    f_map = {os.path.basename(f): f for f in f_files}
    
    # 1. 分批计算并写入临时文件
    batch_buffer = []
    chunk_index = 0
    
    for i, k_path in enumerate(tqdm(k_files, desc="Processing Batches")):
        df = process_single_stock(k_path, f_map)
        if df is not None:
            batch_buffer.append(df)
        
        if len(batch_buffer) >= BATCH_SIZE or (i == len(k_files) - 1 and batch_buffer):
            chunk_df = pd.concat(batch_buffer, ignore_index=True)
            chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
            
            temp_path = f"{TEMP_DIR}/chunk_{chunk_index}.parquet"
            chunk_df.to_parquet(temp_path, index=False, compression='zstd', engine='pyarrow')
            
            batch_buffer = []
            chunk_index += 1
            
    print(f"\n✅ 批处理完成。开始切分归档...")

    # 2. 使用 DuckDB 进行“切分手术”
    try:
        con = duckdb.connect()
        current_year = datetime.datetime.now().year # 2025
        
        # === 切分点 ===
        cutoff_date = f"{current_year}-01-01"
        
        # A. 生成历史归档 (Cold Data: < 2025-01-01)
        # 这个文件用于手动上传到 GitHub Releases
        archive_file = f"{OUTPUT_DIR}/stock_history_2005_{current_year-1}.parquet"
        print(f"🧊 生成历史归档文件: {archive_file} ...")
        
        query_archive = f"""
        COPY (
            SELECT * FROM read_parquet('{TEMP_DIR}/*.parquet')
            WHERE date < '{cutoff_date}'
            ORDER BY code, date
        ) TO '{archive_file}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
        """
        con.execute(query_archive)
        
        # B. 生成当年热数据 (Hot Data: >= 2025-01-01)
        # 这个文件用于上传 OSS 和 Cache
        hot_file = f"{OUTPUT_DIR}/stock_{current_year}.parquet"
        print(f"🔥 生成当年热数据: {hot_file} ...")
        
        query_hot = f"""
        COPY (
            SELECT * FROM read_parquet('{TEMP_DIR}/*.parquet')
            WHERE date >= '{cutoff_date}'
            ORDER BY code, date
        ) TO '{hot_file}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD');
        """
        con.execute(query_hot)
        
        # C. 生成 Cache 文件 (同热数据，但也复制一份到 cache_data 目录)
        cache_file_path = f"{CACHE_DIR}/stock_current_year.parquet"
        shutil.copy2(hot_file, cache_file_path)
        print(f"💾 缓存已准备: {cache_file_path}")

        con.close()
        shutil.rmtree(TEMP_DIR)
        print("✅ 所有数据处理完毕！")
        
    except Exception as e:
        print(f"❌ DuckDB Merge Failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
