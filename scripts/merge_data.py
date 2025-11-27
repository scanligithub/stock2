# scripts/merge_data.py
import pandas as pd
import glob
import os
from tqdm import tqdm
import pandas_ta as ta
import numpy as np
import duckdb
import shutil

# 路径配置
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"
OUTPUT_DIR = "final_output/engine"
TEMP_DIR = "temp_chunks"  # 临时目录，用于存放分批处理的结果

os.makedirs(OUTPUT_DIR, exist_ok=True)
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

# 全局配置：每批处理多少只股票 (500只约占用 300MB 内存，非常安全)
BATCH_SIZE = 500

def process_single_stock(k_path, f_map):
    """
    读取单只股票 -> 合并资金流 -> 计算指标 -> 返回DataFrame
    """
    try:
        # 1. 读取 K 线
        df = pd.read_parquet(k_path)
        if df.empty: return None
        
        filename = os.path.basename(k_path)
        code = filename.replace('.parquet', '')
        
        # 2. 合并资金流
        if filename in f_map:
            f_path = f_map[filename]
            df_f = pd.read_parquet(f_path)
            if not df_f.empty:
                # 转换日期用于合并
                df['date'] = pd.to_datetime(df['date'])
                df_f['date'] = pd.to_datetime(df_f['date'])
                df = pd.merge(df, df_f, on=['date', 'code'], how='left')
        
        # 确保按日期排序 (计算指标必须)
        if df['date'].dtype != 'datetime64[ns]':
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        # ==========================================
        # 3. 计算指标 (MA, MACD, KDJ, RSI, BOLL)
        # ==========================================
        # 此时 df 只有一只股票的数据，计算非常快且省内存
        
        # A. 均线 (Pandas Rolling 极速版)
        for w in [5, 10, 20, 60, 120, 250]:
            df[f'ma{w}'] = df['close'].rolling(w).mean()
        
        # B. 均量
        for w in [5, 10, 20, 30]:
            df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

        # C. 复杂指标 (Pandas-TA)
        # MACD (12, 26, 9)
        try:
            macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
            if macd is not None:
                df['dif'] = macd.iloc[:, 0]
                df['macd'] = macd.iloc[:, 1]
                df['dea'] = macd.iloc[:, 2]
        except: pass

        # KDJ (9, 3, 3)
        try:
            kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
            if kdj is not None:
                df['k'] = kdj.iloc[:, 0]
                df['d'] = kdj.iloc[:, 1]
                df['j'] = kdj.iloc[:, 2]
        except: pass

        # RSI
        try:
            df['rsi6'] = df.ta.rsi(close='close', length=6)
            df['rsi12'] = df.ta.rsi(close='close', length=12)
            df['rsi24'] = df.ta.rsi(close='close', length=24)
        except: pass

        # BOLL (20, 2)
        try:
            boll = df.ta.bbands(close='close', length=20, std=2)
            if boll is not None:
                df['boll_lb'] = boll.iloc[:, 0]
                df['boll_up'] = boll.iloc[:, 2]
        except: pass
        
        # CCI & ATR
        try:
            df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
            df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
        except: pass

        return df

    except Exception as e:
        print(f"Err {k_path}: {e}")
        return None

def main():
    print("🚀 开始内存安全版合并流程...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    print(f"待处理股票: {len(k_files)}")
    
    f_map = {os.path.basename(f): f for f in f_files}
    
    # --- 第一阶段：分批处理并写入临时文件 ---
    batch_buffer = []
    chunk_index = 0
    
    for i, k_path in enumerate(tqdm(k_files, desc="Processing Batches")):
        df = process_single_stock(k_path, f_map)
        if df is not None:
            batch_buffer.append(df)
        
        # 当积攒到一定数量，或者处理到最后一个文件时，写入磁盘
        if len(batch_buffer) >= BATCH_SIZE or (i == len(k_files) - 1 and batch_buffer):
            # 合并当前批次
            chunk_df = pd.concat(batch_buffer, ignore_index=True)
            
            # 类型优化 (Float64 -> Float32)
            float_cols = chunk_df.select_dtypes(include=['float64']).columns
            for col in float_cols:
                chunk_df[col] = chunk_df[col].round(3).astype('float32')
            
            # 还原日期格式为字符串
            chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
            
            # 写入临时文件
            temp_path = f"{TEMP_DIR}/chunk_{chunk_index}.parquet"
            chunk_df.to_parquet(temp_path, index=False, compression='zstd')
            
            # 清空内存
            batch_buffer = []
            chunk_index += 1
            
    print(f"\n✅ 批处理完成，生成了 {chunk_index} 个临时块。开始最终合并...")

    # --- 第二阶段：使用 DuckDB 进行零内存合并 ---
    final_output = f"{OUTPUT_DIR}/stock_full.parquet"
    
    try:
        con = duckdb.connect()
        # DuckDB 的魔法：直接读取所有 chunk 并排序写入，不需要把数据加载到 Python 内存
        print("🦆 DuckDB Merging...")
        
        query = f"""
        COPY (
            SELECT * FROM read_parquet('{TEMP_DIR}/*.parquet')
            ORDER BY code, date
        ) TO '{final_output}' (FORMAT 'PARQUET', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000);
        """
        con.execute(query)
        con.close()
        
        print(f"✅ 最终文件生成完毕: {final_output}")
        
        # 清理临时文件
        shutil.rmtree(TEMP_DIR)
        
    except Exception as e:
        print(f"❌ DuckDB Merge Failed: {e}")
        # 备用方案：如果 DuckDB 失败，尝试用 Pandas 读 chunks (风险较大，通常不会走到这步)

if __name__ == "__main__":
    main()
