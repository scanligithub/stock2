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
TEMP_DIR = "temp_chunks"

os.makedirs(OUTPUT_DIR, exist_ok=True)
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

BATCH_SIZE = 500

def clean_indicators(df):
    """
    强制清洗指标列，防止 'Object' 类型或混入 Timestamp 导致 Parquet 崩溃
    """
    # 定义所有应该是浮点数的指标列
    target_cols = [
        # MACD
        'dif', 'dea', 'macd', 
        # KDJ
        'k', 'd', 'j',
        # RSI
        'rsi6', 'rsi12', 'rsi24',
        # BOLL
        'boll_up', 'boll_lb',
        # Other
        'cci', 'atr',
        # Funds
        'net_flow_amount', 'main_net_flow', 'super_large_net_flow', 
        'large_net_flow', 'medium_small_net_flow',
        # Factors
        'peTTM', 'pbMRQ', 'adjustFactor', 'mkt_cap'
    ]
    
    # 加上所有均线
    for w in [5, 10, 20, 60, 120, 250]:
        target_cols.append(f'ma{w}')
    for w in [5, 10, 20, 30]:
        target_cols.append(f'vol_ma{w}')

    # 执行清洗
    for col in target_cols:
        if col in df.columns:
            # errors='coerce' 是关键：遇到无法转换的脏数据(如Timestamp)，直接变NaN
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
    
    return df

def process_single_stock(k_path, f_map):
    try:
        # 1. 读取 K 线
        df = pd.read_parquet(k_path)
        if df.empty: return None
        
        filename = os.path.basename(k_path)
        code = filename.replace('.parquet', '')
        
        # 2. 合并资金流
        if filename in f_map:
            f_path = f_map[filename]
            try:
                df_f = pd.read_parquet(f_path)
                if not df_f.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df = pd.merge(df, df_f, on=['date', 'code'], how='left')
            except Exception:
                pass # 资金流出错不影响主流程
        
        # 排序
        if df['date'].dtype != 'datetime64[ns]':
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        # 3. 计算指标
        # A. MAs
        for w in [5, 10, 20, 60, 120, 250]:
            df[f'ma{w}'] = df['close'].rolling(w).mean()
        for w in [5, 10, 20, 30]:
            df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

        # B. MACD
        try:
            macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
            if macd is not None:
                df['dif'] = macd.iloc[:, 0]
                df['macd'] = macd.iloc[:, 1]
                df['dea'] = macd.iloc[:, 2]
        except: pass

        # C. KDJ
        try:
            kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
            if kdj is not None:
                df['k'] = kdj.iloc[:, 0]
                df['d'] = kdj.iloc[:, 1]
                df['j'] = kdj.iloc[:, 2]
        except: pass

        # D. RSI
        try:
            df['rsi6'] = df.ta.rsi(close='close', length=6)
            df['rsi12'] = df.ta.rsi(close='close', length=12)
            df['rsi24'] = df.ta.rsi(close='close', length=24)
        except: pass

        # E. BOLL
        try:
            boll = df.ta.bbands(close='close', length=20, std=2)
            if boll is not None:
                df['boll_lb'] = boll.iloc[:, 0]
                df['boll_up'] = boll.iloc[:, 2]
        except: pass
        
        # F. Other
        try:
            df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
            df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
        except: pass

        # 【关键】在返回前强制清洗类型
        df = clean_indicators(df)

        return df

    except Exception as e:
        print(f"Err processing {k_path}: {e}")
        return None

def main():
    print("🚀 开始内存安全版合并流程 (强类型清洗)...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    print(f"待处理股票: {len(k_files)}")
    
    f_map = {os.path.basename(f): f for f in f_files}
    
    batch_buffer = []
    chunk_index = 0
    
    for i, k_path in enumerate(tqdm(k_files, desc="Processing Batches")):
        df = process_single_stock(k_path, f_map)
        if df is not None:
            batch_buffer.append(df)
        
        # 写入临时块
        if len(batch_buffer) >= BATCH_SIZE or (i == len(k_files) - 1 and batch_buffer):
            chunk_df = pd.concat(batch_buffer, ignore_index=True)
            
            # 再次保险：还原日期格式
            chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
            
            temp_path = f"{TEMP_DIR}/chunk_{chunk_index}.parquet"
            # 使用 pyarrow 引擎，确保兼容性
            chunk_df.to_parquet(temp_path, index=False, compression='zstd', engine='pyarrow')
            
            batch_buffer = []
            chunk_index += 1
            
    print(f"\n✅ 批处理完成，生成了 {chunk_index} 个临时块。开始最终合并...")

    final_output = f"{OUTPUT_DIR}/stock_full.parquet"
    
    try:
        con = duckdb.connect()
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
        shutil.rmtree(TEMP_DIR)
        
    except Exception as e:
        print(f"❌ DuckDB Merge Failed: {e}")

if __name__ == "__main__":
    main()
