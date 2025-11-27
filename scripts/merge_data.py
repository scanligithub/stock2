# scripts/merge_data.py
import pandas as pd
import glob
import os
from tqdm import tqdm
import pandas_ta as ta  # 引入技术分析库
import numpy as np
import duckdb
import shutil

# 输入目录
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"

# 输出目录
OUTPUT_DIR = "final_output/engine"
TEMP_DIR = "temp_chunks" # 临时分批目录

os.makedirs(OUTPUT_DIR, exist_ok=True)
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.makedirs(TEMP_DIR, exist_ok=True)

BATCH_SIZE = 500

def clean_indicators(df):
    """
    强制清洗指标列，防止 'Object' 类型或混入 Timestamp
    """
    # 定义需要清洗的列名 (指标 + 资金流 + 因子)
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
    
    # 加上所有均线 (含价格和量能)
    for w in [5, 10, 20, 60, 120, 250]: target_cols.append(f'ma{w}')
    for w in [5, 10, 20, 30]: target_cols.append(f'vol_ma{w}')

    # 执行转换
    for col in target_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('float32')
    
    return df

def process_single_stock(k_path, f_map):
    """
    单只股票处理逻辑：合并资金流 -> 计算指标
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
            try:
                df_f = pd.read_parquet(f_path)
                if not df_f.empty:
                    # 统一转换为 datetime 用于合并
                    df['date'] = pd.to_datetime(df['date'])
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df = pd.merge(df, df_f, on=['date', 'code'], how='left')
            except Exception:
                pass # 资金流出错不影响 K 线
        
        # 确保按日期排序 (计算指标必须)
        if df['date'].dtype != 'datetime64[ns]':
            df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        # 3. 计算技术指标 (Pandas-TA)
        
        # A. 价格均线 (使用 Pandas 原生 Rolling 提速)
        for w in [5, 10, 20, 60, 120, 250]:
            df[f'ma{w}'] = df['close'].rolling(w).mean()
        
        # B. 成交量均线
        for w in [5, 10, 20, 30]:
            df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

        # C. 复杂指标
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

        # RSI (6, 12, 24)
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
        
        # CCI & ATR (14)
        try:
            df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
            df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
        except: pass

        # 4. 强制类型清洗
        df = clean_indicators(df)

        return df

    except Exception as e:
        print(f"Err processing {k_path}: {e}")
        return None

def main():
    print("🚀 开始宽表合并与指标计算 (内存安全版)...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    print(f"待处理股票: {len(k_files)}")
    
    f_map = {os.path.basename(f): f for f in f_files}
    
    # --- 第一阶段：分批计算并写入临时文件 ---
    batch_buffer = []
    chunk_index = 0
    
    for i, k_path in enumerate(tqdm(k_files, desc="Processing Batches")):
        df = process_single_stock(k_path, f_map)
        if df is not None:
            batch_buffer.append(df)
        
        # 凑够一批写入一次
        if len(batch_buffer) >= BATCH_SIZE or (i == len(k_files) - 1 and batch_buffer):
            chunk_df = pd.concat(batch_buffer, ignore_index=True)
            
            # 还原日期格式
            chunk_df['date'] = chunk_df['date'].dt.strftime('%Y-%m-%d')
            
            # 写入
            temp_path = f"{TEMP_DIR}/chunk_{chunk_index}.parquet"
            chunk_df.to_parquet(temp_path, index=False, compression='zstd', engine='pyarrow')
            
            batch_buffer = []
            chunk_index += 1
            
    print(f"\n✅ 批处理完成，生成了 {chunk_index} 个临时块。开始最终合并...")

    # --- 第二阶段：DuckDB 合并 ---
    final_output = f"{OUTPUT_DIR}/stock_full.parquet"
    
    try:
        con = duckdb.connect()
        print("🦆 DuckDB Merging...")
        
        # 使用 COPY 命令将多个小文件合并为一个大文件，且按 code, date 排序
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
        # 如果 DuckDB 失败，脚本报错退出
        exit(1)

if __name__ == "__main__":
    main()
