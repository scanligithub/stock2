# scripts/merge_data.py
import pandas as pd
import glob
import os
from tqdm import tqdm
import pandas_ta as ta  # 引入技术分析库
import numpy as np

# 输入目录
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"

# 输出目录
OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def calculate_indicators(df):
    """
    计算技术指标的核心函数
    注意：传入的 df 必须是按 date 升序排列的单只股票数据
    """
    # 1. 均线 (MA) - 使用 Pandas Rolling (速度最快)
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(window=w).mean()
    
    # 2. 均量 (Vol MA)
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(window=w).mean()

    # 3. MACD (12, 26, 9)
    # pandas_ta 返回列名: MACD_12_26_9, MACDh_..., MACDs_...
    try:
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0]  # 快线
            df['macd'] = macd.iloc[:, 1] # 柱状图 (注意：有些软件dif/dea/macd顺序不同，需核对)
            df['dea'] = macd.iloc[:, 2]  # 慢线
    except: pass

    # 4. KDJ (9, 3, 3)
    try:
        kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
        if kdj is not None:
            df['k'] = kdj.iloc[:, 0]
            df['d'] = kdj.iloc[:, 1]
            df['j'] = kdj.iloc[:, 2]
    except: pass

    # 5. RSI (6, 12, 24)
    try:
        df['rsi6'] = df.ta.rsi(close='close', length=6)
        df['rsi12'] = df.ta.rsi(close='close', length=12)
        df['rsi24'] = df.ta.rsi(close='close', length=24)
    except: pass

    # 6. BOLL (20, 2)
    try:
        boll = df.ta.bbands(close='close', length=20, std=2)
        if boll is not None:
            df['boll_lb'] = boll.iloc[:, 0] # Lower
            # df['boll_mb'] = boll.iloc[:, 1] # Mid (其实就是MA20)
            df['boll_up'] = boll.iloc[:, 2] # Upper
    except: pass

    # 7. CCI (14) & ATR (14)
    try:
        df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
        df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
    except: pass

    return df

def main():
    print("🚀 开始宽表合并与指标计算...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    
    print(f"扫描到 K线: {len(k_files)}, 资金流: {len(f_files)}")
    
    f_map = {os.path.basename(f): f for f in f_files}
    
    all_dfs = []
    
    # 1. 读取并合并基础数据
    print("正在合并基础数据...")
    for k_path in tqdm(k_files, desc="Merging"):
        try:
            filename = os.path.basename(k_path)
            df_k = pd.read_parquet(k_path)
            if df_k.empty: continue
            
            df_k['date'] = pd.to_datetime(df_k['date'])
            
            if filename in f_map:
                df_f = pd.read_parquet(f_map[filename])
                if not df_f.empty:
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df_k = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
            
            all_dfs.append(df_k)
        except Exception as e:
            print(f"Skipping {k_path}: {e}")

    if not all_dfs:
        print("❌ 无有效数据合并")
        return

    # 2. 拼接全量大表
    print("拼接全量 DataFrame...")
    full_df = pd.concat(all_dfs, ignore_index=True)
    
    # 3. 排序 (计算指标前必须按 code, date 排序)
    print("排序 (Code + Date)...")
    full_df.sort_values(['code', 'date'], inplace=True)
    
    # 4. 计算技术指标 (最为耗时的步骤)
    print("🧮 正在计算技术指标 (MA, MACD, KDJ, RSI, BOLL, CCI, ATR)...")
    
    # 方案：使用 groupby().apply()
    # 注意：apply 会比向量化慢，但这是计算复杂指标最稳妥的方法
    # 为了提速，我们先定义好 Schema
    full_df = full_df.groupby('code', group_keys=False).apply(calculate_indicators)
    
    # 5. 数据类型优化 (瘦身)
    print("💾 数据类型优化 (Float64 -> Float32)...")
    # 找出所有浮点列
    float_cols = full_df.select_dtypes(include=['float64']).columns
    # 统一转为 float32 并保留 3 位小数
    for col in float_cols:
        full_df[col] = full_df[col].round(3).astype('float32')

    # 6. 还原日期格式
    full_df['date'] = full_df['date'].dt.strftime('%Y-%m-%d')

    # 7. 保存
    outfile = f"{OUTPUT_DIR}/stock_full.parquet"
    print(f"写入 Parquet (ZSTD)... {outfile}")
    
    full_df.to_parquet(outfile, index=False, compression='zstd', row_group_size=100000)
    print(f"✅ 宽表合并与计算完成！包含 {len(full_df.columns)} 个字段。")

if __name__ == "__main__":
    main()
