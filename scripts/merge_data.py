# scripts/merge_data.py
import pandas as pd
import numpy as np
import pandas_ta as ta
import os
import glob
import datetime
from tqdm import tqdm  # 【关键修复】补上了这个导入

# === 路径配置 ===
CACHE_DIR = "cache_data"          # 历史数据(Release+Cache)存放地
KLINE_DIR = "downloaded_kline"    # 今日K线增量
FLOW_DIR = "downloaded_fundflow"  # 今日资金流增量

# 输出目录
OUTPUT_ENGINE = "final_output/engine"
OUTPUT_DAILY = f"{OUTPUT_ENGINE}/stock_daily"

os.makedirs(OUTPUT_DAILY, exist_ok=True)

# === 指标计算函数 ===
def calculate_indicators(df):
    """
    计算技术指标 (MA, VolMA, MACD, KDJ, RSI, BOLL, CCI, ATR)
    df 必须是单只股票且按日期排序
    """
    # 1. 价格均线 (MA)
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(window=w).mean()
    
    # 2. 成交量均线 (Vol MA)
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(window=w).mean()

    # 3. MACD (12, 26, 9)
    try:
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0]
            df['macd'] = macd.iloc[:, 1]
            df['dea'] = macd.iloc[:, 2]
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
            df['boll_lb'] = boll.iloc[:, 0]
            df['boll_up'] = boll.iloc[:, 2]
    except: pass

    # 7. 其他
    try:
        df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
        df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)
    except: pass

    return df

def process_resample(df_daily, freq, filename):
    """生成周线/月线数据"""
    print(f"   -> 正在生成 {freq} 周期数据 ({filename})...")
    
    # 聚合规则
    agg_rules = {
        'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
    }
    # 资金流累加
    for c in ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']:
        if c in df_daily.columns: agg_rules[c] = 'sum'

    # 重采样
    # 'date' 已经是 datetimeIndex
    df_res = df_daily.set_index('date').groupby('code').resample(freq).agg(agg_rules)
    
    # 清洗无效行
    df_res = df_res.dropna(subset=['close']).reset_index()
    
    # 排序
    df_res.sort_values(['code', 'date'], inplace=True)
    
    # 计算周/月线指标 (简单版，只算均线)
    grouped = df_res.groupby('code')['close']
    df_res['ma5'] = grouped.rolling(5).mean().reset_index(0, drop=True)
    df_res['ma10'] = grouped.rolling(10).mean().reset_index(0, drop=True)
    df_res['ma20'] = grouped.rolling(20).mean().reset_index(0, drop=True)
    
    # 格式化与压缩
    df_res['date'] = df_res['date'].dt.strftime('%Y-%m-%d')
    
    float_cols = df_res.select_dtypes(include=['float64']).columns
    for c in float_cols:
        df_res[c] = df_res[c].round(3).astype('float32')

    out_path = f"{OUTPUT_ENGINE}/{filename}"
    df_res.to_parquet(out_path, index=False, compression='zstd')
    print(f"      ✅ 已保存: {len(df_res)} 行")

def main():
    print("🚀 开始全量合并与周期生成 (内存优化版)...")
    
    current_year = datetime.datetime.now().year
    
    # 1. 加载历史数据 (Release + Cache)
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    if history_files:
        print(f"📦 加载历史文件: {len(history_files)} 个")
        df_history = pd.concat([pd.read_parquet(f) for f in history_files], ignore_index=True)
    else:
        print("⚠️ 未找到历史数据，将进行全量初始化")
        df_history = pd.DataFrame()

    # 2. 加载今日增量
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    print(f"🔥 加载今日增量: {len(k_files)} 个 K线文件")
    
    f_map = {os.path.basename(f): f for f in f_files}
    dfs_new = []
    
    # 【修复点】这里使用了 tqdm，之前报错就是因为没 import
    for k_f in tqdm(k_files, desc="Reading New"):
        try:
            df_k = pd.read_parquet(k_f)
            if df_k.empty: continue
            
            # 统一转 datetime 方便 merge
            df_k['date'] = pd.to_datetime(df_k['date'])
            
            fname = os.path.basename(k_f)
            if fname in f_map:
                df_f = pd.read_parquet(f_map[fname])
                if not df_f.empty:
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df_k = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
            
            dfs_new.append(df_k)
        except: pass
        
    if dfs_new:
        df_new = pd.concat(dfs_new, ignore_index=True)
    else:
        df_new = pd.DataFrame()

    # 3. 合并全量
    if df_history.empty and df_new.empty:
        print("❌ 无数据处理")
        return

    # 统一格式
    if not df_history.empty: df_history['date'] = pd.to_datetime(df_history['date'])
    # df_new 已经是 datetime

    print("🔄 合并历史与新增...")
    df_total = pd.concat([df_history, df_new], ignore_index=True)
    
    # 去重 (防止重复运行)
    df_total.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df_total.sort_values(['code', 'date'], inplace=True)
    
    # 4. 计算全量指标
    print("🧮 计算技术指标 (耗时操作)...")
    # 使用 groupby apply 进行并行计算
    df_total = df_total.groupby('code', group_keys=False).apply(calculate_indicators)
    
    # 5. 生成周线/月线
    # (此时 date 还是 datetime 类型，正好用于 resample)
    print("📅 生成多周期数据...")
    process_resample(df_total, 'W-FRI', 'stock_weekly.parquet')
    process_resample(df_total, 'ME', 'stock_monthly.parquet')

    # 6. 数据类型压缩 (准备保存)
    print("💾 数据类型优化...")
    float_cols = df_total.select_dtypes(include=['float64']).columns
    for c in float_cols:
        df_total[c] = df_total[c].round(3).astype('float32')
        
    # 还原日期为字符串
    df_total['date'] = df_total['date'].dt.strftime('%Y-%m-%d')

    # 7. 切分输出
    # A. 保存 Cache (供明天用，仅保留当年的热数据)
    df_hot = df_total[df_total['date'] >= f"{current_year}-01-01"].copy()
    cache_path = f"{CACHE_DIR}/stock_current_year.parquet"
    print(f"📦 更新 Cache 文件: {cache_path} ({len(df_hot)} 行)")
    df_hot.to_parquet(cache_path, index=False, compression='zstd')

    # B. 保存 OSS (仅保存当年的文件到 stock_daily 目录)
    oss_path = f"{OUTPUT_DAILY}/stock_{current_year}.parquet"
    print(f"☁️ 生成 OSS 文件: {oss_path}")
    df_hot.to_parquet(oss_path, index=False, compression='zstd')

    print("✅ 处理完成！")

if __name__ == "__main__":
    main()
