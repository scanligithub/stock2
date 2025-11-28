# scripts/merge_data.py
import pandas as pd
import numpy as np
import pandas_ta as ta
import os
import glob
import datetime
import gc  # 引入垃圾回收模块
from tqdm import tqdm

# === 路径配置 ===
CACHE_DIR = "cache_data"          
KLINE_DIR = "downloaded_kline"    
FLOW_DIR = "downloaded_fundflow"  

# 输出目录
OUTPUT_ENGINE = "final_output/engine"
OUTPUT_DAILY = f"{OUTPUT_ENGINE}/stock_daily"

os.makedirs(OUTPUT_DAILY, exist_ok=True)

def optimize_float(df):
    """
    【内存优化核心】
    将所有 float64 降级为 float32，节省 50% 内存
    """
    float_cols = df.select_dtypes(include=['float64']).columns
    if len(float_cols) > 0:
        df[float_cols] = df[float_cols].astype('float32')
    return df

def calculate_indicators(df):
    """计算技术指标"""
    # 1. 价格均线 (MA)
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(window=w).mean().astype('float32')
    
    # 2. 成交量均线 (Vol MA)
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(window=w).mean().astype('float32')

    # 3. MACD
    try:
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0].astype('float32')
            df['macd'] = macd.iloc[:, 1].astype('float32')
            df['dea'] = macd.iloc[:, 2].astype('float32')
    except: pass

    # 4. KDJ
    try:
        kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
        if kdj is not None:
            df['k'] = kdj.iloc[:, 0].astype('float32')
            df['d'] = kdj.iloc[:, 1].astype('float32')
            df['j'] = kdj.iloc[:, 2].astype('float32')
    except: pass

    # 5. RSI
    try:
        df['rsi6'] = df.ta.rsi(close='close', length=6).astype('float32')
        df['rsi12'] = df.ta.rsi(close='close', length=12).astype('float32')
        df['rsi24'] = df.ta.rsi(close='close', length=24).astype('float32')
    except: pass

    # 6. BOLL
    try:
        boll = df.ta.bbands(close='close', length=20, std=2)
        if boll is not None:
            df['boll_lb'] = boll.iloc[:, 0].astype('float32')
            df['boll_up'] = boll.iloc[:, 2].astype('float32')
    except: pass

    # 7. 其他
    try:
        df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14).astype('float32')
        df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14).astype('float32')
    except: pass

    return df

def process_resample(df_daily, freq, filename):
    """生成周线/月线数据"""
    print(f"   -> 正在生成 {freq} 周期数据 ({filename})...")
    
    # 仅保留必要列进行 Resample，减少内存压力
    # 资金流累加，价格取首尾
    agg_rules = {
        'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
    }
    for c in ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']:
        if c in df_daily.columns: agg_rules[c] = 'sum'

    # 执行 Resample
    df_res = df_daily.set_index('date').groupby('code').resample(freq).agg(agg_rules)
    df_res = df_res.dropna(subset=['close']).reset_index()
    df_res.sort_values(['code', 'date'], inplace=True)
    
    # 计算基础均线
    grouped = df_res.groupby('code')['close']
    for w in [5, 10, 20]:
        df_res[f'ma{w}'] = grouped.rolling(w).mean().reset_index(0, drop=True).astype('float32')
    
    # 格式化
    df_res['date'] = df_res['date'].dt.strftime('%Y-%m-%d')
    df_res = optimize_float(df_res) # 再次压缩

    out_path = f"{OUTPUT_ENGINE}/{filename}"
    df_res.to_parquet(out_path, index=False, compression='zstd')
    print(f"      ✅ 已保存: {len(df_res)} 行")
    
    # 清理内存
    del df_res
    gc.collect()

def main():
    print("🚀 开始全量合并与周期生成 (内存优化版)...")
    current_year = datetime.datetime.now().year
    
    # 1. 加载历史数据 (逐步加载并压缩)
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    df_history = pd.DataFrame()
    
    if history_files:
        print(f"📦 发现历史文件: {len(history_files)} 个，开始逐个加载...")
        dfs = []
        for f in history_files:
            # 读取时立刻转 float32
            _df = pd.read_parquet(f)
            _df = optimize_float(_df)
            dfs.append(_df)
        
        df_history = pd.concat(dfs, ignore_index=True)
        # 释放临时列表
        del dfs
        gc.collect() 
        print(f"✅ 历史数据加载完毕: {len(df_history)} 行")
    
    # 2. 加载今日增量
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    f_map = {os.path.basename(f): f for f in f_files}
    
    print(f"🔥 处理今日增量: {len(k_files)} 个")
    dfs_new = []
    
    # 分批处理增量，防止 list 过大
    for k_f in tqdm(k_files, desc="Reading New"):
        try:
            df_k = pd.read_parquet(k_f)
            if df_k.empty: continue
            
            df_k['date'] = pd.to_datetime(df_k['date'])
            
            fname = os.path.basename(k_f)
            if fname in f_map:
                df_f = pd.read_parquet(f_map[fname])
                if not df_f.empty:
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df_k = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
            
            # 立刻优化内存
            df_k = optimize_float(df_k)
            dfs_new.append(df_k)
        except: pass
    
    if dfs_new:
        df_new = pd.concat(dfs_new, ignore_index=True)
        del dfs_new
        gc.collect()
    else:
        df_new = pd.DataFrame()

    # 3. 全量合并
    if df_history.empty and df_new.empty:
        print("❌ 无数据处理")
        return

    # 统一日期格式
    if not df_history.empty: df_history['date'] = pd.to_datetime(df_history['date'])
    # df_new 已经是 datetime

    print("🔄 执行全量合并...")
    df_total = pd.concat([df_history, df_new], ignore_index=True)
    
    # 释放旧变量
    del df_history
    del df_new
    gc.collect()
    
    # 去重排序
    print("🔄 排序与去重...")
    df_total.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df_total.sort_values(['code', 'date'], inplace=True)
    
    # 4. 计算全量指标 (这是最吃内存的一步)
    print("🧮 计算技术指标...")
    # 使用 groupby apply 会产生大量临时 DataFrame，这里要小心
    # 如果依然 OOM，可以考虑只计算今年的指标，或者分批计算
    df_total = df_total.groupby('code', group_keys=False).apply(calculate_indicators)
    
    # 再次优化类型 (指标计算可能引入 float64)
    df_total = optimize_float(df_total)
    gc.collect()

    # 5. 生成多周期
    print("📅 生成多周期数据...")
    # 这里的 df_total 很大，传参要注意
    process_resample(df_total, 'W-FRI', 'stock_weekly.parquet')
    process_resample(df_total, 'ME', 'stock_monthly.parquet')

    # 6. 保存逻辑
    print("💾 准备保存...")
    df_total['date'] = df_total['date'].dt.strftime('%Y-%m-%d')

    # A. 更新 Cache (仅保留当年)
    # 为了防止 Cache 越来越大导致 OOM，这里严格只留今年
    df_hot = df_total[df_total['date'] >= f"{current_year}-01-01"].copy()
    cache_path = f"{CACHE_DIR}/stock_current_year.parquet"
    print(f"📦 保存 Cache: {cache_path} ({len(df_hot)} 行)")
    df_hot.to_parquet(cache_path, index=False, compression='zstd')
    
    # B. 保存 OSS (也是只传今年)
    oss_path = f"{OUTPUT_DAILY}/stock_{current_year}.parquet"
    print(f"☁️ 保存 OSS: {oss_path}")
    df_hot.to_parquet(oss_path, index=False, compression='zstd')

    # C. (可选) 如果你需要在 Web 端回测历史，可能需要把全量数据存一份
    # 但考虑到 7GB 内存限制，生成 stock_full.parquet 可能会失败
    # 鉴于你的架构是 "按年归档"，这里我们不再生成 stock_full.parquet
    # 而是依赖 Release 的历史文件 + OSS 的今年文件
    
    print("✅ 处理完成！")

if __name__ == "__main__":
    main()
