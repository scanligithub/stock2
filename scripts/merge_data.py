# scripts/merge_data.py
import pandas as pd
import glob
import os
import gc  # 引入垃圾回收
import pandas_ta as ta
import numpy as np
import datetime

# 路径配置
CACHE_DIR = "cache_data" 
TODAY_DIR = "downloaded_kline" # 注意：这里要对应 artifact 下载后的目录名
FUND_DIR = "downloaded_fundflow"
OUTPUT_DIR = "final_output/engine"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(f"{OUTPUT_DIR}/stock_daily", exist_ok=True)

def optimize_types(df):
    """将 float64 降级为 float32 以节省内存"""
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].astype('float32')
    return df

def calculate_indicators(df):
    """计算单只股票的指标 (输入已排序)"""
    # 1. 价格均线
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(w).mean()
    
    # 2. 量均线
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(w).mean()

    # 3. 复杂指标 (使用 pandas_ta)
    try:
        # MACD (12,26,9)
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0]
            df['dea'] = macd.iloc[:, 2]
            df['macd'] = macd.iloc[:, 1]

        # KDJ (9,3,3)
        kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
        if kdj is not None:
            df['k'] = kdj.iloc[:, 0]
            df['d'] = kdj.iloc[:, 1]
            df['j'] = kdj.iloc[:, 2]

        # RSI (6,12,24)
        df['rsi6'] = df.ta.rsi(close='close', length=6)
        df['rsi12'] = df.ta.rsi(close='close', length=12)
        df['rsi24'] = df.ta.rsi(close='close', length=24)

        # BOLL (20,2)
        boll = df.ta.bbands(close='close', length=20, std=2)
        if boll is not None:
            df['boll_up'] = boll.iloc[:, 2]
            df['boll_lb'] = boll.iloc[:, 0]

        # CCI & ATR
        df['cci'] = df.ta.cci(high='high', low='low', close='close', length=14)
        df['atr'] = df.ta.atr(high='high', low='low', close='close', length=14)

    except Exception:
        pass

    return df

def main():
    print("🚀 开始全量合并与周期生成 (内存优化版)...")
    
    # === 1. 加载并合并资金流 (如果有) ===
    # 为了省内存，我们建立一个 {code_date: flow_data} 的字典或者先不处理
    # 鉴于资金流文件较多，建议先处理 K 线，最后再 Join 资金流，或者分块处理
    # 这里为了逻辑简单，暂不改变整体流程，但加强内存回收
    
    # === 2. 加载 K 线数据 ===
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    new_files = glob.glob(f"{TODAY_DIR}/*.parquet")
    
    # --- 分批读取 History ---
    df_history = pd.DataFrame()
    if history_files:
        print(f"📦 加载历史文件: {len(history_files)} 个")
        # 逐个读取并立即转换类型
        dfs = []
        for f in history_files:
            _df = pd.read_parquet(f)
            _df = optimize_types(_df)
            dfs.append(_df)
        df_history = pd.concat(dfs, ignore_index=True)
        del dfs # 立即释放列表
        gc.collect() # 强制回收

    # --- 分批读取 New Data ---
    df_new = pd.DataFrame()
    if new_files:
        print(f"🔥 加载今日增量: {len(new_files)} 个")
        dfs = []
        for f in tqdm(new_files, desc="Reading New"):
            _df = pd.read_parquet(f)
            _df = optimize_types(_df) # 立即瘦身
            dfs.append(_df)
        df_new = pd.concat(dfs, ignore_index=True)
        del dfs
        gc.collect()

    # === 3. 合并全量 ===
    if df_history.empty and df_new.empty:
        print("❌ 无数据")
        return

    print("🔄 执行全量拼接...")
    # 统一日期格式
    if not df_history.empty:
        df_history['date'] = pd.to_datetime(df_history['date'])
    if not df_new.empty:
        df_new['date'] = pd.to_datetime(df_new['date'])

    df_total = pd.concat([df_history, df_new])
    
    # 释放旧对象
    del df_history, df_new
    gc.collect()
    print("🧹 内存清理完成")

    # 去重排序
    print("⚡ 排序与去重...")
    df_total.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
    df_total.sort_values(['code', 'date'], inplace=True)

    # === 4. 计算指标 ===
    print("🧮 计算技术指标 (分组运算)...")
    # 使用 groupby().apply() 会产生大量临时内存，这里优化一下：
    # 直接在原 DataFrame 上操作可能更省内存，但这需要极其复杂的向量化写法
    # 我们保持 apply 但确保 df_total 已经是 float32
    df_total = df_total.groupby('code', group_keys=False).apply(calculate_indicators)
    
    # 再次优化类型 (指标计算可能引入了 float64)
    df_total = optimize_types(df_total)
    
    # === 5. Join 资金流 (如果有) ===
    # (如果资金流数据量大，建议放在计算指标之前 Join，或者单独处理)
    # 这里假设资金流已包含在 K 线下载逻辑中，或者在此处简单处理
    
    # === 6. 生成周期数据 (周/月) ===
    # 定义聚合规则
    agg_rules = {
        'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
    }
    # 动态添加存在的指标列，通常只对 OHLCV 聚合，指标重新算
    
    def resample_and_save(df_src, freq, name):
        print(f"📅 生成 {name} 数据...")
        # 仅对必要列进行重采样，避免不必要的计算
        valid_cols = [c for c in agg_rules.keys() if c in df_src.columns]
        current_rules = {k: agg_rules[k] for k in valid_cols}
        
        df_res = df_src.set_index('date').groupby('code').resample(freq).agg(current_rules)
        df_res = df_res.dropna(subset=['close']).reset_index()
        df_res.sort_values(['code', 'date'], inplace=True)
        
        # 重算周/月线指标
        df_res = df_res.groupby('code', group_keys=False).apply(calculate_indicators)
        df_res = optimize_types(df_res)
        
        # 保存
        df_res['date'] = df_res['date'].dt.strftime('%Y-%m-%d')
        out = f"{OUTPUT_DIR}/{name}.parquet"
        print(f"💾 保存: {out}")
        df_res.to_parquet(out, index=False, compression='zstd')
        
        del df_res
        gc.collect()

    # 执行重采样
    resample_and_save(df_total, 'W-FRI', 'stock_weekly')
    resample_and_save(df_total, 'ME', 'stock_monthly')

    # === 7. 保存日线数据 ===
    # A. 更新 Cache (全年)
    current_year = datetime.datetime.now().year
    df_cache = df_total[df_total['date'].dt.year == current_year].copy()
    df_cache['date'] = df_cache['date'].dt.strftime('%Y-%m-%d')
    
    print(f"💾 保存 Cache: stock_current_year.parquet")
    df_cache.to_parquet(f"{CACHE_DIR}/stock_current_year.parquet", index=False, compression='zstd')
    del df_cache
    gc.collect()

    # B. 更新 OSS (同上，也就是 Cache 文件)
    # 因为 OSS 也是按年存的，直接复制即可，或者这里再存一份
    oss_path = f"{OUTPUT_DIR}/stock_daily/stock_{current_year}.parquet"
    print(f"💾 保存 OSS: {oss_path}")
    # 这里偷懒直接读刚才存的 cache 文件复制过去，或者用 df_cache (如果没删)
    # 由于刚才 del 了，这里重新筛选一下或者拷贝文件
    # 既然 df_total 还在，再切一次也很快
    df_oss = df_total[df_total['date'].dt.year == current_year].copy()
    df_oss['date'] = df_oss['date'].dt.strftime('%Y-%m-%d')
    df_oss.to_parquet(oss_path, index=False, compression='zstd')

    print("✅ 全部处理完成！")

if __name__ == "__main__":
    main()
