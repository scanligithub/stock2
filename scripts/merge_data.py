# scripts/merge_data.py
import duckdb
import pandas as pd
import pandas_ta as ta
import numpy as np
import os
import glob
import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import gc

# === 路径配置 ===
CACHE_DIR = "cache_data"          
KLINE_DIR = "downloaded_kline"    
FLOW_DIR = "downloaded_fundflow"  

# 输出目录
OUTPUT_ENGINE = "final_output/engine"
OUTPUT_DAILY = f"{OUTPUT_ENGINE}/stock_daily" 
CACHE_OUTPUT_FILE = f"{CACHE_DIR}/stock_buffer.parquet" 

os.makedirs(OUTPUT_DAILY, exist_ok=True)

# === DuckDB 初始化 ===
con = duckdb.connect(database=':memory:')
con.execute("SET memory_limit='4GB';") 
con.execute("SET threads=2;")

def get_all_codes():
    """
    分别扫描历史文件和今日增量文件，获取全量股票代码
    """
    codes = set()
    
    # 1. 从历史 Cache 中获取代码 (如果有)
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    if history_files:
        print(f"📦 扫描历史文件: {len(history_files)} 个")
        # DuckDB 读取列极其快
        try:
            # 这里的 files_sql 是为了让 duckdb 读取列表
            files_sql = str(history_files).replace('[', '').replace(']', '')
            df_codes = con.execute(f"SELECT DISTINCT code FROM read_parquet({files_sql})").fetchdf()
            codes.update(df_codes['code'].tolist())
        except Exception as e:
            print(f"⚠️ 读取历史代码失败 (可能是空文件): {e}")

    # 2. 从今日增量获取代码
    kline_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    print(f"🔥 扫描今日增量: {len(kline_files)} 个")
    for f in kline_files:
        # 文件名即代码 (例如 sz.000001.parquet)
        code = os.path.basename(f).replace('.parquet', '')
        codes.add(code)
        
    return sorted(list(codes)), history_files, kline_files

def calculate_indicators(df):
    """计算技术指标"""
    # 1. 价格均线
    for w in [5, 10, 20, 60, 120, 250]:
        df[f'ma{w}'] = df['close'].rolling(window=w).mean()
    
    # 2. 成交量均线
    for w in [5, 10, 20, 30]:
        df[f'vol_ma{w}'] = df['volume'].rolling(window=w).mean()

    # 3. MACD
    try:
        macd = df.ta.macd(close='close', fast=12, slow=26, signal=9)
        if macd is not None:
            df['dif'] = macd.iloc[:, 0]
            df['macd'] = macd.iloc[:, 1]
            df['dea'] = macd.iloc[:, 2]
    except: pass

    # 4. KDJ
    try:
        kdj = df.ta.kdj(high='high', low='low', close='close', length=9, signal=3)
        if kdj is not None:
            df['k'] = kdj.iloc[:, 0]
            df['d'] = kdj.iloc[:, 1]
            df['j'] = kdj.iloc[:, 2]
    except: pass

    # 5. RSI
    try:
        df['rsi6'] = df.ta.rsi(close='close', length=6)
        df['rsi12'] = df.ta.rsi(close='close', length=12)
        df['rsi24'] = df.ta.rsi(close='close', length=24)
    except: pass

    # 6. BOLL
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

def process_resample(writer_w, writer_m, df_daily):
    """流式生成周/月线"""
    if df_daily.empty: return

    agg_rules = {
        'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
    }
    # 资金流累加
    flow_cols = [c for c in df_daily.columns if 'flow' in c]
    for c in flow_cols: agg_rules[c] = 'sum'

    # 周线
    try:
        df_w = df_daily.set_index('date').resample('W-FRI').agg(agg_rules).dropna(subset=['close']).reset_index()
        if not df_w.empty:
            df_w['code'] = df_daily['code'].iloc[0]
            for w in [5, 10, 20]:
                df_w[f'ma{w}'] = df_w['close'].rolling(w).mean()
            # 写入前转类型
            float_cols = df_w.select_dtypes(include=['float64']).columns
            df_w[float_cols] = df_w[float_cols].astype('float32')
            table_w = pa.Table.from_pandas(df_w)
            writer_w.write_table(table_w)
    except: pass

    # 月线
    try:
        df_m = df_daily.set_index('date').resample('ME').agg(agg_rules).dropna(subset=['close']).reset_index()
        if not df_m.empty:
            df_m['code'] = df_daily['code'].iloc[0]
            for w in [5, 10, 20]:
                df_m[f'ma{w}'] = df_m['close'].rolling(w).mean()
            float_cols = df_m.select_dtypes(include=['float64']).columns
            df_m[float_cols] = df_m[float_cols].astype('float32')
            table_m = pa.Table.from_pandas(df_m)
            writer_m.write_table(table_m)
    except: pass

def main():
    print("🚀 开始 DuckDB 流式合并与计算 (Schema 修复版)...")
    
    # 1. 获取所有股票代码 & 文件列表
    all_codes, history_files, kline_files = get_all_codes()
    
    if not all_codes:
        print("❌ 没有找到任何股票代码")
        return
        
    print(f"✅ 总计需处理: {len(all_codes)} 只股票")

    # 建立文件索引 (Code -> Path)
    # kline_map: 今日增量文件
    kline_map = {os.path.basename(f).replace('.parquet', ''): f for f in kline_files}
    # flow_map: 资金流文件
    flow_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    flow_map = {os.path.basename(f).replace('.parquet', ''): f for f in flow_files}

    # 2. 如果有历史文件，注册到 DuckDB 视图方便查询
    # 注意：这里我们只注册历史文件，不混入今日文件，避免 Schema 冲突
    has_history = False
    if history_files:
        files_sql = str(history_files).replace('[', '').replace(']', '')
        con.execute(f"CREATE OR REPLACE VIEW history_view AS SELECT * FROM read_parquet({files_sql})")
        has_history = True

    # 3. 初始化 Writers
    # 为了获取完整的 Schema (包含所有指标)，我们先模拟处理一只股票
    print("🔍 推断最终 Schema...")
    sample_df = pd.DataFrame()
    
    # 尝试找一个有历史数据的股票来推断 Schema
    if has_history:
        try:
            sample_code = all_codes[0]
            sample_df = con.execute(f"SELECT * FROM history_view WHERE code='{sample_code}' LIMIT 10").fetchdf()
        except: pass
    
    # 如果没历史，或者取失败，造一个空的带基础列的 DF
    if sample_df.empty:
        sample_df = pd.DataFrame(columns=['date', 'code', 'open', 'close', 'high', 'low', 'volume', 'amount', 'turn', 'pctChg', 'peTTM', 'pbMRQ', 'adjustFactor', 'mkt_cap'])
    
    # 确保日期格式
    if 'date' in sample_df.columns and not sample_df.empty:
        sample_df['date'] = pd.to_datetime(sample_df['date'])
        
    # 模拟计算一次以获得完整列 (包含 ma5, macd 等)
    sample_df = calculate_indicators(sample_df)
    
    # 补齐资金流列 (如果历史数据里没资金流，这里补上，防止 Schema 缺失)
    flow_cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
    for c in flow_cols:
        if c not in sample_df.columns: sample_df[c] = np.nan

    # 统一转 float32
    float_cols = sample_df.select_dtypes(include=['float64']).columns
    sample_df[float_cols] = sample_df[float_cols].astype('float32')
    
    # 获取最终 Schema
    final_schema = pa.Table.from_pandas(sample_df).schema
    
    # 定义周/月线 Schema (简化版，先不强校验，用 append 模式)
    # 这里我们只初始化主文件的 Writer
    
    writer_buffer = pq.ParquetWriter(CACHE_OUTPUT_FILE, final_schema, compression='zstd')
    
    current_year = datetime.datetime.now().year
    oss_file = f"{OUTPUT_DAILY}/stock_{current_year}.parquet"
    writer_oss = pq.ParquetWriter(oss_file, final_schema, compression='zstd')
    
    # 周月线因为不好预判 Schema，暂存内存 list，最后一次性写
    weekly_buffer = []
    monthly_buffer = []

    # 4. 🚀 开始循环处理
    print("🌊 开始流式处理...")
    
    for code in tqdm(all_codes):
        # A. 读取历史 (DuckDB)
        df_hist = pd.DataFrame()
        if has_history:
            # 检查该股票是否在历史中
            # 优化：DuckDB 查询带 WHERE code 很快
            df_hist = con.execute(f"SELECT * FROM history_view WHERE code='{code}'").fetchdf()
        
        # B. 读取今日增量 (Pandas)
        df_new = pd.DataFrame()
        if code in kline_map:
            try:
                df_new = pd.read_parquet(kline_map[code])
            except: pass
            
        # C. 合并
        # Pandas concat 会自动对齐列，缺失的列(比如今日数据的MA)会填 NaN，这正是我们想要的
        if df_hist.empty and df_new.empty:
            continue
            
        # 统一日期
        if not df_hist.empty: df_hist['date'] = pd.to_datetime(df_hist['date'])
        if not df_new.empty: df_new['date'] = pd.to_datetime(df_new['date'])
        
        df = pd.concat([df_hist, df_new])
        df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
        df.sort_values('date', inplace=True)
        
        # D. 关联资金流 (仅对今日数据关联，历史数据里应该已经有了)
        # 如果历史数据里缺资金流，这里也会补全
        if code in flow_map: # 资金流文件名通常也是 code
            # 注意：资金流文件可能包含多天，merge 时要注意
            # 但我们的 flow_map 存的是路径，直接读
            pass # 这里简化，资金流在 download 阶段已经包含了吗？
            # 资金流是单独下载的，需要在 merge_data.py 里 merge
            # 我们在 get_files_list 里只拿了路径
            
            # 读取资金流
            # 优化：只读取最近的资金流，避免全量读
            # 这里简单处理：读取该股票所有的资金流文件
            # 我们的 flow_map 是 code -> file path (假设 flow 也是按 code 分片的)
            # 如果下载脚本产生的 flow 是按 code 分片的，那就对了
            
            # 检查是否有对应的资金流文件 (downloaded_fundflow/sz.000001.parquet)
            flow_path = os.path.join(FLOW_DIR, f"{code}.parquet")
            if os.path.exists(flow_path):
                try:
                    df_flow = pd.read_parquet(flow_path)
                    df_flow['date'] = pd.to_datetime(df_flow['date'])
                    
                    # Merge (Left Join)
                    # update: 仅对那些还没有资金流数据的行进行 merge
                    # 为简单起见，直接 merge，pandas 会处理后缀，我们取 _y (new) 覆盖 _x (old) 或者 combine_first
                    # 最简单：直接 merge，如果有重名列，取资金流表里的
                    
                    df = pd.merge(df, df_flow, on=['date', 'code'], how='left', suffixes=('', '_new'))
                    
                    # 如果有 _new 列，说明有更新，覆盖回去
                    for col in flow_cols:
                        if f"{col}_new" in df.columns:
                            df[col] = df[f"{col}_new"].combine_first(df[col])
                            df.drop(columns=[f"{col}_new"], inplace=True)
                except: pass

        # E. 计算指标 (填补 NaN)
        df = calculate_indicators(df)
        
        # F. 生成周/月线 (使用内存中的 df)
        process_resample(None, None, df) # 逻辑不变，存入 buffer
        
        # 内嵌 buffer 收集逻辑
        # (为了代码简洁，这里复制 process_resample 里的 agg 逻辑)
        agg_rules = {
            'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
            'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
            'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
        }
        for c in flow_cols: 
            if c in df.columns: agg_rules[c] = 'sum'
            
        try:
            w_df = df.set_index('date').resample('W-FRI').agg(agg_rules).dropna(subset=['close']).reset_index()
            w_df['code'] = code
            weekly_buffer.append(w_df)
        except: pass
        
        try:
            m_df = df.set_index('date').resample('ME').agg(agg_rules).dropna(subset=['close']).reset_index()
            m_df['code'] = code
            monthly_buffer.append(m_df)
        except: pass

        # G. 写入 Parquet (Buffer & OSS)
        # 类型转换
        float_cols_curr = df.select_dtypes(include=['float64']).columns
        df[float_cols_curr] = df[float_cols_curr].astype('float32')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # 补齐 Schema (防止某些股票缺列)
        for col in final_schema.names:
            if col not in df.columns:
                df[col] = np.nan
        
        # 排序对齐
        df = df[final_schema.names]
        
        # 写入 Cache Buffer
        table = pa.Table.from_pandas(df, schema=final_schema)
        writer_buffer.write_table(table)
        
        # 写入 OSS (Current Year)
        df_curr = df[df['date'] >= f"{current_year}-01-01"]
        if not df_curr.empty:
            table_curr = pa.Table.from_pandas(df_curr, schema=final_schema)
            writer_oss.write_table(table_curr)
            
        # 垃圾回收
        del df, table
        # if i % 100 == 0: gc.collect()

    writer_buffer.close()
    writer_oss.close()
    print("✅ 日线数据写入完成")

    # 5. 保存周/月线
    print("📅 保存周/月线...")
    if weekly_buffer:
        df_w = pd.concat(weekly_buffer, ignore_index=True)
        # 批量计算周线指标
        df_w = df_w.groupby('code', group_keys=False).apply(lambda x: calculate_indicators(x.sort_values('date')))
        
        # 压缩
        float_cols = df_w.select_dtypes(include=['float64']).columns
        df_w[float_cols] = df_w[float_cols].astype('float32')
        df_w['date'] = df_w['date'].dt.strftime('%Y-%m-%d')
        
        df_w.to_parquet(f"{OUTPUT_ENGINE}/stock_weekly.parquet", index=False, compression='zstd')
        
    if monthly_buffer:
        df_m = pd.concat(monthly_buffer, ignore_index=True)
        df_m = df_m.groupby('code', group_keys=False).apply(lambda x: calculate_indicators(x.sort_values('date')))
        
        float_cols = df_m.select_dtypes(include=['float64']).columns
        df_m[float_cols] = df_m[float_cols].astype('float32')
        df_m['date'] = df_m['date'].dt.strftime('%Y-%m-%d')
        
        df_m.to_parquet(f"{OUTPUT_ENGINE}/stock_monthly.parquet", index=False, compression='zstd')

    print("🎉 任务全部完成")

if __name__ == "__main__":
    main()
