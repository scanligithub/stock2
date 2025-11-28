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
    """获取全量股票代码"""
    codes = set()
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    if history_files:
        print(f"📦 扫描历史文件: {len(history_files)} 个")
        try:
            files_sql = str(history_files).replace('[', '').replace(']', '')
            df_codes = con.execute(f"SELECT DISTINCT code FROM read_parquet({files_sql})").fetchdf()
            codes.update(df_codes['code'].tolist())
        except Exception as e:
            print(f"⚠️ 读取历史代码失败: {e}")

    kline_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    print(f"🔥 扫描今日增量: {len(kline_files)} 个")
    for f in kline_files:
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
    print("🚀 开始 DuckDB 流式合并与计算 (Schema 锁定版)...")
    
    all_codes, history_files, kline_files = get_all_codes()
    if not all_codes:
        print("❌ 没有找到任何股票代码")
        return
    print(f"✅ 总计需处理: {len(all_codes)} 只股票")

    # 建立索引
    kline_map = {os.path.basename(f).replace('.parquet', ''): f for f in kline_files}
    flow_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    flow_map = {os.path.basename(f).replace('.parquet', ''): f for f in flow_files}

    # 注册历史视图
    has_history = False
    if history_files:
        files_sql = str(history_files).replace('[', '').replace(']', '')
        con.execute(f"CREATE OR REPLACE VIEW history_view AS SELECT * FROM read_parquet({files_sql})")
        has_history = True

    # =========================================================
    # 【核心修复】构造标准 Dummy Schema
    # 不依赖 sample_df 推断，而是手动定义全量字段模板
    # 确保 Date 是 string，数值是 float32
    # =========================================================
    print("🔒 锁定数据 Schema...")
    
    dummy_data = {
        'date': ['2025-01-01'], # 强制 String
        'code': ['dummy'],      # 强制 String
    }
    # 定义所有可能出现的数值列
    float_cols_def = [
        'open', 'close', 'high', 'low', 'volume', 'amount', 'turn', 'pctChg', 
        'peTTM', 'pbMRQ', 'adjustFactor', 'mkt_cap', 
        'net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow',
        'ma5', 'ma10', 'ma20', 'ma60', 'ma120', 'ma250',
        'vol_ma5', 'vol_ma10', 'vol_ma20', 'vol_ma30',
        'dif', 'dea', 'macd', 'k', 'd', 'j', 'rsi6', 'rsi12', 'rsi24',
        'boll_lb', 'boll_up', 'cci', 'atr'
    ]
    
    for c in float_cols_def:
        dummy_data[c] = [0.0] # 初始为 float
        
    df_schema_template = pd.DataFrame(dummy_data)
    
    # 强制转换类型
    df_schema_template[float_cols_def] = df_schema_template[float_cols_def].astype('float32')
    
    # 获取绝对正确的 Schema
    final_schema = pa.Table.from_pandas(df_schema_template).schema
    
    # 初始化 Writers
    writer_buffer = pq.ParquetWriter(CACHE_OUTPUT_FILE, final_schema, compression='zstd')
    
    current_year = datetime.datetime.now().year
    oss_file = f"{OUTPUT_DAILY}/stock_{current_year}.parquet"
    writer_oss = pq.ParquetWriter(oss_file, final_schema, compression='zstd')
    
    weekly_buffer = []
    monthly_buffer = []

    # 4. 🚀 循环处理
    print("🌊 开始流式处理...")
    
    for code in tqdm(all_codes):
        # A. 读取历史
        df_hist = pd.DataFrame()
        if has_history:
            df_hist = con.execute(f"SELECT * FROM history_view WHERE code='{code}'").fetchdf()
        
        # B. 读取今日
        df_new = pd.DataFrame()
        if code in kline_map:
            try: df_new = pd.read_parquet(kline_map[code])
            except: pass
            
        if df_hist.empty and df_new.empty: continue
            
        # 统一日期
        if not df_hist.empty: df_hist['date'] = pd.to_datetime(df_hist['date'])
        if not df_new.empty: df_new['date'] = pd.to_datetime(df_new['date'])
        
        # 合并
        df = pd.concat([df_hist, df_new])
        # 必须清除空日期的行
        df.dropna(subset=['date'], inplace=True)
        df.drop_duplicates(subset=['code', 'date'], keep='last', inplace=True)
        df.sort_values('date', inplace=True)
        
        # D. 关联资金流
        if code in flow_map:
            if os.path.exists(flow_map[code]):
                try:
                    df_flow = pd.read_parquet(flow_map[code])
                    df_flow['date'] = pd.to_datetime(df_flow['date'])
                    
                    # 仅对 new data 关联，还是全量？全量关联最稳
                    df = pd.merge(df, df_flow, on=['date', 'code'], how='left', suffixes=('', '_new'))
                    
                    # 合并新旧资金流列
                    flow_raw_cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
                    for col in flow_raw_cols:
                        if f"{col}_new" in df.columns:
                            df[col] = df[f"{col}_new"].combine_first(df[col])
                            df.drop(columns=[f"{col}_new"], inplace=True)
                except: pass

        # E. 计算指标
        df = calculate_indicators(df)
        
        # F. 生成周/月线 (使用内存df)
        process_resample(None, None, df)
        
        # 内嵌 Buffer 收集 (周/月线)
        agg_rules = {
            'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
            'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
            'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
        }
        flow_raw_cols = ['net_flow_amount', 'main_net_flow', 'super_large_net_flow', 'large_net_flow', 'medium_small_net_flow']
        for c in flow_raw_cols: 
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

        # G. 写入 Parquet
        # 补齐缺失列 (用 NaN)
        for col in final_schema.names:
            if col not in df.columns:
                df[col] = np.nan
        
        # 按照 Schema 顺序重排
        df = df[final_schema.names]
        
        # 强制类型转换 (Float32)
        df[float_cols_def] = df[float_cols_def].astype('float32')
        # 日期转 String
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # 写入 Buffer
        table = pa.Table.from_pandas(df, schema=final_schema)
        writer_buffer.write_table(table)
        
        # 写入 OSS (Current Year)
        df_curr = df[df['date'] >= f"{current_year}-01-01"]
        if not df_curr.empty:
            table_curr = pa.Table.from_pandas(df_curr, schema=final_schema)
            writer_oss.write_table(table_curr)
            
        del df, table

    writer_buffer.close()
    writer_oss.close()
    print("✅ 日线写入完成")

    # 5. 保存周/月线
    print("📅 保存周/月线...")
    if weekly_buffer:
        df_w = pd.concat(weekly_buffer, ignore_index=True)
        df_w = df_w.groupby('code', group_keys=False).apply(lambda x: calculate_indicators(x.sort_values('date')))
        
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
