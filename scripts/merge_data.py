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
OUTPUT_DAILY = f"{OUTPUT_ENGINE}/stock_daily" # 存放 OSS 用的当年文件
CACHE_OUTPUT_FILE = f"{CACHE_DIR}/stock_buffer.parquet" # 存放给明天用的全量Buffer

os.makedirs(OUTPUT_DAILY, exist_ok=True)

# === DuckDB 初始化 ===
# 限制 DuckDB 使用内存上限，给 Python 预留空间
con = duckdb.connect(database=':memory:')
con.execute("SET memory_limit='4GB';") 
con.execute("SET threads=2;") # 限制线程数，避免 GHA 资源争抢

def get_files_list():
    """获取所有需要合并的文件列表"""
    history_files = glob.glob(f"{CACHE_DIR}/*.parquet")
    kline_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    
    # 资金流文件做成字典，方便 SQL 关联 (如果 DuckDB 直接关联太慢，我们在 Python 里做)
    # 这里策略：资金流先不进 DuckDB 视图，在 Python 处理单只股票时再 merge，这样最稳
    flow_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    flow_map = {os.path.basename(f): f for f in flow_files}
    
    return history_files + kline_files, flow_map

def calculate_indicators(df):
    """
    计算单只股票的技术指标
    df 已经包含了 full history
    """
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
    """
    流式生成周/月线
    注意：这里的 df_daily 只是单只股票的数据，内存很小
    """
    if df_daily.empty: return

    # 聚合规则
    agg_rules = {
        'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
        'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
        'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
    }
    # 资金流字段累加
    flow_cols = [c for c in df_daily.columns if 'flow' in c]
    for c in flow_cols: agg_rules[c] = 'sum'

    # 生成周线
    try:
        df_w = df_daily.set_index('date').resample('W-FRI').agg(agg_rules).dropna(subset=['close']).reset_index()
        # 简单计算周线均线
        if not df_w.empty:
            df_w['code'] = df_daily['code'].iloc[0]
            for w in [5, 10, 20]:
                df_w[f'ma{w}'] = df_w['close'].rolling(w).mean()
            
            # 写入流
            table_w = pa.Table.from_pandas(df_w)
            writer_w.write_table(table_w)
    except: pass

    # 生成月线
    try:
        df_m = df_daily.set_index('date').resample('ME').agg(agg_rules).dropna(subset=['close']).reset_index()
        if not df_m.empty:
            df_m['code'] = df_daily['code'].iloc[0]
            for w in [5, 10, 20]:
                df_m[f'ma{w}'] = df_m['close'].rolling(w).mean()
            
            table_m = pa.Table.from_pandas(df_m)
            writer_m.write_table(table_m)
    except: pass

def main():
    print("🚀 开始 DuckDB 流式合并与计算...")
    
    # 1. 准备文件列表
    input_files, flow_map = get_files_list()
    if not input_files:
        print("❌ 没有找到任何 K 线文件")
        return

    # 2. 在 DuckDB 中建立统一视图 (Virtual View)
    # 使用 list 传递文件路径，DuckDB 会自动处理 union
    # 这一步是零拷贝的，不会读取数据到内存
    print(f"🔗 正在建立 {len(input_files)} 个文件的虚拟视图...")
    
    # 将文件列表转换为 SQL 字符串列表 ['file1', 'file2']
    files_sql = str(input_files).replace('[', '').replace(']', '')
    
    # 创建视图：合并 + 去重 + 排序
    # QUALIFY row_number... 用于去重 (保留最新的记录)
    con.execute(f"""
        CREATE OR REPLACE VIEW raw_kline AS 
        SELECT * FROM read_parquet({input_files})
    """)
    
    # 获取所有股票代码 (Distinct)
    print("🔍 扫描所有股票代码...")
    codes_df = con.execute("SELECT DISTINCT code FROM raw_kline ORDER BY code").fetchdf()
    all_codes = codes_df['code'].tolist()
    print(f"✅ 发现 {len(all_codes)} 只股票")

    # 3. 初始化 Parquet Writers (流式写入器)
    # 我们需要同时写出：
    # A. 缓存文件 (Full History)
    # B. OSS 当年文件 (Current Year)
    # C. 周线/月线文件
    
    current_year = datetime.datetime.now().year
    
    # 定义 Schema 占位符 (先读取第一只股票来确定 Schema)
    first_code = all_codes[0]
    df_sample = con.execute(f"SELECT * FROM raw_kline WHERE code='{first_code}'").fetchdf()
    # 模拟计算一次以获取最终 Schema (包含 MA, MACD 等列)
    df_sample['date'] = pd.to_datetime(df_sample['date'])
    df_sample = calculate_indicators(df_sample)
    
    # 统一转 float32
    float_cols = df_sample.select_dtypes(include=['float64']).columns
    df_sample[float_cols] = df_sample[float_cols].astype('float32')
    
    schema_full = pa.Table.from_pandas(df_sample).schema
    # 周月线 Schema 稍有不同，这里简化处理，在 process_resample 内部处理，暂时不预定义 writer schema
    # 为了简单，周月线我们用 append 模式，或者最后合并。
    # 鉴于周月线数据量小，我们可以暂存在内存 list 中，最后一次性写出
    
    # 打开流式写入器
    writer_buffer = pq.ParquetWriter(CACHE_OUTPUT_FILE, schema_full, compression='zstd')
    
    # OSS 文件路径
    oss_file = f"{OUTPUT_DAILY}/stock_{current_year}.parquet"
    writer_oss = pq.ParquetWriter(oss_file, schema_full, compression='zstd')
    
    # 周月线暂时用内存缓存 (因为它们还要 resample，且体积小)
    # 300MB 的周线如果分片写可能会导致 footer 开销大，这里我们先收集 buffer
    # 如果周线也 OOM，那就也得开 Writer。考虑到 GitHub 7GB 内存，周线全量才 300MB，可以放内存。
    # 为了极致安全，我们还是用 Writer 吧。
    # 需要先推断周线 Schema... 比较麻烦。
    # 方案：周月线单独处理，先生成临时文件，最后合并。
    
    weekly_buffer = []
    monthly_buffer = []

    # 4. 🚀 开始流式处理 (Loop by Code)
    print("🌊 开始流式处理 (Reading -> Calc -> Writing)...")
    
    # 预编译 SQL 提升性能
    # DuckDB 的 prepare 语句在 Python API 中不直接支持带参数的 DF 返回，直接用 f-string 即可，DuckDB解析很快
    
    for code in tqdm(all_codes):
        # A. 从 DuckDB 读取单只股票的全量历史 (内存占用极小)
        # 强制按日期排序
        sql = f"SELECT * FROM raw_kline WHERE code='{code}' ORDER BY date"
        df = con.execute(sql).fetchdf()
        
        # B. 关联资金流 (Pandas Merge)
        # 文件名匹配
        fname = f"{code}.parquet"
        if fname in flow_map:
            try:
                df_flow = pd.read_parquet(flow_map[fname])
                if not df_flow.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df_flow['date'] = pd.to_datetime(df_flow['date'])
                    # Left Join
                    df = pd.merge(df, df_flow, on=['date', 'code'], how='left')
            except: pass
        
        # 确保日期格式
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'])

        # C. 计算指标 (Pandas/TA)
        df = calculate_indicators(df)
        
        # D. 生成周/月线 (Resample)
        process_resample(None, None, df) # 临时禁用 Writer，改为收集
        # 修改 process_resample 逻辑使其返回 df，而不是 write
        # 这里为了不破坏结构，我们把聚合逻辑提取出来
        
        # --- 周/月线 收集逻辑 (内嵌) ---
        agg_rules = {
            'open': 'first', 'close': 'last', 'high': 'max', 'low': 'min',
            'volume': 'sum', 'amount': 'sum', 'turn': 'mean',
            'peTTM': 'last', 'pbMRQ': 'last', 'mkt_cap': 'last', 'adjustFactor': 'last'
        }
        for c in ['net_flow_amount', 'main_net_flow']: 
            if c in df.columns: agg_rules[c] = 'sum'
            
        # 周线
        try:
            w_df = df.set_index('date').resample('W-FRI').agg(agg_rules).dropna(subset=['close']).reset_index()
            w_df['code'] = code
            weekly_buffer.append(w_df)
        except: pass
        
        # 月线
        try:
            m_df = df.set_index('date').resample('ME').agg(agg_rules).dropna(subset=['close']).reset_index()
            m_df['code'] = code
            monthly_buffer.append(m_df)
        except: pass
        # -----------------------------

        # E. 数据类型优化 (准备写入日线)
        float_cols = df.select_dtypes(include=['float64']).columns
        df[float_cols] = df[float_cols].astype('float32')
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        # F. 写入 Full Buffer (给缓存用)
        # 需要对齐列名，防止资金流列有的股票有，有的没有
        # 补齐缺失列
        for col in schema_full.names:
            if col not in df.columns:
                df[col] = np.nan
        
        # 按照 Schema 顺序重排
        df = df[schema_full.names]
        
        table = pa.Table.from_pandas(df, schema=schema_full)
        writer_buffer.write_table(table)
        
        # G. 写入 OSS Current Year (仅今年)
        df_curr = df[df['date'] >= f"{current_year}-01-01"]
        if not df_curr.empty:
            table_curr = pa.Table.from_pandas(df_curr, schema=schema_full)
            writer_oss.write_table(table_curr)
            
        # H. 显式垃圾回收 (非常重要)
        del df, table
        # 每处理 100 只股票 GC 一次，平衡速度与内存
        # if i % 100 == 0: gc.collect() 

    # 5. 关闭 Writers
    writer_buffer.close()
    writer_oss.close()
    print("✅ 日线数据处理完成 (Buffer + OSS)")

    # 6. 统一保存周/月线
    # 因为周月线数据量小 (周线~300MB, 月线~80MB)，concat 没问题
    print("📅 保存周/月线数据...")
    if weekly_buffer:
        df_w = pd.concat(weekly_buffer, ignore_index=True)
        # 计算周线指标 (全量算更快)
        df_w = df_w.groupby('code', group_keys=False).apply(lambda x: calculate_indicators(x.sort_values('date')))
        # 压缩保存
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

    print("🎉 全流程结束！")

if __name__ == "__main__":
    main()
