import pandas as pd
import glob
import os
from tqdm import tqdm

# 输入目录 (GitHub Actions 会把 Artifact 下载到这里)
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"

# 输出目录
OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    print("🚀 开始宽表合并...")
    
    # 1. 扫描文件
    # 注意：artifact 下载后可能有多层目录，这里使用递归搜索
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    
    print(f"K线文件: {len(k_files)}, 资金流文件: {len(f_files)}")
    
    # 建立索引：文件名(code) -> 文件路径
    f_map = {os.path.basename(f): f for f in f_files}
    
    all_dfs = []
    
    for k_path in tqdm(k_files, desc="Merging"):
        try:
            filename = os.path.basename(k_path)
            
            # 读取 K线 (含 PE/PB)
            df_k = pd.read_parquet(k_path)
            if df_k.empty: continue
            
            df_k['date'] = pd.to_datetime(df_k['date'])
            
            # 关联资金流
            if filename in f_map:
                df_f = pd.read_parquet(f_map[filename])
                if not df_f.empty:
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    # Left Join
                    df_k = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
            
            # (可选) 在这里处理 财务数据向下填充 (Forward Fill)
            # 如果你有单独的财务 Parquet，可以在这里再 merge 一次
            
            all_dfs.append(df_k)
            
        except Exception as e:
            print(f"Error {filename}: {e}")

    if all_dfs:
        print("拼接全量表...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        
        print("排序 (Code + Date)...")
        # 这一步对 DuckDB 性能至关重要
        full_df.sort_values(['code', 'date'], inplace=True)
        
        outfile = f"{OUTPUT_DIR}/stock_full.parquet"
        print(f"写入 Parquet (ZSTD)... {outfile}")
        
        # 关键参数：row_group_size=100000 配合 DuckDB 谓词下推
        full_df.to_parquet(outfile, index=False, compression='zstd', row_group_size=100000)
        print("✅ 宽表合并成功")
    else:
        print("❌ 未合并到任何数据")

if __name__ == "__main__":
    main()
