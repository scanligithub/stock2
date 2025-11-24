# scripts/merge_data.py
import pandas as pd
import glob
import os
from tqdm import tqdm

# 输入目录 (GitHub Actions 下载 artifact 后的路径)
KLINE_DIR = "downloaded_kline" 
FLOW_DIR = "downloaded_fundflow"

# 输出目录
OUTPUT_DIR = "final_output/engine"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    print("🚀 开始宽表合并...")
    
    k_files = glob.glob(f"{KLINE_DIR}/**/*.parquet", recursive=True)
    f_files = glob.glob(f"{FLOW_DIR}/**/*.parquet", recursive=True)
    
    print(f"扫描到 K线: {len(k_files)}, 资金流: {len(f_files)}")
    
    # 建立索引：文件名 -> 路径
    f_map = {os.path.basename(f): f for f in f_files}
    
    all_dfs = []
    
    for k_path in tqdm(k_files, desc="Merging"):
        try:
            filename = os.path.basename(k_path)
            
            # 读取 K线
            df_k = pd.read_parquet(k_path)
            if df_k.empty: continue
            
            df_k['date'] = pd.to_datetime(df_k['date'])
            
            # Left Join 资金流
            if filename in f_map:
                df_f = pd.read_parquet(f_map[filename])
                if not df_f.empty:
                    df_f['date'] = pd.to_datetime(df_f['date'])
                    df_k = pd.merge(df_k, df_f, on=['date', 'code'], how='left')
            
            all_dfs.append(df_k)
            
        except Exception as e:
            print(f"Skipping {k_path}: {e}")

    if all_dfs:
        print("Concat full dataframe...")
        full_df = pd.concat(all_dfs, ignore_index=True)
        
        print("Sorting by Code + Date...")
        full_df.sort_values(['code', 'date'], inplace=True)
        
        outfile = f"{OUTPUT_DIR}/stock_full.parquet"
        print(f"Writing to {outfile} (ZSTD)...")
        
        # 关键优化：设置 row_group_size 以支持 DuckDB 谓词下推
        full_df.to_parquet(outfile, index=False, compression='zstd', row_group_size=100000)
        print("✅ 宽表合并成功")
    else:
        print("❌ 无有效数据合并")

if __name__ == "__main__":
    main()
