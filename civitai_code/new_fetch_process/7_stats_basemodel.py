import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待统计的文件列表 (填入上一步生成的 _valid_names_no_other.csv)
csv_files_list = [
    r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.csv" 
]

# 2. 结果保存路径
output_txt_path = r"F:\civitai_new_fetch\summary\stats\base_model_statistics.txt"

# 3. 列名匹配候选
base_model_cols = ['baseModel']

# ===========================================

def generate_statistics():
    print(f"=== 开始统计 Base Model 分布 ===\n")
    
    # 用于存储所有文件中的 base_model 数据
    all_base_models = []
    
    total_rows_scanned = 0

    # --- 1. 循环读取并提取列 ---
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"正在读取: {os.path.basename(file_path)}")

        try:
            df = None
            for enc in ['utf-8', 'gb18030', 'latin1']:
                try:
                    df = pd.read_csv(file_path, dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                print("  [错误] 读取失败。")
                continue

            # 找到存在的列名
            col_base = next((c for c in base_model_cols if c in df.columns), None)
            
            if col_base:
                # 提取该列数据，去除空值，去除首尾空格
                # .dropna() 去掉 NaN
                # .astype(str) 确保是字符串
                # .str.strip() 去除可能导致统计重复的空格 (例如 "SDXL " 和 "SDXL")
                column_data = df[col_base].dropna().astype(str).str.strip()
                
                # 过滤掉空字符串 (如果有的行是空字符串而不是NaN)
                column_data = column_data[column_data != ""]
                
                all_base_models.append(column_data)
                total_rows_scanned += len(df)
            else:
                print(f"  [警告] 文件中未找到 baseModel 列。")

        except Exception as e:
            print(f"  [异常] {e}")

    # --- 2. 合并与计算 ---
    if not all_base_models:
        print("\n[结果] 没有提取到任何数据。")
        return

    print("\n正在聚合数据并计算占比...")
    
    # 将所有列表合并成一个巨大的 Series
    full_series = pd.concat(all_base_models)
    total_count = len(full_series)
    
    # value_counts() 自动统计频次，默认按数量降序排列
    stats = full_series.value_counts()
    
    # --- 3. 输出结果到 TXT ---
    with open(output_txt_path, "w", encoding="utf-8") as f:
        title = f"=== Base Model 统计报告 ===\n"
        time_info = f"生成时间: {pd.Timestamp.now()}\n"
        total_info = f"总样本数 (Total Samples): {total_count}\n"
        line = "-" * 50 + "\n"
        header = f"{'排名':<5} | {'Base Model 名称':<35} | {'数量':<8} | {'占比':<8}\n"
        
        # 写入头部
        f.write(title)
        f.write(time_info)
        f.write(total_info)
        f.write(line)
        f.write(header)
        f.write(line)
        
        print(f"\n写入报告中...")
        print(f"总样本数: {total_count}")
        print("-" * 30)

        # 循环写入每一行
        rank = 1
        for name, count in stats.items():
            percentage = (count / total_count) * 100
            
            # 格式化字符串
            # <5 左对齐占5位, <35 左对齐占35位... .2f 保留两位小数
            row_str = f"{rank:<5} | {name:<35} | {count:<8} | {percentage:.2f}%\n"
            f.write(row_str)
            
            # 在控制台也打印前 10 名预览
            if rank <= 10:
                print(f"Top {rank}: {name} - {count} ({percentage:.2f}%)")
            
            rank += 1
            
        f.write(line)
        f.write("End of Report\n")

    print("-" * 30)
    print(f"统计完成！完整结果已保存至: {output_txt_path}")

if __name__ == "__main__":
    generate_statistics()