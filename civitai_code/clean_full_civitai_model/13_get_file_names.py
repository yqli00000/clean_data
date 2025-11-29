import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待分析的 CSV 文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\base_valid\table-1_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-2_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-3_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-4_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-5_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-6_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-7_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-1_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-2_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-3_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-4_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-5_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-6_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-7_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv"
]

# 2. 存储文件名的列名 (通常是 filename)
target_col = "filename"

# 3. 结果保存路径
output_report_path = "file_extension_statistics.txt"

# ===========================================

def analyze_file_extensions():
    print(f"=== 开始统计文件后缀类型 ===\n")
    
    all_extensions = []
    total_files_scanned = 0

    # --- 1. 循环读取 ---
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"读取文件: {os.path.basename(file_path)}")
        
        try:
            # 智能编码读取
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

            if target_col in df.columns:
                # 提取文件名列，去空，去空格
                file_series = df[target_col].dropna().astype(str).str.strip()
                
                # 过滤掉空字符串
                file_series = file_series[file_series != ""]
                
                # --- 核心逻辑：提取后缀 ---
                # os.path.splitext('image.png') 返回 ('image', '.png')
                # 取 [1] 即为后缀，然后转小写 (.PNG -> .png)
                # 如果文件名没有后缀 (比如 "image_01")，splitext 会返回空字符串
                exts = file_series.apply(lambda x: os.path.splitext(x)[1].lower())
                
                all_extensions.extend(exts.tolist())
                total_files_scanned += len(file_series)
            else:
                print(f"  [警告] 未找到列: {target_col}")

        except Exception as e:
            print(f"  [异常] {e}")

    # --- 2. 统计分析 ---
    if not all_extensions:
        print("未提取到任何数据。")
        return

    print("\n正在计算统计数据...")
    
    # 转换为 Series 进行统计
    ext_series = pd.Series(all_extensions)
    
    # 将空白后缀（即没有后缀的文件）标记为 "(无后缀)" 方便查看
    ext_series = ext_series.replace("", "(无后缀/No Extension)")
    
    stats = ext_series.value_counts()
    total_count = len(ext_series)

    # --- 3. 生成报告 ---
    with open(output_report_path, "w", encoding="utf-8") as f:
        f.write("=== 图片文件类型统计报告 ===\n")
        f.write(f"扫描文件总数: {total_count}\n")
        f.write("="*50 + "\n")
        f.write(f"{'排名':<5} | {'文件后缀 (Extension)':<25} | {'数量':<8} | {'占比':<8}\n")
        f.write("-" * 50 + "\n")
        
        rank = 1
        for ext, count in stats.items():
            percentage = (count / total_count) * 100
            
            f.write(f"{rank:<5} | {ext:<25} | {count:<8} | {percentage:.2f}%\n")
            
            # 控制台打印前5名
            if rank <= 5:
                print(f"Top {rank}: {ext} - {count} ({percentage:.2f}%)")
            
            rank += 1
            
        f.write("="*50 + "\n")

    print(f"\n统计完成！结果已保存至: {output_report_path}")

if __name__ == "__main__":
    analyze_file_extensions()