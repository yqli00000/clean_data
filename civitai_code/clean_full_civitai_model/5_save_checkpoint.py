"""
这份文件是将ref_model_type或者model_type为checkpoint的数据保留
"""
import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待处理的 CSV 文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\no_empty_models\table-1_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-2_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-3_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-4_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-5_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-6_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-7_filtered_no_empty_models.csv"
]

# 2. 需要检查的列名
target_columns = ['ref_model_type', 'model_type']

# 3. 目标值 (会自动忽略大小写，匹配 'checkpoint', 'Checkpoint', 'CHECKPOINT')
target_value = 'checkpoint'

# ===========================================

def filter_only_checkpoints():
    print(f"=== 开始筛选 Checkpoint 类型数据 ===")
    
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"\n正在处理: {os.path.basename(file_path)}")

        try:
            # --- 1. 智能编码读取 ---
            df = None
            for enc in ['utf-8', 'gb18030', 'latin1']:
                try:
                    df = pd.read_csv(file_path, dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                print("  [错误] 无法读取文件。")
                continue
            
            original_count = len(df)
            
            # --- 2. 构建筛选逻辑 (OR 关系) ---
            # 初始化一个全为 False 的掩码 (Mask)，长度等于行数
            final_mask = pd.Series([False] * len(df))
            
            columns_found = False
            
            for col in target_columns:
                if col in df.columns:
                    columns_found = True
                    # 逻辑：当前列转为字符串 -> 去除首尾空格 -> 转小写 -> 对比是否等于 'checkpoint'
                    # 使用 | (OR) 运算，累加符合条件的行
                    # na=False 表示如果这一格是空的，就视为不匹配
                    current_col_mask = df[col].str.strip().str.lower() == target_value
                    final_mask = final_mask | current_col_mask
            
            if not columns_found:
                print(f"  [警告] 该文件中不存在 {target_columns} 中的任何一列！跳过筛选。")
                continue

            # --- 3. 应用筛选 ---
            df_filtered = df[final_mask]
            filtered_count = len(df_filtered)
            deleted_count = original_count - filtered_count
            
            print(f"  -> 原始数量: {original_count}")
            print(f"  -> 保留数量: {filtered_count} (Type 为 Checkpoint)")
            print(f"  -> 剔除数量: {deleted_count}")

            # --- 4. 保存结果 ---
            if filtered_count > 0:
                base, ext = os.path.splitext(file_path)
                # 生成新文件名，例如 data_checkpoints_only.csv
                save_path = f"{base}_checkpoints_only{ext}"
                
                df_filtered.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"  -> 已保存至: {save_path}")
            else:
                print("  -> [警告] 筛选后没有任何数据剩余（可能文件中全是 LORA 或其他类型）。")

        except Exception as e:
            print(f"  [异常] 处理失败: {e}")

if __name__ == "__main__":
    filter_only_checkpoints()