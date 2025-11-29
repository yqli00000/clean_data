"""
脚本功能：剔除 CSV 文件中 Base Model 列为 Other 的行数据，并保存为新文件。
"""
import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待处理的 CSV 文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-1_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-2_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-3_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-4_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-5_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-6_filtered_no_empty_models_checkpoints_only.csv",
    r"F:\civitai_full_dataset\tables\checkpoints_only\table-7_filtered_no_empty_models_checkpoints_only.csv",
]

# 2. 定义要对比的两组列名 (会自动匹配存在的那个)
# 第一组：主要底模列
col_1_candidates = ['base_model']
# 第二组：参考底模列
col_2_candidates = ['ref_base_model']

# 3. 要删除的关键词
target_value = "other"

# ===========================================

def clean_double_other_logic():
    # 全局计数器
    global_deleted = 0
    global_remaining = 0
    global_original = 0

    print(f"=== 开始执行 '双重 Other' 清洗逻辑 ===")
    print(f"只有当两列同时为 '{target_value}' 时才删除行。\n")
    
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"正在处理: {os.path.basename(file_path)}")

        try:
            # --- 1. 读取文件 ---
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

            current_total = len(df)
            global_original += current_total

            # --- 2. 确定当前文件使用哪两个列名 ---
            # 从候选列表中找到当前 CSV 实际拥有的列
            col1 = next((c for c in col_1_candidates if c in df.columns), None)
            col2 = next((c for c in col_2_candidates if c in df.columns), None)

            if not col1 or not col2:
                print(f"  [警告] 无法凑齐两列进行对比。")
                print(f"  找到的列: Col1={col1}, Col2={col2}")
                print(f"  跳过此文件（不满足“同时出现”的条件）。")
                global_remaining += current_total
                continue

            # --- 3. 构建筛选逻辑 (AND 关系) ---
            # 判断 Col1 是否为 Other
            # .fillna('') 将空值填为字符串，防止报错
            is_col1_other = df[col1].fillna('').str.strip().str.lower() == target_value
            
            # 判断 Col2 是否为 Other
            is_col2_other = df[col2].fillna('').str.strip().str.lower() == target_value
            
            # 核心逻辑：只有两个都是 True 时，to_delete 才是 True
            mask_delete = is_col1_other & is_col2_other
            
            # --- 4. 执行删除与统计 ---
            # rows_to_delete = df[mask_delete]
            df_clean = df[~mask_delete] # 取反，保留不需要删除的

            deleted_count = mask_delete.sum() # True 的数量
            remaining_count = len(df_clean)

            # 更新全局计数
            global_deleted += deleted_count
            global_remaining += remaining_count
            
            print(f"  -> 检测列: [{col1}] & [{col2}]")
            print(f"  -> 剔除数量: {deleted_count} (两列全为 Other)")
            print(f"  -> 保留数量: {remaining_count}")

            # --- 5. 保存文件 ---
            if deleted_count > 0:
                base, ext = os.path.splitext(file_path)
                save_path = f"{base}_no_other{ext}"
                
                df_clean.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"  -> 已保存至: {save_path}")
            else:
                print("  -> 没有发现需删除的数据。")
                
        except Exception as e:
            print(f"  [异常] {e}")
            # 如果出错，为了数据安全，假设全部保留
            global_remaining += (current_total if 'current_total' in locals() else 0)

    # === 最终统计报告 ===
    print(f"\n" + "="*30)
    print(f"      全 局 统 计 报 告      ")
    print(f"="*30)
    print(f"处理文件总数: {len(csv_files_list)}")
    print(f"原始数据总量: {global_original}")
    print(f"------------------------------")
    print(f"总计删除 (Deleted):   {global_deleted}")
    print(f"总计剩余 (Remaining): {global_remaining}")
    print(f"="*30)

if __name__ == "__main__":
    clean_double_other_logic()