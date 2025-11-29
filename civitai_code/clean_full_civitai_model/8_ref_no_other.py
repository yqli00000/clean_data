import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待处理的文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\no_other\table-1_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-2_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-3_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-4_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-5_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-6_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-7_filtered_no_empty_models_checkpoints_only_no_other.csv"
]

# 2. 列名设置 (脚本会自动匹配 CSV 中实际存在的列名)
col_model_name_candidates = ['model_name']
col_ref_name_candidates = ['ref_model_name']
col_ref_base_candidates = ['ref_base_model']

# 3. 要剔除的关键词
invalid_base_value = "other"

# ===========================================

def filter_ref_models_clean():
    # 全局统计变量
    g_original = 0
    g_kept = 0
    g_excluded_structure = 0 # 因不满足(无model_name且有ref_name)而被剔除的
    g_excluded_base_other = 0 # 因 ref_base_model 为 Other 而被剔除的

    print(f"=== 开始筛选: 无 model_name 但有 ref_model_name (且 Base != Other) ===\n")

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

            original_count = len(df)
            g_original += original_count

            # --- 2. 自动匹配列名 ---
            col_model = next((c for c in col_model_name_candidates if c in df.columns), None)
            col_ref_name = next((c for c in col_ref_name_candidates if c in df.columns), None)
            col_ref_base = next((c for c in col_ref_base_candidates if c in df.columns), None)

            if not col_model or not col_ref_name:
                print(f"  [跳过] 缺少必要的列 (需要 model_name 和 ref_model_name)。")
                continue
            
            # --- 3. 构建筛选掩码 ---
            
            # (1) model_name 必须为空
            # 逻辑：取反(~)有值 = 无值
            is_model_name_empty = ~df[col_model].fillna('').str.strip().astype(bool)
            
            # (2) ref_model_name 必须有值
            has_ref_model_name = df[col_ref_name].fillna('').str.strip().astype(bool)
            
            # 组合前两个条件：这是我们要关注的“目标群体”
            target_group_mask = is_model_name_empty & has_ref_model_name
            
            # (3) ref_base_model 不是 Other
            if col_ref_base:
                # 如果有这一列，检查不是 Other (空值不算 Other，保留)
                is_ref_base_valid = df[col_ref_base].fillna('').str.strip().str.lower() != invalid_base_value
            else:
                # 如果没这一列，默认有效
                is_ref_base_valid = pd.Series([True] * len(df))

            # --- 4. 应用最终逻辑 ---
            # 最终保留 = (目标群体) AND (Base有效)
            final_mask = target_group_mask & is_ref_base_valid
            
            df_clean = df[final_mask]
            kept_count = len(df_clean)
            
            # --- 统计细节 ---
            # 1. 第一层过滤掉的 (不属于我们要找的结构，即: 有model_name 或者 无ref_name 的)
            excluded_structure_count = (~target_group_mask).sum()
            
            # 2. 第二层过滤掉的 (结构对，但 Base 是 Other)
            # 逻辑：属于目标群体 AND Base是Other
            excluded_base_other_count = (target_group_mask & (~is_ref_base_valid)).sum()

            # 更新全局
            g_excluded_structure += excluded_structure_count
            g_excluded_base_other += excluded_base_other_count
            g_kept += kept_count

            print(f"  -> 原始数量: {original_count}")
            print(f"  -> 剔除 (结构不符): {excluded_structure_count} (即:有主模型名 或 无参考名)")
            print(f"  -> 剔除 (Base为Other): {excluded_base_other_count}")
            print(f"  -> 最终保留: {kept_count}")

            # --- 5. 保存文件 ---
            if kept_count > 0:
                base, ext = os.path.splitext(file_path)
                # 命名建议：ref_only 指仅有参考信息
                save_path = f"{base}_ref_valid{ext}"
                
                df_clean.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"  -> 已保存至: {save_path}")
            else:
                print("  -> 筛选结果为空，不生成文件。")

        except Exception as e:
            print(f"  [异常] {e}")

    # === 全局总结 ===
    print(f"\n" + "="*40)
    print(f"          全 局 统 计 报 告          ")
    print(f"="*40)
    print(f"原始总数: {g_original}")
    print(f"----------------------------------------")
    print(f"剔除 - 结构不符:       {g_excluded_structure}")
    print(f"剔除 - Base为Other:    {g_excluded_base_other}")
    print(f"----------------------------------------")
    print(f"最终保留 (Result):     {g_kept}")
    print(f"="*40)

if __name__ == "__main__":
    filter_ref_models_clean()