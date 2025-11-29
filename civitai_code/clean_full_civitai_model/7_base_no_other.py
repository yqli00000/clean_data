"""
筛选出含有model_name信息的数据，并且针对这部分数据，只保留base_model不是other的数据
"""
import pandas as pd
import os

# ================= 配置区域 =================

# 1. 文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\no_other\table-1_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-2_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-3_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-4_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-5_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-6_filtered_no_empty_models_checkpoints_only_no_other.csv",
    r"F:\civitai_full_dataset\tables\no_other\table-7_filtered_no_empty_models_checkpoints_only_no_other.csv"
]

# 2. 候选列名设置 (脚本会自动匹配存在的列)
# 只要找到其中一个，就认为是该属性的列
model_name_cols = ['model_name'] 
base_model_cols = ['base_model']

# 3. 要排除的底模关键词
invalid_base = "other"

# ===========================================

def filter_valid_model_names_and_clean_base():
    global_original = 0
    global_kept = 0
    global_dropped_no_name = 0
    global_dropped_is_other = 0

    print(f"=== 开始筛选任务 ===")
    print(f"1. 筛选出含有 model_name 的数据")
    print(f"2. 剔除其中 base_model 为 '{invalid_base}' 的数据\n")

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
            global_original += original_count

            # --- 2. 识别列名 ---
            # 找到实际存在的 model_name 列
            col_model = next((c for c in model_name_cols if c in df.columns), None)
            # 找到实际存在的 base_model 列
            col_base = next((c for c in base_model_cols if c in df.columns), None)

            if not col_model:
                print(f"  [警告] 文件中找不到 model_name 列！跳过。")
                continue
            
            # 如果没找到 base_model 列，为了安全起见，假设所有底模都不是 Other (即只执行第一步筛选)
            if not col_base:
                print(f"  [提示] 找不到 base_model 列，将只执行 'model_name 非空' 筛选。")

            # --- 3. 构建筛选逻辑 ---
            
            # 步骤 A: 筛选 model_name 非空
            # fillna('') 把 NaN 转为空字符串，strip() 去除空格，astype(bool) 判断是否有内容
            # 结果：有内容为 True，空为 False
            has_model_name = df[col_model].fillna('').str.strip().astype(bool)
            
            # 步骤 B: 筛选 base_model 不是 Other
            if col_base:
                # 逻辑：(值转小写 != 'other')
                # 注意：如果 base_model 是空值 (NaN)，它 != 'other'，所以会被保留 (True)，这符合需求
                is_not_other = df[col_base].fillna('').str.strip().str.lower() != invalid_base
            else:
                # 如果没有 base_model 列，这一步默认全过
                is_not_other = pd.Series([True] * len(df))

            # --- 4. 组合逻辑与统计 ---
            # 最终保留的 mask：既要有名字 AND 底模不能是 Other
            final_keep_mask = has_model_name & is_not_other
            
            # 统计具体原因 (方便调试)
            # 1. 因为没名字被删的
            dropped_no_name_mask = ~has_model_name
            count_no_name = dropped_no_name_mask.sum()
            
            # 2. 因为是 Other 被删的 (前提是它得先有名字，否则就被算在上面了)
            dropped_other_mask = has_model_name & (~is_not_other)
            count_is_other = dropped_other_mask.sum()

            df_clean = df[final_keep_mask]
            kept_count = len(df_clean)
            
            # 更新全局统计
            global_dropped_no_name += count_no_name
            global_dropped_is_other += count_is_other
            global_kept += kept_count

            print(f"  -> 原始数量: {original_count}")
            print(f"  -> 删除 (无模型名): {count_no_name}")
            print(f"  -> 删除 (底模为Other): {count_is_other}")
            print(f"  -> 最终保留: {kept_count}")

            # --- 5. 保存文件 ---
            if kept_count < original_count:
                base, ext = os.path.splitext(file_path)
                save_path = f"{base}_base_valid{ext}"
                
                df_clean.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"  -> 已保存至: {save_path}")
            else:
                print("  -> 数据未发生变化，无需保存。")

        except Exception as e:
            print(f"  [异常] {e}")

    # === 总结 ===
    print(f"\n" + "="*30)
    print(f"      全 局 统 计      ")
    print(f"="*30)
    print(f"原始总数: {global_original}")
    print(f"因无模型名删除: {global_dropped_no_name}")
    print(f"因底模是Other删除: {global_dropped_is_other}")
    print(f"------------------------------")
    print(f"最终剩余: {global_kept}")
    print(f"="*30)

if __name__ == "__main__":
    filter_valid_model_names_and_clean_base()