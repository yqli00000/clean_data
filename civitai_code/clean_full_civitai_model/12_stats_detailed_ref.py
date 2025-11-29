import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待分析的文件列表 (建议使用上一步生成的 _ref_only_valid.csv)
csv_files_list = [
   r"F:\civitai_full_dataset\tables\clean_ext\ref\table-1_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-2_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-3_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-4_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-5_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-6_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\ref\table-7_filtered_no_empty_models_checkpoints_only_no_other_ref_valid_clean_ext.csv"
]

# 2. 结果保存路径
output_report_path = r"civitai_code\clean_full_civitai_model\ref_base_to_ref_model_report.txt"

# 3. 列名匹配 (脚本会自动匹配存在的列)
# Ref Base 列候选
ref_base_cols = ['ref_base_model']
# Ref Model Name 列候选
ref_model_cols = ['ref_model_name']

# 4. 每个 Ref Base 下只显示前 N 个热门模型 (设为 None 则显示所有)
top_n_per_base = None

# ===========================================

def analyze_ref_hierarchy():
    print(f"=== 开始分析 Ref Base Model -> Ref Model Name 层级关系 ===\n")
    
    all_data = []

    # --- 1. 读取数据 ---
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"读取文件: {os.path.basename(file_path)}")
        
        try:
            df = None
            for enc in ['utf-8', 'gb18030', 'latin1']:
                try:
                    df = pd.read_csv(file_path, dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None: continue

            # 匹配列名
            c_ref_base = next((c for c in ref_base_cols if c in df.columns), None)
            c_ref_model = next((c for c in ref_model_cols if c in df.columns), None)

            if c_ref_base and c_ref_model:
                # 提取两列数据
                subset = df[[c_ref_base, c_ref_model]].copy()
                subset.columns = ['ref_base', 'ref_model'] # 统一列名方便合并
                
                # 清洗：去空值、去空格
                subset = subset.dropna()
                subset['ref_base'] = subset['ref_base'].str.strip()
                subset['ref_model'] = subset['ref_model'].str.strip()
                
                # 过滤空字符串
                subset = subset[(subset['ref_base'] != "") & (subset['ref_model'] != "")]
                
                all_data.append(subset)
            else:
                print(f"  [警告] 缺少必要的 Ref 列。")

        except Exception as e:
            print(f"  [异常] {e}")

    if not all_data:
        print("没有有效数据。")
        return

    print("\n正在聚合计算...")
    full_df = pd.concat(all_data)

    # --- 2. 核心统计逻辑 ---
    # 先统计每个 Ref Base Model 的总数，用于大类排序
    base_counts = full_df['ref_base'].value_counts()

    # --- 3. 写入报告 ---
    with open(output_report_path, "w", encoding="utf-8") as f:
        f.write("=== Ref Base Model 与 Ref Model Name 对应关系报告 ===\n")
        f.write(f"总数据量: {len(full_df)}\n")
        f.write("="*60 + "\n\n")

        # 遍历每一个 Ref Base (按数量从多到少)
        for base_name, total_count in base_counts.items():
            # 获取该 Ref Base 下的所有数据
            sub_group = full_df[full_df['ref_base'] == base_name]
            
            # 统计该组内 ref_model 的分布
            model_counts = sub_group['ref_model'].value_counts()
            
            # 写入标题行
            f.write(f"【Ref Base: {base_name}】 (Total: {total_count})\n")
            f.write("-" * 60 + "\n")
            
            # 遍历具体的 Ref Model Name
            count_in_loop = 0
            for m_name, m_count in model_counts.items():
                # 计算组内占比
                ratio = (m_count / total_count) * 100
                
                f.write(f"   • {m_name:<40} | {m_count:<6} | {ratio:.2f}%\n")
                
                count_in_loop += 1
                if top_n_per_base and count_in_loop >= top_n_per_base:
                    remaining = len(model_counts) - count_in_loop
                    if remaining > 0:
                        f.write(f"   ... (还有 {remaining} 个模型被省略)\n")
                    break
            
            f.write("\n") # 空行分隔

    print(f"统计完成！报告已生成: {output_report_path}")

if __name__ == "__main__":
    analyze_ref_hierarchy()