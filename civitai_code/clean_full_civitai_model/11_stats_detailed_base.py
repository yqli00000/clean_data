import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待分析的文件列表 (建议使用上一步生成的 _valid_names_no_other.csv)
csv_files_list = [
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-1_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-2_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-3_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-4_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-5_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-6_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv",
    r"F:\civitai_full_dataset\tables\clean_ext\base\table-7_filtered_no_empty_models_checkpoints_only_no_other_base_valid_clean_ext.csv"
]

# 2. 结果保存路径
output_report_path = r"civitai_code\clean_full_civitai_model\base_model_to_model_name_report.txt"

# 3. 列名匹配 (脚本会自动匹配)
base_cols = ['base_model']
model_cols = ['model_name']

# 4. 为了报告简洁，每个 Base Model 下只显示前 N 个热门模型 (设为 None 则显示所有)
top_n_per_base = None

# ===========================================

def analyze_hierarchy():
    print(f"=== 开始分析 Base Model -> Model Name 层级关系 ===\n")
    
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
            c_base = next((c for c in base_cols if c in df.columns), None)
            c_model = next((c for c in model_cols if c in df.columns), None)

            if c_base and c_model:
                # 提取两列数据
                subset = df[[c_base, c_model]].copy()
                subset.columns = ['base_model', 'model_name'] # 统一列名方便合并
                
                # 清洗：去空值、去空格
                subset = subset.dropna()
                subset['base_model'] = subset['base_model'].str.strip()
                subset['model_name'] = subset['model_name'].str.strip()
                
                # 过滤空字符串
                subset = subset[(subset['base_model'] != "") & (subset['model_name'] != "")]
                
                all_data.append(subset)
            else:
                print(f"  [警告] 缺少必要的列。")

        except Exception as e:
            print(f"  [异常] {e}")

    if not all_data:
        print("没有有效数据。")
        return

    print("\n正在聚合计算...")
    full_df = pd.concat(all_data)

    # --- 2. 核心统计逻辑 ---
    # 先统计每个 Base Model 的总数，用于排序 Base Model (大类按热度排)
    base_counts = full_df['base_model'].value_counts()

    # --- 3. 写入报告 ---
    with open(output_report_path, "w", encoding="utf-8") as f:
        f.write("=== Base Model 与 Model Name 对应关系报告 ===\n")
        f.write(f"总数据量: {len(full_df)}\n")
        f.write("="*60 + "\n\n")

        # 遍历每一个 Base Model (按数量从多到少)
        for base_name, total_count in base_counts.items():
            # 获取该 Base 下的所有数据
            sub_group = full_df[full_df['base_model'] == base_name]
            
            # 统计该组内 model_name 的分布
            model_counts = sub_group['model_name'].value_counts()
            
            # 写入 Base Model 标题行
            # 格式: [排名 1] SDXL 1.0 (共 5000 条)
            f.write(f"【Base Model: {base_name}】 (Total: {total_count})\n")
            f.write("-" * 60 + "\n")
            
            # 遍历具体的 Model Name
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
            
            f.write("\n") # 空行分隔不同 Base Model

    print(f"统计完成！报告已生成: {output_report_path}")

if __name__ == "__main__":
    analyze_hierarchy()