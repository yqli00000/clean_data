"""
这份代码用于检查base_model和model_name是否具有存在一致性
"""
import pandas as pd
import os

# ================= 配置区域 =================
csv_files_list = [
    r"F:\civitai_full_dataset\tables\no_empty_models\table-1_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-2_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-3_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-4_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-5_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-6_filtered_no_empty_models.csv",
    r"F:\civitai_full_dataset\tables\no_empty_models\table-7_filtered_no_empty_models.csv"
]

col_base = "base_model"
col_model = "model_name" 

# 是否要把不一致的数据保存下来查看？
save_inconsistent_rows = True
# ===========================================

def analyze_consistency():
    print(f"=== 开始一致性分析 ===")
    print(f"检测列: [{col_base}] 和 [{col_model}]\n")

    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue

        print(f"正在分析文件: {os.path.basename(file_path)}")

        try:
            # 1. 读取文件 (包含编码修复逻辑)
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

            # 检查列是否存在
            if col_base not in df.columns or col_model not in df.columns:
                print(f"  [跳过] 列名不匹配。文件中包含的列: {list(df.columns)}")
                continue

            # 2. 数据预处理：把空格、空字符串统一变成 NaN，方便判断
            # 这一步只是在内存中临时处理，不影响原文件
            df_temp = df.copy()
            df_temp[col_base] = df_temp[col_base].replace(r'^\s*$', pd.NA, regex=True)
            df_temp[col_model] = df_temp[col_model].replace(r'^\s*$', pd.NA, regex=True)

            # 3. 创建布尔掩码 (Mask)
            has_base = df_temp[col_base].notna()   # Base 有值
            has_model = df_temp[col_model].notna() # Model 有值

            # 4. 分类统计
            # 情况 A: 都有 (一致)
            both_exist = df[has_base & has_model]
            
            # 情况 B: 都没有 (一致)
            neither_exist = df[(~has_base) & (~has_model)]
            
            # 情况 C: 只有 Base (不一致)
            base_only = df[has_base & (~has_model)]
            
            # 情况 D: 只有 Model (不一致)
            model_only = df[(~has_base) & has_model]

            # 5. 打印报告
            total = len(df)
            print(f"  -> 总行数: {total}")
            print(f"  ------------------------------------------------")
            print(f"  [一致] 两者都有 (Both Exist):     {len(both_exist)} 条 ({len(both_exist)/total:.1%})")
            print(f"  [一致] 两者都无 (Neither Exist):  {len(neither_exist)} 条 ({len(neither_exist)/total:.1%})")
            print(f"  ------------------------------------------------")
            print(f"  [不一致] 只有 Base (Base Only):   {len(base_only)} 条")
            print(f"  [不一致] 只有 Model (Model Only): {len(model_only)} 条")
            print(f"  ------------------------------------------------")

            # 6. 保存不一致的数据 (可选)
            if save_inconsistent_rows:
                inconsistent_df = pd.concat([base_only, model_only])
                if not inconsistent_df.empty:
                    base, ext = os.path.splitext(file_path)
                    save_path = f"{base}_inconsistent_rows{ext}"
                    inconsistent_df.to_csv(save_path, index=False, encoding='utf-8-sig')
                    print(f"  -> [警告] 发现 {len(inconsistent_df)} 条不一致数据，已单独保存至: \n     {save_path}")
                else:
                    print(f"  -> 完美！没有发现不一致的数据。")
            
            print("\n")

        except Exception as e:
            print(f"  [异常] 分析失败: {e}")

if __name__ == "__main__":
    analyze_consistency()