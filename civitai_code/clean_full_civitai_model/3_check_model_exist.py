"""
这一份代码用来检测给出的数据中一条模型相关信息都没有的数据，并且把要删除的数据filename保存。
共删除了 971996 条无模型信息的记录。
被删除的文件名列表已保存至: delete_no_model_log.txt
"""
import pandas as pd
import os

# ================= 配置区域 =================

# 1. 待处理的 CSV 文件列表
csv_files_list = [
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-1_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-2_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-3_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-4_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-5_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-6_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-7_filtered.csv"
]

# 2. 用来判断是否有模型信息的列名
check_columns = [
    "base_model",
    "model_name",
    "ref_model_name"
]

# 3. 存储被删除文件名的日志文件路径
log_txt_path = "delete_no_model_log.txt"

# ===========================================

def clean_empty_model_rows():
    total_deleted = 0
    
    # 准备写入日志文件（使用 'w' 模式，每次运行会清空旧日志）
    with open(log_txt_path, "w", encoding="utf-8") as log_file:
        log_file.write(f"=== 删除记录日志 (因没有任何模型信息) ===\n")

        for file_path in csv_files_list:
            if not os.path.exists(file_path):
                print(f"[跳过] 文件不存在: {file_path}")
                continue

            print(f"\n正在分析: {os.path.basename(file_path)}")

            try:
                # --- 1. 智能编码读取 (防止编码报错) ---
                df = None
                for enc in ['utf-8', 'gb18030', 'latin1']:
                    try:
                        # dtype=str 保持所有数据为字符串，方便判断空值
                        df = pd.read_csv(file_path, dtype=str, encoding=enc)
                        break
                    except UnicodeDecodeError:
                        continue
                
                if df is None:
                    print("  [错误] 无法读取文件，跳过。")
                    continue
                # -----------------------------------

                original_count = len(df)

                # --- 2. 数据预处理：将纯空格或空字符串转换为 NaN ---
                # 这一步很重要，因为有时候 CSV 里存的是 "" 而不是真正的空值
                # 这一步不会修改原文件，只在内存中操作
                temp_df = df.copy()
                for col in check_columns:
                    if col in temp_df.columns:
                        # 将空白字符替换为 NaN
                        temp_df[col] = temp_df[col].replace(r'^\s*$', pd.NA, regex=True)

                # --- 3. 核心筛选逻辑 ---
                # 找出当前 CSV 中实际存在的列（防止某个 CSV 缺列报错）
                existing_check_cols = [c for c in check_columns if c in df.columns]

                if not existing_check_cols:
                    print("  [警告] 该文件中不存在任何指定的模型列，所有数据可能都将被标记为无效？(请人工核查)")
                    # 这里暂时跳过不处理，避免误删全表
                    continue

                # 判定条件：指定的列 全都是 (all) NaN
                # mask_bad 为 True 的行就是我们要删除的行
                mask_bad = temp_df[existing_check_cols].isna().all(axis=1)
                
                # --- 4. 提取要删除的文件名并写入 TXT ---
                bad_rows = df[mask_bad]
                if not bad_rows.empty:
                    filenames_to_delete = bad_rows['filename'].tolist()
                    
                    # 写入日志
                    log_file.write(f"\n文件: {os.path.basename(file_path)}\n")
                    for fname in filenames_to_delete:
                        # 写入 filename，如果为空则写标记
                        fname_str = str(fname) if pd.notna(fname) else "(无文件名)"
                        log_file.write(f"{fname_str}\n")
                    
                    total_deleted += len(bad_rows)
                
                # --- 5. 保留有效数据并保存 ---
                # 取反 mask_bad，即保留那些“至少有一列有值”的行
                df_clean = df[~mask_bad]
                
                clean_count = len(df_clean)
                print(f"  -> 原始数量: {original_count}")
                print(f"  -> 删除数量: {len(bad_rows)}")
                print(f"  -> 剩余数量: {clean_count}")

                if len(bad_rows) > 0:
                    # 生成新文件名
                    base, ext = os.path.splitext(file_path)
                    save_path = f"{base}_no_empty_models{ext}"
                    
                    df_clean.to_csv(save_path, index=False, encoding='utf-8-sig')
                    print(f"  -> 已保存清洗后的文件: {save_path}")
                else:
                    print("  -> 没有发现完全缺失模型信息的行，无需重新保存。")

            except Exception as e:
                print(f"  [异常] 处理出错: {e}")

    print(f"\n=== 全部完成 ===")
    print(f"共删除了 {total_deleted} 条无模型信息的记录。")
    print(f"被删除的文件名列表已保存至: {log_txt_path}")

if __name__ == "__main__":
    clean_empty_model_rows()