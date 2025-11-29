import pandas as pd
import os
import glob

whitelist_txt_path = r'F:\AI_generated_DB-main\civitai_code\qualified_nsfw_filenames.txt' 
csv_folder_path = r'F:\civitai_full_dataset\tables\nsfw_column_filtered' 
target_column_name = 'filename' 
overwrite_original = True

def filter_csv_files():
    # 1. 读取白名单
    print(f"正在读取白名单: {whitelist_txt_path} ...")
    try:
        with open(whitelist_txt_path, 'r', encoding='utf-8') as f:
            whitelist = set(line.strip() for line in f if line.strip())
        print(f"-> 白名单共包含 {len(whitelist)} 个文件名")
    except FileNotFoundError:
        print("错误：找不到 txt 文件。")
        return

    csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))
    if not csv_files:
        print("未找到 CSV 文件。")
        return

    # ==== 【新增】全局已处理文件名集合 ====
    # 用于记录在之前的 CSV 中已经保存过的文件名，防止跨文件重复
    global_seen_filenames = set()
    # ===================================

    for csv_file in csv_files:
        print(f"\n正在处理: {os.path.basename(csv_file)} ...")
        
        try:
            # 修复 DtypeWarning: 添加 low_memory=False
            df = pd.read_csv(csv_file, low_memory=False)
            
            if target_column_name not in df.columns:
                print(f"  [跳过] 找不到列: {target_column_name}")
                continue
            
            original_count = len(df)
            
            # 1. 初步筛选：只保留白名单里的
            df_filtered = df[df[target_column_name].astype(str).isin(whitelist)]
            
            # 2. 文件内去重：防止单文件内部自己重复
            # keep='first' 表示保留第一条
            df_filtered = df_filtered.drop_duplicates(subset=[target_column_name], keep='first')

            # 3. ==== 【核心】跨文件去重 ====
            # 筛选出那些 "不在" 全局集合里的行
            # 逻辑：如果这个文件名之前没见过 (False)，就保留；见过 (True) 就由 ~ 取反变成 False (删掉)
            mask = ~df_filtered[target_column_name].astype(str).isin(global_seen_filenames)
            df_final = df_filtered[mask]
            
            # 4. 更新全局集合
            # 把这一轮新保留的文件名，加入到全局记忆中，供下一个 CSV 参考
            current_filenames = set(df_final[target_column_name].astype(str))
            global_seen_filenames.update(current_filenames)
            # ============================
            
            filtered_count = len(df_final)
            
            # 如果筛选后没剩东西，就不保存了（可选）
            if filtered_count == 0:
                print("  -> 筛选/去重后为空，跳过保存。")
                continue

            print(f"  -> 原始: {original_count} | 白名单+文件内去重: {len(df_filtered)} | 跨文件去重后: {filtered_count}")
            print(f"  -> 当前全局已收录唯一数据量: {len(global_seen_filenames)}")

            # 保存文件逻辑
            if overwrite_original:
                save_path = csv_file
            else:
                base, ext = os.path.splitext(csv_file)
                save_path = f"{base}_filtered{ext}"
            
            df_final.to_csv(save_path, index=False, encoding='utf-8-sig')
            print(f"  -> 已保存至: {save_path}")

        except Exception as e:
            print(f"  [错误] 处理文件失败: {e}")

if __name__ == "__main__":
    filter_csv_files()