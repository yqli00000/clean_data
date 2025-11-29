import pandas as pd
import os
import glob

whitelist_txt_path = r'F:\AI_generated_DB-main\civitai_code\qualified_nsfw_filenames.txt' 
csv_folder_path = r'F:\civitai_full_dataset\tables\nsfw_column_filtered' 
target_column_name = 'filename' 
# True = 直接修改原文件 (风险较高)
# False = 生成新文件 (例如 data.csv -> data_filtered.csv) (推荐)
overwrite_original = True


def filter_csv_files():
    # 1. 读取白名单到集合 (Set) 中，Set 的查询速度是 O(1)
    print(f"正在读取白名单: {whitelist_txt_path} ...")
    try:
        with open(whitelist_txt_path, 'r', encoding='utf-8') as f:
            # strip() 去除每行的换行符和空格
            whitelist = set(line.strip() for line in f if line.strip())
        print(f"-> 白名单共包含 {len(whitelist)} 个文件名")
    except FileNotFoundError:
        print("错误：找不到 txt 文件，请检查路径。")
        return

    # 2. 获取文件夹下所有 csv 文件
    # csv_files = glob.glob(os.path.join(csv_folder_path, "*.csv"))
    csv_files = [r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-5_filtered.csv"]
    if not csv_files:
        print(f"在 {csv_folder_path} 下没有找到 CSV 文件。")
        return

    # 3. 循环处理每个 CSV
    for csv_file in csv_files:
        print(f"\n正在处理: {os.path.basename(csv_file)} ...")
        
        try:
            # 读取 CSV
            df = pd.read_csv(csv_file)
            
            # 检查列名是否存在
            if target_column_name not in df.columns:
                print(f"  [跳过] 警告：该文件中找不到列名 '{target_column_name}'，可用的列有: {list(df.columns)}")
                continue
            
            original_count = len(df)
            
            # ==== 核心筛选逻辑 ====
            # 保留那些 target_column_name 在 whitelist 里的行
            # 强制转为 string 对比，防止 CSV 里是数字类型而 txt 里是字符串
            df_filtered = df[df[target_column_name].astype(str).isin(whitelist)]
            # ====================
            # duplicates = df_filtered[target_column_name].duplicated()
            # if duplicates.any():
            #     print(f"  [注意] 发现重复的文件名! 重复数量: {duplicates.sum()}")
            #     # 查看具体是哪些重复了
            #     print(df_filtered[df_filtered[target_column_name].duplicated(keep=False)].head())
            df_filtered = df_filtered.drop_duplicates(subset=[target_column_name], keep='first')
            filtered_count = len(df_filtered)
            print(f"  -> 原始数据: {original_count} 条")
            print(f"  -> 筛选后剩余: {filtered_count} 条 (删除了 {original_count - filtered_count} 条)")

            # 保存文件
            if overwrite_original:
                save_path = csv_file
            else:
                # 比如将 data.csv 保存为 data_filtered.csv
                base, ext = os.path.splitext(csv_file)
                save_path = f"{base}_filtered{ext}"
            
            # index=False 表示不把 pandas 的索引列写入文件
            df_filtered.to_csv(save_path, index=False, encoding='utf-8-sig')
            print(f"  -> 已保存至: {save_path}")

        except Exception as e:
            print(f"  [错误] 处理文件失败: {e}")

if __name__ == "__main__":
    filter_csv_files()
