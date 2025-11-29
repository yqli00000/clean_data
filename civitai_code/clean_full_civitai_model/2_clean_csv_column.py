import pandas as pd
import os

# ================= 配置区域 =================
csv_files_list = [
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-1_filtered.csv"
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-2_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-3_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-4_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-5_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-6_filtered.csv",
    r"F:\civitai_full_dataset\tables\nsfw_column_filtered\table-7_filtered.csv"
]

# 2. 这里填入你【想要删除】的列名 (区分大小写)
columns_to_drop = [
    "type", 
    "mimetype", 
    "ref_model_version_created_at", 
    "ref_model_version_published_at",
    "post_id",
    "post_title",
    "index",
    "user_id",
    "user_name",
    "preview_url",
    "collected_count",
    "comment_count",
    "cry_count",
    "dislike_count",
    "heart_count",
    "laugh_count",
    "like_count",
    "tipped_amount",
    "view_count",
    "tag_ids",
    "published_at",
    "created_at",
    "file_size"
]

# 3. 是否覆盖原文件？
# True = 直接修改原文件 (风险较高，建议先备份)
# False = 生成新文件 (例如 file.csv -> file_cleaned.csv)
overwrite_original = True 

# ===========================================

def remove_columns_from_csvs():
    print(f"准备处理 {len(csv_files_list)} 个文件...")
    
    for file_path in csv_files_list:
        if not os.path.exists(file_path):
            print(f"[跳过] 文件不存在: {file_path}")
            continue
            
        print(f"\n正在处理: {os.path.basename(file_path)}")
        
        try:
            # 1. 读取 CSV
            # dtype=str 建议加上，防止误把 ID 当数字读取，保持数据原样
            # df = pd.read_csv(file_path, dtype=str)
            try:
                df = pd.read_csv(file_path, dtype=str, encoding='utf-8')
            except UnicodeDecodeError:
                print(f"  -> UTF-8 读取失败，尝试 GB18030 (兼容中文)...")
                try:
                    # 尝试方法 2: GB18030 (涵盖 GBK，解决 Excel 保存的中文乱码)
                    df = pd.read_csv(file_path, dtype=str, encoding='gb18030')
                except UnicodeDecodeError:
                    print(f"  -> GB18030 读取失败，尝试 Latin1 (暴力读取)...")
                    # 尝试方法 3: Latin1 (绝招，不会报错，但特殊字符可能会乱码)
                    df = pd.read_csv(file_path, dtype=str, encoding='latin1')
            # 2. 删除列
            # errors='ignore' 的作用是：如果某个文件里本来就没有这一列，不会报错，直接忽略
            df.drop(columns=columns_to_drop, axis=1, errors='ignore', inplace=True)
            
            # 3. 保存文件
            if overwrite_original:
                save_path = file_path
            else:
                base, ext = os.path.splitext(file_path)
                save_path = f"{base}_cleaned{ext}"
            
            # index=False 表示不要把 pandas 自带的行号写入 CSV
            df.to_csv(save_path, index=False, encoding='utf-8-sig')
            
            print(f"  -> 处理完成，已保存至: {save_path}")
            
        except Exception as e:
            print(f"  [错误] 处理失败: {e}")

if __name__ == "__main__":
    # 确保已安装 pandas: pip install pandas
    remove_columns_from_csvs()