import os
import glob
import pandas as pd

def collect_qualified_filenames(folder_path, nsfw_check_list):
    """
    遍历文件夹中的所有CSV文件，根据 nsfw_check_list 筛选 nsfw_level，
    并统计所有符合要求的 filename。
    
    参数:
    folder_path (str): 要搜索的文件夹路径。
    nsfw_check_list (list): 要筛选的 nsfw_level 值的列表。
    
    返回:
    list: 包含所有符合要求的文件名的列表（已去重）。
    """
    
    # 使用 set 来自动处理重复的文件名
    qualified_filenames = set()
    
    # 构造搜索路径
    search_pattern = os.path.join(folder_path, '*.csv')
    csv_files = glob.glob(search_pattern)
    
    if not csv_files:
        print(f"在 '{folder_path}' 文件夹中没有找到任何 .csv 文件。")
        return []

    print(f"开始处理 '{folder_path}' 中的 {len(csv_files)} 个 CSV 文件...")

    # 遍历所有找到的 CSV 文件
    for csv_filepath in csv_files:
        filename_only = os.path.basename(csv_filepath)
        try:
            df = pd.read_csv(csv_filepath, on_bad_lines='skip', low_memory=False)
            
            if 'nsfw_level' not in df.columns:
                print(f"警告：跳过 '{filename_only}'，缺少 'nsfw_level' 列。")
                continue
            if 'filename' not in df.columns:
                print(f"警告：跳过 '{filename_only}'，缺少 'filename' 列。")
                continue
                
            # 1. 使用 .isin() 筛选出 'nsfw_level' 在 nsfw_check_list 中的所有行
            df_filtered = df[df['nsfw_level'].isin(nsfw_check_list)]
            
            # 2. 如果找到了符合条件的行
            if not df_filtered.empty:
                # 3. 提取这些行中的 'filename' 列，并去除空值和重复项
                filenames_in_file = df_filtered['filename'].dropna().unique()
                
                # 4. 将找到的文件名添加到总集合中
                qualified_filenames.update(filenames_in_file)
        
        except pd.errors.EmptyDataError:
            print(f"警告：跳过 '{filename_only}'，文件为空。")
        except Exception as e:
            print(f"处理 '{filename_only}' 时发生错误: {e}")

    print(f"处理完毕。总共找到 {len(qualified_filenames)} 个符合要求的文件名。")
    return list(qualified_filenames)

# --- --- ---
# --- 使用示例 ---
# --- --- ---
if __name__ == "__main__":
    
    FOLDER_PATH = r'F:\civitai_full_dataset\tables'
    NSFW_CHECK_LIST = [2]
    all_qualified_files = collect_qualified_filenames(FOLDER_PATH, NSFW_CHECK_LIST)

    if all_qualified_files:
        output_filename = r"civitai_code\qualified_nsfw_filenames_2.txt"
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                for f_name in all_qualified_files:
                    f.write(f"{f_name}\n")
            print(f"\n成功：已将所有 {len(all_qualified_files)} 个文件名保存到 '{output_filename}'")
        
        except Exception as e:
            print(f"\n错误：保存文件到 '{output_filename}' 时失败: {e}")
    else:
        print("\n未找到任何符合要求的文件名。")