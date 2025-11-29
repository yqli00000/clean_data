import os
import glob
import pandas as pd

target_dir = "F:\\civitai_full_dataset\\tables"

def find_latest_created_at(folder_path):
    """
    遍历指定文件夹中的所有 CSV 文件，
    统计每个文件中 'created_at' 列的最新日期。
    """
    
    # 1. 构造文件搜索路径，匹配所有 .csv 文件
    search_pattern = os.path.join(folder_path, '*.csv')
    csv_files = glob.glob(search_pattern)

    if not csv_files:
        print(f"在 '{folder_path}' 文件夹中没有找到任何 .csv 文件。")
        return

    print(f"开始处理 '{folder_path}' 中的 {len(csv_files)} 个 CSV 文件...\n")
    
    # 2. 准备一个字典来存储结果
    results = {}

    # 3. 遍历找到的每个 CSV 文件
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        
        try:
            # 4. 读取 CSV 文件
            # 使用 on_bad_lines='skip' 来跳过格式错误的行
            df = pd.read_csv(filepath, on_bad_lines='skip')

            # 5. 检查 'created_at' 列是否存在
            if 'created_at' not in df.columns:
                results[filename] = "错误：找不到 'created_at' 列"
                continue

            # 6. 检查 'created_at' 列是否完全为空
            if df['created_at'].isnull().all():
                results[filename] = "警告：'created_at' 列存在，但全为空值"
                continue

            # 7. 将 'created_at' 列转换为日期时间对象
            # errors='coerce' 会将无法解析的日期变成 NaT (Not a Time)
            # 这可以防止因单个错误日期格式而导致整个脚本失败
            dates = pd.to_datetime(df['created_at'], unit='s', errors='coerce')

            # 8. 找到最大（即最近）的日期，.max() 会自动忽略 NaT
            max_date = dates.max()

            # 9. 存储结果
            if pd.isna(max_date):
                results[filename] = "警告：'created_at' 列中没有找到有效的日期格式"
            else:
                # 将 Timestamp 对象转换为易读的字符串
                results[filename] = max_date.strftime('%Y-%m-%d %H:%M:%S')

        except pd.errors.EmptyDataError:
            results[filename] = "错误：文件为空"
        except Exception as e:
            results[filename] = f"处理时发生未知错误: {e}"

    # 10. 打印所有结果
    print("--- 统计结果 ---")
    if not results:
        print("没有文件被处理。")
    else:
        # 格式化输出，使其对齐
        max_len = max(len(f) for f in results.keys()) + 2
        with open("civitai_code\\output_created_at.txt", "w", encoding="utf-8") as f:
            for filename, result in results.items():
                print(f"文件: {filename:<{max_len}} | 最近日期: {result}")
                f.write(f"文件: {filename:<{max_len}} | 最近日期: {result}\n")
    
    print("\n--- 处理完毕 ---")

def read_created_at_from_file(file_name):
    """
    从指定的文本文件中读取 'created_at' 统计结果并打印。
    """
    df = pd.read_csv(file_name)
    # 查看每一列的名称
    df_columns = df.columns.tolist()
    print("列名称:", df_columns)
    # 打印第一行数据
    print("第一行数据:", df.iloc[0])
    dates = pd.to_datetime(df['created_at'], unit='s', errors='coerce')
    print(dates.head(5))
def check_base_model(file_path, check_list):
    """
    检查给定文件路径中的模型文件是否包含基础模型信息。
    """
    try:
        for file_name in glob.glob(os.path.join(file_path, '*.csv')):
            df = pd.read_csv(file_name, on_bad_lines='skip')
            
            if 'base_model' not in df.columns:
                print(f"错误：在 '{file_name}' 中找不到 'base_model' 列。")
                return
            
            base_models = df['base_model'].dropna().unique()
            
            print(f"在 '{file_name}' 中找到以下基础模型:")
            with open("civitai_code\\output_base_models.txt", "a", encoding="utf-8") as f:
                f.write(f"文件：{file_name}下的base_model有:\n")
                for model in base_models:
                    print(f" - {model}")
                    f.write(f" - {model}\n")
        
    except Exception as e:
        print(f"处理文件 '{file_path}' 时发生错误: {e}")


def check_model_name(file_path, check_list):
    """
    检查给定文件路径中的模型文件是否包含模型name
    """
    try:
        for file_name in glob.glob(os.path.join(file_path, '*.csv')):
            df = pd.read_csv(file_name, on_bad_lines='skip')
            
            if 'model_name' not in df.columns:
                print(f"错误：在 '{file_name}' 中找不到 'base_model' 列。")
                return
            
            model_names = df['model_name'].dropna().unique()
            
            print(f"在 '{file_name}' 中找到以下模型:")
            with open("civitai_code\\output_model_names.txt", "a", encoding="utf-8") as f:
                f.write(f"文件：{file_name}下的model_name有:\n")
                for model in model_names:
                    print(f" - {model}")
                    f.write(f" - {model}\n")
        
    except Exception as e:
        print(f"处理文件 '{file_path}' 时发生错误: {e}")
def check_ref_model_name(file_path, check_list):
    """
    检查给定文件路径中的模型文件是否包含模型name
    """
    try:
        for file_name in glob.glob(os.path.join(file_path, '*.csv')):
            df = pd.read_csv(file_name, on_bad_lines='skip')
            
            if 'ref_model_name' not in df.columns:
                print(f"错误：在 '{file_name}' 中找不到 'ref_model_name' 列。")
                return
            
            ref_model_names = df['ref_model_name'].dropna().unique()
            
            print(f"在 '{file_name}' 中找到以下模型:")
            with open("civitai_code\\output_ref_model_names.txt", "a", encoding="utf-8") as f:
                f.write(f"文件：{file_name}下的ref_model_name有:\n")
                for model in ref_model_names:
                    print(f" - {model}")
                    f.write(f" - {model}\n")
        
    except Exception as e:
        print(f"处理文件 '{file_path}' 时发生错误: {e}")
if __name__ == "__main__":
    # find_latest_created_at(target_dir)
    # read_created_at_from_file("F:\\civitai_full_dataset\\tables\\table-1.csv")
    # check_base_model("F:\\civitai_full_dataset\\tables", None)
    # check_model_name("F:\\civitai_full_dataset\\tables", None)
    check_ref_model_name("F:\\civitai_full_dataset\\tables", None)
