import pandas as pd
import os

# --- 配置区域 ---
# 1. 在这里输入你的两个文件夹路径
folder_paths = [
    r'F:\civitai_full_dataset\tables\clean_ext\base',  # 替换为第一个文件夹路径
    r'F:\civitai_full_dataset\tables\clean_ext\ref'   # 替换为第二个文件夹路径
]

# 2. 输出 txt 文件的保存路径和名称
output_file = r'F:\AI_generated_DB-main\civitai_code\clean_full_civitai_model\extracted_filenames.txt'

# ---------------

def extract_filenames_to_txt(folders, output_path):
    all_filenames = []
    
    print("开始处理...")

    for folder in folders:
        # 检查文件夹是否存在
        if not os.path.exists(folder):
            print(f"警告: 文件夹不存在 - {folder}")
            continue
            
        # 遍历文件夹中的文件
        for file in os.listdir(folder):
            if file.endswith('.csv'):
                file_path = os.path.join(folder, file)
                
                try:
                    # 读取CSV文件
                    # encoding='utf-8' 是通用编码，如果报错乱码，尝试改成 'gbk'
                    df = pd.read_csv(file_path, encoding='utf-8')
                    
                    # 检查 'filename' 列是否存在
                    if 'filename' in df.columns:
                        # 提取该列数据并转换为列表，过滤掉空值
                        filenames = df['filename'].dropna().astype(str).tolist()
                        all_filenames.extend(filenames)
                        print(f"已读取: {file} - 提取了 {len(filenames)} 条数据")
                    else:
                        print(f"跳过: {file} - 未找到 'filename' 列")
                        
                except Exception as e:
                    print(f"错误: 无法读取 {file}. 原因: {e}")

    # 将结果写入 txt 文件
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for name in all_filenames:
                f.write(f"{name}\n")
        print(f"\n成功! 共提取 {len(all_filenames)} 条数据。")
        print(f"文件已保存至: {os.path.abspath(output_path)}")
        
    except Exception as e:
        print(f"写入TXT文件失败: {e}")

if __name__ == '__main__':
    extract_filenames_to_txt(folder_paths, output_file)