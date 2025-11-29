import os
import sys

# --- 配置 ---

# 1. 您要拆分的源文件
INPUT_FILE_PATH = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl"

# 2. 拆分后，第一部分保存的路径
OUTPUT_FILE_1_PATH = r"F:\civitai_new_fetch\summary\merged_only_images_part1.jsonl"

# 3. 拆分后，第二部分保存的路径
OUTPUT_FILE_2_PATH = r"F:\civitai_new_fetch\summary\merged_only_images_part2.jsonl"

# --- 脚本 ---

def split_jsonl_file(input_file, output_file1, output_file2):
    """
    将一个 .jsonl 文件均分为两个。
    """
    
    # 1. 检查输入文件是否存在
    if not os.path.exists(input_file):
        print(f"错误: 找不到输入文件: {input_file}")
        sys.exit(1)

    # --- 步骤 1: 计算总行数 ---
    print(f"正在计算 {input_file} 的总行数...")
    total_lines = 0
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for _ in f:
                total_lines += 1
    except Exception as e:
        print(f"读取文件时出错: {e}")
        sys.exit(1)
        
    if total_lines == 0:
        print("文件是空的，无需拆分。")
        return

    print(f"文件总行数: {total_lines}")

    # --- 步骤 2: 计算拆分点 ---
    # (total_lines + 1) // 2 确保在总行数为奇数时，
    # 第一个文件会多拿一行 (例如: 11 行会拆分为 6 行 和 5 行)
    split_point = (total_lines + 1) // 2
    lines_in_file2 = total_lines - split_point
    
    print(f"拆分点: 第 {split_point} 行")
    print(f" -> {output_file1} 将包含 {split_point} 行")
    print(f" -> {output_file2} 将包含 {lines_in_file2} 行")

    # --- 步骤 3: 再次读取并写入文件 ---
    print("\n正在开始拆分...")
    current_line_number = 0
    lines_written_1 = 0
    lines_written_2 = 0
    
    try:
        # 同时打开三个文件
        with open(input_file, 'r', encoding='utf-8') as f_in, \
             open(output_file1, 'w', encoding='utf-8') as f_out1, \
             open(output_file2, 'w', encoding='utf-8') as f_out2:
            
            for line in f_in:
                current_line_number += 1
                
                # 判断当前行应该写入哪个文件
                if current_line_number <= split_point:
                    f_out1.write(line)
                    lines_written_1 += 1
                else:
                    f_out2.write(line)
                    lines_written_2 += 1
        
        print("\n========= 拆分完成 ==========")
        print(f"成功写入 {lines_written_1} 行到 {output_file1}")
        print(f"成功写入 {lines_written_2} 行到 {output_file2}")

    except IOError as e:
        print(f"读写文件时发生错误: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")

# --- 执行拆分脚本 ---
if __name__ == "__main__":
    split_jsonl_file(INPUT_FILE_PATH, OUTPUT_FILE_1_PATH, OUTPUT_FILE_2_PATH)