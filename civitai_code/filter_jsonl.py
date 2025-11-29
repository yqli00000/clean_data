import json
import os
import sys

# --- 配置 ---

# 1. "文件1": 包含您不想要的记录 (无模型数据的 .jsonl)
#    (请确保这是您统计为“(未指定)”的那个文件)
BAD_RECORDS_FILE = r"F:\civitai_new_fetch\out_of_state_models_v2.jsonl" # 示例路径

# 2. "文件2": 您的主文件 (完整的 .jsonl)
ALL_DATA_FILE = r"F:\civitai_new_fetch\civitai_images.jsonl" # 示例路径

# 3. 输出文件: 将保存清理后结果的新文件
CLEAN_OUTPUT_FILE = r"F:\civitai_new_fetch\civitai_images_cleaned.jsonl"

# --- 脚本 ---

def filter_jsonl_file():
    """
    从 ALL_DATA_FILE 中移除 BAD_RECORDS_FILE 中出现过的所有记录。
    """
    
    # --- 步骤 1: 建立要移除的 ID 黑名单 ---
    
    # 检查“坏”文件是否存在
    if not os.path.exists(BAD_RECORDS_FILE):
        print(f"错误: 找不到“文件1” (坏数据): {BAD_RECORDS_FILE}")
        sys.exit(1)

    # 检查“主”文件是否存在
    if not os.path.exists(ALL_DATA_FILE):
        print(f"错误: 找不到“文件2” (主文件): {ALL_DATA_FILE}")
        sys.exit(1)

    print(f"正在从 {BAD_RECORDS_FILE} (文件1) 中读取要移除的 ID...")
    
    # 使用 set (集合) 来高效存储 ID
    ids_to_remove = set()
    
    try:
        with open(BAD_RECORDS_FILE, 'r', encoding='utf-8') as f_bad:
            for line in f_bad:
                try:
                    data = json.loads(line.strip())
                    # 从 "坏" 记录中获取 ID
                    record_id = data.get("id")
                    if record_id is not None:
                        ids_to_remove.add(record_id)
                except json.JSONDecodeError:
                    print(f"警告: {BAD_RECORDS_FILE} 中有损坏的 JSON 行，已跳过。")

    except IOError as e:
        print(f"错误: 无法读取 {BAD_RECORDS_FILE}. {e}")
        sys.exit(1)

    if not ids_to_remove:
        print("警告: “文件1” 中没有找到任何 ID，无法进行筛选。")
        return

    print(f"已建立“黑名单”，包含 {len(ids_to_remove)} 个要移除的唯一 ID。")
    print("---")

    # --- 步骤 2: 筛选主文件并写入新文件 ---
    
    print(f"正在处理 {ALL_DATA_FILE} (文件2)...")
    
    total_read = 0
    total_written = 0
    total_skipped = 0
    
    try:
        # 同时打开 "主文件" (读) 和 "新文件" (写)
        with open(ALL_DATA_FILE, 'r', encoding='utf-8') as f_all, \
             open(CLEAN_OUTPUT_FILE, 'w', encoding='utf-8') as f_clean:
            
            for line in f_all:
                total_read += 1
                try:
                    data = json.loads(line.strip())
                    record_id = data.get("id")
                    
                    if record_id is None:
                        # 如果主文件中有行没有ID，我们默认保留它
                        print("警告: 主文件行无ID，默认保留。")
                        f_clean.write(line)
                        total_written += 1
                        continue
                    
                    # --- 核心逻辑 ---
                    # 检查当前 ID 是否在黑名单中
                    if record_id not in ids_to_remove:
                        # 不在 -> 保留 -> 写入新文件
                        f_clean.write(line)
                        total_written += 1
                    else:
                        # 在 -> 丢弃
                        total_skipped += 1
                        
                except json.JSONDecodeError:
                    print(f"警告: {ALL_DATA_FILE} 中有损坏的 JSON 行，已跳过。")
                    total_skipped += 1

    except IOError as e:
        print(f"错误: 读/写文件时出错. {e}")
        sys.exit(1)
    
    print("\n========= 清理完成 ==========")
    print(f"总共读取 (文件2): {total_read} 行")
    print(f"被移除的 (来自文件1): {total_skipped} 行")
    print(f"写入新文件: {total_written} 行")
    print(f"\n清理后的文件已保存到: {CLEAN_OUTPUT_FILE}")

# --- 执行脚本 ---
if __name__ == "__main__":
    filter_jsonl_file()