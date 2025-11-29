import json
import csv
from collections import Counter

def get_keys_recursively(data, parent_key=""):
    """
    递归提取字典中的所有键。
    对于列表，会添加 [] 标记，并继续进入列表内部扫描。
    """
    keys = []
    
    if isinstance(data, dict):
        for k, v in data.items():
            # 构建当前的完整路径，例如 meta.resources
            current_path = f"{parent_key}.{k}" if parent_key else k
            keys.append(current_path)
            
            # 递归深入下一层
            keys.extend(get_keys_recursively(v, current_path))
            
    elif isinstance(data, list):
        # 如果是列表，我们在路径后加上 [] 表示这是个数组
        # 并且扫描列表里的每一个元素（通常取第一个非空元素就够，但为了保险我们扫描所有）
        list_path = f"{parent_key}[]"
        for item in data:
            keys.extend(get_keys_recursively(item, list_path))
            
    return keys

def scan_jsonl_structure(jsonl_file, output_csv):
    print(f"🕵️‍♀️ 开始全量扫描文件: {jsonl_file} ...")
    
    # 使用 Counter 来统计每个键出现了多少次
    # 这能帮你区分哪些是“核心字段”（出现率100%），哪些是“稀有字段”
    key_counter = Counter()
    total_lines = 0
    
    try:
        with open(jsonl_file, 'r', encoding='utf-8') as infile:
            for line in infile:
                line = line.strip()
                if not line: continue
                
                try:
                    data = json.loads(line)
                    total_lines += 1
                    
                    # 获取这一行数据里所有的键路径
                    paths = get_keys_recursively(data)
                    
                    # 更新统计
                    key_counter.update(set(paths)) # 使用 set 去重，确保一行数据里同一个键只算一次
                    
                    if total_lines % 1000 == 0:
                        print(f"已扫描 {total_lines} 行...")
                        
                except json.JSONDecodeError:
                    continue

        # --- 导出结果 ---
        print(f"✅ 扫描完成！共分析 {total_lines} 条数据。")
        print(f"正在写入结果到 {output_csv} ...")
        
        with open(output_csv, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Key_Path', 'Count', 'Coverage_Rate (%)']) # 表头
            
            # 按出现频率从高到低排序
            for key, count in key_counter.most_common():
                coverage = (count / total_lines) * 100 if total_lines > 0 else 0
                writer.writerow([key, count, f"{coverage:.2f}%"])
                
        print(f"📄 结果已生成！请查看: {output_csv}")

    except FileNotFoundError:
        print(f"❌ 找不到文件: {jsonl_file}")

# ==========================================
# 运行设置
# ==========================================
if __name__ == "__main__":
    # 输入文件名
    input_file = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl" 
    # 输出文件名
    output_file = r"F:\civitai_new_fetch\summary\all_keys_report.csv"
    
    scan_jsonl_structure(input_file, output_file)