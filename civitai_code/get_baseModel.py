import json
import os
import sys
import re

# --- 配置 ---

# 1. 输入: 您的 "哈希 -> BaseModel" 映射文件
JSON_MAP_FILE = r"F:\civitai_new_fetch\hash_to_basemodel_map.json"

# 2. 输入: 您的 "模型名称 -> 统计" 文件
#    (请使用您最新的联合统计文件)
STATS_TXT_FILE = r"F:\civitai_new_fetch\base_model_stats.txt"

# 3. 输出: 最终的、带有 BaseModel 标注的统计文件
OUTPUT_TXT_FILE = r"F:\civitai_new_fetch\base_model_stats_final.txt"

# --- 脚本 ---

def load_and_invert_map(json_file_path):
    """
    加载 hash -> {name, baseModel} 的 JSON 文件,
    并将其反转为一个 name -> set(baseModels) 的字典。
    """
    
    if not os.path.exists(json_file_path):
        print(f"错误: 找不到 JSON 映射文件: {json_file_path}")
        return None

    print(f"正在加载并反转映射文件: {json_file_path} ...")
    
    name_to_basemodel_map = {}
    
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            hash_map = json.load(f)
        
        # 遍历 "hash -> {name, baseModel}" 映射
        for hash_val, data in hash_map.items():
            model_name = data.get("name")
            base_model = data.get("baseModel", "Unknown")
            
            # 过滤掉 "Not Found" 或 "Error" 的条目
            if not model_name or model_name in ["Not Found", "Error", "Unknown Name"]:
                continue
                
            # 我们使用 set (集合) 来处理同名但不同 BaseModel 的情况
            if model_name not in name_to_basemodel_map:
                name_to_basemodel_map[model_name] = set()
                
            # 将 BaseModel 添加到集合中
            name_to_basemodel_map[model_name].add(base_model)
            
        print(f"反转映射创建成功，包含 {len(name_to_basemodel_map)} 个唯一的模型名称。")
        return name_to_basemodel_map

    except Exception as e:
        print(f"加载或反转 JSON 时出错: {e}")
        return None

def annotate_stats_file(stats_file_path, output_file_path, name_map):
    """
    读取统计 TXT 文件，使用映射表进行标注，并写入新文件。
    """
    
    if not os.path.exists(stats_file_path):
        print(f"错误: 找不到统计 TXT 文件: {stats_file_path}")
        return

    print(f"正在处理统计文件: {stats_file_path} ...")
    
    try:
        with open(stats_file_path, 'r', encoding='utf-8') as f_in, \
             open(output_file_path, 'w', encoding='utf-8') as f_out:
            
            # 逐行读取统计文件
            for line in f_in:
                # 尝试从行中解析模型名称
                # 格式: "  - modelName: 123 条记录 (45.67%)"
                match = re.match(r'^\s*-\s*(.+?):\s*\d+\s*条记录', line)
                
                if match:
                    # 找到了一个模型行
                    model_name = match.group(1).strip()
                    
                    # 2. 在我们的反转映射中查找这个名称
                    found_basemodels = name_map.get(model_name)
                    
                    basemodel_str = ""
                    if found_basemodels:
                        # 找到了！将 set 转换为逗号分隔的字符串
                        # (例如 "SDXL 1.0, Pony")
                        basemodel_str = ", ".join(sorted(list(found_basemodels)))
                    else:
                        # 这个模型名称不在我们的 JSON 映射中
                        basemodel_str = "?? (未在哈希表中找到)"
                    
                    # 3. 写入带标注的新行
                    # (移除原始行尾的换行符，然后添加我们的标注)
                    new_line = f"{line.strip()}  [BaseModel: {basemodel_str}]\n"
                    f_out.write(new_line)
                    
                else:
                    # 这行不是模型行 (例如标题、总结行)
                    # 原样写入
                    f_out.write(line)

    except Exception as e:
        print(f"处理文件时出错: {e}")
        return

    print(f"\n========= 标注完成 ==========")
    print(f"已将带标注的结果保存到: {output_file_path}")

# --- 主执行程序 ---
if __name__ == "__main__":
    
    # 步骤 1: 加载并反转 JSON 映射
    name_map = load_and_invert_map(JSON_MAP_FILE)
    
    if name_map:
        # 步骤 2: 使用映射表标注统计文件
        annotate_stats_file(STATS_TXT_FILE, OUTPUT_TXT_FILE, name_map)