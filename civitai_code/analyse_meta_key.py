import json
import os
import sys

# --- 配置 ---

# 1. 输入文件：我们配置为读取您拆分后的两个文件
#    (如果您有更多文件，也可以在这里添加)
JSONL_INPUT_FILES = [
    r"F:\civitai_new_fetch\civitai_images.jsonl",
]

# 2. 输出的统计结果文件名
STATS_OUTPUT_FILE = r"F:\civitai_new_fetch\meta_all_keys.txt"

# --- 脚本 ---

def analyze_meta_keys():
    """
    读取所有 .jsonl 文件，统计 'meta' 字段中出现过的所有唯一键名。
    """
    
    # 1. 使用一个 "set" (集合) 来自动存储唯一的键名
    all_meta_keys = set()
    
    total_records = 0
    total_meta_records = 0
    
    print("开始分析所有 'meta' 字段的键...")
    
    # 2. 遍历您配置的每一个输入文件
    for input_file in JSONL_INPUT_FILES:
        if not os.path.exists(input_file):
            print(f"警告: 找不到文件 {input_file}，跳过。")
            continue
            
        print(f"--- 正在处理文件: {input_file} ---")

        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                # 3. 逐行读取
                for line in f:
                    try:
                        total_records += 1
                        
                        image_data = json.loads(line.strip())
                        
                        # 4. 提取 'meta' 字段
                        meta_data = image_data.get("meta")
                        
                        # 5. 检查 'meta' 是否存在且是一个字典 (dict)
                        if isinstance(meta_data, dict):
                            total_meta_records += 1
                            
                            # 6. 将这个 'meta' 里的所有键名添加到我们的 set 中
                            #    .update() 会自动处理重复值
                            all_meta_keys.update(meta_data.keys())
                            
                    except json.JSONDecodeError:
                        # 忽略损坏的行
                        pass
                    except Exception:
                        # 忽略其他行处理错误
                        pass

        except IOError as e:
            print(f"错误: 无法读取文件 {input_file}. {e}")
        except Exception as e:
            print(f"发生未知错误: {e}")

    # 7. (已修改) 准备并保存最终统计结果
    print("\n========= Meta 键名分析完成 ==========")
    print(f"结果将保存到: {STATS_OUTPUT_FILE}")

    try:
        with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
            
            def log_result(message):
                print(message)
                f_out.write(message + '\n') 

            log_result("========= Meta 键名分析完成 ==========")
            log_result(f"分析文件: {', '.join(JSONL_INPUT_FILES)}")
            log_result(f"总共扫描了 {total_records} 条记录。")
            log_result(f"其中 {total_meta_records} 条记录包含 'meta' 字典。")
            log_result(f"总共找到了 {len(all_meta_keys)} 个唯一的 'meta' 键名：")
            
            if all_meta_keys:
                # 8. 转换为列表并按字母顺序排序，方便阅读
                sorted_keys = sorted(list(all_meta_keys))
                
                # 9. 写入所有键名
                log_result("--- 按字母顺序排序 ---")
                for i, key in enumerate(sorted_keys):
                    log_result(f"  {i + 1}. {key}")
            
        print(f"\n统计结果已成功保存到: {STATS_OUTPUT_FILE}")

    except IOError as e:
        print(f"错误: 无法写入统计文件 {STATS_OUTPUT_FILE}. {e}")
    except Exception as e:
        print(f"保存统计时发生未知错误: {e}")

# --- 执行分析脚本 ---
if __name__ == "__main__":
    analyze_meta_keys()