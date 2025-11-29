import json
import os
import sys

JSONL_INPUT_FILE = r"F:\civitai_new_fetch\Dalle3\only_images.jsonl"
STATS_OUTPUT_FILE = r"F:\civitai_new_fetch\Dalle3\only_images_base_model_stats.txt"
OUTOFSTATEPUT_FILE = r"F:\civitai_new_fetch\Dalle3\only_images_out_of_state_models_v2.jsonl"


def analyze_models():
    """
    读取 .jsonl 文件，统计 'baseModel' 字段的分布，
    并将结果打印到控制台和指定的文本文件。
    """
    
    # 1. 检查输入文件
    if not os.path.exists(JSONL_INPUT_FILE):
        print(f"错误: 找不到输入的 .jsonl 文件: {JSONL_INPUT_FILE}")
        sys.exit(1)

    print(f"开始分析文件: {JSONL_INPUT_FILE}")
    
    # 2. 用于存储计数的字典
    base_model_counts = {}
    
    total_records = 0
    
    # 3. 逐行读取 .jsonl 文件
    try:
        with open(JSONL_INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # 4. 解析 JSON
                    image_data = json.loads(line.strip())
                    total_records += 1
                    model_name = None # <--- 先设为 None

                    # 1. 尝试顶层 baseModel (您原来的逻辑)
                    model_name = image_data.get("baseModel")
                    meta_data = image_data.get("meta")
                    # 2. 如果顶层为空，进入 meta 字段
                    if not model_name and isinstance(meta_data, dict):
                        
                        # 2a. 优先尝试 'baseModel'
                        model_name = meta_data.get("baseModel")
                        if not model_name:
                            model_name = meta_data.get("Model") 
                        # 2b. 其次尝试 'model'
                        if not model_name:
                            model_name = meta_data.get("model")

                        # 2b. 其次尝试 'model'
                        
                        # 2c. 再次尝试 'Model hash' (作为备选)
                        # if not model_name:
                        #     model_name = meta_data.get("Model hash")
                            
                        # # 2d. 尝试从 'hashes' 字典中提取
                        # if not model_name:
                        #     meta_hashes = meta_data.get("hashes")
                        #     if isinstance(meta_hashes, dict):
                        #         model_name = meta_hashes.get("model") # 这将得到一个哈希值

                    # 3. 如果全部失败，才归类为 (未指定)
                    if not model_name:
                        model_name = "(未指定)"
                        with open(OUTOFSTATEPUT_FILE, 'a', encoding='utf-8') as f_out_of_state:
                            f_out_of_state.write(line)
                        
                    # 7. 累加计数
                    current_count = base_model_counts.get(model_name, 0)
                    base_model_counts[model_name] = current_count + 1
                    
                except json.JSONDecodeError:
                    print(f"警告: 发现损坏的 JSON 行，跳过: {line[:50]}...")
                except Exception as e:
                    print(f"处理数据时发生未知错误: {e}")

    except IOError as e:
        print(f"错误: 无法读取文件 {JSONL_INPUT_FILE}. {e}")
        sys.exit(1)

    # 8. (已修改) 准备并保存最终统计结果
    print("\n========= 分析完成 ==========")
    print(f"结果将保存到: {STATS_OUTPUT_FILE}")

    # 使用 try...except 确保文件写入安全
    try:
        # 以 'w' (写入) 模式打开输出文件
        # 'encoding='utf-8'' 确保中文（如果baseModel里有的话）正常显示
        with open(STATS_OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
            
            # 定义一个辅助函数，同时打印并写入文件
            def log_result(message):
                print(message)
                f_out.write(message + '\n') # 写入文件并换行

            # --- 开始写入统计数据 ---
            log_result("========= 分析完成 ==========")
            log_result(f"分析文件: {JSONL_INPUT_FILE}")
            log_result(f"总共扫描了 {total_records} 条记录。")
            log_result(f"共找到 {len(base_model_counts)} 种不同的 'baseModel'：")
            
            if base_model_counts and total_records > 0:
                # 按数量从多到少排序
                sorted_counts = sorted(base_model_counts.items(), key=lambda item: item[1], reverse=True)
                
                for model, count in sorted_counts:
                    percentage = (count / total_records) * 100
                    # 格式化输出
                    line_text = f"  - {model}: {count} 条记录 ({percentage:.2f}%)"
                    log_result(line_text)
            
            elif total_records == 0:
                log_result("文件中没有找到任何记录。")
            else:
                log_result("未找到任何 'baseModel' 数据。")
        
        print(f"\n统计结果已成功保存到: {STATS_OUTPUT_FILE}")

    except IOError as e:
        print(f"错误: 无法写入统计文件 {STATS_OUTPUT_FILE}. {e}")
    except Exception as e:
        print(f"保存统计时发生未知错误: {e}")

# --- 执行分析脚本 ---
if __name__ == "__main__":
    analyze_models()