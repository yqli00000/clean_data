import json
import os

def merge_scattered_jsonl(output_filename):
    input_files = [
        r"F:\civitai_new_fetch\Chroma\only_images.jsonl",
        r"F:\civitai_new_fetch\Dalle3\only_images.jsonl",
        r"F:\civitai_new_fetch\Illustrious\only_images.jsonl",
        r"F:\civitai_new_fetch\Imagen4\only_images.jsonl",
        r"F:\civitai_new_fetch\Nano_Banana\only_images.jsonl",
        r"F:\civitai_new_fetch\qwen\only_images.jsonl",
        r"F:\civitai_new_fetch\openai\only_images.jsonl",
        r"F:\civitai_new_fetch\modern\only_images.jsonl",
    ]
    
    # 过滤掉不存在的文件，防止报错
    valid_files = []
    for f in input_files:
        if os.path.exists(f):
            valid_files.append(f)
        else:
            print(f"⚠️ 跳过不存在的文件: {f}")

    if not valid_files:
        print("❌ 没有找到有效的输入文件！")
        return

    print(f"📂 准备合并 {len(valid_files)} 个文件...")
    
    seen_ids = set()
    total_count = 0
    duplicate_count = 0
    
    # 获取输出文件的绝对路径，防止它和输入文件重名导致读取错误
    abs_output_path = os.path.abspath(output_filename)

    with open(output_filename, 'w', encoding='utf-8') as outfile:
        for file_path in valid_files:
            # 防止读取到正在写入的输出文件
            if os.path.abspath(file_path) == abs_output_path:
                continue

            print(f"正在读取: {file_path} ...")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as infile:
                    for line_num, line in enumerate(infile):
                        line = line.strip()
                        if not line: continue
                        
                        try:
                            data = json.loads(line)
                            
                            # 优先识别 ID
                            item_id = data.get('id')
                            if not item_id and 'meta' in data and isinstance(data['meta'], dict):
                                item_id = data['meta'].get('id')

                            # 去重逻辑
                            if item_id and item_id in seen_ids:
                                duplicate_count += 1
                                continue 
                            
                            if item_id:
                                seen_ids.add(item_id)
                            
                            outfile.write(line + '\n')
                            total_count += 1
                            
                        except json.JSONDecodeError:
                            print(f"⚠️ 格式错误跳过: {file_path} 第 {line_num+1} 行")
                            
            except Exception as e:
                print(f"❌ 读取出错: {file_path} -> {e}")

    print("-" * 30)
    print(f"✅ 合并完成！")
    print(f"📄 输出文件: {abs_output_path}")
    print(f"📊 有效数据: {total_count}")
    print(f"🗑️ 剔除重复: {duplicate_count}")

if __name__ == "__main__":
    output_filename = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl"
    merge_scattered_jsonl(output_filename)