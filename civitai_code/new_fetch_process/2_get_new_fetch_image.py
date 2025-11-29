import json

# 1. 配置文件路径
input_file =  r"F:\civitai_new_fetch\summary\merged_only_images.jsonl"          # 你的输入文件名
output_file = r'F:\civitai_new_fetch\summary\valid_only_images.jsonl'  # 筛选后的输出文件名

# 定义筛选条件函数（请根据你的数据结构修改这里）
def is_image_data(data):
    """
    判断单条数据是否为图片格式
    """
    # -----------------------------------------------------------
    # 情况 A：如果数据中有明确的 'type' 或 'media_type' 字段
    # if data.get('type') == 'image':
    #     return True
    
    # -----------------------------------------------------------
    # 情况 B：如果你需要检查 URL 后缀 (例如 civitai 等数据集)
    # 假设你的图片链接字段叫 'url' 或 'image_url'
    image_url = data.get('url', '')  # 请将 'url' 替换为你实际的字段名
    if image_url and image_url.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.bmp')):
        return True
        
    # -----------------------------------------------------------
    # 情况 C：如果数据里有一个 'width' 和 'height'，通常也是图片
    # if data.get('width') and data.get('height'):
    #     return True

    return False

def filter_images():
    count = 0
    print("开始筛选...")
    
    with open(input_file, 'r', encoding='utf-8') as f_in, \
         open(output_file, 'w', encoding='utf-8') as f_out:
        
        for line in f_in:
            try:
                line = line.strip()
                if not line: continue
                
                data = json.loads(line)
                
                # 调用上面的判断函数
                if is_image_data(data):
                    # 将符合条件的数据写入新文件
                    f_out.write(json.dumps(data, ensure_ascii=False) + '\n')
                    count += 1
                    
            except json.JSONDecodeError:
                print(f"跳过无法解析的行")
                continue

    print(f"筛选完成！共找到 {count} 条图片数据，已保存至 {output_file}")

if __name__ == "__main__":
    filter_images()