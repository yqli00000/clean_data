import json

# 读取 json 文件并计算长度
file_path = r'F:\civitai_full_dataset\bad_image_ids.json'  # 假设你的文件名是这个

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        count = len(data)
        print(f"一共有 {count} 个 ID")
except FileNotFoundError:
    print("找不到文件，请检查路径")