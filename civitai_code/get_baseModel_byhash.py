import requests
import datetime
import json
import os
import sys
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry 

# --- 配置 ---

# 1. 输入文件：您所有包含 meta 数据的 .jsonl 文件
#    (脚本会扫描所有这些文件来收集哈希值)
JSONL_INPUT_FILES = [
    r"F:\civitai_new_fetch\civitai_images_cleaned.jsonl"
]

# 2. 输出文件：最终的 "哈希 -> BaseModel" 映射表
OUTPUT_MAP_FILE = r"F:\civitai_new_fetch\hash_to_basemodel_map.json"

# 3. API 设置
API_HASH_ENDPOINT = "https://civitai.com/api/v1/model-versions/by-hash/"
POLITE_DELAY_SECONDS = 0.5 # 每次 API 调用之间的等待时间 (秒)

# --- (这是我们之前创建的重试 Session) ---
def create_retry_session():
    """
    创建一个 requests Session，
    它会在遇到网络错误或服务器错误时自动重试。
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
# ---------------------------------------------

def extract_unique_hashes(file_list):
    """
    步骤 1: 扫描所有 .jsonl 文件并收集唯一的模型哈希值。
    """
    print("步骤 1: 开始扫描文件以收集唯一的模型哈希值...")
    unique_hashes = set()
    
    for input_file in file_list:
        if not os.path.exists(input_file):
            print(f"警告: 找不到文件 {input_file}，已跳过。")
            continue
            
        print(f"--- 正在扫描: {input_file} ---")
        
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line.strip())
                        meta = data.get("meta")
                        
                        if not isinstance(meta, dict):
                            continue # 跳过没有 meta 字典的行
                        
                        hash_val = None
                        
                        # 优先尝试 "Model hash"
                        hash_val = meta.get("Model hash")
                        
                        # 如果没有，再尝试 "hashes" 字典中的 "model"
                        if not hash_val:
                            meta_hashes = meta.get("hashes")
                            if isinstance(meta_hashes, dict):
                                hash_val = meta_hashes.get("model")
                        
                        # 如果找到了哈希值，就添加到 set 中
                        if hash_val:
                            unique_hashes.add(hash_val)
                            
                    except (json.JSONDecodeError, Exception):
                        # 忽略此行错误
                        pass
        except IOError as e:
            print(f"错误: 无法读取文件 {input_file}. {e}")

    print(f"扫描完成。总共找到 {len(unique_hashes)} 个唯一的模型哈希值。")
    return unique_hashes

def get_base_model_map(unique_hashes, session):
    """
    步骤 2: 遍历哈希值，调用 API 获取 BaseModel。
    """
    print("\n步骤 2: 开始调用 API 查询 BaseModel... (这可能需要很长时间)")
    
    hash_to_basemodel_map = {}
    total = len(unique_hashes)
    
    for i, hash_val in enumerate(unique_hashes):
        
        # 打印进度
        print(f"  [{i+1}/{total}] 正在查询: {hash_val}...")
        
        # 1. 构建 URL
        url = f"{API_HASH_ENDPOINT}{hash_val}"
        
        try:
            # 2. 调用 API
            response = session.get(url, timeout=10)
            
            # 3. 分析响应
            if response.status_code == 200:
                version_data = response.json()
                base_model = version_data.get("baseModel", "Unknown") # 提取 baseModel
                name = version_data.get("name", "Unknown Name")      # 提取模型名
                
                hash_to_basemodel_map[hash_val] = {
                    "baseModel": base_model,
                    "name": name
                }
                print(f"    -> 成功: {name} (BaseModel: {base_model})")
                
            elif response.status_code == 404:
                # 404 是一个 "正常" 的错误，意味着这个哈希值在 Civitai 上没有记录
                hash_to_basemodel_map[hash_val] = {
                    "baseModel": "Not Found",
                    "name": "Not Found"
                }
                print(f"    -> 失败 (404): 未找到该哈希值。")
                
            else:
                # 其他 HTTP 错误 (例如 401, 500 等)
                hash_to_basemodel_map[hash_val] = {
                    "baseModel": f"Error {response.status_code}",
                    "name": "Error"
                }
                print(f"    -> 失败: HTTP 状态码 {response.status_code}")

        except requests.exceptions.RequestException as e:
            # 网络层面的错误 (超时, SSL 错误等)
            print(f"    -> 失败 (网络错误): {e}")
            hash_to_basemodel_map[hash_val] = {
                "baseModel": "Network Error",
                "name": "Network Error"
            }
        
        # 4. 礼貌性延迟
        time.sleep(POLITE_DELAY_SECONDS)

    print("API 查询完成。")
    return hash_to_basemodel_map

def save_map_to_file(model_map, output_file):
    """
    步骤 3: 将最终的映射表保存到文件。
    """
    print(f"\n步骤 3: 正在将结果保存到 {output_file} ...")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(model_map, f, indent=4, ensure_ascii=False)
        print("保存成功！")
    except IOError as e:
        print(f"错误: 无法写入文件 {output_file}. {e}")

# --- 主执行程序 ---
if __name__ == "__main__":
    
    # 0. 检查是否已有一个映射文件
    if os.path.exists(OUTPUT_MAP_FILE):
        print(f"警告: 映射文件 {OUTPUT_MAP_FILE} 已存在。")
        answer = input("您是否要 (s)跳过API调用并使用现有文件，还是 (r)重新运行所有API调用？ [s/r]: ").lower()
        if answer == 's':
            print("操作已跳过。脚本退出。")
            sys.exit(0)
        elif answer != 'r':
            print("无效输入。退出。")
            sys.exit(1)
        # 如果是 'r'，则继续执行
        print("将重新运行所有查询并覆盖现有文件...")

    # 1. 创建 Session
    session = create_retry_session()
    
    # 2. 步骤 1: 提取哈希
    hashes = extract_unique_hashes(JSONL_INPUT_FILES)
    
    if not hashes:
        print("未能从文件中提取任何哈希值。脚本退出。")
        sys.exit(1)
        
    # 3. 步骤 2: 调用 API
    model_map = get_base_model_map(hashes, session)
    
    # 4. 步骤 3: 保存文件
    save_map_to_file(model_map, OUTPUT_MAP_FILE)
    
    print("\n========= 全部完成 ==========")