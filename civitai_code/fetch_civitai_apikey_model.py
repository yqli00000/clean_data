import requests
import datetime
import json
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================= 配置区域 =================
API_TOKEN = "18ec1c6cf7baaf0e90e3ed32f865768e" 
# 想要抓取的数据截止日期
TARGET_DATE = datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
# 需要的 NSFW 等级
NSFW_LEVELS = ["None"]

# 输出文件路径
OUTPUT_FILE = r"F:\civitai_new_fetch\Illustrious\metadata_database.jsonl"
# 进度记录文件 (用于断点续传)
PROGRESS_FILE = r"F:\civitai_new_fetch\Illustrious\completed_ids.txt"

# 想要抓取的模型名称列表 (越多越好)
TARGET_MODELS_NAMES = [
    # "openai"
    # "Sora 2",
    # "Seedream",
    # "Nano Banana"
    # "Imagen 4"
    # "Illustrious XL 1.1",
    # "Qwen",
    # "Chroma"
    # "Dalle-3",
    # "Dalle 3",
    # "Dalle3"
    "Illustrious XL 1.1"
]
# ===========================================

def load_completed_ids():
    """读取已经完成的模型ID，防止重复抓取"""
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())

def mark_id_as_completed(model_id):
    """将完成的模型ID写入记录文件"""
    with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{model_id}\n")

def request_until_success(url, params, headers, description=""):
    """
    【核心函数】无限重试机制
    无论发生什么错误（断网、报错、服务器炸了），都不会停止，
    只会等待更长时间后重试，直到成功拿到数据。
    """
    wait_seconds = 5
    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=20)
            
            # 如果是 429 (请求太快)，特殊处理
            if response.status_code == 429:
                print(f"⚠️ [{description}] 触发限流 (429)，休息 30 秒...")
                time.sleep(30)
                continue
            
            # 如果是 404 (找不到)，那也没办法重试，直接放弃
            if response.status_code == 404:
                print(f"❌ [{description}] 资源未找到 (404)，跳过。")
                return None

            # 其他非 200 错误，视为服务器故障，需要重试
            if response.status_code != 200:
                print(f"⚠️ [{description}] 服务器状态码 {response.status_code}，稍后重试...")
                raise Exception(f"Status Code {response.status_code}")

            return response.json() # 成功！

        except Exception as e:
            print(f"🔥 [{description}] 连接中断: {e}")
            print(f"   -> 等待 {wait_seconds} 秒后重试...")
            time.sleep(wait_seconds)
            # 指数退避：下次等得更久一点，最大等待 2 分钟
            wait_seconds = min(wait_seconds * 1.5, 120) 

def get_model_ids_by_name_robust(name):
    """搜索模型ID (带无限重试)"""
    url = "https://civitai.com/api/v1/models"
    params = {"query": name, "limit": 100, "sort": "Highest Rated"} # 拿前5个匹配的
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    
    data = request_until_success(url, params, headers, description=f"搜索 {name}")
    
    ids = []
    if data and "items" in data:
        for item in data["items"]:
            ids.append(item["id"])
        ids.append(2167369)
    return ids

def fetch_model_metadata_robust(model_id):
    """
    抓取单个模型的所有图片元数据
    包含：无限重试 + 自动翻页
    """
    url = "https://civitai.com/api/v1/images"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Connection": "close"}
    
    # 初始化参数
    params = {
        "modelId": model_id,
        "limit": 100,
        "sort": "Newest"
    }
    
    next_page_url = url
    local_buffer = [] # 暂存这个模型抓到的数据
    
    page_num = 0
    
    while next_page_url:
        page_num += 1
        # 使用我们定义的无限重试函数发请求
        # 注意：如果是第一页用 params，后面用 next_page_url
        if page_num == 1:
            data = request_until_success(url, params, headers, description=f"ID {model_id} 第{page_num}页")
        else:
            # 提取 next_page_url 里的 cursor 参数，或者直接请求 url
            data = request_until_success(next_page_url, {}, headers, description=f"ID {model_id} 第{page_num}页")
        
        if not data or "items" not in data:
            break
            
        items = data["items"]
        if not items:
            break
            
        found_older = False
        for item in items:
            created_at_str = item.get("createdAt")
            if not created_at_str: continue
            
            dt = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
            
            # 时间截断
            if dt <= TARGET_DATE:
                found_older = True
                break # 这一页剩下的都太老了
                
            # NSFW 筛选
            lvl = item.get("nsfwLevel")
            if (lvl in NSFW_LEVELS) or (str(lvl) in NSFW_LEVELS):
                local_buffer.append(item)
        
        if found_older:
            break
            
        next_page_url = data.get("metadata", {}).get("nextPage")
        # 稍微休息一下，对服务器友好
        time.sleep(0.5)

    return local_buffer

def main_process():
    print("=== 启动死磕型数据抓取器 ===")
    print(f"结果保存至: {OUTPUT_FILE}")
    print(f"进度记录: {PROGRESS_FILE}")
    
    # 1. 读取之前的进度
    completed_ids = load_completed_ids()
    print(f"已跳过 {len(completed_ids)} 个之前处理完的模型。")
    
    # 2. 解析所有模型 ID
    print("\nStep 1: 将模型名称转换为 ID 列表...")
    target_ids = []
    for name in TARGET_MODELS_NAMES:
        print(f"正在搜索: {name} ...")
        ids = get_model_ids_by_name_robust(name)
        if ids:
            print(f" -> 找到 {len(ids)} 个相关模型")
            target_ids.extend(ids)
        time.sleep(1)
    
    # 去重
    target_ids = list(set(target_ids))
    print(f"\nStep 2: 准备处理 {len(target_ids)} 个唯一模型 ID")
    
    # 3. 开始并发抓取
    # 使用 ThreadPoolExecutor 进行多线程下载
    # max_workers=3 比较保守，因为我们在每个线程里都有无限重试，线程太多容易被服务器封IP
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_id = {}
        
        for mid in target_ids:
            # 如果这个 ID 之前跑过了，直接跳过
            if str(mid) in completed_ids:
                print(f"⏩ ID {mid} 已在历史记录中，跳过。")
                continue
            
            future = executor.submit(fetch_model_metadata_robust, mid)
            future_to_id[future] = mid
        
        # 实时处理结果
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f_out:
            for future in as_completed(future_to_id):
                mid = future_to_id[future]
                try:
                    results = future.result()
                    
                    # 写入文件
                    if results:
                        for item in results:
                            f_out.write(json.dumps(item, ensure_ascii=False) + '\n')
                        print(f"✅ ID {mid}: 抓取成功，保存 {len(results)} 条元数据。")
                    else:
                        print(f"⚪ ID {mid}: 无符合条件的新数据。")
                    
                    # 【关键】标记为已完成
                    mark_id_as_completed(mid)
                    
                except Exception as e:
                    print(f"❌ ID {mid} 发生严重未知错误 (理论上不应到达这里): {e}")
    with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
        count = sum(1 for line in f)
    print(f"共有 {count} 条数据")
    print("\n=== 所有任务完成 ===")

if __name__ == "__main__":
    # 确保文件存在
    if not os.path.exists(os.path.dirname(OUTPUT_FILE)):
        os.makedirs(os.path.dirname(OUTPUT_FILE))
        
    main_process()