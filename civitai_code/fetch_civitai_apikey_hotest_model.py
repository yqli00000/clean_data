import requests
import datetime
import json
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== 配置区域 ==================
API_TOKEN = "22a46f1adc5e83d9b67db0014a259be2"
# 目标：抓取多少个模型？(想数据多，就设大一点，比如 200 或 500)
# 抓 200 个热门模型，通常能产生 10万~30万条有效图片数据
MODEL_LIMIT = 500

TARGET_DATE = datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
NSFW_LEVELS = ["None"] # 根据你的需求

# 文件路径
OUTPUT_FILE = r"F:\civitai_new_fetch\massive_data_aug15.jsonl"
PROGRESS_FILE = r"F:\civitai_new_fetch\massive_completed_ids.txt"
# =============================================

def request_with_retry(url, params, headers):
    """稳定重试请求"""
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                time.sleep(10) # 限流等待
            else:
                time.sleep(2)
        except Exception:
            time.sleep(random.uniform(2, 5))
    return None

def fetch_top_model_ids(limit):
    """
    【核心修改】
    自动获取全站最热门的 Top N 个模型 ID。
    这能保证我们覆盖的数据量足够大，接近全站数据。
    """
    print(f"正在扫描全站最热门的 {limit} 个模型...")
    ids = []
    page = 1
    base_url = "https://civitai.com/api/v1/models"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
    
    while len(ids) < limit:
        params = {
            "limit": 100, 
            "page": page,
            "sort": "Highest Rated", # 抓评分最高的（通常也是图最多的）
            "types": "Checkpoint"    # 只抓大模型
        }
        data = request_with_retry(base_url, params, headers)
        if not data or "items" not in data or not data["items"]:
            break
        
        for item in data["items"]:
            ids.append(item["id"])
            
        print(f" -> 已发现 {len(ids)} 个热门模型...")
        page += 1
        time.sleep(1)
        
    return ids[:limit] # 截取前 Limit 个

def fetch_images_for_model(model_id):
    """抓取单个模型的图片"""
    url = "https://civitai.com/api/v1/images"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Connection": "close"}
    params = {"modelId": model_id, "limit": 100, "sort": "Newest"}
    
    results = []
    next_url = url
    
    # 只有第一页带 params，后面带 cursor
    first_page = True
    
    while next_url:
        try:
            if first_page:
                data = request_with_retry(next_url, params, headers)
                first_page = False
            else:
                data = request_with_retry(next_url, {}, headers)
                
            if not data or "items" not in data: break
            
            items = data["items"]
            if not items: break
            
            found_older = False
            for img in items:
                c_str = img.get("createdAt")
                if not c_str: continue
                dt = datetime.datetime.fromisoformat(c_str.replace("Z", "+00:00"))
                
                # 时间截止判断
                if dt <= TARGET_DATE:
                    found_older = True
                    break
                
                # NSFW 筛选
                lvl = img.get("nsfwLevel")
                if (lvl in NSFW_LEVELS) or (str(lvl) in NSFW_LEVELS):
                    results.append(img)
            
            if found_older: break
            
            next_url = data.get("metadata", {}).get("nextPage")
            if not next_url: break
            
            time.sleep(0.5) # 防封
            
        except Exception:
            break
            
    return results

def load_processed_ids():
    if not os.path.exists(PROGRESS_FILE): return set()
    with open(PROGRESS_FILE, 'r') as f:
        return set(line.strip() for line in f)

def save_processed_id(mid):
    with open(PROGRESS_FILE, 'a') as f:
        f.write(f"{mid}\n")

def main():
    # 1. 获取 ID 列表
    target_ids = fetch_top_model_ids(MODEL_LIMIT)
    print(f"目标列表准备完毕，共 {len(target_ids)} 个模型。\n开始抓取图片...")
    
    processed = load_processed_ids()
    total_saved_images = 0
    
    # 2. 多线程抓取
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_mid = {}
        for mid in target_ids:
            if str(mid) in processed:
                print(f"跳过已处理模型 ID: {mid}")
                continue
            future = executor.submit(fetch_images_for_model, mid)
            future_to_mid[future] = mid
            
        with open(OUTPUT_FILE, 'a', encoding='utf-8') as f_out:
            for future in as_completed(future_to_mid):
                mid = future_to_mid[future]
                try:
                    imgs = future.result()
                    if imgs:
                        for img in imgs:
                            f_out.write(json.dumps(img, ensure_ascii=False) + '\n')
                        total_saved_images += len(imgs)
                        print(f"✅ 模型 {mid}: 保存 {len(imgs)} 张图 (总计: {total_saved_images})")
                    else:
                        print(f"⚪ 模型 {mid}: 无符合条件数据")
                    
                    save_processed_id(mid)
                    
                except Exception as e:
                    print(f"❌ 模型 {mid} 出错: {e}")

    print(f"\n任务完成！总共抓取: {total_saved_images} 条数据。")

if __name__ == "__main__":
    main()