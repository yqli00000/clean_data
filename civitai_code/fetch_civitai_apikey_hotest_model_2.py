import requests
import datetime
import json
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== 配置区域 ==================
API_TOKEN = "18ec1c6cf7baaf0e90e3ed32f865768e"

# 抓取数量：建议 100-200 个，这就足够覆盖目前所有主流的新模型了
MODEL_LIMIT = 300 

# 截止日期
TARGET_DATE = datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)

# NSFW 筛选
NSFW_LEVELS = ["None"] 

# 文件路径
OUTPUT_FILE = r"F:\civitai_new_fetch\modern\modern_models_data.jsonl"
PROGRESS_FILE = r"F:\civitai_new_fetch\modern\modern_completed_ids.txt"
# =============================================

def request_with_retry(url, params, headers):
    """稳定重试请求"""
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                print("  [429] 触发限流，休息 10 秒...")
                time.sleep(10)
            else:
                time.sleep(2)
        except Exception:
            time.sleep(random.uniform(2, 5))
    return None

def fetch_trending_modern_models(limit):
    """
    【核心修改】
    获取“过去一年(Year)”内“下载量最多(Most Downloaded)”的模型。
    这能过滤掉 2023 年的老模型，只保留 Pony, Flux, SDXL 等新时代的模型。
    """
    print(f"正在扫描【过去一年】最火的 {limit} 个新时代模型...")
    ids = []
    page = 1
    base_url = "https://civitai.com/api/v1/models"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Content-Type": "application/json"}
    
    while len(ids) < limit:
        params = {
            "limit": 100, 
            "page": page,
            # 改动1: 按下载量排序，代表真实热度
            "sort": "Most Downloaded", 
            # 改动2: 【关键】只看过去一年。这会把 ChilloutMix 等老古董过滤掉
            # 如果你想要更新的，可以改成 "Month"
            "period": "Year",          
            "types": "Checkpoint"      
        }
        
        data = request_with_retry(base_url, params, headers)
        if not data or "items" not in data or not data["items"]:
            break
        
        for item in data["items"]:
            # 打印一下名字，让你确认是不是新模型
            # print(f"  发现: {item['name']}") 
            ids.append(item["id"])
            
        print(f" -> 第 {page} 页扫描完毕，累计发现 {len(ids)} 个模型...")
        page += 1
        time.sleep(1)
        
    return ids[:limit]

def fetch_images_for_model(model_id):
    """抓取单个模型的图片 (保持不变)"""
    url = "https://civitai.com/api/v1/images"
    headers = {"Authorization": f"Bearer {API_TOKEN}", "Connection": "close"}
    params = {"modelId": model_id, "limit": 100, "sort": "Newest"}
    
    results = []
    next_url = url
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
                
                if dt <= TARGET_DATE:
                    found_older = True
                    break
                
                lvl = img.get("nsfwLevel")
                if (lvl in NSFW_LEVELS) or (str(lvl) in NSFW_LEVELS):
                    results.append(img)
            
            if found_older: break
            next_url = data.get("metadata", {}).get("nextPage")
            if not next_url: break
            time.sleep(0.5)
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
    # 1. 获取 新潮 模型列表
    target_ids = fetch_trending_modern_models(MODEL_LIMIT)
    print(f"\n目标列表准备完毕，开始抓取 {len(target_ids)} 个热门模型的图片数据...\n")
    
    processed = load_processed_ids()
    total_saved_images = 0
    
    # 2. 多线程抓取
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_mid = {}
        for mid in target_ids:
            if str(mid) in processed:
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
                        print(f"✅ 模型ID {mid}: 保存 {len(imgs)} 条数据 (总计: {total_saved_images})")
                    else:
                        print(f"⚪ 模型ID {mid}: 无近期数据")
                    
                    save_processed_id(mid)
                except Exception as e:
                    print(f"❌ 模型ID {mid} 出错: {e}")

    print(f"\n任务完成！总共抓取: {total_saved_images} 条数据。")

if __name__ == "__main__":
    main()