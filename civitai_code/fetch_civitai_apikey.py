import requests
import datetime
import json
import time
import random # 引入随机数，让等待时间不那么死板

def get_authenticated_civitai_images(api_token, target_date_after, nsfw_levels, output_filename):
    base_url = "https://civitai.com/api/v1/images"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
        # 【关键修改】主动告诉服务器：处理完这个请求就断开连接，不要保持 Keep-Alive。
        # 这虽然会稍微降低速度，但能极大减少 10054 错误。
        "Connection": "close", 
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    
    params = {
        "sort": "Newest",  
        "period": "Year",
        "limit": 100   
    }

    print(f"开始抓取... 结果将实时保存到: {output_filename}")
    
    next_page_url = base_url
    total_found_count = 0
    page_count = 0
    
    # 最大重试次数
    MAX_RETRIES = 5 

    with open(output_filename, 'w', encoding='utf-8') as f:
        
        while next_page_url:
            page_count += 1
            print(f"\n--- 正在处理第 {page_count} 页 ---")
            
            # === 重试机制开始 ===
            success = False
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    # 如果是第一页，用 params；后续页 url 里自带参数
                    if page_count == 1:
                        response = requests.get(next_page_url, headers=headers, params=params, timeout=30) # 增加 timeout 防止卡死
                    else:
                        response = requests.get(next_page_url, headers=headers, timeout=30)

                    if response.status_code == 200:
                        success = True
                        break # 成功了！跳出重试循环
                    elif response.status_code == 429:
                        print(f"[重试 {attempt}/{MAX_RETRIES}] 触发频率限制 (429)，休息 10 秒...")
                        time.sleep(10)
                    else:
                        print(f"[重试 {attempt}/{MAX_RETRIES}] 服务器返回状态码: {response.status_code}")
                        time.sleep(3)
                
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                    # 这里专门捕获 10054 这类连接错误
                    print(f"[重试 {attempt}/{MAX_RETRIES}] 网络连接中断 (10054 等): {e}")
                    # 遇到这种错误，多休息一会儿，比如 5~10 秒
                    time.sleep(random.uniform(5, 10))
                except Exception as e:
                    print(f"[重试 {attempt}/{MAX_RETRIES}] 未知错误: {e}")
                    time.sleep(3)
            
            # 如果重试了 5 次还是失败
            if not success:
                print("!!! 连续重试失败，程序被迫停止。之前的数据已保存。 !!!")
                break
            # === 重试机制结束 ===

            # 下面是正常的数据处理逻辑
            try:
                data = response.json()
                items = data.get("items")
                
                if not items:
                    print("本页无数据。")
                    break

                found_older_image = False
                page_new_finds = 0 

                for image in items:
                    created_at_str = image.get("createdAt")
                    img_nsfw_level = image.get("nsfwLevel")
                    
                    if not created_at_str: continue

                    created_at = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                    
                    if created_at <= target_date_after:
                        found_older_image = True
                        print(f"达到截止日期 ({created_at.date()})，停止抓取。")
                        break 

                    # 兼容数字和字符串类型的 NSFW Level
                    if (img_nsfw_level in nsfw_levels) or (str(img_nsfw_level) in nsfw_levels):
                        f.write(json.dumps(image, ensure_ascii=False) + '\n')
                        total_found_count += 1
                        page_new_finds += 1

                print(f"本页入库: {page_new_finds}。总计: {total_found_count}")

                if found_older_image:
                    break
                    
                next_page_url = data.get("metadata", {}).get("nextPage")
                if not next_page_url:
                    print("已到达最后一页。")
                    break
                
                # 【关键】每翻一页，主动休息一下，防止被服务器判定为攻击
                time.sleep(random.uniform(1, 2)) 

            except Exception as e:
                print(f"解析数据时出错: {e}")
                break

    print(f"\n抓取结束。总共保存: {total_found_count} 条。")


def convert_jsonl_to_json(jsonl_filename, json_filename):
    print(f"正在转换格式: .jsonl -> .json ...")
    data_list = []
    try:
        with open(jsonl_filename, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    data_list.append(json.loads(line.strip()))
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=4, ensure_ascii=False)
        print(f"转换完成，已保存至: {json_filename}")
    except Exception as e:
        print(f"转换失败: {e}")


if __name__ == "__main__":
    
    # ================= 配置区域 =================
    
    # 1. 填入你的 API Key
    # 获取方式: Civitai 网站 -> Settings -> API Key -> Add API Key
    API_TOKEN = "22a46f1adc5e83d9b67db0014a259be2" 
    
    # 2. 截止日期 (2024年8月15日)
    TARGET_DATE = datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
    
    # 3. 你想要保存的等级 (虽然 API 拿了全部，但这里决定存哪些)
    # 确保包含你想要的，比如 "X" 和 "Mature"
    TARGET_NSFW_LEVELS = ["None","Soft"]
    
    # 4. 文件路径
    JSONL_FILE = r"F:\civitai_new_fetch\all_images_year_soft.jsonl"
    JSON_FILE = r"F:\civitai_new_fetch\all_images_year_soft.json"
    
    # ================= 开始运行 =================
    
    # if API_TOKEN != "22a46f1adc5e83d9b67db0014a259be2":
    #     print("请先在代码中填入你的 API Token！")
    # else:
    get_authenticated_civitai_images(API_TOKEN, TARGET_DATE, TARGET_NSFW_LEVELS, JSONL_FILE)
    convert_jsonl_to_json(JSONL_FILE, JSON_FILE)