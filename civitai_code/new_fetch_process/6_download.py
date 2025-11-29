import json
import os
import requests
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
                           
# ================= 配置区域 =================
INPUT_FILE = r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.jsonl"      # 输入文件
OUTPUT_ROOT = r"F:\civitai_new_fetch\images"           # 根存储目录
MAX_WORKERS = 8                             # 并发线程数
TIMEOUT = 30                                 # 超时时间
# 3. 设置代理 (【重要】如果你在国内，必须配置这个)
# 如果你没有代理，请把下面设为 None，即: PROXIES = None
PROXIES = {
    "http": "http://127.0.0.1:7890",   # 👈 请将 7890 改为你的代理端口
    "https": "http://127.0.0.1:7890"   # 👈 请将 7890 改为你的代理端口
}
# ==========================================================

def get_session():
    """创建一个带有重试机制的 Session"""
    session = requests.Session()
    
    # 重试策略: 失败后重试 3 次，间隔时间递增 (backoff_factor)
    # status_forcelist: 遇到 500, 502, 503, 504, 429 错误时重试
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 429])
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    session.proxies = PROXIES if PROXIES else {}
    return session

def get_extension_from_url(url):
    try:
        parsed = urlparse(url)
        path = parsed.path
        ext = os.path.splitext(path)[1]
        if ext.lower() in ['.jpg', '.jpeg', '.png', '.webp', '.gif']:
            return ext
    except:
        pass
    return ".jpeg"

def calculate_storage_path(image_id):
    s_id = str(image_id).zfill(4)
    last_4 = s_id[-4:]
    thousand_digit = s_id[-4]
    return os.path.join(OUTPUT_ROOT, thousand_digit, last_4)

def process_item(data, session):
    image_id = data.get('id')
    image_url = data.get('url')
    
    if not image_id or not image_url: return "MissingInfo"

    try:
        save_dir = calculate_storage_path(image_id)
        if not os.path.exists(save_dir):
            os.makedirs(save_dir, exist_ok=True)

        ext = get_extension_from_url(image_url)
        img_path = os.path.join(save_dir, f"{image_id}{ext}")
        json_path = os.path.join(save_dir, f"{image_id}.json")

        # 保存 JSON
        if not os.path.exists(json_path):
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        # 检查图片是否已存在且完整
        if os.path.exists(img_path) and os.path.getsize(img_path) > 100: # 大于100字节算有效
            return "Skipped"

        # === 核心下载逻辑 ===
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
            "Referer": "https://civitai.com/"
        }
        
        # 使用传入的 session (包含代理和重试)
        with session.get(image_url, headers=headers, stream=True, timeout=TIMEOUT) as response:
            if response.status_code == 200:
                with open(img_path, 'wb') as f:
                    for chunk in response.iter_content(4096):
                        f.write(chunk)
                return "Success"
            elif response.status_code == 404:
                return "Error_404_NotFound"
            elif response.status_code == 403:
                return "Error_403_Forbidden"
            else:
                return f"Error_{response.status_code}"

    except requests.exceptions.ProxyError:
        return "ProxyError"
    except requests.exceptions.ConnectTimeout:
        return "Timeout"
    except Exception as e:
        return f"Exception_{str(e)[:20]}"

def main():
    if not os.path.exists(INPUT_FILE):
        print("❌ 找不到输入文件")
        return

    # 1. 筛选出所有需要下载的任务
    print("📖 读取任务中...")
    tasks = []
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    d = json.loads(line)
                    if d.get('type') == 'image': tasks.append(d)
                except: pass

    # 过滤掉已经下载成功的（简单预检查，加快启动速度）
    # 如果你确定之前的下载没问题，可以开启这个预检查逻辑，否则注释掉
    # print("🔍 预检查已完成的文件...")
    # final_tasks = []
    # for t in tasks:
    #     sid = str(t['id']).zfill(4)
    #     path = os.path.join(OUTPUT_ROOT, sid[-4], sid[-4:], f"{t['id']}.jpeg") # 假定是jpeg
    #     if not os.path.exists(path): # 这里只是粗略检查
    #         final_tasks.append(t)
    # tasks = final_tasks

    print(f"🚀 开始下载 {len(tasks)} 张图片")
    print(f"🔌 代理设置: {PROXIES if PROXIES else '无 (直连)'}")
    print(f"🧵 线程数: {MAX_WORKERS}")

    stats = {"Success": 0, "Skipped": 0, "Failed": 0, "Errors": {}}

    # 创建一个线程安全的 Session 工厂
    # 注意：requests.Session 不是线程安全的，但在 ThreadPoolExecutor 里
    # 我们通常为每个线程或每次请求建立连接，或者小心使用。
    # 为了简单稳妥，我们在 process_item 外部不共享 session，
    # 但为了复用连接，我们可以让每个线程拥有一个 session (这里简化为每次请求新建带重试的连接，或者使用全局session配合锁)
    # *更优解*：在 ThreadPool 里，requests 会自动管理连接池。我们直接传 session 进去。
    
    global_session = get_session() 

    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    #     future_to_id = {executor.submit(process_item, item, global_session): item['id'] for item in tasks}
        
    #     # 使用 tqdm 显示进度
    #     pbar = tqdm(as_completed(future_to_id), total=len(tasks), unit="img")
        
        # for future in pbar:
        #     res = future.result()
            
        #     # 统计结果
        #     if res == "Success":
        #         stats["Success"] += 1
        #     elif res == "Skipped":
        #         stats["Skipped"] += 1
        #     else:
        #         stats["Failed"] += 1
        #         # 记录具体错误原因
        #         err_type = res.split('_')[0]
        #         stats["Errors"][err_type] = stats["Errors"].get(err_type, 0) + 1

        # === 新增：准备一个文件来记录失败的ID ===
    failed_log_file = open(r"F:\civitai_new_fetch\summary\failed_records.txt", "a", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 注意：这里我们保存了完整的 item 而不仅仅是 id，方便获取 URL
        future_to_item = {executor.submit(process_item, item, global_session): item for item in tasks}
        
        pbar = tqdm(as_completed(future_to_item), total=len(tasks), unit="img")
        
        for future in pbar:
            # 获取当前任务对应的原始数据
            current_item = future_to_item[future]
            current_id = current_item['id']
            current_url = current_item.get('url', 'No URL')

            try:
                res = future.result()
            except Exception as e:
                res = f"Exception_{str(e)}"

            # 统计结果
            if res == "Success":
                stats["Success"] += 1
            elif res == "Skipped":
                stats["Skipped"] += 1
            else:
                # === 🔴 失败处理逻辑在这里 ===
                stats["Failed"] += 1
                
                # 1. 打印到控制台 (加 \n 防止打断进度条)
                tqdm.write(f"❌ 失败 [ID: {current_id}] 原因: {res} | URL: {current_url}")
                
                # 2. 写入日志文件 (ID, URL, 原因)
                failed_log_file.write(f"{current_id},{current_url},{res}\n")
                failed_log_file.flush()  # 立即写入，防止程序崩溃丢失
                
                # 记录具体错误原因
                err_type = res.split('_')[0]
                stats["Errors"][err_type] = stats["Errors"].get(err_type, 0) + 1
            
            pbar.set_postfix(fail=stats["Failed"], err=list(stats["Errors"].items())[:2])

        # 记得关闭文件
        failed_log_file.close() 
            # 动态更新进度条后缀，显示当前失败率
            # pbar.set_postfix(fail=stats["Failed"], err=list(stats["Errors"].items())[:2])

    print("\n" + "="*30)
    print(f"📥 成功: {stats['Success']}")
    print(f"⏭️ 跳过: {stats['Skipped']}")
    print(f"❌ 失败: {stats['Failed']}")
    print("⚠️ 错误详情:")
    for k, v in stats["Errors"].items():
        print(f"   - {k}: {v}")
    print("="*30)

if __name__ == "__main__":
    main()