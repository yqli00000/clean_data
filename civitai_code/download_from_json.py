import requests
import json
import os
import time
from urllib.parse import urlparse # 用于解析 URL

JSONL_INPUT_FILE = r"F:\civitai_new_fetch\civitai_images_cleaned.jsonl"
DOWNLOAD_OUTPUT_DIR = r"F:\civitai_new_fetch\images"

def get_file_extension(url):
    """从 URL 中安全地提取文件扩展名 (例如 .jpeg, .png)"""
    try:
        path = urlparse(url).path
        # os.path.splitext 将路径分为 (文件名, .扩展名)
        ext = os.path.splitext(path)[1]
        
        # 处理一些常见的、无扩展名但在 URL 中的情况
        if ext:
            return ext
        else:
            # 如果 URL 真的没有扩展名 (虽然不太可能)
            return ".jpg" # 默认 Jpeg
    except Exception:
        return ".jpg" # 出错时默认为 Jpeg

def download_images():
    """
    读取 .jsonl 文件并下载所有图片。
    """
    
    # 1. 确保输出目录存在
    if not os.path.exists(DOWNLOAD_OUTPUT_DIR):
        print(f"创建下载目录: {DOWNLOAD_OUTPUT_DIR}")
        os.makedirs(DOWNLOAD_OUTPUT_DIR)
        
    # 2. 检查输入文件是否存在
    if not os.path.exists(JSONL_INPUT_FILE):
        print(f"错误: 找不到输入的 .jsonl 文件: {JSONL_INPUT_FILE}")
        print("请先运行抓取脚本。")
        return

    print(f"开始下载... 将保存到: {DOWNLOAD_OUTPUT_DIR}")
    
    total_downloaded = 0
    total_skipped = 0
    
    # 3. 逐行读取 .jsonl 文件 (高效，不占内存)
    try:
        with open(JSONL_INPUT_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    # 将单行 JSON 字符串转换为 Python 字典
                    image_data = json.loads(line.strip())
                    
                    # 4. 提取关键信息
                    image_id = image_data.get("id")
                    image_url = image_data.get("url")

                    if not image_id or not image_url:
                        print(f"警告: 找到一条不完整的记录，跳过: {line.strip()}")
                        continue
                    
                    # 5. 生成文件名 (例如: 12345.jpeg)
                    file_ext = get_file_extension(image_url)
                    output_filename = f"{image_id}{file_ext}"
                    output_filepath = os.path.join(DOWNLOAD_OUTPUT_DIR, output_filename)
                    
                    # 6. 检查文件是否已存在 (实现断点续传)
                    if os.path.exists(output_filepath):
                        print(f" [已跳过] {output_filename} (文件已存在)")
                        total_skipped += 1
                        continue
                        
                    # 7. 下载文件
                    print(f" [下载中] {image_url} -> {output_filename}")
                    
                    # 使用 session 提高性能
                    with requests.Session() as s:
                        response = s.get(image_url, timeout=15)
                        
                        # 检查下载是否成功
                        if response.status_code == 200:
                            # 8. 保存文件
                            with open(output_filepath, 'wb') as f_out:
                                f_out.write(response.content)
                            total_downloaded += 1
                        else:
                            print(f"  [失败!] 无法下载 {image_id} (状态码: {response.status_code})")
                    
                    # 礼貌性暂停，避免请求过于频繁
                    time.sleep(0.1) 
                    
                except json.JSONDecodeError:
                    print(f"警告: 发现损坏的 JSON 行，跳过: {line[:50]}...")
                except requests.exceptions.RequestException as e:
                    print(f"  [失败!] 下载 {image_id} 时发生网络错误: {e}")
                except Exception as e:
                    print(f"  [失败!] 发生未知错误: {e}")

    except IOError as e:
        print(f"错误: 无法读取文件 {JSONL_INPUT_FILE}. {e}")

    print("\n========= 下载完成 ==========")
    print(f"成功下载: {total_downloaded} 个新文件")
    print(f"跳过 (已存在): {total_skipped} 个文件")

# --- 执行下载脚本 ---
if __name__ == "__main__":
    download_images()