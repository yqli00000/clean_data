import requests
import datetime
import json

def get_civitai_images_and_save_jsonl(target_date_after, nsfw_levels, output_filename):
    """
    获取 Civitai 上的图片，筛选 NSFW 等级和发布日期，
    并实时保存为 JSON Lines (.jsonl) 文件。

    :param target_date_after: (datetime.datetime) 只保留此日期之后发布的图片
    :param nsfw_levels: (list) 要筛选的 NSFW 等级列表, 例如 ["Soft", "Mature"]
    :param output_filename: (str) 保存 .jsonl 文件的路径
    """
    
    base_url = "https://civitai.com/api/v1/images"
    
    params = {
        "sort": "Newest",
        "limit": 100
    }

    print(f"开始获取数据... 目标日期: > {target_date_after.date()}")
    print(f"目标 NSFW 等级: {nsfw_levels}")
    print(f"将实时保存到: {output_filename}")
    
    next_page_url = base_url
    
    # 计数器，替代内存中的列表
    total_found_count = 0
    page_count = 0

    try:
        # 在循环开始前，以 "w" (写入) 模式打开文件
        # 这会清空/创建新文件，确保我们从一个干净的文件开始
        with open(output_filename, 'w', encoding='utf-8') as f:
            
            while next_page_url:
                page_count += 1
                print(f"\n--- 正在获取第 {page_count} 页 ---")
                
                try:
                    if page_count == 1:
                        response = requests.get(next_page_url, params=params)
                    else:
                        response = requests.get(next_page_url)

                    if response.status_code != 200:
                        print(f"请求失败，状态码: {response.status_code}")
                        break

                    data = response.json()
                    
                    items = data.get("items")
                    if not items:
                        print("未找到 'items'，或者列表为空。")
                        break

                    found_older_image = False
                    
                    # 计数这一页新找到了多少
                    page_new_finds = 0 

                    for image in items:
                        created_at_str = image.get("createdAt")
                        nsfw_level = image.get("nsfwLevel")
                        
                        if not created_at_str or not nsfw_level:
                            continue

                        created_at = datetime.datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                        
                        if created_at <= target_date_after:
                            found_older_image = True
                            break 

                        if nsfw_level in nsfw_levels:
                            # 1. 准备要保存的数据
                            # image_data = {
                            #     "id": image.get("id"),
                            #     "url": image.get("url"),
                            #     "created_at": created_at.isoformat(), # 转换为字符串
                            #     "nsfw_level": nsfw_level
                            # }
                            
                            # 2. 将这个单独的字典转换为 JSON 字符串
                            json_line = json.dumps(image, ensure_ascii=False)
                            
                            # 3. 写入文件，并添加换行符
                            f.write(json_line + '\n')
                            
                            total_found_count += 1
                            page_new_finds += 1

                    print(f"本页找到 {page_new_finds} 条新数据。总计: {total_found_count}")

                    if found_older_image:
                        print("已找到早于目标日期的图片，停止翻页。")
                        break
                        
                    next_page_url = data.get("metadata", {}).get("nextPage")
                    if not next_page_url:
                        print("已到达最后一页。")
                        break

                except requests.exceptions.RequestException as e:
                    print(f"发生网络错误: {e}")
                    # 网络错误时，我们选择中断，但已保存的数据是安全的
                    break
                except Exception as e:
                    print(f"处理数据时发生错误: {e}")

    except IOError as e:
        print(f"打开/写入文件时发生严重错误: {e}")
    
    print("\n========= 抓取完成 ==========")
    print(f"总共找到并保存了 {total_found_count} 张符合条件的图片。")
    print(f"数据已安全保存在: {output_filename}")
    return total_found_count



def convert_jsonl_to_json(jsonl_filename, json_filename):
    """
    将 JSON Lines (.jsonl) 文件转换为标准的 JSON 列表文件。
    """
    print(f"开始转换 {jsonl_filename} -> {json_filename} ...")
    
    # 存储所有从 .jsonl 读出的数据
    data_list = []
    
    try:
        # 读取 .jsonl 文件
        with open(jsonl_filename, 'r', encoding='utf-8') as f_in:
            for line in f_in:
                # 移除行尾的换行符并解析 JSON
                data_list.append(json.loads(line.strip()))
        
        # 写入 .json 文件
        with open(json_filename, 'w', encoding='utf-8') as f_out:
            # indent=4 让 JSON 文件格式化，更易读
            json.dump(data_list, f_out, indent=4, ensure_ascii=False)
            
        print(f"转换成功！总共 {len(data_list)} 条记录。")
        print(f"已保存到: {json_filename}")

    except FileNotFoundError:
        print(f"错误: 未找到输入的 .jsonl 文件: {jsonl_filename}")
    except json.JSONDecodeError as e:
        print(f"错误: .jsonl 文件内容格式错误: {e}")
    except Exception as e:
        print(f"发生未知错误: {e}")

# --- 执行代码 ---

if __name__ == "__main__":
    
    target_date = datetime.datetime(2024, 8, 15, 0, 0, 0, tzinfo=datetime.timezone.utc)
    target_nsfw = ["None"]
    jsonl_filename = r"F:\civitai_new_fetch\civitai_images.jsonl"
    json_filename = r"F:\civitai_new_fetch\civitai_images.json"
    get_civitai_images_and_save_jsonl(target_date, target_nsfw, jsonl_filename)
    print("\n--- 数据抓取完毕，开始转换为标准 JSON 文件 ---\n")
    convert_jsonl_to_json(jsonl_filename, json_filename)
    print("\n--- 所有操作完成 ---")