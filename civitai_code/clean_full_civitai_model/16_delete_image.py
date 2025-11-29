import os
import time

def keep_images_from_txt_list(image_folder_path, whitelist_txt_path):
    """
    1. 读取 whitelist_txt_path (由之前CSV提取出的txt文件)。
    2. 递归扫描 image_folder_path。
    3. 如果图片的文件名(不含扩展名) 存在于 txt列表中 -> 保留。
    4. 如果不存在 -> 删除图片及其同名的 .json 文件。
    """
    
    # --- 第 1 步：加载白名单 (读取txt文件) ---
    print(f"正在读取白名单文件: '{whitelist_txt_path}' ...")
    
    if not os.path.isfile(whitelist_txt_path):
        print(f"错误：找不到白名单文件: '{whitelist_txt_path}'。已取消操作。")
        return

    try:
        valid_names_set = set()
        with open(whitelist_txt_path, "r", encoding="utf-8") as f:
            for line in f:
                # 去除换行符和空格
                name = line.strip()
                if name:
                    # 兼容性处理：如果txt里的名字带了后缀(如 data.jpg)，我们去掉后缀只留 data
                    # 如果原本就是纯文件名(如 data)，不受影响
                    base_name = os.path.splitext(name)[0]
                    valid_names_set.add(base_name)
        
        if not valid_names_set:
            print("错误：白名单文件为空。这将导致删除所有图片！已取消操作以策安全。")
            return
            
        print(f"白名单加载完毕。共包含 {len(valid_names_set)} 个有效文件名。")
        
    except Exception as e:
        print(f"读取白名单时发生错误: {e}")
        return

    # --- 第 2 步：递归扫描并清理 ---
    
    print(f"\n--- !!! 准备开始清理 !!! ---")
    print(f"目标文件夹: '{image_folder_path}'")
    print(f"规则: 仅保留 txt 文件中列出的文件名，其余删除。")
    
    # 安全倒计时
    try:
        print("程序将在 5 秒后开始执行删除操作... (按 Ctrl+C 可取消)")
        for i in range(5, 0, -1):
            print(f"{i}...", end=" ", flush=True)
            time.sleep(1)
        print("\n开始执行！")
    except KeyboardInterrupt:
        print("\n操作已取消。")
        return

    if not os.path.isdir(image_folder_path):
        print(f"错误：找不到图片文件夹: '{image_folder_path}'")
        return
        
    deleted_count = 0
    scanned_count = 0
    # 定义我们要处理的图片格式
    image_extensions = {'.png', '.jpg', '.jpeg', '.webp', '.bmp', '.gif'} 

    for root, dirs, files in os.walk(image_folder_path):
        for filename in files:
            scanned_count += 1
            
            # 获取文件名(不带后缀) 和 后缀
            base_name, ext = os.path.splitext(filename)
            ext_lower = ext.lower()
            
            # 只处理图片文件
            if ext_lower in image_extensions:
                
                # 核心判断：如果这个图片的名字 不在 白名单集合中
                if base_name not in valid_names_set:
                    
                    file_to_remove = os.path.join(root, filename)
                    json_to_remove = os.path.join(root, base_name + ".json") # 假设伴随json文件
                    
                    # 1. 删除图片
                    try:
                        os.remove(file_to_remove)
                        print(f" [删除图片] {filename} (不在白名单中)")
                        deleted_count += 1
                    except Exception as e:
                        print(f" [删除失败] {filename}: {e}")

                    # 2. 删除同名 JSON (如果存在)
                    if os.path.exists(json_to_remove):
                        try:
                            os.remove(json_to_remove)
                            print(f" [删除JSON] {base_name}.json")
                            deleted_count += 1
                        except Exception as e:
                            print(f" [删除JSON失败] {base_name}.json: {e}")
            
    print(f"\n--- 清理完成 ---")
    print(f"总共扫描文件: {scanned_count}")
    print(f"总共删除文件: {deleted_count}")

if __name__ == "__main__":
    
    # --- 配置区域 ---
    
    # 1. 之前生成的 txt 文件路径
    TXT_PATH = r'F:\AI_generated_DB-main\civitai_code\clean_full_civitai_model\extracted_filenames.txt'  
    
    # 2. 你的图片文件夹路径
    IMAGES_FOLDER = r'F:\civitai_full_dataset\images' 

    # ----------------
    
    keep_images_from_txt_list(IMAGES_FOLDER, TXT_PATH)