import json
import csv
import os
import shutil

# ================= 配置区域 =================
FAILED_LOG_FILE = r"F:\civitai_new_fetch\summary\failed_records_clean.txt"      # 记录失败ID的文件
INPUT_CSV = r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.csv"        # 你的 CSV 数据表
INPUT_JSONL = r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.jsonl"  # 你的 JSONL 数据集
IMAGE_ROOT = r"F:\civitai_new_fetch\images"           # 图片根目录
# ===========================================

def calculate_storage_path(image_id):
    """计算存储路径"""
    s_id = str(image_id).zfill(4)
    last_4 = s_id[-4:]
    thousand_digit = s_id[-4]
    return os.path.join(IMAGE_ROOT, thousand_digit, last_4)

def load_failed_ids():
    """从日志文件中提取所有失败的 ID"""
    ids = set()
    if not os.path.exists(FAILED_LOG_FILE):
        print(f"❌ 找不到失败日志文件: {FAILED_LOG_FILE}")
        return ids
    
    print(f"📖 正在读取失败记录...")
    with open(FAILED_LOG_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            # 日志格式: ID,URL,Reason
            # 我们只需要逗号前的第一个部分 (ID)
            parts = line.split(',')
            if parts:
                target_id = parts[0].strip()
                ids.add(target_id)
    
    print(f"🎯 共提取到 {len(ids)} 个待删除的 ID。")
    return ids

def batch_delete_data():
    # 1. 获取要删除的 ID 列表
    target_ids = load_failed_ids()
    if not target_ids:
        print("   没有需要删除的数据，程序退出。")
        return

    print(f"🚀 开始批量清理操作...")

    # -------------------------------------------------
    # 2. 批量清理 CSV 文件
    # -------------------------------------------------
    if os.path.exists(INPUT_CSV):
        print(f"📄 正在过滤 CSV: {INPUT_CSV} ...")
        temp_csv = INPUT_CSV + ".tmp"
        deleted_csv_count = 0
        
        with open(INPUT_CSV, 'r', encoding='utf-8-sig') as f_in, \
             open(temp_csv, 'w', newline='', encoding='utf-8-sig') as f_out:
            
            reader = csv.DictReader(f_in)
            writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
            writer.writeheader()
            
            for row in reader:
                # 检查当前行 ID 是否在黑名单里
                if str(row.get('id')) in target_ids:
                    deleted_csv_count += 1
                else:
                    writer.writerow(row)
        
        os.replace(temp_csv, INPUT_CSV)
        print(f"   ✅ CSV 清理完成，移除了 {deleted_csv_count} 行。")
    else:
        print(f"   ⚠️ 未找到 CSV 文件。")

    # -------------------------------------------------
    # 3. 批量清理 JSONL 文件
    # -------------------------------------------------
    if os.path.exists(INPUT_JSONL):
        print(f"💾 正在过滤 JSONL: {INPUT_JSONL} ...")
        temp_jsonl = INPUT_JSONL + ".tmp"
        deleted_jsonl_count = 0
        
        with open(INPUT_JSONL, 'r', encoding='utf-8') as f_in, \
             open(temp_jsonl, 'w', encoding='utf-8') as f_out:
            
            for line in f_in:
                if not line.strip(): continue
                try:
                    data = json.loads(line)
                    # 检查当前 JSON ID 是否在黑名单里
                    if str(data.get('id')) in target_ids:
                        deleted_jsonl_count += 1
                    else:
                        f_out.write(line)
                except:
                    f_out.write(line)
        
        os.replace(temp_jsonl, INPUT_JSONL)
        print(f"   ✅ JSONL 清理完成，移除了 {deleted_jsonl_count} 行。")
    else:
        print(f"   ⚠️ 未找到 JSONL 文件。")

    # -------------------------------------------------
    # 4. 批量清理 Images 文件夹 (残留文件)
    # -------------------------------------------------
    print(f"📂 正在清理文件系统残留...")
    files_deleted = 0
    
    for target_id in target_ids:
        target_dir = calculate_storage_path(target_id)
        
        if os.path.exists(target_dir):
            # 遍历目录，删除以该 ID 开头的文件 (图片+JSON)
            # 这样做比直接 os.remove 更安全，防止后缀对不上
            for filename in os.listdir(target_dir):
                if filename.startswith(str(target_id) + "."):
                    file_path = os.path.join(target_dir, filename)
                    try:
                        os.remove(file_path)
                        files_deleted += 1
                    except Exception:
                        pass
    
    print("-" * 30)
    print(f"🎉 批量清理结束！")
    print(f"   CSV 移除: {deleted_csv_count}")
    print(f"   JSONL 移除: {deleted_jsonl_count}")
    print(f"   文件移除: {files_deleted}")

if __name__ == "__main__":
    batch_delete_data()