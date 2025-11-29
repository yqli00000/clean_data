import json
import os

# ================= 配置区域 =================
# 你要把哪个文件去重？请修改这里
INPUT_FILE = r"F:\civitai_new_fetch\summary\failed_records.txt"  # 输入文件名
OUTPUT_FILE =r"F:\civitai_new_fetch\summary\failed_records_clean.txt"    # 输出文件名 (去重后)
# ===========================================

def deduplicate_dataset():
    print(f"🚀 开始对 {INPUT_FILE} 进行 ID 去重...")
    
    seen_ids = set()      # 用于记录出现过的 ID
    unique_count = 0      # 有效数据计数
    duplicate_count = 0   # 重复数据计数
    
    # 检查文件是否存在
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 找不到文件: {INPUT_FILE}")
        return

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out, \
         open(INPUT_FILE, 'r', encoding='utf-8') as f_in:
        
        for line_num, line in enumerate(f_in):
            line = line.strip()
            if not line: continue
            
            try:
                # 1. 解析数据
                # 如果是 JSONL 格式
                if line.startswith("{") and line.endswith("}"):
                    data = json.loads(line)
                    current_id = str(data.get('id')) # 强制转字符串，防止 123 != "123"
                
                # 如果是简单的 ID 列表 (TXT) 或 CSV
                else:
                    # 尝试用逗号分隔取第一列，或者整行作为 ID
                    parts = line.split(',')
                    current_id = str(parts[0]).strip()
                
                # 2. 检查 ID 是否已存在
                if current_id in seen_ids:
                    duplicate_count += 1
                    # 这里直接跳过，不写入新文件
                    continue
                
                # 3. 如果是新 ID
                seen_ids.add(current_id)
                f_out.write(line + '\n')
                unique_count += 1
                
            except json.JSONDecodeError:
                print(f"⚠️ 第 {line_num+1} 行格式错误，已跳过")
                continue
            except Exception as e:
                print(f"⚠️ 处理第 {line_num+1} 行时出错: {e}")
                continue

    print("-" * 30)
    print(f"✅ 去重完成！")
    print(f"📊 原始行数: {unique_count + duplicate_count}")
    print(f"💾 保留唯一: {unique_count}")
    print(f"🗑️ 删除重复: {duplicate_count}")
    print(f"📄 结果已保存至: {OUTPUT_FILE}")

if __name__ == "__main__":
    deduplicate_dataset()