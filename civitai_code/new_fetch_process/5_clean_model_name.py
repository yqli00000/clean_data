import json
import csv
import re
import time
import requests

# ================= 配置区域 =================
INPUT_FILE = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl"          # 输入文件
OUTPUT_CSV = r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.csv"  
OUTPUT_JSONL = r"F:\civitai_new_fetch\summary\final_clean_model_checkpoint_dataset.jsonl"
API_DELAY = 0.5                                  # API 延迟
# ===========================================

# 缓存与正则定义
model_cache = {}
uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
id_filename_pattern = re.compile(r'^(\d+)(\.safetensors|\.ckpt|\.pt)?$', re.IGNORECASE)
prefix_pattern = re.compile(r'^\d+[_ \.-]?')
suffix_pattern = re.compile(r'[_ -]?(fp8|fp16|bf16|nf4|int8|noclip|gguf|q4_k|q8_0|pruned|baked|vae).*', re.IGNORECASE)
ext_pattern = re.compile(r'\.(safetensors|ckpt|pt)$', re.IGNORECASE)

def fetch_model_info_from_api(version_id=None, file_hash=None):
    """API 联网查询"""
    cache_key = str(version_id) if version_id else f"hash_{file_hash}"
    if cache_key in model_cache: return model_cache[cache_key]

    url = ""
    if version_id: url = f"https://civitai.com/api/v1/model-versions/{version_id}"
    elif file_hash: url = f"https://civitai.com/api/v1/model-versions/by-hash/{file_hash}"
    else: return None

    try:
        print(f"   🌐 [API] 正在查询: {cache_key} ...")
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            name = f"{data.get('model', {}).get('name', 'Unknown')} {data.get('name', '')}"
            model_cache[cache_key] = name
            time.sleep(API_DELAY)
            return name
    except:
        pass
    
    model_cache[cache_key] = "API_Fail"
    return None

def clean_technical_name(raw_name):
    """清洗技术文件名"""
    if not raw_name: return "Unknown"
    name = str(raw_name).strip()
    name = ext_pattern.sub('', name)
    name = prefix_pattern.sub('', name)
    name = suffix_pattern.sub('', name)
    return name.strip('_ -')

def process_strict_clean_export_all():
    print(f"🚀 开始执行：严格筛选 + 智能清洗 + 双重导出 (CSV & JSONL)...")
    
    csv_headers = ['id', 'baseModel', 'clean_merged_name', 'original_name', 'data_source', 'url']
    
    valid_count = 0
    skipped_not_checkpoint = 0
    skipped_no_name = 0
    
    # 同时打开 CSV 和 JSONL 文件进行写入
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f_csv, \
         open(OUTPUT_JSONL, 'w', encoding='utf-8') as f_jsonl, \
         open(INPUT_FILE, 'r', encoding='utf-8') as infile:
        
        writer = csv.writer(f_csv)
        writer.writerow(csv_headers)
        
        for line in infile:
            line = line.strip()
            if not line: continue
            
            try:
                data = json.loads(line)
                
                # --- 关卡 1: 必须是图片 ---
                if data.get('type') != 'image': continue
                
                # --- 关卡 2: 必须有 BaseModel ---
                base_model = data.get('baseModel')
                if not base_model: continue
                
                # --- 提取阶段 ---
                local_name = None
                local_hash = None
                local_id = None
                source_type = "None"
                found_checkpoint_evidence = False

                meta = data.get('meta', {})
                if isinstance(meta, dict) and 'meta' in meta: meta = meta['meta']
                if not isinstance(meta, dict): meta = {}

                # A. 检查 resources
                res_list = meta.get('resources', [])
                if isinstance(res_list, list):
                    for r in res_list:
                        if r.get('type') == 'model':
                            local_name = r.get('name')
                            local_hash = r.get('hash')
                            source_type = "resources"
                            found_checkpoint_evidence = True
                            break
                
                # B. 检查 civitaiResources
                civ_list = meta.get('civitaiResources', [])
                if isinstance(civ_list, list):
                    for r in civ_list:
                        if r.get('type') == 'checkpoint':
                            found_checkpoint_evidence = True
                            if not local_id: local_id = r.get('modelVersionId')
                            if not local_name: source_type = "civitaiResources"
                            break

                # C. 检查 meta.Model
                if not found_checkpoint_evidence:
                    m = meta.get('Model')
                    if m:
                        found_checkpoint_evidence = True
                        if not str(m).startswith('urn:'):
                            local_name = m
                            source_type = "meta.Model"
                
                # --- 关卡 3: 严格 Checkpoint 校验 ---
                if not found_checkpoint_evidence:
                    skipped_not_checkpoint += 1
                    continue

                # --- 清洗与修复阶段 ---
                final_name = local_name
                is_bad = False
                
                if not local_name: is_bad = True
                elif uuid_pattern.match(str(local_name)) or id_filename_pattern.match(str(local_name)):
                    is_bad = True
                
                if is_bad and (local_id or local_hash):
                    api_name = fetch_model_info_from_api(local_id, local_hash)
                    if api_name and "API_Fail" not in api_name:
                        final_name = api_name
                        source_type = "API_Fixed"
                    else:
                        final_name = f"Unknown_Hash_{local_hash}" if local_hash else "Unknown_UUID"

                if final_name is None:
                    final_name = "Unknown"

                clean_name = final_name
                if source_type != "API_Fixed" and "Unknown" not in str(final_name):
                    clean_name = clean_technical_name(final_name)
                
                # --- 再次确认有效性 ---
                if "Unknown" in str(clean_name) and not local_hash and not local_id:
                    skipped_no_name += 1
                    continue

                # ================= 核心修改：更新并写入数据 =================
                
                # 1. 写入 CSV (仅摘要)
                writer.writerow([
                    data.get('id'),
                    base_model,
                    clean_name,
                    local_name,
                    source_type,
                    data.get('url')
                ])
                
                # 2. 写入 JSONL (完整数据 + 新增字段)
                # 我们把清洗出来的关键信息注入到 JSON 对象里，方便以后使用
                data['clean_merged_name'] = clean_name       # 最重要的清洗名
                data['original_model_name'] = local_name     # 原始名
                data['model_source_type'] = source_type      # 来源
                
                # 如果是 API 修复的，还可以把 ID 补进去
                if local_id:
                    data['fixed_model_id'] = local_id

                f_jsonl.write(json.dumps(data, ensure_ascii=False) + '\n')
                
                valid_count += 1
                
            except json.JSONDecodeError:
                continue

    print(f"\n✅ 全部处理完成！")
    print(f"📥 有效数据: {valid_count} 条")
    print(f"🚫 剔除无Checkpoint: {skipped_not_checkpoint} 条")
    print(f"🗑️ 剔除无名数据: {skipped_no_name} 条")
    print(f"------------------------------------------------")
    print(f"📊 CSV 报表: {OUTPUT_CSV}")
    print(f"💾 JSONL 数据: {OUTPUT_JSONL} (已包含 clean_merged_name 字段)")

if __name__ == "__main__":
    process_strict_clean_export_all()
# import json
# import csv
# import re
# import time
# import requests

# # ================= 配置区域 =================
# INPUT_FILE = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl"          # 输入文件
# OUTPUT_CSV = r"F:\civitai_new_fetch\summary\final_clean_dataset.csv"           # 输出文件
# API_DELAY = 0.5                                  # API 请求间隔
# # ===========================================

# # 缓存字典
# model_cache = {}

# # 正则定义
# # 1. 坏名字：UUID 或 纯数字ID
# uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
# id_filename_pattern = re.compile(r'^(\d+)(\.safetensors|\.ckpt|\.pt)?$', re.IGNORECASE)

# # 2. 清洗规则：去除前缀数字 (如 "2758_") 和 技术后缀 (如 "fp8", "noclip")
# # 匹配开头的数字+非单词字符 (如 "1234_", "01 ")
# prefix_pattern = re.compile(r'^\d+[_ \.-]?')
# # 匹配常见的量化/修剪后缀 (不区分大小写)
# suffix_pattern = re.compile(r'[_ -]?(fp8|fp16|bf16|nf4|int8|noclip|gguf|q4_k|q8_0|pruned|baked|vae).*', re.IGNORECASE)
# # 匹配文件扩展名
# ext_pattern = re.compile(r'\.(safetensors|ckpt|pt)$', re.IGNORECASE)

# def fetch_model_info_from_api(version_id=None, file_hash=None):
#     """API 联网查询"""
#     cache_key = str(version_id) if version_id else f"hash_{file_hash}"
#     if cache_key in model_cache: return model_cache[cache_key]

#     url = ""
#     if version_id: url = f"https://civitai.com/api/v1/model-versions/{version_id}"
#     elif file_hash: url = f"https://civitai.com/api/v1/model-versions/by-hash/{file_hash}"
#     else: return None

#     try:
#         print(f"   🌐 [API] 正在查询: {cache_key} ...")
#         response = requests.get(url, timeout=5)
#         if response.status_code == 200:
#             data = response.json()
#             # 组合名称：模型名 + 版本名
#             name = f"{data.get('model', {}).get('name', 'Unknown')} {data.get('name', '')}"
#             model_cache[cache_key] = name
#             time.sleep(API_DELAY)
#             return name
#     except:
#         pass
    
#     model_cache[cache_key] = "API_Fail"
#     return None

# def clean_technical_name(raw_name):
#     """
#     清洗技术文件名，用于合并统计
#     输入: 2758FluxAsianUtopian_v51KreaFp8Noclip.safetensors
#     输出: FluxAsianUtopian_v51Krea
#     """
#     if not raw_name: return "Unknown"
    
#     name = str(raw_name).strip()
    
#     # 1. 去掉扩展名
#     name = ext_pattern.sub('', name)
    
#     # 2. 去掉开头的排序数字 (如 "2758")
#     name = prefix_pattern.sub('', name)
    
#     # 3. 去掉技术后缀 (如 "Fp8", "Noclip")
#     name = suffix_pattern.sub('', name)
    
#     # 4. 去掉多余的下划线或空格
#     name = name.strip('_ -')
    
#     return name

# def process_and_clean():
#     print(f"🚀 开始处理数据，生成清洗后的合并列...")
    
#     # 新增列：clean_merged_name
#     headers = ['id', 'baseModel', 'clean_merged_name', 'original_name', 'data_source', 'url']
    
#     valid_count = 0
    
#     with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8-sig') as f_csv, \
#          open(INPUT_FILE, 'r', encoding='utf-8') as infile:
        
#         writer = csv.writer(f_csv)
#         writer.writerow(headers)
        
#         for line in infile:
#             line = line.strip()
#             if not line: continue
            
#             try:
#                 data = json.loads(line)
#                 if data.get('type') != 'image': continue
                
#                 base_model = data.get('baseModel')
#                 if not base_model: continue
                
#                 # --- 1. 提取原始信息 ---
#                 local_name = None
#                 local_hash = None
#                 local_id = None
#                 source_type = "None"
                
#                 # 处理嵌套 meta
#                 meta = data.get('meta', {})
#                 if isinstance(meta, dict) and 'meta' in meta: meta = meta['meta']
#                 if not isinstance(meta, dict): meta = {}

#                 # 优先找 resources
#                 res_list = meta.get('resources', [])
#                 if isinstance(res_list, list):
#                     for r in res_list:
#                         if r.get('type') == 'model':
#                             local_name = r.get('name')
#                             local_hash = r.get('hash')
#                             source_type = "resources"
#                             break
                
#                 # 补漏找 civitaiResources (为了ID)
#                 civ_list = meta.get('civitaiResources', [])
#                 if isinstance(civ_list, list):
#                     for r in civ_list:
#                         if r.get('type') == 'checkpoint':
#                             if not local_id: local_id = r.get('modelVersionId')
#                             if not local_name: source_type = "civitaiResources" # 此时还没名字
#                             break

#                 # 兜底找 meta.Model
#                 if not local_name:
#                     m = meta.get('Model')
#                     if m and not str(m).startswith('urn:'):
#                         local_name = m
#                         source_type = "meta.Model"

#                 # --- 2. 决策与修复 ---
#                 final_name = local_name
#                 is_bad = False
                
#                 # 判定是否为“坏名字” (UUID / 纯数字 / 空)
#                 if not local_name: is_bad = True
#                 elif uuid_pattern.match(str(local_name)) or id_filename_pattern.match(str(local_name)):
#                     is_bad = True
                
#                 # 如果是坏名字，尝试 API 修复
#                 if is_bad and (local_id or local_hash):
#                     api_name = fetch_model_info_from_api(local_id, local_hash)
#                     if api_name and "API_Fail" not in api_name:
#                         final_name = api_name
#                         source_type = "API_Fixed"
#                     else:
#                         # API 也没救回来，只能用 Hash 代替，保证这一列有值
#                         final_name = f"Unknown_Hash_{local_hash}" if local_hash else "Unknown"

#                 # --- 3. 最终清洗 (生成合并列) ---
#                 # 如果是 API 修复回来的名字 (如 "Chroma v5")，通常很干净，不需要正则洗
#                 # 如果是本地文件名 (如 "2758Flux...Fp8")，需要正则洗
                
#                 clean_name = final_name
#                 # if source_type != "API_Fixed" and "Unknown" not in final_name:
#                 # # 强制把 final_name 转成字符串再判断，这样 None 就会变成 "None"，就不会报错了
#                 if source_type != "API_Fixed" and "Unknown" not in str(final_name):
#                     clean_name = clean_technical_name(final_name)
                
#                 # --- 4. 写入 ---
#                 writer.writerow([
#                     data.get('id'),
#                     base_model,
#                     clean_name,      # <--- 这是你要的合并列
#                     local_name,      # 原始名字 (方便查证)
#                     source_type,
#                     data.get('url')
#                 ])
#                 valid_count += 1
                
#             except json.JSONDecodeError:
#                 continue

#     print(f"\n✅ 完成！结果保存在: {OUTPUT_CSV}")
#     print(f"📊 这里的 'clean_merged_name' 列已经去除了 Fp8/Noclip 等后缀，可直接用于合并统计。")

# if __name__ == "__main__":
#     process_and_clean()