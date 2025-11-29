import json
import csv
import os

def filter_checkpoints_with_info(input_file, output_jsonl, output_csv):
    print(f"🔍 开始筛选 Checkpoint 数据: {input_file} ...")
    
    valid_count = 0
    skipped_count = 0
    
    # 准备 CSV 表头
    csv_headers = ['id', 'baseModel', 'final_model_name', 'source_type', 'url']
    
    with open(output_jsonl, 'w', encoding='utf-8') as f_json, \
         open(output_csv, 'w', newline='', encoding='utf-8-sig') as f_csv:
        
        writer = csv.writer(f_csv)
        writer.writerow(csv_headers)
        
        with open(input_file, 'r', encoding='utf-8') as infile:
            for line in infile:
                line = line.strip()
                if not line: continue
                
                try:
                    data = json.loads(line)
                    
                    # 1. 过滤非图片
                    if data.get('type') != 'image':
                        skipped_count += 1
                        continue
                        
                    # 2. 获取 Base Model (必须存在)
                    base_model = data.get('baseModel')
                    if not base_model:
                        skipped_count += 1
                        continue
                        
                    # 3. 深入挖掘 Model Name (多级回退策略)
                    model_name = None
                    source_type = None
                    
                    # 注意：根据你的报告，结构是 meta -> meta -> ...
                    meta_root = data.get('meta', {})
                    # 容错处理：有时可能是 meta -> meta，有时直接是 meta
                    inner_meta = meta_root.get('meta') if isinstance(meta_root, dict) and 'meta' in meta_root else meta_root
                    
                    if not isinstance(inner_meta, dict):
                        skipped_count += 1
                        continue

                    # --- 策略 A: 查 resources (type=model) ---
                    resources = inner_meta.get('resources', [])
                    if isinstance(resources, list):
                        for res in resources:
                            if res.get('type') == 'model':
                                model_name = res.get('name')
                                source_type = 'resources'
                                break
                    
                    # --- 策略 B: 查 civitaiResources (type=checkpoint) ---
                    if not model_name:
                        civ_resources = inner_meta.get('civitaiResources', [])
                        if isinstance(civ_resources, list):
                            for res in civ_resources:
                                if res.get('type') == 'checkpoint':
                                    # 有时 civitaiResources 里只有 versionId 没有 name，这里做个标记
                                    model_name = res.get('modelName') or f"CivitaiID_{res.get('modelVersionId')}"
                                    source_type = 'civitaiResources'
                                    break
                                    
                    # --- 策略 C: 查 meta.Model 字段 ---
                    if not model_name:
                        direct_model = inner_meta.get('Model')
                        if direct_model:
                            model_name = direct_model
                            source_type = 'meta.Model'
                            
                    # 4. 最终判定
                    # 只有当 model_name 找到了，才算有效数据
                    if model_name:
                        # 更新数据中的标记，方便后续使用
                        data['extracted_model_name'] = model_name
                        data['extracted_source'] = source_type
                        
                        # 写入 JSONL
                        f_json.write(json.dumps(data) + '\n')
                        
                        # 写入 CSV 概览
                        writer.writerow([
                            data.get('id'),
                            base_model,
                            model_name,
                            source_type,
                            data.get('url')
                        ])
                        valid_count += 1
                    else:
                        skipped_count += 1
                        
                except json.JSONDecodeError:
                    continue

    print(f"✅ 筛选完成！")
    print(f"📥 保留有效数据: {valid_count} 条 (已保存至 {output_jsonl})")
    print(f"🗑️ 过滤无效数据: {skipped_count} 条")
    print(f"📊 概览表格已生成: {output_csv}")

if __name__ == "__main__":
    # 输入文件 (合并后的文件)
    input_filename = r"F:\civitai_new_fetch\summary\merged_only_images.jsonl" 
    
    # 输出文件
    output_jsonl_filename = r"F:\civitai_new_fetch\summary\filtered_checkpoints.jsonl"
    output_csv_filename = r"F:\civitai_new_fetch\summary\filtered_checkpoints_summary.csv"
    
    filter_checkpoints_with_info(input_filename, output_jsonl_filename, output_csv_filename)