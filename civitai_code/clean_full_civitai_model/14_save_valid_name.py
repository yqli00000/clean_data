import pandas as pd
import os

# ================= 配置区域 =================

csv_files_list = [
    r"F:\civitai_full_dataset\tables\base_valid\table-1_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-2_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-3_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-4_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-5_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-6_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\base_valid\table-7_filtered_no_empty_models_checkpoints_only_no_other_base_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-1_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-2_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-3_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-4_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-5_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-6_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv",
    r"F:\civitai_full_dataset\tables\ref_valid\table-7_filtered_no_empty_models_checkpoints_only_no_other_ref_valid.csv"
]

# 只保留这些后缀 (全部小写)
# 注意：jfif 也是 jpeg 的一种，建议保留
valid_extensions = {'.png', '.jpg', '.jpeg', '.jfif'}

# ===========================================

def filter_image_extensions():
    total_original = 0
    total_kept = 0
    total_deleted = 0

    print(f"=== 开始按文件后缀筛选 ===\n")
    print(f"保留列表: {valid_extensions}")
    print(f"剔除列表: .webp, .gif, .bmp, (无后缀) 等\n")

    for file_path in csv_files_list:
        if not os.path.exists(file_path): 
            continue
            
        print(f"处理文件: {os.path.basename(file_path)}")
        
        try:
            # 读取
            df = None
            for enc in ['utf-8', 'gb18030', 'latin1']:
                try:
                    df = pd.read_csv(file_path, dtype=str, encoding=enc)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None: continue

            original_count = len(df)
            total_original += original_count

            if 'filename' not in df.columns:
                print("  [警告] 找不到 filename 列，跳过。")
                continue

            # --- 核心筛选逻辑 ---
            # 1. 确保 filename 是字符串，去空格
            # 2. 提取后缀 (os.path.splitext)
            # 3. 转小写
            # 4. 判断是否在 valid_extensions 集合中
            
            def is_valid_ext(fname):
                if pd.isna(fname): return False
                fname = str(fname).strip()
                if not fname: return False
                
                _, ext = os.path.splitext(fname)
                return ext.lower() in valid_extensions

            # 应用筛选
            mask = df['filename'].apply(is_valid_ext)
            
            df_clean = df[mask]
            
            kept_count = len(df_clean)
            deleted_count = original_count - kept_count
            
            total_kept += kept_count
            total_deleted += deleted_count

            print(f"  -> 原始: {original_count}")
            print(f"  -> 保留: {kept_count}")
            print(f"  -> 剔除: {deleted_count}")

            # 保存
            if kept_count > 0:
                base, ext = os.path.splitext(file_path)
                save_path = f"{base}_clean_ext{ext}"
                df_clean.to_csv(save_path, index=False, encoding='utf-8-sig')
                print(f"  -> 已保存: {save_path}")

        except Exception as e:
            print(f"  [异常] {e}")

    print(f"\n" + "="*30)
    print(f"筛选完成")
    print(f"总剔除 (WebP/GIF/无后缀): {total_deleted}")
    print(f"总保留 (PNG/JPG): {total_kept}")
    print(f"="*30)

if __name__ == "__main__":
    filter_image_extensions()