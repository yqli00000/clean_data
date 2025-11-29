from pathlib import Path
import time

def find_and_delete_compressed(root_dir: str, dry_run: bool = False):
    """
    递归遍历文件夹，查找并删除压缩文件。

    :param root_dir: 要扫描的根目录路径。
    :param dry_run: 是否为演习模式。
                    True = 只查找并列出文件，不删除。
                    False = 查找并执行删除操作。
    """
    
    # 1. 定义要删除的压缩文件扩展名列表
    # 您可以根据需要添加或删除，例如 '.arj', '.lha' 等
    compressed_extensions = {
        '.zip', '.rar', '.7z', '.tar', 
        '.gz', '.bz2', '.xz', '.tgz'
    }

    # 将输入的字符串路径转换为 Path 对象
    root_path = Path(root_dir)

    # 2. 检查路径是否存在且是否为文件夹
    if not root_path.is_dir():
        print(f"错误: 路径 '{root_dir}' 不是一个有效的文件夹。")
        return

    print(f"--- 正在扫描: {root_path.resolve()} ---")
    
    # 3. 递归查找所有匹配的文件
    # root_path.rglob('*') 会递归遍历所有子目录
    files_to_delete = []
    for file_path in root_path.rglob('*'):
        # 检查是否为文件，并且其后缀（.suffix）在我们的目标列表中
        if file_path.is_file() and file_path.suffix.lower() in compressed_extensions:
            files_to_delete.append(file_path)

    # 4. 报告结果
    if not files_to_delete:
        print("未找到任何匹配的压缩文件。")
        return

    print(f"--- 找到了 {len(files_to_delete)} 个匹配的压缩文件 ---")
    for f in files_to_delete:
        print(f"  [FOUND] {f}")
    print("--------------------------------------\n")

    # 5. 根据模式执行操作
    if dry_run:
        print("✅ [演习模式] 完成。未删除任何文件。")
        print("如需删除，请在运行时选择 'no' (关闭演习模式)。")
    else:
        print(f"⚠️ [!! 执行删除 !!] 您有 5 秒钟的时间按 Ctrl+C 中止操作...")
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n操作已中止。未删除任何文件。")
            return
            
        print("开始删除...")
        deleted_count = 0
        error_count = 0
        
        for file_path in files_to_delete:
            try:
                file_path.unlink()  # pathlib.Path.unlink() 用于删除文件
                print(f"  [DELETED] {file_path}")
                deleted_count += 1
            except OSError as e:
                print(f"  [ERROR] 无法删除 {file_path}: {e}")
                error_count += 1
        
        print(f"\n✅ 操作完成。成功删除 {deleted_count} 个文件，失败 {error_count} 个。")


# --- 脚本主执行入口 ---
if __name__ == "__main__":
    # 1. 获取用户输入
    target_directory = r"F:\civitai_full_dataset\images"
    # 2. 询问是否为演习模式（默认为 'yes'，更安全）
    # dry_run_input = input("是否仅运行演习（dry run）？(只列出文件，不删除) (yes/no) [默认为 yes]: ").strip().lower()
    
    # is_dry_run = True
    # if dry_run_input == 'no':
    #     is_dry_run = False
        
    #     # 3. 如果不是演习模式，进行最终确认（非常重要！）
    #     print("\n" + "="*40)
    #     print(f" 警告：您已选择 **执行删除** 操作。")
    #     print(f" 这将永久删除 '{target_directory}' 及其所有子目录下的所有压缩文件。")
    #     print(" 此操作无法撤销。")
    #     print("="*40 + "\n")
        
    #     confirm = input(f"您确定要继续吗？ (请输入 'yes' 确认): ").strip().lower()
        
    #     if confirm == 'yes':
    #         find_and_delete_compressed(target_directory, dry_run=False)
    #     else:
    #         print("操作已取消。")
    # else:
    find_and_delete_compressed(target_directory, dry_run=False)