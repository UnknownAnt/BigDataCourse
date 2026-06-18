"""
===============================================================================
实验 03 - CSV 转 Parquet 列式存储与分区
===============================================================================
实验目的：将 3.4GB CSV 原始数据转换为高效的 Parquet 列式存储格式

业务清洗规则:
  1. 剔除 timestamp <= 0 的异常数据（1970 年系统漏洞）
  2. 剔除 timestamp < 31536000 的数据（1971 年之前）
  3. 剔除 behavior_type 为空字符串的行
  4. 只保留有效行为类型：pv, buy, cart, fav

数仓级分区策略:
  - 按 behavior_type 进行物理文件夹分区
  - 输出目录：clean_data_partitioned/
  - 目录结构:
      clean_data_partitioned/
      ├── behavior_type=pv/
      │   └── data.parquet
      ├── behavior_type=buy/
      │   └── data.parquet
      ├── behavior_type=cart/
      │   └── data.parquet
      └── behavior_type=fav/
          └── data.parquet
===============================================================================
"""

import polars as pl
import duckdb
import time
import os
import shutil


def format_bytes(size_bytes: int) -> str:
    """格式化字节大小为人类可读格式"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} TB"


def polars_to_parquet():
    """使用 Polars 进行数据清洗并写入分区 Parquet"""
    
    print("\n" + "🔄" * 35)
    print("【Polars 方案】CSV → Parquet 分区转换")
    print("🔄" * 35)
    
    output_dir = "clean_data_partitioned"
    
    # 清理旧目录
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    
    start = time.time()
    
    # Step 1: 懒加载 CSV
    print("\n【Step 1】懒加载 CSV 文件...")
    q = pl.scan_csv("user_behavior_100M.csv", has_header=False).rename({
        "column_1": "user_id", "column_2": "session_id", "column_3": "item_id",
        "column_4": "behavior_type", "column_5": "timestamp"
    })
    
    # Step 2: 应用业务清洗规则
    print("\n【Step 2】应用业务清洗规则...")
    q_clean = q.filter(
        (pl.col("timestamp") > 31536000) &
        (pl.col("behavior_type").str.strip_chars() != "") &
        (pl.col("behavior_type").is_in(["pv", "buy", "cart", "fav"]))
    )
    
    # Step 3: 统计清洗前后数据量
    print("\n【Step 3】统计清洗前后数据量...")
    total_original = pl.scan_csv("user_behavior_100M.csv", has_header=False).select(
        pl.len().alias("count")
    ).collect(engine="streaming").item(0, 0)
    
    total_clean = q_clean.select(pl.len().alias("count")).collect(engine="streaming").item(0, 0)
    
    print(f"  原始数据量：{total_original:,} 条")
    print(f"  清洗后数据量：{total_clean:,} 条")
    print(f"  剔除脏数据：{total_original - total_clean:,} 条")
    
    # Step 4: 按 behavior_type 分区写入 Parquet
    print(f"\n【Step 4】按 behavior_type 分区写入 Parquet...")
    
    behaviors = q_clean.select(pl.col("behavior_type").unique()).collect()["behavior_type"].to_list()
    partition_stats = {}
    
    for behavior in behaviors:
        partition_path = os.path.join(output_dir, f"behavior_type={behavior}")
        os.makedirs(partition_path, exist_ok=True)
        
        q_partition = q_clean.filter(pl.col("behavior_type") == behavior)
        parquet_file = os.path.join(partition_path, "data.parquet")
        
        q_partition.collect(engine="streaming").write_parquet(
            parquet_file,
            compression="zstd",
            compression_level=3
        )
        
        file_size = os.path.getsize(parquet_file)
        partition_stats[behavior] = file_size
        print(f"  ✅ {behavior}: {format_bytes(file_size)}")
    
    elapsed = time.time() - start
    
    # 汇总统计
    print("\n" + "=" * 70)
    print("📊 Polars 转换汇总")
    print("=" * 70)
    
    total_parquet_size = sum(partition_stats.values())
    original_size = os.path.getsize("user_behavior_100M.csv")
    
    print(f"""
【转换统计】
  原始 CSV 大小：{format_bytes(original_size)}
  Parquet 总大小：{format_bytes(total_parquet_size)}
  压缩比：{total_parquet_size/original_size*100:.2f}%
  空间节省：{100 - total_parquet_size/original_size*100:.2f}%
  压缩倍数：{original_size/total_parquet_size:.1f}x
  
【分区详情】""")
    
    for behavior, size in sorted(partition_stats.items(), key=lambda x: x[1], reverse=True):
        pct = size / total_parquet_size * 100
        print(f"  {behavior:>6}: {format_bytes(size):>12} ({pct:>5.1f}%)")
    
    print(f"\n⏱  总耗时：{elapsed:.2f} 秒")
    
    return {"partition_stats": partition_stats, "elapsed": elapsed}


def duckdb_to_parquet():
    """使用 DuckDB SQL 进行分区 Parquet 转换"""
    
    print("\n\n" + "🔄" * 35)
    print("【DuckDB 方案】CSV → Parquet 分区转换 (SQL)")
    print("🔄" * 35)
    
    output_dir = "clean_data_partitioned_duckdb"
    
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    
    start = time.time()
    con = duckdb.connect()
    
    # 使用 COPY + PARTITION BY 一键完成
    partition_query = f"""
    COPY (
        SELECT 
            column0 AS user_id,
            column1 AS session_id,
            column2 AS item_id,
            column3 AS behavior_type,
            column4 AS timestamp
        FROM read_csv_auto('user_behavior_100M.csv')
        WHERE column4 > 31536000
          AND TRIM(column3) != ''
          AND column3 IN ('pv', 'buy', 'cart', 'fav')
    ) TO '{output_dir}' 
    (FORMAT PARQUET, PARTITION_BY behavior_type, COMPRESSION zstd)
    """
    
    con.execute(partition_query)
    con.close()
    
    elapsed = time.time() - start
    
    # 统计分区大小
    print(f"\n【分区结果】")
    partition_stats = {}
    for behavior in ['pv', 'buy', 'cart', 'fav']:
        partition_path = os.path.join(output_dir, f"behavior_type={behavior}")
        if os.path.exists(partition_path):
            total_size = 0
            for root, dirs, files in os.walk(partition_path):
                for file in files:
                    if file.endswith('.parquet'):
                        total_size += os.path.getsize(os.path.join(root, file))
            partition_stats[behavior] = total_size
            print(f"  ✅ {behavior}: {format_bytes(total_size)}")
    
    print(f"\n⏱  总耗时：{elapsed:.2f} 秒")
    
    return {"partition_stats": partition_stats, "elapsed": elapsed}


def main():
    print("\n" + "🚀" * 35)
    print("CSV → Apache Parquet 列式存储转换")
    print("原始文件：user_behavior_100M.csv (3.42GB, ~1 亿行)")
    print("🚀" * 35)
    
    # 执行转换
    polars_result = polars_to_parquet()
    duckdb_result = duckdb_to_parquet()
    
    # 对比总结
    print("\n\n" + "=" * 70)
    print("📋 转换结果对比")
    print("=" * 70)
    
    original_size = os.path.getsize("user_behavior_100M.csv")
    polars_total = sum(polars_result["partition_stats"].values())
    duckdb_total = sum(duckdb_result["partition_stats"].values())
    
    print(f"""
【性能对比】
                    Polars          DuckDB
  转换耗时：        {polars_result["elapsed"]:>6.2f}秒        {duckdb_result["elapsed"]:>6.2f}秒
  Parquet 大小：    {format_bytes(polars_total):>10}   {format_bytes(duckdb_total):>10}
  压缩比：          {polars_total/original_size*100:>6.2f}%         {duckdb_total/original_size*100:>6.2f}%
  
【结论】
  - Polars: 适合精细控制分区逻辑
  - DuckDB: SQL 语法简洁，PARTITION BY 一键分区
  - 压缩效果：节省约 78-81% 存储空间
""")


if __name__ == "__main__":
    main()
