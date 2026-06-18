"""
===============================================================================
实验 04 - Parquet vs CSV 性能对比测试
===============================================================================
实验目的：对比 Parquet 列式存储与 CSV 文本格式的读取性能差异

测试场景:
  1. 全量读取 - 读取所有数据
  2. 分区查询 - 只读取特定行为类型 (谓词下推)
  3. 列裁剪 - 只读取单列数据

预期结果:
  - Parquet 全量读取：快 1-2 倍
  - Parquet 分区查询：快 50-100 倍 (只读单个分区文件)
  - Parquet 列裁剪：快 5-10 倍 (只读需要的列)
===============================================================================
"""

import polars as pl
import time


def benchmark():
    print("\n" + "⚡" * 35)
    print("Parquet vs CSV 读取性能对比")
    print("⚡" * 35)
    
    results = {}
    
    # -------------------------------------------------------------------------
    # 测试 1: 全量读取
    # -------------------------------------------------------------------------
    print("\n【测试 1】全量读取所有数据")
    print("-" * 50)
    
    # CSV
    start = time.time()
    df_csv = pl.scan_csv("user_behavior_100M.csv", has_header=False).collect(engine="streaming")
    csv_time = time.time() - start
    csv_rows = df_csv.height
    print(f"  CSV:  {csv_time:.2f}秒  ({csv_rows:,} 行)")
    results['full_csv'] = csv_time
    
    # Parquet
    start = time.time()
    df_pq = pl.scan_parquet("clean_data_partitioned/**/*.parquet").collect()
    pq_time = time.time() - start
    pq_rows = df_pq.height
    print(f"  Parquet:  {pq_time:.2f}秒  ({pq_rows:,} 行)")
    results['full_parquet'] = pq_time
    print(f"  加速比：{csv_time/pq_time:.2f}x")
    
    # -------------------------------------------------------------------------
    # 测试 2: 分区查询 (只读 buy 行为)
    # -------------------------------------------------------------------------
    print("\n【测试 2】只读取 buy 行为 (谓词下推)")
    print("-" * 50)
    
    # CSV - 需扫描整个文件
    start = time.time()
    df_csv_buy = (
        pl.scan_csv("user_behavior_100M.csv", has_header=False).rename({"column_4": "behavior_type"})
        .filter(pl.col("behavior_type") == "buy")
        .collect(engine="streaming")
    )
    csv_buy_time = time.time() - start
    print(f"  CSV:  {csv_buy_time:.2f}秒  (需扫描整个 3.4GB 文件)")
    results['partition_csv'] = csv_buy_time
    
    # Parquet - 只扫描 buy 分区
    start = time.time()
    df_pq_buy = pl.scan_parquet("clean_data_partitioned/behavior_type=buy/*.parquet").collect()
    pq_buy_time = time.time() - start
    print(f"  Parquet (直接读 buy 分区):  {pq_buy_time:.2f}秒")
    results['partition_parquet'] = pq_buy_time
    print(f"  加速比：{csv_buy_time/pq_buy_time:.2f}x")
    
    # -------------------------------------------------------------------------
    # 测试 3: 列裁剪 (只读 user_id)
    # -------------------------------------------------------------------------
    print("\n【测试 3】只读取 user_id 列 (列裁剪)")
    print("-" * 50)
    
    # CSV
    start = time.time()
    df_csv_col = (
        pl.scan_csv("user_behavior_100M.csv", has_header=False)
        .select(pl.col("column_1").alias("user_id"))
        .collect(engine="streaming")
    )
    csv_col_time = time.time() - start
    print(f"  CSV:  {csv_col_time:.2f}秒")
    results['column_csv'] = csv_col_time
    
    # Parquet
    start = time.time()
    df_pq_col = (
        pl.scan_parquet("clean_data_partitioned/**/*.parquet")
        .select(pl.col("user_id"))
        .collect()
    )
    pq_col_time = time.time() - start
    print(f"  Parquet:  {pq_col_time:.2f}秒")
    results['column_parquet'] = pq_col_time
    print(f"  加速比：{csv_col_time/pq_col_time:.2f}x")
    
    # -------------------------------------------------------------------------
    # 总结
    # -------------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("📊 性能对比总结")
    print("=" * 60)
    
    print(f"""
  全量读取：  Parquet 比 CSV 快 {results['full_csv']/results['full_parquet']:.1f}x
  分区查询：  Parquet 比 CSV 快 {results['partition_csv']/results['partition_parquet']:.1f}x
  列裁剪：    Parquet 比 CSV 快 {results['column_csv']/results['column_parquet']:.1f}x

【结论】
  1. Parquet 列式存储在分析型查询场景下优势明显
  2. 分区查询可避免读取无关数据，加速 50-100 倍
  3. 机器学习训练时通常只需部分特征列，Parquet 列裁剪大幅提速
  
【应用场景】
  - 数据仓库：推荐使用 Parquet + 分区策略
  - 机器学习：Parquet 可加速数据加载 5-10 倍
  - 即席查询：分区 + 列裁剪可实现秒级响应
""")


if __name__ == "__main__":
    benchmark()
