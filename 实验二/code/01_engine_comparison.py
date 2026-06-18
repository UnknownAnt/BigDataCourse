"""
===============================================================================
实验 01 - 三大计算引擎性能对比 (Pandas vs DuckDB vs Polars)
===============================================================================
实验目的：对比三种不同计算引擎处理 3.4GB CSV 文件的性能差异

核心概念:
  - Pandas: 预全量加载 (Eager Evaluation) - 适合小数据
  - DuckDB: 核外计算 (Out-of-core) - SQL 友好
  - Polars: 延迟计算 + 流式执行 (Lazy API + Streaming) - 性能最优

数据文件：user_behavior_100M.csv (3.42 GB, 100,150,807 行)
查询任务：GROUP BY behavior_type COUNT(*)
===============================================================================
"""

import time
import psutil
import os


def get_memory_mb():
    """返回当前进程内存占用 (MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def polars_approach():
    """Polars Lazy API + Streaming 方案"""
    import polars as pl
    
    print("\n" + "=" * 70)
    print("【Polars 方案】Lazy API + Streaming 引擎")
    print("=" * 70)
    
    mem_before = get_memory_mb()
    start = time.time()
    
    # 懒加载 CSV 并重命名
    q = pl.scan_csv("user_behavior_100M.csv", has_header=False).rename({
        "column_1": "user_id",
        "column_2": "session_id",
        "column_3": "item_id",
        "column_4": "behavior_type",
        "column_5": "timestamp"
    })
    
    # 构建聚合查询
    result = (
        q.group_by("behavior_type")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    
    # 流式执行
    df_result = result.collect(engine="streaming")
    
    elapsed = time.time() - start
    mem_after = get_memory_mb()
    
    print(f"\nbehavior_type 分布统计:")
    print(df_result)
    print(f"\n⏱  耗时：{elapsed:.2f} 秒")
    print(f"💾  内存变化：{mem_before:.2f} MB → {mem_after:.2f} MB")
    
    return {"method": "Polars", "time": elapsed, "memory_delta": mem_after - mem_before}


def duckdb_approach():
    """DuckDB SQL 方案"""
    import duckdb
    
    print("\n" + "=" * 70)
    print("【DuckDB 方案】SQL + 核外计算")
    print("=" * 70)
    
    mem_before = get_memory_mb()
    start = time.time()
    
    con = duckdb.connect()
    
    # 直接对 CSV 执行 SQL
    query = """
    SELECT 
        column3 AS behavior_type,
        COUNT(*) AS count
    FROM read_csv_auto('user_behavior_100M.csv')
    GROUP BY behavior_type
    ORDER BY count DESC
    """
    
    result = con.execute(query).fetchdf()
    con.close()
    
    elapsed = time.time() - start
    mem_after = get_memory_mb()
    
    print(f"\nbehavior_type 分布统计:")
    print(result.to_string())
    print(f"\n⏱  耗时：{elapsed:.2f} 秒")
    print(f"💾  内存变化：{mem_before:.2f} MB → {mem_after:.2f} MB")
    
    return {"method": "DuckDB", "time": elapsed, "memory_delta": mem_after - mem_before}


def main():
    print("\n" + "🚀" * 35)
    print("三大计算引擎性能对比")
    print("数据文件：user_behavior_100M.csv (3.42GB, ~1 亿行)")
    print("🚀" * 35)
    
    results = []
    
    # 执行 Polars 方案
    results.append(polars_approach())
    
    # 执行 DuckDB 方案
    results.append(duckdb_approach())
    
    # 对比总结
    print("\n" + "=" * 70)
    print("📊 性能对比总结")
    print("=" * 70)
    
    print(f"\n{'方案':<20} {'耗时 (秒)':<15} {'内存增量 (MB)':<15}")
    print("-" * 50)
    for r in results:
        print(f"{r['method']:<20} {r['time']:<15.2f} {r['memory_delta']:<15.2f}")
    
    winner = min(results, key=lambda x: x['time'])
    print("-" * 50)
    print(f"🏆 最快方案：{winner['method']} ({winner['time']:.2f} 秒)")
    
    print("\n【结论】")
    print("  - Polars: 最快，适合 Python 原生 API")
    print("  - DuckDB: SQL 友好，性能稳定")
    print("  - Pandas: 不推荐用于大数据（会 OOM）")


if __name__ == "__main__":
    main()
