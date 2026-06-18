import time
import shutil
import polars as pl
import os
from typing import Dict, Any

from ml_pipeline import MlDataPipeline
from ml_pipeline_opt import MlDataPipelineOpt

def main() -> None:
    print("==================================================")
    print("        🚀 启动管道处理效能基准测试 Benchmark       ")
    print("==================================================")
    config: Dict[str, Any] = {
        'has_header': False,
        'rename_cols': {
            "column_1": "user_id", "column_2": "session_id", "column_3": "item_id",
            "column_4": "behavior_type", "column_5": "timestamp"
        },
        'filter_rules': [
            pl.col("timestamp") > 31536000,
            pl.col("behavior_type").str.strip_chars() != "",
            pl.col("behavior_type").is_in(["pv", "buy", "cart", "fav"])
        ],
        'dedup_subset': ["user_id", "item_id", "behavior_type", "timestamp"],
        'partition_by': 'behavior_type',
        'compression': 'zstd'
    }
    
    # 由于之前的 100M 数据集在旧版引擎中会导致 OOM 进程强制杀死中断
    # 我们使用其提取出来的 5M 条前缀行截断子集来进行时间对比
    input_file = "d:/Un_Projects/BigDataCourse/user_behavior_5M.csv" 
    
    if not os.path.exists(input_file):
        print(f"致命错误：基准数据集 {input_file} 无法找到！请更正路径以跑测。")
        return

    # --------- 1. 运行优化前版本 ---------
    old_out = "d:/Un_Projects/BigDataCourse/benchmark_test_old"
    try:
        shutil.rmtree(old_out)
    except Exception:
        pass
        
    print("\n--- [启动] 运行原未优化版本 (ml_pipeline.py) ---")
    start = time.time()
    old_pipeline = MlDataPipeline(input_file, old_out, config)
    try:
        old_pipeline.run()
        old_time = time.time() - start
    except Exception as e:
        print(f"原版本运行发生异常: {e}")
        old_time = 0
    
    # --------- 2. 运行优化后版本 ---------
    opt_out = "d:/Un_Projects/BigDataCourse/benchmark_test_opt"
    try:
        shutil.rmtree(opt_out)
    except Exception:
        pass
        
    print("\n--- [启动] 运行新优化版本 (ml_pipeline_opt.py) ---")
    start = time.time()
    opt_pipeline = MlDataPipelineOpt(input_file, opt_out, config)
    try:
        opt_pipeline.run()
        opt_time = time.time() - start
    except Exception as e:
        print(f"优化版本运行发生异常: {e}")
        opt_time = 0
    
    # --------- 3. 输出汇总对照 ---------
    print("\n" + "=" * 50)
    print("📊 Benchmark 优化效果汇总统计报告")
    print("=" * 50)
    
    print(f" * 未优化版本耗时: {old_time:.2f} 秒")
    print(f" * 优化版本耗时:   {opt_time:.2f} 秒\n")
    if old_time > 0 and opt_time > 0:
        improvement = (old_time - opt_time) / old_time * 100
        print(f" 🚀 效率总共提升:     {improvement:.2f}%")
        speedup = old_time / opt_time
        print(f" 🚀 整体速度缩减倍数: {speedup:.2f} 倍")
    else:
        print(" 警告: 部分脚本中断，无法计算最终提升率。")

if __name__ == "__main__":
    main()
