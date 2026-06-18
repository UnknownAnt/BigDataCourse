import os
import polars as pl
from ml_pipeline import MlDataPipeline

def run_experiment_1():
    """
    运行实验一中的业务逻辑流水线
    包含了过滤空值、修正错别字、以及基于 SHA256 对字段哈希等
    """
    print("\n" + "=" * 50)
    print("▶ 正在启动【实验一】的数据流水线（测试模式）")
    print("=" * 50)
    
    config = {
        'has_header': True,
        'filter_rules': [
            pl.col("event_id").is_not_null(),
            pl.col("event_id") != ""
        ],
        'typo_column': 'event_type',
        'typo_corrections': {
            "clik": "click", "clic": "click", "cllick": "click", "ckick": "click",
            "purchse": "purchase", "purhcase": "purchase", "puchase": "purchase",
            "serch": "search", "seach": "search", "serach": "search",
            "vew": "view", "vieew": "view", "viwe": "view",
            "logut": "logout", "log-out": "logout", "logot": "logout",
            "logn": "login", "loign": "login", "LogIn": "login", "login ": "login"
        },
        'hash_column': 'user_id',
        'new_hash_column': 'masked_user_id',
        'drop_original_hash_column': True,
        'compression': 'snappy'
    }
    
    # 模拟路径 (若真实存在大文件则直接运行)
    # 本地项目结构中 `large_data.csv` 可作为实际的输入替换
    input_file = "d:/Un_Projects/BigDataCourse/large_data.csv"
    output_file = "d:/Un_Projects/BigDataCourse/clean_data_exp1.parquet"
    
    pipeline = MlDataPipeline(input_file, output_file, config)
    
    # 检测原始文件是否存在
    if not os.path.exists(input_file):
        print(f"提示: 未发现输入大文件 '{input_file}'，此流水线将跳过。")
        return
    pipeline.run()


def run_experiment_2_and_3():
    """
    运行实验二与实验三组合的业务逻辑流水线
    包含动态改名无表头、条件规整、物理分区导出、高维精密去重等
    """
    print("\n" + "=" * 50)
    print("▶ 正在启动【实验二、三融合】的数据流水线（测试模式）")
    print("=" * 50)
    
    config = {
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
    
    input_file = "d:/Un_Projects/BigDataCourse/user_behavior_100M.csv"
    output_dir = "d:/Un_Projects/BigDataCourse/clean_data_exp23_partitioned"
    
    pipeline = MlDataPipeline(input_file, output_dir, config)
    
    if not os.path.exists(input_file):
        print(f"提示: 未发现输入大文件 '{input_file}'，此流水线将跳过。")
        return
    pipeline.run()

if __name__ == "__main__":
    print()
    print(" 🚀 欢迎使用通用型大数据清洗框架！")
    print(" 本程序封装自标准类库 MlDataPipeline")
    print()
    
    # 只调用跑您的真实大文件任务
    run_experiment_2_and_3()
    
    print("\n脚本已执行完毕。按需取消注释即可运行您的海量数据全阶段清洗。")
