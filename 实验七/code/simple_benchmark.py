import time
import joblib
import pandas as pd

def run_benchmark():
    print("Loading model...")
    model = joblib.load('model.pkl')
    
    # 准备 1000 条测试数据
    dummy_data = {'category_id': 100, 'hour': 12, 'dayofweek': 3}
    records = [dummy_data for _ in range(1000)]
    df_records = pd.DataFrame(records)
    
    # 预热
    model.predict(df_records.iloc[:10])
    
    print("\n--- Running Single Inference (B=1) ---")
    start_time = time.perf_counter()
    for i in range(1000):
        # 模拟逐条处理
        single_df = pd.DataFrame([records[i]])
        model.predict(single_df)
    single_time = time.perf_counter() - start_time
    single_throughput = 1000 / single_time
    
    print(f"Total time: {single_time:.4f} s")
    print(f"Throughput: {single_throughput:.2f} records/s")
    
    print("\n--- Running Micro-Batch Inference (B=50) ---")
    start_time = time.perf_counter()
    batch_size = 50
    for i in range(0, 1000, batch_size):
        # 模拟批量处理
        batch_df = pd.DataFrame(records[i:i+batch_size])
        model.predict(batch_df)
    batch_time = time.perf_counter() - start_time
    batch_throughput = 1000 / batch_time
    
    print(f"Total time: {batch_time:.4f} s")
    print(f"Throughput: {batch_throughput:.2f} records/s")
    
    print("\n=== Benchmark Summary ===")
    print(f"方案 | 总耗时 | 吞吐量 (条/秒)")
    print(f"逐条推理 (B=1) | {single_time:.4f} s | {single_throughput:.2f}")
    print(f"Micro-Batch (B=50) | {batch_time:.4f} s | {batch_throughput:.2f}")

if __name__ == '__main__':
    run_benchmark()