import time
import csv
import joblib
import pandas as pd

class PipelineBenchmark:
    def __init__(self, input_file, model_path, max_records=1000):
        self.input_file = input_file
        self.max_records = max_records

        print("Initializing model for benchmark...")
        self.model = joblib.load(model_path)
        print("Model loaded successfully.")

        # 预先加载数据并进行特征工程，纯粹对比模型 predict 的耗时
        print("Pre-processing data...")
        self.events = []
        with open(self.input_file, 'r', encoding='utf-8') as f:
            fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
            reader = csv.DictReader(f, fieldnames=fieldnames)
            for row in reader:
                ts = pd.Timestamp(int(row['timestamp']), unit='s')
                self.events.append({
                    'category_id': int(row['category_id']),
                    'hour': ts.hour,
                    'dayofweek': ts.dayofweek
                })
                if len(self.events) >= self.max_records:
                    break
        print(f"Loaded {len(self.events)} records.")

    def run_single_inference(self):
        print("\n--- Running Single Inference (B=1) ---")
        count = 0

        start_time = time.perf_counter()

        for event in self.events:
            features = pd.DataFrame([event])

            # 逐条推理
            pred_label = self.model.predict(features)
            buy_prob = self.model.predict_proba(features)
            count += 1

        total_time = time.perf_counter() - start_time

        throughput = count / total_time if total_time > 0 else 0
        print(f"Single Inference Finished. Processed {count} records in {total_time:.4f} seconds.")
        print(f"Throughput: {throughput:.2f} records/sec")
        return total_time, throughput

    def run_micro_batch(self, batch_size=50):
        print(f"\n--- Running Micro-Batch Inference (B={batch_size}) ---")
        count = 0
        buffer = []

        start_time = time.perf_counter()

        for event in self.events:
            buffer.append(event)

            if len(buffer) >= batch_size:
                batch_features = pd.DataFrame(buffer)

                # 批量推理
                preds = self.model.predict(batch_features)
                probs = self.model.predict_proba(batch_features)

                count += len(buffer)
                buffer.clear()

        # 处理剩余不足一个 batch 的数据
        if buffer:
            batch_features = pd.DataFrame(buffer)
            preds = self.model.predict(batch_features)
            probs = self.model.predict_proba(batch_features)
            count += len(buffer)
            buffer.clear()

        total_time = time.perf_counter() - start_time

        throughput = count / total_time if total_time > 0 else 0
        print(f"Micro-Batch Finished. Processed {count} records in {total_time:.4f} seconds.")
        print(f"Throughput: {throughput:.2f} records/sec")
        return total_time, throughput

if __name__ == "__main__":
    benchmark = PipelineBenchmark(
        input_file='user_behavior_100M.csv',
        model_path='model.pkl',
        max_records=1000
    )

    # 预热
    print("Warming up...")
    benchmark.run_micro_batch(batch_size=10)

    t_single, tp_single = benchmark.run_single_inference()
    t_batch, tp_batch = benchmark.run_micro_batch(batch_size=50)

    print("\n=== Benchmark Summary ===")
    print(f"方案 | 总耗时 | 吞吐量 (条/秒)")
    print(f"逐条推理 (B=1) | {t_single:.4f} s | {tp_single:.2f}")
    print(f"Micro-Batch (B=50) | {t_batch:.4f} s | {tp_batch:.2f}")