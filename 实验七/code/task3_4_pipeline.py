import threading
import queue
import time
import csv
import pandas as pd
import joblib

class StreamInferencePipeline:
    def __init__(self, data_file, model_file, output_file, batch_size=1, max_records=500):
        self.data_file = data_file
        self.model_file = model_file
        self.output_file = output_file
        self.batch_size = batch_size
        self.max_records = max_records

        self.data_queue = queue.Queue(maxsize=1000)
        self.running = False
        self.total_produced = 0
        self.total_consumed = 0
        self.start_time = None
        self.end_time = None

        # Load model once (Plan A)
        self.model = joblib.load(self.model_file)

    def producer(self):
        with open(self.data_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if not self.running or self.total_produced >= self.max_records:
                    break
                # row format: user_id, item_id, category_id, behavior_type, timestamp
                event = {
                    'user_id': row[0],
                    'item_id': row[1],
                    'category_id': row[2],
                    'behavior_type': row[3],
                    'timestamp': row[4]
                }
                self.data_queue.put(event)
                self.total_produced += 1
                # simulate some read delay
                time.sleep(0.001)

    def extract_features(self, event):
        ts = pd.Timestamp(int(event['timestamp']), unit='s')
        return {
            'category_id': int(event['category_id']),
            'hour': ts.hour,
            'dayofweek': ts.dayofweek
        }

    def consumer(self):
        BATCH_TIMEOUT = 0.5
        buffer = []
        last_flush = time.time()

        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp', 'predicted_label', 'buy_probability', 'error'])

            while self.running or not self.data_queue.empty() or len(buffer) > 0:
                try:
                    event = self.data_queue.get(timeout=0.1)
                    buffer.append(event)
                except queue.Empty:
                    pass

                # 触发推理的两个条件：攒满 B 条 或 超时
                if len(buffer) >= self.batch_size or (len(buffer) > 0 and time.time() - last_flush > BATCH_TIMEOUT) or (not self.running and self.data_queue.empty() and len(buffer) > 0):
                    # Process batch
                    try:
                        # Extract features for batch
                        features_list = []
                        for e in buffer:
                            ts = pd.Timestamp(int(e['timestamp']), unit='s')
                            features_list.append({
                                'category_id': int(e['category_id']),
                                'hour': ts.hour,
                                'dayofweek': ts.dayofweek
                            })
                        batch_features = pd.DataFrame(features_list)

                        preds = self.model.predict(batch_features)
                        probs = self.model.predict_proba(batch_features)

                        for i, e in enumerate(buffer):
                            e['predicted_label'] = int(preds[i])
                            e['buy_probability'] = float(probs[i][1])
                            e['error'] = ''

                            # Log output for first 15 lines if single mode
                            if self.batch_size == 1 and self.total_consumed + i < 15:
                                print(f"[{self.total_consumed + i + 1}] 预测完成 - 原始事件: user={e['user_id']} category={e['category_id']} => 预测标签: {e['predicted_label']}, 购买概率: {e['buy_probability']:.4f}")

                    except Exception as err:
                        for e in buffer:
                            e['predicted_label'] = -1
                            e['buy_probability'] = -1.0
                            e['error'] = str(err)

                    # Write to CSV
                    for e in buffer:
                        writer.writerow([e['user_id'], e['item_id'], e['category_id'], e['behavior_type'], e['timestamp'], e['predicted_label'], e['buy_probability'], e['error']])

                    self.total_consumed += len(buffer)
                    buffer.clear()
                    last_flush = time.time()

                if self.total_consumed >= self.max_records:
                    self.running = False
                    break


    def run(self):
        print(f"开始实验: batch_size={self.batch_size}, max_records={self.max_records}")
        self.running = True
        self.start_time = time.perf_counter()

        prod_thread = threading.Thread(target=self.producer)
        cons_thread = threading.Thread(target=self.consumer)

        prod_thread.start()
        cons_thread.start()

        prod_thread.join()
        cons_thread.join()

        self.end_time = time.perf_counter()

        elapsed = self.end_time - self.start_time
        throughput = self.total_consumed / elapsed
        print(f"实验完成: 耗时 {elapsed:.4f}s, 吞吐量 {throughput:.2f} 条/秒")
        return elapsed, throughput

if __name__ == "__main__":
    print("=== 任务 3：端到端打标流水线（逐条推理 B=1） ===")
    pipeline_single = StreamInferencePipeline('user_behavior_100M.csv', 'model.pkl', 'scored_events.csv', batch_size=1, max_records=1000)
    time_single, tp_single = pipeline_single.run()

    print("\n=== 任务 4：Micro-Batch 优化（B=50） ===")
    pipeline_batch = StreamInferencePipeline('user_behavior_100M.csv', 'model.pkl', 'scored_events_batch.csv', batch_size=50, max_records=1000)
    time_batch, tp_batch = pipeline_batch.run()

    print("\n=== 吞吐量对比 ===")
    print(f"逐条推理 (B=1):  总耗时 {time_single:.4f}s, 吞吐量 {tp_single:.2f} 条/秒")
    print(f"Micro-Batch (B=50): 总耗时 {time_batch:.4f}s, 吞吐量 {tp_batch:.2f} 条/秒")
