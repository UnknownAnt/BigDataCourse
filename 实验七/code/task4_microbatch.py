import time
import csv
import threading
import queue
import joblib
import pandas as pd

class PipelineMicroBatch:
    def __init__(self, input_file, output_file, model_path, max_records=1000):
        self.input_file = input_file
        self.output_file = output_file
        self.max_records = max_records

        self.data_queue = queue.Queue(maxsize=2000)
        self.running = True

        print("Initializing model...")
        self.model = joblib.load(model_path)
        print("Model loaded successfully.")

    def producer(self):
        count = 0
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
                reader = csv.DictReader(f, fieldnames=fieldnames)

                for row in reader:
                    if not self.running or count >= self.max_records:
                        break

                    self.data_queue.put(row)
                    count += 1
                    # 生产者推入数据，稍微休眠模拟真实流
                    time.sleep(0.001)

        except Exception as e:
            print(f"Producer error: {e}")
        finally:
            self.running = False
            print(f"Producer finished. Total produced: {count}")

    def extract_features_batch(self, buffer):
        features_list = []
        for event in buffer:
            ts = pd.Timestamp(int(event['timestamp']), unit='s')
            features_list.append({
                'category_id': int(event['category_id']),
                'hour': ts.hour,
                'dayofweek': ts.dayofweek
            })
        return pd.DataFrame(features_list)

    def consumer(self):
        count = 0
        BATCH_SIZE = 50
        BATCH_TIMEOUT = 0.5  # 秒
        buffer = []
        last_flush = time.time()

        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp', 'predicted_label', 'buy_probability', 'error']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            while self.running or not self.data_queue.empty() or buffer:
                try:
                    # 如果生产者已停止且队列为空，强制刷出剩余的 buffer
                    if not self.running and self.data_queue.empty() and buffer:
                        event = None
                    else:
                        event = self.data_queue.get(timeout=0.1)
                        buffer.append(event)

                except queue.Empty:
                    pass

                # 触发推理的两个条件：攒满 B 条 或 超时 或 生产者结束且队列为空(收尾)
                if len(buffer) >= BATCH_SIZE or (buffer and time.time() - last_flush > BATCH_TIMEOUT) or (not self.running and self.data_queue.empty() and buffer):
                    try:
                        batch_features = self.extract_features_batch(buffer)

                        # 批量推理
                        preds = self.model.predict(batch_features)
                        probs = self.model.predict_proba(batch_features)

                        # 结果回流
                        for i, ev in enumerate(buffer):
                            ev['predicted_label'] = int(preds[i])
                            ev['buy_probability'] = float(probs[i][1])
                            ev['error'] = ''

                            # 打印部分结果以供观察
                            if i < 2 or i >= len(buffer) - 2:
                                print(f"[Batch] 预测完成 - user={ev['user_id']} category={ev['category_id']} => 预测标签: {ev['predicted_label']}, 购买概率: {ev['buy_probability']:.4f}")

                    except Exception as e:
                        # 批量异常容错
                        print(f"Batch inference error: {e}")
                        for ev in buffer:
                            ev['predicted_label'] = -1
                            ev['buy_probability'] = -1.0
                            ev['error'] = str(e)

                    # 输出 / 持久化 buffer 中的所有事件
                    for ev in buffer:
                        writer.writerow(ev)
                        if 'user_id' in ev:  # 确保是从队列中取出的正常事件
                            self.data_queue.task_done()
                        count += 1

                    buffer.clear()
                    last_flush = time.time()

                    if count >= self.max_records:
                        self.running = False
                        break

        print(f"Consumer finished. Total consumed: {count}")

    def run(self):
        t_producer = threading.Thread(target=self.producer)
        t_consumer = threading.Thread(target=self.consumer)

        start_time = time.perf_counter()

        t_producer.start()
        t_consumer.start()

        t_producer.join()
        t_consumer.join()

        total_time = time.perf_counter() - start_time
        print(f"Pipeline execution completed in {total_time:.4f} seconds.")
        return total_time

if __name__ == "__main__":
    pipeline = PipelineMicroBatch(
        input_file='user_behavior_100M.csv',
        output_file='scored_events_microbatch.csv',
        model_path='model.pkl',
        max_records=1000  # 处理 1000 条数据用于验证 Micro-Batch 性能
    )
    pipeline.run()
