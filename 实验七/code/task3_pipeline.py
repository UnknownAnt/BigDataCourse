import time
import csv
import threading
import queue
import joblib
import pandas as pd

class Pipeline:
    def __init__(self, input_file, output_file, model_path, max_records=15):
        self.input_file = input_file
        self.output_file = output_file
        self.max_records = max_records

        self.data_queue = queue.Queue(maxsize=100)
        self.running = True

        # 任务2的结论：模型在循环外加载，常驻内存（方案A）
        print("Initializing model...")
        self.model = joblib.load(model_path)
        print("Model loaded successfully.")

    def producer(self):
        count = 0
        try:
            with open(self.input_file, 'r', encoding='utf-8') as f:
                # 文件没有表头，我们需要手动指定字段名
                fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
                reader = csv.DictReader(f, fieldnames=fieldnames)

                for row in reader:
                    if not self.running or count >= self.max_records:
                        break

                    self.data_queue.put(row)
                    count += 1
                    # 模拟真实流式数据，可配置的推入速率（这里稍微休眠一下避免过快）
                    time.sleep(0.01)

        except Exception as e:
            print(f"Producer error: {e}")
        finally:
            self.running = False
            print(f"Producer finished. Total produced: {count}")

    def consumer(self):
        count = 0
        # 准备写入 CSV
        with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp', 'predicted_label', 'buy_probability', 'error']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            while self.running or not self.data_queue.empty():
                try:
                    # 使用 timeout 避免消费者永久阻塞
                    event = self.data_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                try:
                    # 1. 特征提取与类型转换
                    ts = pd.Timestamp(int(event['timestamp']), unit='s')
                    features = pd.DataFrame([{
                        'category_id': int(event['category_id']),
                        'hour': ts.hour,
                        'dayofweek': ts.dayofweek
                    }])

                    # 2. 模型推理
                    # predict 返回类别标签，predict_proba 返回概率分布
                    pred_label = int(self.model.predict(features)[0])
                    buy_prob = float(self.model.predict_proba(features)[0][1])

                    # 3. 结果回流
                    event['predicted_label'] = pred_label
                    event['buy_probability'] = buy_prob
                    event['error'] = ''

                    # 4. 终端展示
                    print(f"[{count+1}] 预测完成 - 原始事件: user={event['user_id']} category={event['category_id']} => 预测标签: {pred_label}, 购买概率: {buy_prob:.4f}")

                except Exception as e:
                    # 异常容错机制：标记为推理失败
                    event['predicted_label'] = -1
                    event['buy_probability'] = -1.0
                    event['error'] = str(e)
                    print(f"[{count+1}] 推理异常 - user={event.get('user_id')} category={event.get('category_id')} => 错误: {str(e)}")

                try:
                    # 5. 结果持久化
                    writer.writerow(event)

                    self.data_queue.task_done()
                    count += 1

                    if count >= self.max_records:
                        self.running = False
                        break

                except Exception as e:
                    print(f"Consumer write error: {e}")

        print(f"Consumer finished. Total consumed: {count}")

    def run(self):
        t_producer = threading.Thread(target=self.producer)
        t_consumer = threading.Thread(target=self.consumer)

        t_producer.start()
        t_consumer.start()

        t_producer.join()
        t_consumer.join()
        print("Pipeline execution completed.")

if __name__ == "__main__":
    pipeline = Pipeline(
        input_file='user_behavior_100M.csv',
        output_file='scored_events.csv',
        model_path='model.pkl',
        max_records=500  # 处理 500 条数据以满足最终验证要求
    )
    pipeline.run()
