import threading
import queue
import time
import csv
import argparse
import random
import pandas as pd
from datetime import datetime
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer

class StreamExperimentPlatform:
    def __init__(self, produce_rate, consume_time, n_consumers, queue_size=-1, backpressure=False,
                 output_file="experiment_metrics.csv", burst_mode=False, jitter_factor=0.0,
                 burst_multiplier=5.0, burst_duration=1.0, burst_interval=5.0, data_file=None, fit_n=1000):
        self.produce_rate = produce_rate
        self.consume_time = consume_time
        self.n_consumers = n_consumers
        self.queue_size = queue_size
        self.backpressure_enabled = backpressure
        self.output_file = output_file
        self.burst_mode = burst_mode # Task 3: Burst pulse mode
        self.jitter_factor = jitter_factor # Task 3: Jitter factor
        self.data_file = data_file # Task 4: Data source file
        self.fit_n = fit_n # Task 4: Number of samples for offline fit

        # Burst parameters
        self.burst_interval = burst_interval    # Cycle duration (s)
        self.burst_duration = burst_duration    # Pulse duration (s)
        self.burst_multiplier = burst_multiplier  # Pulse intensity multiplier

        # Initialize queue
        self.data_queue = queue.Queue(maxsize=queue_size)
        self.running = False
        self.backpressure_active = False
        self.start_time = None

        # Task 4: Initialize and fit preprocessing pipeline
        self.preprocess_pipe = self.get_preprocess_pipeline()
        if self.data_file:
            self.offline_fit()

        # Statistics
        self.total_produced = 0
        self.total_consumed = 0
        self.stats_lock = threading.Lock()

        # Backpressure watermarks (if enabled)
        self.high_watermark = 0.85
        self.low_watermark = 0.30

        # Threads
        self.threads = []

    def get_preprocess_pipeline(self):
        """Task 4: 构建针对数值和类别字段的预处理 Pipeline"""
        # 数值字段处理：填补中位数 + 标准化
        numeric_features = ['category_id', 'timestamp']
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])

        # 类别字段处理：填补缺失值 + 独热编码 (提取数值特征)
        categorical_features = ['behavior_type']
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
            ('onehot', OneHotEncoder(handle_unknown='ignore', sparse_output=False))
        ])

        # 合并处理链 (ColumnTransformer 本身可以作为一个 Pipeline 的步骤)
        preprocess_pipe = ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, numeric_features),
                ('cat', categorical_transformer, categorical_features)
            ]
        )

        # 也可以构建一个单一的 Pipeline，将 ColumnTransformer 作为第一步
        # 这样可以在预处理后接一个评估器（如 LogisticRegression）
        full_pipe = Pipeline([
            ('preprocessor', preprocess_pipe)
        ])

        return full_pipe

    def offline_fit(self):
        """Task 4: 从数据集中读取前 N 行进行离线拟合"""
        print(f"[*] 正在从 {self.data_file} 读取前 {self.fit_n} 行进行离线 Fit...")
        try:
            # 仅读取前 N 行
            df = pd.read_csv(self.data_file, nrows=self.fit_n, header=None,
                             names=['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp'])

            # 拟合 Pipeline
            self.preprocess_pipe.fit(df)
            print(f"[+] 离线 Fit 完成。锁定均值与标准差。")
        except Exception as e:
            print(f"[-] 离线 Fit 失败: {e}")

    def producer(self):
        """Producer thread: generates data or reads from file at the specified rate."""
        base_rate = self.produce_rate
        max_delay = 1.0 # 1s max delay

        # Task 4: Initialize dataset reader if data_file is provided
        f = None
        reader = None
        if self.data_file:
            try:
                f = open(self.data_file, "r", encoding="utf-8")
                # 使用自定义列名，因为 CSV 无表头
                fieldnames = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']
                reader = csv.DictReader(f, fieldnames=fieldnames)
                print(f"Producer using data source: {self.data_file}")
            except Exception as e:
                print(f"Error opening data file {self.data_file}: {e}")
                f = None

        while self.running:
            try:
                elapsed_time = time.time() - self.start_time

                # Model B: Periodic Burst
                if self.burst_mode:
                    cycle_pos = elapsed_time % self.burst_interval
                    in_burst = cycle_pos < self.burst_duration
                    current_rate = base_rate * self.burst_multiplier if in_burst else base_rate
                else:
                    current_rate = base_rate

                # Model A: Uniform Random Jitter
                base_delay = 1.0 / current_rate
                if self.jitter_factor > 0:
                    prod_delay = base_delay * random.uniform(1 - self.jitter_factor, 1 + self.jitter_factor)
                else:
                    prod_delay = base_delay

                # Exponential backoff (Backpressure control)
                if self.backpressure_enabled and self.backpressure_active:
                    current_delay = min(prod_delay * 5, max_delay)
                else:
                    current_delay = prod_delay

                # Task 4: Get data from CSV reader or generate dummy data
                if reader:
                    try:
                        data = next(reader)
                        data["producer_ts"] = datetime.now() # Add timestamp for tracking
                    except StopIteration:
                        print("\nReached end of data file.")
                        break
                else:
                    data = {"ts": datetime.now(), "val": time.time()}

                # Bounded Queue (Implicit backpressure)
                try:
                    self.data_queue.put(data, block=True, timeout=1)
                    with self.stats_lock:
                        self.total_produced += 1
                except queue.Full:
                    pass

                time.sleep(current_delay)
            except Exception as e:
                print(f"Producer error: {e}")
                break

        if f:
            f.close()

    def consumer(self, consumer_id):
        """Consumer thread: processes data from the queue."""
        while self.running:
            try:
                # Try to get data with timeout to allow thread to check running flag
                data = self.data_queue.get(timeout=1)

                # Task 4: Online Transform
                if self.data_file and self.preprocess_pipe:
                    # 将单条字典数据转换为 DataFrame
                    item_df = pd.DataFrame([data])
                    # 确保数值字段是正确的类型，否则 Pipeline 会报错
                    item_df['category_id'] = pd.to_numeric(item_df['category_id'], errors='coerce')
                    item_df['timestamp'] = pd.to_numeric(item_df['timestamp'], errors='coerce')

                    # 执行转换
                    try:
                        transformed = self.preprocess_pipe.transform(item_df)

                        # 仅在 total_consumed 为 10 的倍数时打印，避免刷屏
                        if self.total_consumed % 10 == 0:
                            print(f"\n[Consumer {consumer_id}] 数据标准化对比:")
                            print(f"  原始值: cat_id={data.get('category_id')}, ts={data.get('timestamp')}, type={data.get('behavior_type')}")
                            # 打印转换后的前几个特征
                            print(f"  特征向量 (前6位): {transformed[0][:6]}")
                    except Exception as te:
                        # 避免频繁报错
                        if self.total_consumed % 50 == 0:
                            print(f"Transform error: {te}")

                time.sleep(self.consume_time) # Simulate work
                with self.stats_lock:
                    self.total_consumed += 1
                self.data_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Consumer {consumer_id} error: {e}")
                break

    def monitor(self):
        """Monitor thread: samples metrics periodically."""
        with open(self.output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "elapsed_sec", "queue_depth", "load_pct", "backpressure_on"])

            while self.running:
                depth = self.data_queue.qsize()
                load_pct = depth / self.queue_size if self.queue_size > 0 else 0
                elapsed = time.time() - self.start_time

                # Watermark Probes & Alarms
                if self.backpressure_enabled:
                    if not self.backpressure_active and load_pct >= self.high_watermark:
                        print(f"\n▲ 触发背压：下游过载 (负载 {load_pct:.1%})，强制削峰中...")
                        self.backpressure_active = True
                    elif self.backpressure_active and load_pct <= self.low_watermark:
                        print(f"\n▼ 压力缓解 (负载 {load_pct:.1%})：逐渐恢复吞吐")
                        self.backpressure_active = False

                writer.writerow([
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    round(elapsed, 2),
                    depth,
                    round(load_pct, 2),
                    self.backpressure_active
                ])
                time.sleep(0.5)

    def run(self, duration=15):
        """Runs the experiment for a specified duration in seconds."""
        print(f"Starting experiment: lambda={self.produce_rate}, t={self.consume_time}, n={self.n_consumers}, backpressure={self.backpressure_enabled}")
        self.running = True
        self.start_time = time.time()

        # Start monitor
        monitor_t = threading.Thread(target=self.monitor, daemon=True)
        monitor_t.start()
        self.threads.append(monitor_t)

        # Start consumers
        for i in range(self.n_consumers):
            t = threading.Thread(target=self.consumer, args=(i,), daemon=True)
            t.start()
            self.threads.append(t)

        # Start producer
        producer_t = threading.Thread(target=self.producer, daemon=True)
        producer_t.start()
        self.threads.append(producer_t)

        try:
            time.sleep(duration)
        except KeyboardInterrupt:
            print("\nStopping experiment...")
        finally:
            self.running = False
            # Wait a bit for threads to exit
            time.sleep(1.5)

            print("\n" + "="*30)
            print("      实验统计摘要")
            print("="*30)
            print(f"生产总量: {self.total_produced}")
            print(f"消费总量: {self.total_consumed}")
            print(f"最终队列深度: {self.data_queue.qsize()}")
            print("="*30)
            print(f"详细指标已保存至 {self.output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream Processing Backpressure Experiment")
    parser.add_argument("--rate", type=float, default=10, help="Production rate (lambda)")
    parser.add_argument("--time", type=float, default=0.2, help="Consumption time per item (t)")
    parser.add_argument("--consumers", type=int, default=1, help="Number of consumers (n)")
    parser.add_argument("--qsize", type=int, default=100, help="Queue capacity")
    parser.add_argument("--backpressure", action="store_true", help="Enable backpressure mechanism")
    parser.add_argument("--duration", type=int, default=15, help="Experiment duration in seconds")
    parser.add_argument("--output", type=str, default="experiment_metrics.csv", help="Output CSV filename")
    parser.add_argument("--burst", action="store_true", help="Enable burst pulse mode")
    parser.add_argument("--burst_multiplier", type=float, default=5.0, help="Burst pulse intensity multiplier")
    parser.add_argument("--burst_duration", type=float, default=1.0, help="Burst pulse duration in seconds")
    parser.add_argument("--burst_interval", type=float, default=5.0, help="Burst cycle duration in seconds")
    parser.add_argument("--jitter", type=float, default=0.0, help="Uniform random jitter factor (0.0 to 1.0)")
    parser.add_argument("--data", type=str, default=None, help="Path to CSV dataset for Producer")
    parser.add_argument("--fit_n", type=int, default=1000, help="Number of samples for offline Pipeline fit")

    args = parser.parse_args()

    platform = StreamExperimentPlatform(
        produce_rate=args.rate,
        consume_time=args.time,
        n_consumers=args.consumers,
        queue_size=args.qsize,
        backpressure=args.backpressure,
        output_file=args.output,
        burst_mode=args.burst,
        jitter_factor=args.jitter,
        burst_multiplier=args.burst_multiplier,
        burst_duration=args.burst_duration,
        burst_interval=args.burst_interval,
        data_file=args.data,
        fit_n=args.fit_n
    )
    platform.run(duration=args.duration)
