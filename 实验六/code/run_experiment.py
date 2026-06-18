import threading
import queue
import time
import csv
from datetime import datetime
from producer import run_producer

# 核心容器：内存通信桥梁
data_queue = queue.Queue(maxsize=100)  # 有界队列，最大深度100
running = True  # 控制线程运行的标志

# 统计数据
stats = {
    "produced": 0,
    "consumed": 0,
    "final_depth": 0
}

def producer_thread(produce_rate):
    """生产者线程：按指定速率生成数据"""
    global stats
    while running:
        if not data_queue.full():
            data = {"timestamp": datetime.now(), "value": time.time()}
            data_queue.put(data)
            stats["produced"] += 1
            time.sleep(1 / produce_rate)  # 控制生产速率

def consumer_thread(consume_time):
    """消费者线程：按指定耗时处理数据"""
    global stats
    while running:
        try:
            data = data_queue.get(timeout=1)  # 从队列取数据
            time.sleep(consume_time)  # 模拟处理耗时
            data_queue.task_done()
            stats["consumed"] += 1
        except queue.Empty:
            continue

def monitor_thread():
    """监控线程：定期记录队列状态"""
    with open("experiment_metrics.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "elapsed_sec", "queue_depth"])
        start_time = time.time()
        while running:
            depth = data_queue.qsize()
            elapsed = time.time() - start_time
            writer.writerow([datetime.now(), elapsed, depth])
            time.sleep(0.5)

# 启动线程
producer = threading.Thread(target=producer_thread, args=(10,))  # 生产速率10条/秒
consumer = threading.Thread(target=consumer_thread, args=(0.2,))  # 每条耗时0.2秒
monitor = threading.Thread(target=monitor_thread)

producer.start()
consumer.start()
monitor.start()

# 等待线程结束
try:
    producer.join()
    consumer.join()
    monitor.join()
except KeyboardInterrupt:
    running = False
    producer.join()
    consumer.join()
    monitor.join()

# 打印统计摘要
stats["final_depth"] = data_queue.qsize()
print("\n实验统计摘要：")
print(f"生产总量: {stats['produced']}")
print(f"消费总量: {stats['consumed']}")
print(f"最终队列深度: {stats['final_depth']}")