#!/usr/bin/env python3
"""
run_pipeline.py —— M2 流处理管道统一启动入口

Milestone 2 (M2) 收官交付物：
  - CLI 参数化编排 (argparse)
  - Producer 流生成 + 混沌异常注入
  - Queue 背压缓冲
  - Consumer Micro-Batch 推理 + DLQ 死信容错
  - 打标结果 & 死信日志双路输出

用法:
  python run_pipeline.py                                          # 默认参数
  python run_pipeline.py --qps 200 --queue_limit 1000             # 自定义流量
  python run_pipeline.py --qps 1000 --chaos_rate 0.01 --max_records 600000  # 混沌测试
"""

import argparse
import csv
import os
import queue
import random
import sys
import threading
import time

import joblib
import pandas as pd


# ============================================================
# CLI 参数定义
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description="M2 流处理管道 —— 联调测试与流处理阶段交付",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_pipeline.py
  python run_pipeline.py --qps 200 --queue_limit 1000 --max_records 50000
  python run_pipeline.py --qps 1000 --chaos_rate 0.01 --max_records 600000
        """,
    )

    parser.add_argument("--qps", type=int, default=100,
                        help="生产者每秒生成的数据条数 (默认: 100)")
    parser.add_argument("--queue_limit", type=int, default=500,
                        help="内存队列的最大容量, 背压触发阈值 (默认: 500)")
    parser.add_argument("--batch_size", type=int, default=50,
                        help="消费者 Micro-Batch 推理批次大小 (默认: 50)")
    parser.add_argument("--batch_timeout", type=float, default=0.5,
                        help="Micro-Batch 超时兜底时间/秒 (默认: 0.5)")
    parser.add_argument("--max_records", type=int, default=10000,
                        help="本次运行处理的最大记录数 (默认: 10000)")
    parser.add_argument("--chaos_rate", type=float, default=0.0,
                        help="混沌测试中注入异常数据的概率 0.0~1.0 (默认: 0.0)")
    parser.add_argument("--model_path", type=str, default="model.pkl",
                        help="序列化 Pipeline 模型文件路径 (默认: model.pkl)")
    parser.add_argument("--data_file", type=str,
                        default="../共享数据/user_behavior_100M.csv",
                        help="输入数据源 CSV 文件路径")
    parser.add_argument("--output_dir", type=str, default="outputs",
                        help="输出目录 (默认: outputs)")
    parser.add_argument("--output_file", type=str, default="scored_events.csv",
                        help="打标结果输出文件名 (默认: scored_events.csv)")

    return parser.parse_args()


# ============================================================
# M2 流处理管线
# ============================================================
class M2Pipeline:
    """Milestone 2 流批一体数据管道"""

    def __init__(self, args):
        self.args = args

        # 输出目录
        os.makedirs(args.output_dir, exist_ok=True)
        self.output_path = os.path.join(args.output_dir, args.output_file)
        self.dead_letter_path = os.path.join(args.output_dir, "dead_letter.log")

        # 数据队列 (带容量上限 → 背压)
        self.data_queue = queue.Queue(maxsize=args.queue_limit)

        # 控制标志
        self.running = False

        # 统计计数器
        self.produced = 0
        self.consumed = 0
        self.dlq_count = 0
        self.backpressure_events = 0

        # ── 初始化隔离原则：循环外一次性加载模型 ──
        print(f"[Init] 加载模型: {args.model_path} ...")
        t0 = time.perf_counter()
        self.model = joblib.load(args.model_path)
        print(f"[Init] 模型加载完成, 耗时 {time.perf_counter() - t0:.2f}s")

    # ========================================================
    # Producer
    # ========================================================
    def producer(self):
        """数据生产者：从 CSV 流式读取，按 QPS 控速，注入混沌异常"""
        sleep_interval = 1.0 / self.args.qps if self.args.qps > 0 else 0.0
        chaos_rate = self.args.chaos_rate

        if not os.path.exists(self.args.data_file):
            print(f"[Producer] 错误: 数据文件不存在 → {self.args.data_file}")
            self.running = False
            return

        try:
            with open(self.args.data_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not self.running or self.produced >= self.args.max_records:
                        break

                    # ── 混沌异常注入 ──
                    if chaos_rate > 0 and random.random() < chaos_rate:
                        event = self._generate_chaos_event()
                    else:
                        # 正常解析
                        try:
                            event = {
                                "user_id": row[0],
                                "item_id": row[1],
                                "category_id": row[2],
                                "behavior_type": row[3],
                                "timestamp": row[4],
                            }
                        except IndexError:
                            event = self._generate_chaos_event()

                    # ── 推入队列 (带背压) ──
                    while self.running:
                        try:
                            self.data_queue.put(event, timeout=0.01)
                            self.produced += 1
                            break
                        except queue.Full:
                            # 背压触发：队列满，Producer 阻塞等待
                            self.backpressure_events += 1
                            if self.backpressure_events <= 3 or self.backpressure_events % 50 == 0:
                                print(f"[Backpressure] 队列已满! Producer 等待中... "
                                      f"(第 {self.backpressure_events} 次, Queue≈{self.data_queue.qsize()})")

                    if sleep_interval > 0:
                        time.sleep(sleep_interval)

        except Exception as e:
            print(f"[Producer] 异常退出: {e}")
        finally:
            self.running = False
            print(f"[Producer] 结束. 共生产 {self.produced} 条, "
                  f"背压触发 {self.backpressure_events} 次")

    def _generate_chaos_event(self):
        """生成混沌异常事件"""
        chaos_type = random.choice(["missing_field", "bad_type", "empty_row"])
        if chaos_type == "missing_field":
            return {"user_id": "chaos", "item_id": "chaos"}
        elif chaos_type == "bad_type":
            return {
                "user_id": "chaos",
                "item_id": "chaos",
                "category_id": "NOT_A_NUMBER",
                "behavior_type": "pv",
                "timestamp": "INVALID_TS",
            }
        else:  # empty_row
            return {}

    # ========================================================
    # Consumer
    # ========================================================
    def consumer(self):
        """数据消费者：Micro-Batch 缓冲 + DLQ 双层容错 + 模型推理"""
        buffer = []
        last_flush = time.time()

        with open(self.output_path, "w", newline="", encoding="utf-8") as out_f, \
             open(self.dead_letter_path, "w", encoding="utf-8") as dlq_f:

            writer = csv.writer(out_f)
            writer.writerow([
                "user_id", "item_id", "category_id", "behavior_type",
                "timestamp", "predicted_label", "buy_probability", "error",
            ])

            while self.running or not self.data_queue.empty() or buffer:
                # 从队列获取事件
                try:
                    event = self.data_queue.get(timeout=0.1)
                    buffer.append(event)
                except queue.Empty:
                    pass

                # ── 触发推理的三个条件 ──
                should_flush = (
                    len(buffer) >= self.args.batch_size
                    or (buffer and time.time() - last_flush > self.args.batch_timeout)
                    or (not self.running and self.data_queue.empty() and buffer)
                )

                if not should_flush:
                    continue

                valid_buffer = []

                # ══════════════════════════════════════
                # DLQ 层 1：特征提取容错
                # ══════════════════════════════════════
                for e in buffer:
                    try:
                        ts = pd.Timestamp(int(e["timestamp"]), unit="s")
                        e["_features"] = {
                            "category_id": int(e["category_id"]),
                            "hour": ts.hour,
                            "dayofweek": ts.dayofweek,
                        }
                        valid_buffer.append(e)
                    except Exception as feat_err:
                        self.dlq_count += 1
                        dlq_f.write(
                            f"{time.strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"FEATURE_ERROR | {feat_err} | "
                            f"raw_data={e}\n"
                        )
                        dlq_f.flush()
                        print(f"  [DLQ] 特征提取失败 → dead_letter.log | {feat_err}")

                if valid_buffer:
                    try:
                        # ══════════════════════════════════════
                        # 批量模型推理
                        # ══════════════════════════════════════
                        features_df = pd.DataFrame(
                            [e["_features"] for e in valid_buffer]
                        )
                        preds = self.model.predict(features_df)
                        probs = self.model.predict_proba(features_df)

                        for i, e in enumerate(valid_buffer):
                            e["predicted_label"] = int(preds[i])
                            e["buy_probability"] = float(probs[i][1])
                            e["error"] = ""

                    except Exception as infer_err:
                        # ══════════════════════════════════════
                        # DLQ 层 2：推理失败容错
                        # ══════════════════════════════════════
                        self.dlq_count += len(valid_buffer)
                        for e in valid_buffer:
                            e["predicted_label"] = -1
                            e["buy_probability"] = -1.0
                            e["error"] = str(infer_err)
                        dlq_f.write(
                            f"{time.strftime('%Y-%m-%d %H:%M:%S')} | "
                            f"INFERENCE_ERROR | {infer_err} | "
                            f"batch_size={len(valid_buffer)}\n"
                        )
                        dlq_f.flush()
                        print(f"  [DLQ] 批量推理失败 → dead_letter.log | {infer_err}")

                # ── 结果持久化 ──
                for e in valid_buffer:
                    writer.writerow([
                        e.get("user_id", ""),
                        e.get("item_id", ""),
                        e.get("category_id", ""),
                        e.get("behavior_type", ""),
                        e.get("timestamp", ""),
                        e.get("predicted_label", -1),
                        e.get("buy_probability", -1.0),
                        e.get("error", ""),
                    ])

                self.consumed += len(valid_buffer)
                buffer.clear()
                last_flush = time.time()

                # 进度输出
                if self.consumed % 500 == 0 or self.consumed >= self.args.max_records:
                    elapsed = time.perf_counter() - self.start_time
                    rate = self.consumed / elapsed if elapsed > 0 else 0
                    print(f"[Progress] 已消费 {self.consumed}/{self.args.max_records} 条 "
                          f"| 吞吐 {rate:.1f} 条/s | DLQ累计 {self.dlq_count} | "
                          f"Queue≈{self.data_queue.qsize()}")

                if self.consumed >= self.args.max_records:
                    self.running = False
                    break

        print(f"[Consumer] 结束. 共消费 {self.consumed} 条, "
              f"死信拦截 {self.dlq_count} 条")

    # ========================================================
    # 启动入口
    # ========================================================
    def run(self):
        print("=" * 60)
        print("M2 流处理管道 启动")
        print("=" * 60)
        print(f"  QPS: {self.args.qps}")
        print(f"  Queue 容量: {self.args.queue_limit}")
        print(f"  Batch Size: {self.args.batch_size}")
        print(f"  Batch Timeout: {self.args.batch_timeout}s")
        print(f"  最大记录数: {self.args.max_records}")
        print(f"  混沌率: {self.args.chaos_rate * 100:.1f}%")
        print(f"  数据源: {self.args.data_file}")
        print(f"  输出: {self.output_path}")
        print(f"  死信日志: {self.dead_letter_path}")
        print("=" * 60)

        self.running = True
        self.start_time = time.perf_counter()

        prod_thread = threading.Thread(target=self.producer, name="Producer")
        cons_thread = threading.Thread(target=self.consumer, name="Consumer")

        prod_thread.start()
        cons_thread.start()

        prod_thread.join()
        cons_thread.join()

        elapsed = time.perf_counter() - self.start_time
        throughput = self.consumed / elapsed if elapsed > 0 else 0

        print("\n" + "=" * 60)
        print("M2 管道运行完成")
        print("=" * 60)
        print(f"  总耗时:         {elapsed:.2f}s")
        print(f"  生产记录数:     {self.produced}")
        print(f"  消费记录数:     {self.consumed}")
        print(f"  吞吐量:         {throughput:.2f} 条/s")
        print(f"  死信拦截数:     {self.dlq_count}")
        print(f"  背压触发次数:   {self.backpressure_events}")
        print(f"  打标结果:       {self.output_path}")
        print(f"  死信日志:       {self.dead_letter_path}")
        print("=" * 60)

        return elapsed, throughput


# ============================================================
# main
# ============================================================
if __name__ == "__main__":
    args = parse_args()
    pipeline = M2Pipeline(args)
    pipeline.run()
