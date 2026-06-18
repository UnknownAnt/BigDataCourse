import json
import time
import os

# 配置文件名
LOG_FILE = "streaming_logs.jsonl"
WINDOW_STRETCH = 10 # 统计窗口时间（秒）

def watch_logs():
    print(f"👁️  [Watcher] 正在启动，实时分析窗口：{WINDOW_STRETCH}s...")
    print(f"🔎 监控目标：{LOG_FILE}\n")
    
    if not os.path.exists(LOG_FILE):
        print(f"⚠️  错误：文件 {LOG_FILE} 不存在，请先运行 producer.py")
        return

    # 初始化统计指标
    clicks = 0
    purchases = 0
    start_time = time.time()

    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            # 移动到文件流末尾（实现 tail -f 逻辑）
            f.seek(0, os.SEEK_END)
            
            while True:
                line = f.readline()
                if not line:
                    # 如果没有新行，略作等待
                    time.sleep(0.1)
                else:
                    # 解析新行
                    try:
                        data = json.loads(line)
                        behavior = data.get("behavior_type")
                        if behavior == "view":
                            clicks += 1
                        elif behavior == "purchase":
                            purchases += 1
                    except json.JSONDecodeError:
                        continue

                # 检查是否到达 10 秒窗口
                current_time = time.time()
                if current_time - start_time >= WINDOW_STRETCH:
                    timestamp = time.strftime("%H:%M:%S", time.localtime(current_time))
                    print(f"📊 [{timestamp}] 窗口统计 -> 点击(View): {clicks:3} | 购买(Purchase): {purchases:3}")
                    
                    # 重置计数器和计时器
                    clicks = 0
                    purchases = 0
                    start_time = current_time
                    
    except KeyboardInterrupt:
        print("\n🛑 [Watcher] 已收到停止信号，监控结束。")

if __name__ == "__main__":
    watch_logs()
