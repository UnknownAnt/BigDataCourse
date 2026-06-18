import json
import time
import random
import numpy as np
from datetime import datetime
from collections import defaultdict

# 配置项 (优化后的范围，增加碰撞率以便联调展示)
LOG_FILE = "streaming_logs.jsonl"
QPS_RANGE = (10, 50)
BEHAVIORS = ['view', 'cart', 'purchase']
WEIGHTS = [0.80, 0.15, 0.05]
USER_RANGE = (10000, 11000) # 缩小范围，增加回头客
ITEM_COUNT = 500           # 缩小范围，模拟爆款商品池
ZIPF_A = 1.2  # Zipf 分布参数

# 业务状态池：追踪用户行为路径
user_history = defaultdict(set)

def get_zipf_item_id():
    """使用 Zipf 分布生成商品 ID，模拟爆款效应"""
    raw_id = np.random.zipf(a=ZIPF_A)
    return int(raw_id) % ITEM_COUNT + 1

def generate_event():
    """生成条高仿真业务日志"""
    user_id = random.randint(*USER_RANGE)
    item_id = get_zipf_item_id() 
    
    # 按照漏斗权重选择行为
    behavior = random.choices(BEHAVIORS, weights=WEIGHTS)[0]
    
    # 业务逻辑自洽性检查改进：
    if behavior in ['cart', 'purchase']:
        # 如果该用户在此次 Session 没看过该商品，则有 70% 概率降级为浏览
        # 预留 30% 概率让其“直接购买”，以模拟脚本启动前的存量浏览行为，确保统计数据不为 0
        if item_id not in user_history[user_id] and random.random() < 0.7:
            behavior = 'view'
            user_history[user_id].add(item_id)
    else:
        user_history[user_id].add(item_id)

    event = {
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "user_id": user_id,
        "item_id": item_id,
        "behavior_type": behavior,
        "session_id": f"sess_{user_id}_{random.randint(100, 999)}"
    }
    return event

def run_producer():
    """主循环：模拟高并发流式写入"""
    print(f"🚀 [Producer] 正在启动... 输出文件={LOG_FILE}")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            while True:
                count = random.randint(*QPS_RANGE)
                for _ in range(count):
                    event = generate_event()
                    f.write(json.dumps(event, ensure_ascii=False) + "\n")
                f.flush()
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 已写入 {count} 条日志...")
                time.sleep(1)
                
                # 清理过期状态防止内存溢出
                if len(user_history) > 10000:
                    user_history.clear()
                    
    except KeyboardInterrupt:
        print("\n🛑 [Producer] 检测到停止信号，正在关屏并退出...")

if __name__ == "__main__":
    run_producer()
