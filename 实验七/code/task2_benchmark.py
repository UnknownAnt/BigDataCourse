import time
import joblib
import pandas as pd

# Load 100 sample records
df = pd.read_csv('user_behavior_100M.csv', names=['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp'], nrows=100)
df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
df['hour'] = df['timestamp'].dt.hour
df['dayofweek'] = df['timestamp'].dt.dayofweek
X = df[['category_id', 'hour', 'dayofweek']]

# Preconstruct samples to eliminate DataFrame creation overhead from benchmark
samples = [pd.DataFrame([X.iloc[i]]) for i in range(100)]

# Plan A
start_A = time.perf_counter()
model_A = joblib.load('model.pkl')
for i in range(100):
    pred = model_A.predict(samples[i])
time_A = time.perf_counter() - start_A

# Plan B
start_B = time.perf_counter()
for i in range(100):
    model_B = joblib.load('model.pkl')
    pred = model_B.predict(samples[i])
time_B = time.perf_counter() - start_B

print(f"方案 A 总耗时: {time_A:.4f}s, 平均耗时: {time_A/100:.4f}s")
print(f"方案 B 总耗时: {time_B:.4f}s, 平均耗时: {time_B/100:.4f}s")
print(f"B 是 A 的 {time_B/time_A:.2f} 倍")
