import joblib
import pandas as pd
import json

def validate_model():
    print("开始加载模型文件 model.pkl ...")
    # 反序列化加载模型
    model = joblib.load('model.pkl')
    print("模型加载成功！")
    
    # 构造 3 条模拟样本数据（根据特征工程的需要，包含 category_id, hour, dayofweek）
    sample_data = [
        {'category_id': 2520377, 'hour': 10, 'dayofweek': 2},
        {'category_id': 4181361, 'hour': 22, 'dayofweek': 5},
        {'category_id': 149192,  'hour': 8,  'dayofweek': 0}
    ]
    df_samples = pd.DataFrame(sample_data)
    
    print("\n待预测的样本数据：")
    print(df_samples)
    
    # 执行推理
    print("\n正在执行推理 (predict & predict_proba) ...")
    predictions = model.predict(df_samples)
    probabilities = model.predict_proba(df_samples)
    
    # 打印结果
    print("\n推理结果：")
    for i, row in df_samples.iterrows():
        print(f"样本 {i+1} [类目: {row['category_id']}, 小时: {row['hour']}, 星期: {row['dayofweek']}]")
        print(f"  -> 预测标签 (predict): {predictions[i]}")
        print(f"  -> 概率分布 (predict_proba): 负样本(不购买)={probabilities[i][0]:.4f}, 正样本(购买)={probabilities[i][1]:.4f}")

if __name__ == "__main__":
    validate_model()
