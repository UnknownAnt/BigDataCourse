import pandas as pd

def analyze_scored_events(file_path):
    df = pd.read_csv(file_path)
    total_records = len(df)
    
    positive_count = df['predicted_label'].sum()
    positive_ratio = (positive_count / total_records) * 100
    
    avg_buy_prob = df['buy_probability'].mean()
    
    print(f"统计文件: {file_path}")
    print(f"总处理记录数: {total_records}")
    print(f"正样本(预测为购买)数量: {positive_count}")
    print(f"正样本占比: {positive_ratio:.2f}%")
    print(f"平均购买概率: {avg_buy_prob:.4f}")

if __name__ == "__main__":
    analyze_scored_events('scored_events.csv')
