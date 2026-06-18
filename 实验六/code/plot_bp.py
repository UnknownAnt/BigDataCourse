import pandas as pd
import matplotlib.pyplot as plt

def plot_backpressure_comparison():
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Group 1: λ=20, t=0.5
    df_no1 = pd.read_csv("metrics_no_bp.csv")
    df_with1 = pd.read_csv("metrics_with_bp.csv")
    ax1.plot(df_no1["elapsed_sec"], df_no1["queue_depth"], label="Group 1 (λ=20, t=0.5): No BP", color="red", linestyle="--")
    ax1.plot(df_with1["elapsed_sec"], df_with1["queue_depth"], label="Group 1 (λ=20, t=0.5): With BP", color="green")
    ax1.set_title("Case 1: Moderate Overflow")
    ax1.set_ylabel("Queue Depth")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=85, color='gray', linestyle=':', alpha=0.5)
    ax1.axhline(y=30, color='gray', linestyle=':', alpha=0.5)

    # Group 2: λ=50, t=0.2
    df_no2 = pd.read_csv("metrics_no_bp_v2.csv")
    df_with2 = pd.read_csv("metrics_with_bp_v2.csv")
    ax2.plot(df_no2["elapsed_sec"], df_no2["queue_depth"], label="Group 2 (λ=50, t=0.2): No BP", color="red", linestyle="--")
    ax2.plot(df_with2["elapsed_sec"], df_with2["queue_depth"], label="Group 2 (λ=50, t=0.2): With BP", color="green")
    ax2.set_title("Case 2: Severe Overflow")
    ax2.set_xlabel("Elapsed Time (s)")
    ax2.set_ylabel("Queue Depth")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=85, color='gray', linestyle=':', alpha=0.5)
    ax2.axhline(y=30, color='gray', linestyle=':', alpha=0.5)
    
    plt.tight_layout()
    output_path = "backpressure_comparison.png"
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")

if __name__ == "__main__":
    plot_backpressure_comparison()
