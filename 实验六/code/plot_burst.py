import pandas as pd
import matplotlib.pyplot as plt

def plot_burst_analysis():
    # Load data
    df_u = pd.read_csv("metrics_uniform.csv")
    df_b = pd.read_csv("metrics_burst.csv")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # Plot 1: Queue Depth vs Time
    ax1.plot(df_u["elapsed_sec"], df_u["queue_depth"], label="Uniform Traffic (λ=10)", color="blue", alpha=0.7)
    ax1.plot(df_b["elapsed_sec"], df_b["queue_depth"], label="Burst Pulse Traffic (Avg λ≈18)", color="orange", linewidth=2)
    ax1.set_title("Queue Depth: Uniform vs Burst Traffic")
    ax1.set_xlabel("Elapsed Time (s)")
    ax1.set_ylabel("Queue Depth")
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Histogram of Queue Depth Distribution
    ax2.hist(df_u["queue_depth"], bins=15, alpha=0.5, label="Uniform", color="blue", density=True)
    ax2.hist(df_b["queue_depth"], bins=15, alpha=0.5, label="Burst", color="orange", density=True)
    ax2.set_title("Queue Depth Distribution (Histogram)")
    ax2.set_xlabel("Queue Depth")
    ax2.set_ylabel("Probability Density")
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path = "burst_traffic_analysis.png"
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")

if __name__ == "__main__":
    plot_burst_analysis()
