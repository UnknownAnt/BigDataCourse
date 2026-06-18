import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def plot_task3_4groups():
    # Load data
    files = {
        "None": "metrics_1_none.csv",
        "Jitter": "metrics_2_jitter.csv",
        "Mild Burst": "metrics_3_mild.csv",
        "Intense Burst": "metrics_4_intense.csv"
    }

    data = {}
    for name, file in files.items():
        if os.path.exists(file):
            data[name] = pd.read_csv(file)

    # Plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 7))
    colors = {"None": "blue", "Jitter": "green", "Mild Burst": "orange", "Intense Burst": "red"}

    # 1. Left Subplot: Queue Depth Time Series
    for name, df in data.items():
        ax1.plot(df["elapsed_sec"], df["queue_depth"], label=f"{name}", color=colors[name], alpha=0.8, linewidth=1.5)

    ax1.set_title("Queue Depth Time Series (4 Disturbance Groups)", fontsize=14)
    ax1.set_xlabel("Elapsed Time (s)", fontsize=12)
    ax1.set_ylabel("Queue Depth", fontsize=12)
    ax1.legend()
    ax1.grid(True, linestyle='--', alpha=0.6)

    # 2. Right Subplot: Queue Depth Distribution (Histogram)
    # Determine common bins
    all_depths = pd.concat([df["queue_depth"] for df in data.values()])
    max_depth = all_depths.max()
    bins = range(0, int(max_depth) + 10, max(1, int(max_depth/20)))

    for name, df in data.items():
        sns.histplot(df["queue_depth"], bins=bins, ax=ax2, label=name, color=colors[name], alpha=0.4, element="step")

    ax2.set_title("Queue Depth Distribution (Log Scale)", fontsize=14)
    ax2.set_xlabel("Queue Depth", fontsize=12)
    ax2.set_ylabel("Frequency (Count, Log Scale)", fontsize=12)
    ax2.set_yscale('log')
    ax2.set_ylim(0.5, None) # Ensure log scale visibility
    ax2.legend()
    ax2.grid(True, which="both", linestyle='--', alpha=0.3)

    plt.tight_layout()

    # Save to assets
    if not os.path.exists("assets"):
        os.makedirs("assets")
    plt.savefig("assets/task3_disturbance_comparison_4groups.png", dpi=300)
    plt.show()

if __name__ == "__main__":
    plot_task3_4groups()
