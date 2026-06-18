import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_queue_depth():
    experiments = [
        {"file": "metrics_A1.csv", "label": "A1: λ=10, t=0.2, n=1", "color": "blue"},
        {"file": "metrics_A2.csv", "label": "A2: λ=10, t=0.05, n=1", "color": "green"},
        {"file": "metrics_B1.csv", "label": "B1: λ=50, t=0.2, n=1", "color": "red"},
        {"file": "metrics_B2.csv", "label": "B2: λ=50, t=0.2, n=3", "color": "orange"},
        {"file": "metrics_C1.csv", "label": "C1: λ=20, t=0.05, n=1", "color": "purple"},
        {"file": "metrics_C2.csv", "label": "C2: λ=100, t=0.05, n=2", "color": "brown"},
    ]

    plt.figure(figsize=(12, 7))

    for exp in experiments:
        if os.path.exists(exp["file"]):
            df = pd.read_csv(exp["file"])
            plt.plot(df["elapsed_sec"], df["queue_depth"], label=exp["label"], color=exp["color"], linewidth=1.5)
        else:
            print(f"Warning: {exp['file']} not found.")

    plt.xlabel("Elapsed Time (seconds)")
    plt.ylabel("Queue Depth")
    plt.title("Queue Depth vs Time for Different Stream Processing Parameters")
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()

    output_img = "queue_depth_analysis.png"
    plt.savefig(output_img)
    print(f"Plot saved as {output_img}")
    plt.show()

if __name__ == "__main__":
    # Use non-interactive backend for server environments if needed
    # import matplotlib
    # matplotlib.use('Agg')
    plot_queue_depth()
