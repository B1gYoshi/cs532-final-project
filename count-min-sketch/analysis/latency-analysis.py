import glob
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

files = sorted(glob.glob("../logs/latency*"))
input_rate = []
mean_latencies = []
top_latencies = []

for filepath in files:
    # Load all latency values (in ms)
    with open(filepath, encoding="utf-8") as f:
        values = [float(line) for line in f if line.strip()]

    # Compute mean and 99th percentile
    mean_latencies.append(np.mean(values))
    top_latencies.append(np.percentile(values, 99))

    # Derive items/sec from filename “latency-<ms>”
    ms = float(Path(filepath).stem.split("latency-")[1])
    input_rate.append(1000.0 / ms)

# Sort by input rate
x, mean_y, top_y = zip(*sorted(zip(input_rate, mean_latencies, top_latencies)))

# Plot both series
plt.figure()
plt.plot(x, mean_y, marker='o', linestyle='-', label='Avg.')
plt.plot(x, top_y, marker='s', linestyle='--', label='99th Percentile')
plt.xscale('log')
plt.xlabel("Items per Second")
plt.ylabel("Latency (ms)")
plt.title("Latency vs. Input Rate")
plt.legend()
plt.grid(True)
plt.show()