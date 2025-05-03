import glob
from pathlib import Path

# Extract and calculate average latencies
for name in sorted(glob.glob("latency*")):
    path = Path(name)
    with path.open(encoding="utf-8") as file:
        latencies = [int(line) for line in file if line.strip()]
        avg_ms = (sum(latencies) / len(latencies)) / 1_000_000  
        print(f"{path.name}: {avg_ms:.4f} ms")