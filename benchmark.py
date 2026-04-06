import time
import requests
import statistics
import matplotlib.pyplot as plt

BASE_URL = "http://127.0.0.1:8001"

# ----------------------------
# CONFIG
# ----------------------------
NUM_RUNS = 10
TEST_FILE_SIZE_MB = 1


# ----------------------------
# HELPERS
# ----------------------------
def generate_data(size_mb):
    return b"x" * (size_mb * 1024 * 1024)


def measure_upload():
    latencies = []

    for i in range(NUM_RUNS):
        file_data = generate_data(TEST_FILE_SIZE_MB)

        files = {"file": (f"test_{i}.txt", file_data)}

        start = time.time()
        r = requests.post(f"{BASE_URL}/files/test_{i}.txt", files=files)
        end = time.time()

        if r.status_code != 200:
            print("Upload failed:", r.text)
            continue

        latencies.append((end - start) * 1000)

    return latencies


def measure_download():
    latencies = []

    for i in range(NUM_RUNS):
        start = time.time()
        r = requests.get(f"{BASE_URL}/files/test_{i}.txt")
        end = time.time()

        if r.status_code != 200:
            print("Download failed:", r.text)
            continue

        latencies.append((end - start) * 1000)

    return latencies


def measure_delete():
    latencies = []

    for i in range(NUM_RUNS):
        start = time.time()
        r = requests.delete(f"{BASE_URL}/files/test_{i}.txt")
        end = time.time()

        latencies.append((end - start) * 1000)

    return latencies


# ----------------------------
# RUN BENCHMARK
# ----------------------------
print("\n Starting Distributed System Benchmark...\n")

upload_times = measure_upload()
download_times = measure_download()
delete_times = measure_delete()

# ----------------------------
# RESULTS
# ----------------------------
def summarize(name, data):
    print(f"\n{name}")
    print(f"Avg: {statistics.mean(data):.2f} ms")
    print(f"Min: {min(data):.2f} ms")
    print(f"Max: {max(data):.2f} ms")
    print(f"Std Dev: {statistics.stdev(data):.2f} ms")


summarize("UPLOAD LATENCY", upload_times)
summarize("DOWNLOAD LATENCY", download_times)
summarize("DELETE LATENCY", delete_times)


# ----------------------------
# PLOTS
# ----------------------------
plt.figure()
plt.plot(upload_times, label="Upload")
plt.plot(download_times, label="Download")
plt.plot(delete_times, label="Delete")
plt.legend()
plt.title("Distributed File System Benchmark")
plt.xlabel("Run")
plt.ylabel("Latency (ms)")
plt.show()