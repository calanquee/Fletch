import os
import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
from pathlib import Path
import glob
from collections import defaultdict

file_pattern = re.compile(r"tmp_mdtest_(?P<zeta>\d+\.\d+)_(?P<server1>\w+)_(?P<server2>\w+)_static_(?P<server_number>\d+)_mixed\.out")
line_pattern = re.compile("Total" + r"\s+:(\d+)\s+ms,\s+BOTTLENECK\s+:\s+(\d+\.\d+)ops/sec,\s+ROTATION\s+:\s+(\d+\.\d+)ops/sec,\s+BOTTLENECK HIT\s+:\s*(\d+(?:\.\d+)?),\s+ROTATION HIT\s+:\s*(\d+(?:\.\d+)?),")



def calculate_thpt(directory):
    bottleneck_thpts = []
    rotation_thpts = []
    switch_thpts = []
    hit_cnt = []
    file_stats = []
    bottleneck_thpt = 1
    server_logical_num = 0
    for filename in os.listdir(directory):
        file_match = file_pattern.match(filename)
        if file_match:
            # print(filename)
            file_info = file_match.groupdict()
            file_path = os.path.join(directory, filename)
            server_logical_num = file_info["server_number"]
            with open(file_path, "r") as f:
                for line in f:
                    line_match = line_pattern.search(line)
                    if line_match:
                        # print(line)
                        file_stat = line_match.group(1)
                        file_stat = float(file_stat) / 1000.0
                        bottleneck = line_match.group(2)
                        rotation = line_match.group(3)
                        switch_op_0 = line_match.group(4)
                        switch_op_1 = line_match.group(5)
                        # print(file_stat, bottleneck, rotation, switch_op_0, switch_op_1)
                        if file_info["server1"] == file_info["server2"]:
                            bottleneck_thpt = float(bottleneck)  
                            switch_thpts.append(
                                float(switch_op_0) / float(file_stat))
                            hit_cnt.append(float(switch_op_0))
                        else:
                            switch_thpts.append(
                                float(switch_op_1) / float(file_stat))
                            hit_cnt.append(float(switch_op_1))
                        bottleneck_thpts.append(float(bottleneck))
                        rotation_thpts.append(float(rotation))
                        file_stats.append(float(file_stat))
    total_Op = 0.0
    for i in range(len(rotation_thpts)):
        total_Op += file_stats[i] * rotation_thpts[i]
        if rotation_thpts[i] <= 1.0:
            total_Op += file_stats[i] * bottleneck_thpts[i]

    for i in range(len(rotation_thpts)):
        total_Op += file_stats[i] * switch_thpts[i]
    # print("total_Op", total_Op)
    for i, bottleneck in enumerate(bottleneck_thpts):
        if bottleneck == 0:
            continue
        # print(rotation_thpts[i],bottleneck_thpt,bottleneck)
        rotation_thpts[i] = rotation_thpts[i] * (bottleneck_thpt / bottleneck)
        switch_thpts[i] = switch_thpts[i] * (bottleneck_thpt / bottleneck)
    total_thpt = sum(rotation_thpts) + bottleneck_thpt + sum(switch_thpts)
    return total_thpt


def compute(dfs):
    combined_df = pd.concat(dfs)
    weighted_avg_latency = np.average(combined_df['Value'], weights=combined_df['Count'])
    sorted_data = np.repeat(combined_df['Value'], combined_df['Count'])
    sorted_data = np.sort(sorted_data)
    p95 = np.percentile(sorted_data, 95)
    p99 = np.percentile(sorted_data, 99)

    return weighted_avg_latency, p95, p99

def analyze_line_chart(directory, dst):
    res = defaultdict(list)
    files = glob.glob((directory / "*.csv").as_posix())
    # print(f"Found {len(files)} files in {directory}")
    mv_files = []
    delete_files = []
    for file in files:
        parts = file.split("/")[-1].split('_')
        bottle_id = parts[1]
        rotation_id = parts[2]
        running_id = parts[3]
        operation = parts[4].replace('.csv', '')
        if operation == "WARMUP" or rotation_id != running_id: #or running_id not in ["0", "15"]:
            continue
        if operation == "MV":
            mv_files.append(file)
        elif operation == "CREATE":
            delete_files.append(file)
        else:
            continue
        # print(f"Operation: {operation}, Bottle ID: {bottle_id}, Rotation ID: {rotation_id}, Running ID: {running_id}")
        # df = pd.read_csv(file)
        # res[operation].append(df)
    mv_files = sorted(mv_files, key=lambda x: int(x.split("/")[-1].split('_')[3]))
    mv_files = mv_files[-1:] + mv_files[:-1]  # Move the last file to the front

    delete_files = sorted(delete_files, key=lambda x: int(x.split("/")[-1].split('_')[3]))
    delete_files = delete_files[-1:] + delete_files[:-1]  # Move the last file to the front

    mv_dfs = []
    mv_averages = []
    mv_p95s = []
    mv_p99s = []
    for file in mv_files:
        df = pd.read_csv(file)
        mv_dfs.append(df)
        weighted_avg_latency_mv, p95_mv, p99_mv = compute(mv_dfs)
        mv_averages.append(weighted_avg_latency_mv)
        mv_p95s.append(p95_mv)
        mv_p99s.append(p99_mv)
    plt.figure()
    X = np.arange(len(mv_files))
    plt.plot(X, mv_averages, label='Average Latency', marker='o')
    plt.plot(X, mv_p95s, label='P95 Latency', marker='x')
    plt.plot(X, mv_p99s, label='P99 Latency', marker='s')
    plt.legend()
    plt.title("MV Latency Line Chart")
    plt.savefig(dst / "mv_latency_line_chart.png")
    plt.close()
    de_dfs = []
    de_averages = []
    de_p95s = []
    de_p99s = []
    for file in delete_files:
        df = pd.read_csv(file)
        de_dfs.append(df)
        weighted_avg_latency_de, p95_de, p99_de = compute(de_dfs)
        de_averages.append(weighted_avg_latency_de)
        de_p95s.append(p95_de)
        de_p99s.append(p99_de)
    plt.figure()
    X = np.arange(len(delete_files))
    plt.plot(X, de_averages, label='Average Latency', marker='o')
    plt.plot(X, de_p95s, label='P95 Latency', marker='x')
    plt.plot(X, de_p99s, label='P99 Latency', marker='s')
    plt.legend()
    plt.title("Create Latency Line Chart")
    plt.savefig(dst / "create_latency_line_chart.png")
    plt.close()

def analyze(directory, dst):
    res = defaultdict(list)
    files = glob.glob((directory / "*.csv").as_posix())
    # print(f"Found {len(files)} files in {directory}")
    for file in files:
        parts = file.split("/")[-1].split('_')
        bottle_id = parts[1]
        rotation_id = parts[2]
        running_id = parts[3]
        operation = parts[4].replace('.csv', '')
        if operation == "WARMUP" or rotation_id != running_id: # or running_id not in ["15"]:
            continue
        # print(f"Operation: {operation}, Bottle ID: {bottle_id}, Rotation ID: {rotation_id}, Running ID: {running_id}")
        df = pd.read_csv(file)
        res[operation].append(df)
    # print(f"Total operations found: {len(res)}")
    with open(dst / "final_throughput.txt", "w") as file:
        all_dfs = []
        for operation, dfs in res.items():
            combined_df = pd.concat(dfs)
            all_dfs.extend(dfs)
            weighted_avg_latency = np.average(combined_df['Value'], weights=combined_df['Count'])

            sorted_data = np.repeat(combined_df['Value'], combined_df['Count'])
            sorted_data = np.sort(sorted_data)
            p95 = np.percentile(sorted_data, 95)
            p99 = np.percentile(sorted_data, 99)
            print(f"Operation: {operation}, Average: {weighted_avg_latency}, P95 Latency: {p95}, P99 Latency: {p99}")
            file.write(f"Operation: {operation}, Average: {weighted_avg_latency}, P95 Latency: {p95}, P99 Latency: {p99}\n")
            histogram, bin_edges = np.histogram(sorted_data, range=(0, 20000), bins=100)
            plt.figure(figsize=(10, 6))
            plt.bar(bin_edges[:-1], histogram, width=np.diff(bin_edges), edgecolor='black', align='edge')
            plt.title(f"Latency Histogram for {operation}")
            plt.xlabel("Latency (ms)")
            plt.ylabel("Frequency")
            plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.savefig(dst / f"{operation}_latency_histogram.png")
            plt.close()
        
        combined_all_df = pd.concat(all_dfs)
        weighted_avg_latency_all = np.average(combined_all_df['Value'], weights=combined_all_df['Count'])
        sorted_data_all = np.repeat(combined_all_df['Value'], combined_all_df['Count'])
        sorted_data_all = np.sort(sorted_data_all)
        # sorted_data_all[sorted_data_all > 20000] = 0
        p95_all = np.percentile(sorted_data_all, 95)
        p99_all = np.percentile(sorted_data_all, 99)
        print(f"All Operations Combined: Average: {weighted_avg_latency_all}, P95 Latency: {p95_all}, P99 Latency: {p99_all}")
        file.write(f"All Operations Combined: Average: {weighted_avg_latency_all}, P95 Latency: {p95_all}, P99 Latency: {p99_all}\n")

root = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results"
round_idx = "round_exp4_r1_16"
# /home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results/round_exp4_r1_16/0/4/cscaching_0.9_32000000_10_4_3
workload = 0 # 0 for alibaba, 1 for read-only
ccache = "cscaching_0.9_32000000_10_4_3"
fletchplus = "csfletch_0.9_32000000_10_4_3"
nocache = "nocache_0.9_32000000_10_4_3"
fletch = "netfetch_0.9_32000000_10_4_3"

old_list = [
        "round_exp4_except_delete_mv_16", 
        "round_exp4_delete_mv_16", 
        "round_exp4_open_delete_mv_16", 
        "round_exp4_debug9_16", 
        "round_exp4_delete_mv_debug_16",
        "round_exp4_rename_16"
][-1:]

li3 = [
    # "round_exp4_fletchplus_rename_only_16"
    # "round_exp4_fletchplus_mixedxxx4_16"
    # "round_exp4_fletchplus_rename_only3_16"
    # "round_exp4_fletchplus_mixed_r4_16",
    "round_exp4_both_mixed_r6_16",
    # "round_exp4_both_overall_c10000_16"
]


target = li3

def one_dir():
    for dir in target:
        root_dir = Path(fr"{root}/{dir}/{workload}")
        # print(f"Processing directory: {root_dir}")
        rate_limits = os.listdir(root_dir)
        for rate_limiter in rate_limits:
            for selected_method in [ccache, fletchplus]:
                directory = root_dir /  Path(fr"{rate_limiter}/{selected_method}")
                DST = Path("outs") / dir / rate_limiter / f"{selected_method.split('_')[0]}-mixed"
                DST.mkdir(exist_ok=True, parents=True)
                print(f"Output directory: {DST}")
                try:
                    analyze(directory, DST)
                    analyze_line_chart(directory, DST)
                    thpt = calculate_thpt(directory)
                    print("Throughput (KOPS):", thpt / 1000)
                except:
                    continue
            print()
if __name__ == "__main__":
    one_dir()