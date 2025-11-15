import os
import pandas as pd
import numpy as np
import re


file_pattern = re.compile(r"tmp_mdtest_(?P<zeta>\d+\.\d+)_(?P<server1>\w+)_(?P<server2>\w+)_static_(?P<server_number>\d+)_mixed\.out")
line_pattern = re.compile("Total" + r"\s+:(\d+)\s+ms,\s+BOTTLENECK\s+:\s+(\d+\.\d+)ops/sec,\s+ROTATION\s+:\s+(\d+\.\d+)ops/sec,\s+BOTTLENECK HIT\s+:\s*(\d+(?:\.\d+)?),\s+ROTATION HIT\s+:\s*(\d+(?:\.\d+)?),")

def get_final_latency_thpt(directory):

    def calculate_thpt(directory):
        bottleneck_thpts = []
        rotation_thpts = []
        switch_thpts = []
        hit_cnt = []
        file_stats = []
        bottleneck_thpt = 1
        server_logical_num = 0
        sub_dir_path = directory
        for filename in os.listdir(sub_dir_path):
            file_match = file_pattern.match(filename)
            if file_match:
                # print(filename)
                file_info = file_match.groupdict()
                file_path = os.path.join(sub_dir_path, filename)
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

    def calculate_latency_statistics(df):
        weighted_avg_latency = np.average(df['Value'], weights=df['Count'])
        total_count = df['Count'].sum()
        sorted_data = np.repeat(df['Value'], df['Count'])
        sorted_data = np.sort(sorted_data)
        p95 = np.percentile(sorted_data, 95)
        p99 = np.percentile(sorted_data, 99)
        return weighted_avg_latency, p95, p99

    def process_files(directory):
        all_operations = {}
        combined_data_all_operations = []
        for filename in os.listdir(directory):
            if filename.endswith('.csv'):
                parts = filename.split('_')
                bottle_id = parts[1]
                rotation_id = parts[2]
                running_id = parts[3]
                operation = parts[4].replace('.csv', '')
                if operation == 'WARMUP':
                    continue
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path)
                if rotation_id == running_id:
                    if operation not in all_operations:
                        all_operations[operation] = []
                    all_operations[operation].append(df)
                    combined_data_all_operations.append(df)

        operation_statistics = []
        for operation, dfs in all_operations.items():
            combined_df = pd.concat(dfs)
            weighted_avg_latency, p95, p99 = calculate_latency_statistics(combined_df)
            operation_statistics.append({
                'operation': operation,
                'weighted_avg_latency': weighted_avg_latency,
                'p95_latency': p95,
                'p99_latency': p99
            })

        combined_df_all_operations = pd.concat(combined_data_all_operations)
        weighted_avg_latency_all, p95_all, p99_all = calculate_latency_statistics(combined_df_all_operations)

        operation_statistics.append({
            'operation': 'ALL_OPERATIONS',
            'weighted_avg_latency': weighted_avg_latency_all,
            'p95_latency': p95_all,
            'p99_latency': p99_all
        })

        return operation_statistics

    operation_stats = process_files(directory)
    total_thpt = calculate_thpt(directory)
    return total_thpt, operation_stats[-1]

# get the final throughput and latency statistics

root_dir = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results"
round_idx = "round_exp4_r5_16"

workload = 0 # 0 for alibaba, 1 for read-only
ccache = "cscaching_0.9_32000000_10_4_3"
fletchplus = "csfletch_0.9_32000000_10_4_3"
nocache = "nocache_0.9_32000000_10_4_3"
fletch = "netfetch_0.9_32000000_10_4_3"



print("Fletch+, Throughput (KOPS), Avg Latency (us), P95 Latency (us), P99 Latency (us)")

for rate_limiter_idx in range(20):
    try:
        directory = fr"{root_dir}/{round_idx}/{workload}/{rate_limiter_idx}/{fletchplus}"
        total_thpt, all_op_latency = get_final_latency_thpt(directory)
        print(rate_limiter_idx, total_thpt/1000, all_op_latency['weighted_avg_latency'] , all_op_latency['p95_latency'], all_op_latency['p99_latency'])
    except Exception as e:
        continue

print("CCache, Throughput (KOPS), Avg Latency (us), P95 Latency (us), P99 Latency (us)")

for rate_limiter_idx in range(20):
    try:
        directory = fr"{root_dir}/{round_idx}/{workload}/{rate_limiter_idx}/{ccache}"
        total_thpt, all_op_latency = get_final_latency_thpt(directory)
        print(rate_limiter_idx, total_thpt/1000, all_op_latency['weighted_avg_latency'] , all_op_latency['p95_latency'], all_op_latency['p99_latency'])
    except Exception as e:
        continue
    
print("Fletch, Throughput (KOPS), Avg Latency (us), P95 Latency (us), P99 Latency (us)")

for rate_limiter_idx in range(20):
    try:
        directory = fr"{root_dir}/{round_idx}/{workload}/{rate_limiter_idx}/{fletch}"
        total_thpt, all_op_latency = get_final_latency_thpt(directory)
        print(rate_limiter_idx, total_thpt/1000, all_op_latency['weighted_avg_latency'] , all_op_latency['p95_latency'], all_op_latency['p99_latency'])
    except Exception as e:
        continue

print("NoCache, Throughput (KOPS), Avg Latency (us), P95 Latency (us), P99 Latency (us)")

for rate_limiter_idx in range(20):
    try:
        directory = fr"{root_dir}/{round_idx}/{workload}/{rate_limiter_idx}/{nocache}"
        total_thpt, all_op_latency = get_final_latency_thpt(directory)
        print(rate_limiter_idx, total_thpt/1000, all_op_latency['weighted_avg_latency'] , all_op_latency['p95_latency'], all_op_latency['p99_latency'])
    except Exception as e:
        continue