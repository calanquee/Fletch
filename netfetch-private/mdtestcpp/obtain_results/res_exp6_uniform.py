import os
import re
import numpy as np
from numpy import sort

root_dir = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results"

dir_pattern = re.compile(r"(?P<method>[a-zA-Z0-9\-]+)_uniform_(?P<datasize>\d+)_(?P<depth>\d+)_(?P<branch_factor>\d+)_uniform")
file_pattern = re.compile(r"tmp_mdtest_uniform_(?P<server1>\w+)_(?P<server2>\w+)_static_(?P<server_number>\d+)\.out")
line_pattern = re.compile("Total" + r"\s+:(\d+)\s+ms,\s+BOTTLENECK\s+:\s+(\d+\.\d+)ops/sec,\s+ROTATION\s+:\s+(\d+\.\d+)ops/sec,\s+BOTTLENECK HIT\s+:\s*(\d+(?:\.\d+)?),\s+ROTATION HIT\s+:\s*(\d+(?:\.\d+)?),")

round = "round_exp6_uniform_r4_16"

print(
    f"Dir "
    + f"Method "
    + f"Zeta "
    + f"DataSize "
    + f"Depth "
    + f"Branch "
    + f"Generation "
)

def calculate_fairness_index(thpts,bottleneck_thpt):
    modified_thpts = thpts.copy()
    modified_thpts[modified_thpts == 0] = bottleneck_thpt
    rmsd = np.sqrt(np.mean(np.square(modified_thpts)))
    total_sum = np.sum(modified_thpts)
    square_sum = np.sum(np.square(modified_thpts))
    fairness_index = (total_sum ** 2) / (len(modified_thpts) * square_sum)
    return fairness_index

copy_to_excel = []

for idx in range(4):
    for sub_dir in os.listdir(f"{root_dir}/{round}/{idx}"):
        match = dir_pattern.match(sub_dir)
        if match:
            parsed_info = match.groupdict()
            sub_dir_path = f"{root_dir}/{round}/{idx}/{sub_dir}"
            bottleneck_thpts = []
            rotation_thpts = []
            switch_thpts = []
            hit_cnt = []
            file_stats = []
            bottleneck_thpt = 1
            server_logical_num = 0
            for filename in os.listdir(sub_dir_path):
                file_match = file_pattern.match(filename)
                if file_match:
                    file_info = file_match.groupdict()
                    file_path = os.path.join(sub_dir_path, filename)
                    server_logical_num = file_info["server_number"]
                    with open(file_path, "r") as f:
                        for line in f:
                            line_match = line_pattern.search(line)
                            if line_match:
                                file_stat = line_match.group(1)
                                file_stat = float(file_stat) / 1000.0
                                bottleneck = line_match.group(2)
                                rotation = line_match.group(3)
                                switch_op_0 = line_match.group(4)
                                switch_op_1 = line_match.group(5)
                                if file_info["server1"] == file_info["server2"]:
                                    bottleneck_thpt = float(bottleneck)
                                    switch_thpts.append(float(switch_op_0) / float(file_stat))
                                    hit_cnt.append(float(switch_op_0))
                                else:
                                    switch_thpts.append(float(switch_op_1) / float(file_stat))
                                    hit_cnt.append(float(switch_op_1))
                                bottleneck_thpts.append(float(bottleneck))
                                rotation_thpts.append(float(rotation))
                                file_stats.append(float(file_stat))
            total_Op = 0.0
            for i in range(len(rotation_thpts)):
                total_Op += file_stats[i] * rotation_thpts[i]
                if rotation_thpts[i] <= 1.0:
                    total_Op += file_stats[i] * bottleneck_thpts[i]
            print("total_Op without cache hit", total_Op)
            for i in range(len(rotation_thpts)):
                total_Op += file_stats[i] * switch_thpts[i]
            print("total_Op", total_Op)
            for i, bottleneck in enumerate(bottleneck_thpts):
                if bottleneck == 0:
                    continue
                rotation_thpts[i] = rotation_thpts[i] * (bottleneck_thpt / bottleneck)
                switch_thpts[i] = switch_thpts[i] * (bottleneck_thpt / bottleneck)

            total_thpt = sum(rotation_thpts) + bottleneck_thpt + sum(switch_thpts)
            print(f"thpt: {total_thpt:.2f}")
            if total_Op > 0:
                print(f"Hit Rate {(sum(hit_cnt)/total_Op*100):.2f}%")
            print(f"details:")
            print(f"{sum(rotation_thpts) + bottleneck_thpt:.2f} + {sum(switch_thpts):.2f}")
            print(f"bottleneck_thpt {bottleneck_thpt:.2f}")
            print(f"bottleneck, " + ", ".join([f"{x:.0f}" for x in bottleneck_thpts]) + " ")
            print(f"rotation, " + ", ".join([f"{x:.0f}" for x in rotation_thpts]) + " ")
            print(f"switch," + ", ".join([f"{x:.0f}" for x in switch_thpts])+ "")
            copy_to_excel.append([
                    round,
                    idx,
                    parsed_info["method"],
                    "uniform",
                    parsed_info["depth"],
                    parsed_info["branch_factor"],
                    f" {total_thpt/1000:.2f}",
                    f" {(sum(hit_cnt)/total_Op*100 if total_Op>0 else 0):.2f}%",
                    # calculate_fairness_index(rotation_thpts,bottleneck_thpt)
            ])     
            print(f"-" * 32)


copy_to_excel.sort()

for item in copy_to_excel:
    # split with comma
    print(",".join(map(str, item)))
