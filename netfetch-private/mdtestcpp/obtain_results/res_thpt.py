import os
import re
import numpy as np
from numpy import sort

dir_pattern = re.compile(
    r"(?P<method>[a-zA-Z0-9\-]+)_(?P<zeta>\d+\.\d+)_(?P<datasize>\d+)_(?P<depth>\d+)_(?P<branch_factor>\d+)_(?P<gen_method>\w+)"
)
file_pattern = re.compile(
    r"tmp_mdtest_(?P<zeta>\d+\.\d+)_(?P<server1>\w+)_(?P<server2>\w+)_static_(?P<server_number>\d+)_mixed\.out"
)
line_pattern = re.compile(
    "Total" +
    r"\s+:(\d+)\s+ms,\s+BOTTLENECK\s+:\s+(\d+\.\d+)ops/sec,\s+ROTATION\s+:\s+(\d+\.\d+)ops/sec,\s+BOTTLENECK HIT\s+:\s*(\d+(?:\.\d+)?),\s+ROTATION HIT\s+:\s*(\d+(?:\.\d+)?),"
)

root_dir = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results"
# all_round = "round_exp6_r5_16"
# all_round = "round_exp6_debug2_16"
# all_round = "round_mkdir_create_r4_16"
all_round = "round_exp7_r5_16"
# all_round = "round_rm_mv_r4_16"
# all_round = "round_stat_r4_16"

print(f"Dir " + f"Method " + f"Zeta " + f"DataSize " + f"Depth " + f"Branch " +
      f"Generation ")


def calculate_fairness_index(thpts, bottleneck_thpt):
    modified_thpts = thpts.copy()
    modified_thpts[modified_thpts == 0] = bottleneck_thpt
    rmsd = np.sqrt(np.mean(np.square(modified_thpts)))
    total_sum = np.sum(modified_thpts)
    square_sum = np.sum(np.square(modified_thpts))
    fairness_index = (total_sum**2) / (len(modified_thpts) * square_sum)
    return fairness_index


def check_validity(method, zeta, hit_ratio):
    if method != "csfletch":
        return True
    if zeta >= 0.9:
        print(f"check_validity {method} {zeta} {hit_ratio}")
        # return hit_ratio >= 10
        return True
    else:
        # return hit_ratio > 0
        return True


all_copy_to_excel = []
# for four workloads
for idx in range(1):
    copy_to_excel = []
    for sub_dir in os.listdir(f"{root_dir}/{all_round}/{idx}"):
        match = dir_pattern.match(sub_dir)
        if match:
            print(idx, sub_dir)
            parsed_info = match.groupdict()
            sub_dir_path = f"{root_dir}/{all_round}/{idx}/{sub_dir}"
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
                                if file_info["server1"] == file_info[
                                        "server2"]:
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
            if len(bottleneck_thpts) == 0:
                print(f"No bottleneck thpt for {idx} {sub_dir}")
                continue
            bottleneck_thpt = max(bottleneck_thpts)
            for i in range(len(rotation_thpts)):
                if rotation_thpts[i] > bottleneck_thpt:
                    bottleneck_thpt = rotation_thpts[i]
            for i in range(len(rotation_thpts)):
                total_Op += file_stats[i] * rotation_thpts[i]
                if rotation_thpts[i] <= 1.0:
                    total_Op += file_stats[i] * bottleneck_thpt
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
            print(sum(rotation_thpts), bottleneck_thpt, sum(switch_thpts))
            print(f"{all_round}" + f" {parsed_info['method']}" +
                  f" {parsed_info['zeta']}"
                  # + " uniform"
                  + f" {parsed_info['datasize']}" +
                  f" {parsed_info['depth']}" +
                  f" {parsed_info['branch_factor']}" +
                  f" {parsed_info['gen_method']}")
            print(f"thpt: {total_thpt:.2f}")
            if total_Op > 0:
                print(f"Hit Rate {(sum(hit_cnt)/total_Op*100):.2f}%")
            print(f"details:")
            print(f"{sum(rotation_thpts) + bottleneck_thpt:.2f} + {sum(switch_thpts):.2f}")
            print(f"bottleneck_thpt {bottleneck_thpt:.2f}")
            print(f"bottleneck, " + ", ".join([f"{x:.0f}" for x in bottleneck_thpts]) + " ")
            print(f"rotation, " + ", ".join([f"{x:.0f}" for x in rotation_thpts]) + " ")
            print(f"switch," + ", ".join([f"{x:.0f}" for x in switch_thpts]) + "")
            print(f"copy_to_excel: " + f"{all_round}" +
                  f" {parsed_info['method']}" + f" {parsed_info['zeta']}"
                  # +" uniform"
                  + f" {int(total_Op)}" + f" {parsed_info['depth']}" +
                  f" {parsed_info['branch_factor']}" +
                  f" {parsed_info['gen_method']}" + f" {total_thpt:.2f}" +
                  f" {(sum(hit_cnt)/total_Op*100 if total_Op>0 else 0):.2f}%")
            cache_hit_ratio = sum(hit_cnt) / total_Op * 100 if total_Op > 0 else 0
            if check_validity(parsed_info["method"], float(parsed_info["zeta"]), cache_hit_ratio):
                copy_to_excel.append([
                    all_round,
                    idx,
                    parsed_info["method"],
                    parsed_info["zeta"],
                    parsed_info["depth"],
                    parsed_info["branch_factor"],
                    parsed_info["gen_method"],
                    bottleneck_thpt,
                    f" {total_thpt/1000:.2f}",
                    f" {(sum(hit_cnt)/total_Op*100 if total_Op>0 else 0):.2f}%",
                    # calculate_fairness_index(rotation_thpts,bottleneck_thpt)
                ])

            print(f"-" * 32)
    copy_to_excel.sort()
    for item in copy_to_excel:
        # split with comma
        print(",".join(map(str, item)))
    all_copy_to_excel.extend(copy_to_excel)

print(f"-" * 32)
print()
for item in all_copy_to_excel:
    # split with comma
    print(", ".join(map(str, item)))
