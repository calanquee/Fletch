import numpy as np
import random
import sys
from collections import defaultdict
import os
import configparser
# WORKLOAD_HOME_DIR = '/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/workload'
# ACCESS_FILE_PATH = os.path.join(WORKLOAD_HOME_DIR, "workload_gen_src/access_file.out.fast")
WARMUP_FILE_PATH = "./warmup/workload_top_60000.out"
# FILE1_PATH = os.path.join(WORKLOAD_HOME_DIR, "file1.out")

ini = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/config.ini"
config = configparser.ConfigParser()
config.read(ini)
file_offset = int(config["global"]["path_resolution_offset"])
def parse_filename(filename):
    # mark
     return filename.count("/") - file_offset


def batch_freq_dist(file_depths, total_files, depth):
    ret = []
    idx = 0
    while len(ret) < total_files:
        sub_group = [file_depths[d][idx] for d in range(depth) if len(file_depths[d]) > idx]
        ret.extend(sub_group)
        idx += 1
    return ret

def rev_batch_freq_dist(file_depths, total_files, depth):
    ret = []
    idx = 0
    while len(ret) < total_files:
        sub_group = [file_depths[d][idx] for d in range(depth-1,-1,-1) if len(file_depths[d]) > idx]
        ret.extend(sub_group)
        idx += 1
    return ret

def freq_dist_from_low(file_depths):
    return [file for depth_list in file_depths for file in depth_list]


def freq_dist_from_high(file_depths):
    return [file for depth_list in reversed(file_depths) for file in depth_list]


def freq_dist_random(file_depths):
    ret = [file for depth_list in file_depths for file in depth_list]
    SEED = 9
    random.seed(SEED)
    random.shuffle(ret)
    return ret


def get_file_depth(filenames, total_files, depth):
    file_depths = [[] for _ in range(depth)]
    for index, filename in enumerate(filenames[:total_files]):
        depth_index = parse_filename(filename) - 1
        if 0 <= depth_index < depth:
            file_depths[depth_index].append(index)
    return file_depths


def get_freq_dist_for_each_level(access_pattern, file_count, start_index_in_each_level):
    access_dist = defaultdict(int)
    with open(access_pattern, "r") as f:
        for line in map(str.strip, f):
            access_index = int(line)
            for i, (count, start_index) in enumerate(zip(file_count, start_index_in_each_level)):
                if start_index <= access_index < start_index + count:
                    access_dist[i] += 1
                    break
    return access_dist

def zipf_func(x, a):
    # x is object rank (>= 1) and a is Zipfian constant
    return 1.0 / (x ** a)

def generate_uniform(total_files, total_accesses, access_pattern_out):
    # generate uniform access pattern
    access_pattern = np.random.randint(0, total_files, total_accesses)
    np.random.shuffle(access_pattern)
    np.savetxt(access_pattern_out, access_pattern, fmt='%d')
    return access_pattern.tolist()
def generate_workloads(s, total_files, total_accesses, depth, method_for_workload_generation, sorted_file_path, access_pattern_out):
    # access_pattern = np.random.zipf(s, total_accesses)
    # zipf_constant = 0.9
    # Generate probabilities for all ranks
    ranks = np.arange(1, total_files + 1)
    perrank_probs = zipf_func(ranks, s)
    perrank_probs /= perrank_probs.sum()  
    num_requests = total_accesses
    # print(num_requests,total_files,len(perrank_probs))
    requests = np.random.choice(ranks, size=num_requests, p=perrank_probs)

    # # Count frequency of each request ID
    # print(len(requests))
    file_access_counts = np.bincount(requests, minlength=total_files)
    print(len(file_access_counts))
    # file_access_counts = np.bincount((access_pattern - 1) % total_files, minlength=total_files)
    current_sum = file_access_counts.sum()
    print("Sum 20% Request frequencies:", sum(file_access_counts[:int(num_requests/5)])/current_sum)
    print("Sum 10000 Request frequencies:", sum(file_access_counts[:10000])/current_sum)
    print("Sum 30000 Request frequencies:", sum(file_access_counts[:30000])/current_sum)
    with open(sorted_file_path, "r") as f:
        filenames = f.read().splitlines()

    file_depths = get_file_depth(filenames, total_files, depth)

    if method_for_workload_generation == 1:
        dist_method = freq_dist_from_low
    elif method_for_workload_generation == 2:
        dist_method = freq_dist_from_high
    elif method_for_workload_generation == 3:
        dist_method = freq_dist_random
    else:
        # error
        print("Invalid workload generation method")
        exit(1)
        # return
    # elif method_for_workload_generation == 4:
    #     dist_method = lambda depths: batch_freq_dist(depths, total_files, depth)
    # elif method_for_workload_generation == 5:
    #     dist_method = lambda depths: rev_batch_freq_dist(depths, total_files, depth)

    dist_files = dist_method(file_depths)
    
    # Optimized file access count finalization using NumPy
    file_access_counts_final = np.zeros(total_files, dtype=int)
    file_access_counts_final[dist_files[:total_files]] = file_access_counts[:len(dist_files)]

    access_pattern = np.repeat(np.arange(total_files), file_access_counts_final)

    # if method_for_workload_generation != 3:
    np.random.shuffle(access_pattern)
    np.savetxt(access_pattern_out, access_pattern, fmt='%d')

    return access_pattern


def workload_analysis(lines):
    workload_access_indices = defaultdict(int)
    for line in map(str.strip, lines):
        workload_access_indices[line] += 1

    sorted_access = sorted(workload_access_indices.items(), key=lambda x: x[1], reverse=True)

    with open(WARMUP_FILE_PATH, "w") as f:
        count = 0
        for key, value in sorted_access:
            if count >= 60000:
                break
            f.write("\n".join([str(key)] * value) + "\n")
            count += 1
        print("Total files: ", count)


# def gen_workload(zeta,total_files,access_times,depth,workload_generation_method):
#     access_pattern = generate_workloads(zeta, total_files, access_times, depth, workload_generation_method)
