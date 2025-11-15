from collections import defaultdict
import sys
from hash_ring import HashRing
from common import get_single_recirculation, parse_filename

# use 100 servers for simulation
memcache_servers = ['ns%s' % i for i in range(1, 101)]

netcache_cache_hit = 0
netcache_cache_miss = 0
netcache_recirculation_count = 0

def get_cached_file_indices(cached_lines):
    cached_file_index = []
    files = []
    for line in cached_lines:
        parts = line.split("/")
        for part in parts:
            if part.startswith("file.mdtest.0"):
                files.append(line)
                number = part.split('.')[-1]
                cached_file_index.append(number)
    return cached_file_index, files

def workload_analysis(lines):
    workload_access_indices = defaultdict(int)
    for line in lines:
        line = line.strip()
        if line not in workload_access_indices:
            workload_access_indices[line] = 1
        else:
            workload_access_indices[line] += 1
    return workload_access_indices

def handle_cache_hit(workload_access_indices, cached_file_index):
    cache_hit = 0
    for index in cached_file_index:
        if index in workload_access_indices:
            cache_hit += workload_access_indices[index]
            # print("cache hit: ", index, "weight: ", workload_access_indices[index])
    return cache_hit

# for netcache: cache hit + recirculation count
def handle_cache_hit_recirculation_count(entries, cached):
    global netcache_cache_hit, netcache_cache_miss, netcache_recirculation_count
    for e in entries:
        if e not in cached:
            netcache_cache_miss += 1
            return
        else:
            netcache_recirculation_count += 1
    netcache_cache_hit += 1


def jains_fairness_index(loads):
    n = len(loads)
    sum_of_loads = sum(loads)
    sum_of_loads_squared = sum(load ** 2 for load in loads)

    if sum_of_loads_squared == 0:
        return 0

    fairness_index = (sum_of_loads ** 2) / (n * sum_of_loads_squared)
    return fairness_index


def compute_load_balance_ratio(files, lines, filenames):
    ring = HashRing(memcache_servers)
    server_load = defaultdict(int)
    for server in memcache_servers:
        server_load[server] = 0
    for idx in lines:
        filename = filenames[int(idx)].strip()
        if filename in files:
            continue
        else:
            current_server = ring.get_node(filename)
            server_load[current_server] += 1
    loads = []
    for key, value in server_load.items():
        loads.append(value)
    # compute fairness index
    fairness_index = jains_fairness_index(loads)
    return fairness_index


def compute_load_balance_ratio_nocache(lines, filenames):
    ring = HashRing(memcache_servers)
    server_load = defaultdict(int)
    for server in memcache_servers:
        server_load[server] = 0
    for idx in lines:
        filename = filenames[int(idx)].strip()
        current_server = ring.get_node(filename)
        server_load[current_server] += 1
    loads = []
    for key, value in server_load.items():
        loads.append(value)
    # compute fairness index
    fairness_index = jains_fairness_index(loads)
    return fairness_index


def elements_in_A_not_in_B(A, B):
    set_A = set(A)
    set_B = set(B)
    return list(set_A - set_B)


def cache_analysis(filenames, lines, netcache_cached, netfetch_cached):
    # workload analysis
    workload_access_indices = workload_analysis(lines)

    # cache hit ratio
    netcache_cached_file_indices, netcache_cached_files = get_cached_file_indices(netcache_cached)
    # print(netcache_cached_file_indices)
    print("netcache cached file number: ", len(netcache_cached_file_indices))
    cache_hit = handle_cache_hit(workload_access_indices, netcache_cached_file_indices)
    print("netcache cache hit: ", cache_hit)

    # netfetch
    netfetch_cached_file_indices, netfetch_cached_files = get_cached_file_indices(netfetch_cached)
    print("netfetch cached file number: ", len(netfetch_cached_file_indices))
    cache_hit = handle_cache_hit(workload_access_indices, netfetch_cached_file_indices)
    print("netfetch cache hit: ", cache_hit)
    print("---------------------------------------------------------")

    # compute load balance ratio
    fairness_index = compute_load_balance_ratio_nocache(lines, filenames)
    print("no cache load balance ratio: ", fairness_index)
    fairness_index = compute_load_balance_ratio(netcache_cached_files, lines, filenames)
    print("netcache load balance ratio: ", fairness_index)
    fairness_index = compute_load_balance_ratio(netfetch_cached_files, lines, filenames)
    print("netfetch load balance ratio: ", fairness_index)
    print("---------------------------------------------------------")

    # print("show differences:")
    # # elements in netcache not in netfetch
    # elements = elements_in_A_not_in_B(netcache_cached_file_indices, netfetch_cached_file_indices)
    # print("netcache not in netfetch: ", len(elements))
    # # for element in elements:
    # #     print("netcache file index: ", element, "weight: ", workload_access_indices[element])

    # # elements in netfetch not in netcache
    # elements = elements_in_A_not_in_B(netfetch_cached_file_indices, netcache_cached_file_indices)
    # print("netfetch not in netcache: ", len(elements))
    # # for element in elements:
    # #     print("netfetch file index: ", element, "weight: ", workload_access_indices[element])
    # print("----------------------------------------------------------")
