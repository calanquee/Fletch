import sys
from utils.cache_gen_util.common import parse_filename, get_single_recirculation
from collections import defaultdict

# init access for each path
def handle_entry(entry, uncached):
    if entry in uncached:
        uncached[entry] += 1
    else:
        uncached[entry] = 1

def write_cache(uncached, cache_capacity, depth):
    # sort
    cached = defaultdict(int)
    sorted_access = sorted(uncached.items(), key=lambda x: x[1], reverse=True)
    count = 0
    for key, value in sorted_access:
        if count == cache_capacity:
            break
        # Qingxiu: comment the following verification since the the most levels is 4 in tested dataset 
        if get_single_recirculation(key) <= depth:
            cached[key] = 0
            count += 1
    return cached

def netcache_handle_cache(cache_capacity, lines, filenames, depth):
    # netcache: generate cache
    netcache_uncached = defaultdict(int)
    for idx in lines:
        filename = filenames[int(idx)].strip()
        handle_entry(filename, netcache_uncached)
            

    netcache_cached = write_cache(netcache_uncached, cache_capacity, depth)
    return netcache_cached
    