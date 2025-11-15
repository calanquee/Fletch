from collections import defaultdict
import sys
import datetime
import time

import utils.cache_gen_util.netcache
# from netcache import netcache_handle_cache

# from netfetch import netfetch_handle_cache
# from analysis import cache_analysis

CACHE_HOME_DIR = (
    "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/workload/caches"
)


def gen_cache_list_for_netcache(filename_out, access_out, cache_capacity, depth):

    with open(filename_out, "r") as f:
        filenames = f.readlines()
    with open(access_out, "r") as f:
        lines = f.readlines()
    netcache_cached = utils.cache_gen_util.netcache.netcache_handle_cache(cache_capacity, lines, filenames, depth)
    with open(f"{CACHE_HOME_DIR}/netcache.out", "w") as f:
        for i in netcache_cached.keys():
            parts = i.split("/")
            for part in parts:
                if part.startswith("file.mdtest.0"):
                    number = part.split(".")[-1]
                    f.write(number + "\n")


def gen_cache_list_for_netfetch(filename_out, access_out, cache_capacity, depth):

    return