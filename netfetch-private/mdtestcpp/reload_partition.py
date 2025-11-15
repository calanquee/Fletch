import os
import subprocess
import configparser
import time
import argparse
import shutil
import threading
import multiprocessing
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
# import hdfs3
from hdfs3 import HDFileSystem

# import utils.cache_gen_util.api
from utils.workload_gen_util.work_generate import *
from utils.workload_gen_util.fast_sort_lines import *
from utils.cache_gen_util.api import *
from utils.remote_util import *
from utils.dir_tree import *

# import utils.workload_gen_util
# # import utils.workload_gen_util.fast_sort_lines
# # import utils.workload_gen_util.work_generate
# import utils.cache_gen_util
import random

max_requests = 32000000

cache_capacity = 10000
# zetas = [1.001, 1.01, 1.05, 1.1, 1.2, 1.3]
# wo change zeta to power law
zetas = [0.9]
# zetas = [1.001, 1.01, 1.05, 1.1, 1.3, 1.5, 1.8, 2.0, 2.5]
workload_generation_methods = [3]
# 0 netcache 1 netfetch 2 nocache
method_list = [
    "netcache",  # 0
    "netfetch",  # 1
    "nocache",  # 2
    "netfetch-design1",  # 3
    "netfetch-design2",  # 4
    "netfetch-design3",  # 5
    "cscaching",  # 6
    "csfletch" #7
]
# 0,3,
current_methods = [2]
client_thread = 128
server_logical_num = 16
server_logical_num_list = [16]

total_files_for_mdtest = 32000000
total_number_of_requests = 32000000
depth = 10
depths = [10]
fs_tree_breath = 4
id_0 = 15
rotation_list = [2] 

HOME_DIR = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp"

create = {
    'mixed': {
        'open_close': 0,
        'stat': 0,
        'create': 100,
        'delete': 0,
        'rename': 0.0,
        'chmod': 0.0,
        'readdir': 0.0,
        'statdir': 0.0,
        'mkdir': 0.0,
        'rmdir': 0.0
    }
}

update_list_mixed_list = [create]


def reload(
    files_for_mdtest,
    depth,
):
    access_dir = f"{HOME_DIR}/workload/workloads"

    access_out = "uniform"
    adjusted_depth = depth - file_offset
    # fmt: off
    # ./mdtest -a 64 -n 63962892 -m -z 8 -b 4 -d / -Y /home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/workload/workloads/access_file_63962892_64000000_10_4_0.9_3.out -P 300000
    command = [
        # 'stdbuf', '-o0',
        # 'mpirun', '-n', str(client_thread),
        './mdtest',
        '-a', str(client_thread),
        '-n', str(files_for_mdtest),
        '-m',
        '-z', str(adjusted_depth),
        '-b', str(fs_tree_breath),
        '-d', '/',
        # '-i', '5',
        # '-V', '3',
        '-Y', 'uniform',
        '-P',str(1)
    ]

    print(f"exec {' '.join(command)}")
    
    with open(f'nothing.out', 'w') as outfile:
        subprocess.run(command, stdout=outfile)

def update_config(file_path, updates):
    config = configparser.ConfigParser()
    config.read(file_path)
    for section, variables in updates.items():
        if section in config:
            for key, value in variables.items():
                if key in config[section]:
                    config[section][key] = str(value)
                else:
                    print(f"Key '{key}' not found in section '{section}'")
        else:
            print(f"Section '{section}' not found in the config file")

    with open(file_path, "w") as configfile:
        config.write(configfile)
def sort_file_out_for_workload_generation(filenames_path):

    line_counts = 0
    per_count = 0
    full_path = os.path.join(filenames_path, "file.out")
    new_path = full_path + ".sorted"
    # print(new_path)
    if fs_tree_breath != 1:
        if os.path.exists(new_path):
            print(
                f"[sort_file_out] {new_path} exists, you could rm it and re-generate one"
            )
        else:
            sort_lines(full_path, new_path)
    else:
        new_path = full_path
    line_count = subprocess.run(
        ["wc", "-l", new_path],
        capture_output=True,
        text=True,
    ).stdout.split()[0]
    line_count = int(line_count)
    return [line_count, line_count]
    
# stdbuf -o0 mpirun -n 256 ./mdtest -n 65536 -S -C -z 2 -b 4 -d /
if __name__ == "__main__":
    # some parameters

    init_connections()
    # # test_sudo()
    # # stop_servers()
    # close_all_connection()
    for tmp_server_logical_num in server_logical_num_list:
        server_logical_num = tmp_server_logical_num

        for tmp_depth in depths:
            depth = tmp_depth

            print("start testbed nocache for loading")
            # stop_all()
            # start_testbed("nocache")
            # time.sleep(5)
            print("loading")

            for current_method in current_methods:
                file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"

                full_path = os.path.join(
                    f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}",
                    "file.out",
                )


                stop_all()
                start_testbed("nocache")
                [per_workload_files, total_files] = sort_file_out_for_workload_generation(
                    f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}"
                )

                print("start mdtest")
                update_config(f"{HOME_DIR}/config.ini", create)
                
                for rotation_id in rotation_list:
                    updates = {
                        "global": {
                            "mode": 1,
                            "file_out": f"{HOME_DIR}/workload/file.out",
                            "server_logical_num": server_logical_num,
                            "bottleneck_id": id_0,
                            "rotation_id": rotation_id,
                            "current_method": current_method,
                            "flag_for_file_out_generation": 0,
                        },
                    }
                    update_config(f"{HOME_DIR}/config.ini", updates)
                    for serverSSH in serverSSHs:
                        sync_file_to_server(
                            serverSSH,
                            f"{HOME_DIR}/config.ini",
                            f"{HOME_DIR}/config.ini",
                        )
                    for switchSSH in switchSSHs:
                        sync_file_to_server(
                            switchSSH,
                            f"{HOME_DIR}/config.ini",
                            f"{SWITCH_HOME[0]}/netfetch-private/mdtestcpp/config.ini",
                        )

                    reload(
                        total_files,
                        depth,
                    )

                    time.sleep(2)



    # test end
    close_all_connection()
