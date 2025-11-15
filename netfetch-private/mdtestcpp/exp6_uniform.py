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

import hydra
from omegaconf import DictConfig, OmegaConf

from exp_common import (method_list, HOME_DIR, generate_file_out,
                        sort_file_out_for_workload_generation,
                        count_lines_in_file, update_config, load_hdfs)

def generate_workload(
    total_files,
    depth,
    total_number_of_requests,
    fs_tree_breath,
):
    destination_dir = f"{HOME_DIR}/workload/workloads/access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out"
    # check whether we have generate it before
    # print("destination_dir",destination_dir)
    if os.path.exists(destination_dir):
        print(
            f"[generate_workload] we have generate {destination_dir} skip, you could rm it to re-gen one"
        )
        return
    original_path = f"{HOME_DIR}/workload/workloads/access_file.out"
    generate_uniform(total_files, total_number_of_requests, original_path)

    destination = f"{HOME_DIR}/workload/workloads/access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out"
    os.rename(original_path, destination)


def dumptrace(filenames_out, access_out, freq_out, server_logical_num, total_files, total_number_of_requests, depth, fs_tree_breath):
    updates = {
        "global": {
            "server_logical_num": server_logical_num,
        }
    }
    update_config(f"{HOME_DIR}/config.ini", updates)
    bottleneck_dir = f"{HOME_DIR}/workload/caches"
    bottleneck_file = os.path.join(
        bottleneck_dir,
        f"bottleneck_file_{server_logical_num}_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out",
    )
    if os.path.exists(bottleneck_file):
        print(f"[dump] we have generate {bottleneck_file} skip")
        with open(bottleneck_file, "r", encoding="utf-8") as file:
            lines = file.readlines()
        bottleneck_id = None
        for line in lines:
            if "bottleneck_id:" in line:
                bottleneck_id = int(line.split(":")[1].strip())
        return [bottleneck_id, "ns" + str(bottleneck_id + 1)]
    command = [f"{HOME_DIR}/dumptrace_fast", filenames_out, access_out, freq_out]
    print(f"exec {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        print("Error running the command:")
        print(result.stderr)
        return

    namenode = None
    for line in result.stdout.splitlines():
        if "The namenode with the most load is:" in line:
            namenode = line.split("The namenode with the most load is: ns")[1].strip()

            break

    bottleneck_id = int(namenode) - 1
    bottleneck_namenode = "ns" + namenode

    with open(bottleneck_file, "w") as file:
        file.write(
            f"server_num: {server_logical_num}\nbottleneck_id: {bottleneck_id}"
        )  # 写入字符串到文件

    return [bottleneck_id, bottleneck_namenode]


def load_mdtest(total_files, depth, fs_tree_breath, client_thread, total_files_for_mdtest):
    stop_all()
    start_testbed("nocache")
    time.sleep(5)
    # add dfsrouter admin
    adjusted_depth = depth - file_offset
    print(f"/#test-dir.0.d.{adjusted_depth}.n.{total_files}.b.{fs_tree_breath}")

    # fmt: off
    command = [
        'stdbuf', '-o0',
        # 'mpirun', '-n', str(client_thread),
        './mdtest',
        '-a', str(client_thread),
        '-n', str(total_files_for_mdtest),
        '-S',
        '-C',
        '-z', str(adjusted_depth),
        '-b', str(fs_tree_breath),
        '-d', '/',
        '-A' #load phase
        # '-V', '3',
        # '-Y', f'{HOME_DIR}/workload/workloads/access_file_{zeta}.out'
    ]
    print(f"exec {command}")
    with open(f'{HOME_DIR}/tmp_mdtest_uniform_load.out', 'w') as outfile:
        subprocess.run(command, stdout=outfile)

def run_mdtest(
    files_for_mdtest,
    depth,
    bottleneck_id,
    rotation_id,
    rotation_servers_num,
    run_results_dir,
    request_pruning_factor,
    client_thread,
    fs_tree_breath
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
        '-Y', access_out,
        '-P',str(request_pruning_factor)
    ]

    print(f"exec {' '.join(command)}")
    exp_type ='dynamic' if rotation_servers_num == 2 else 'static'
    with open(run_results_dir / f'tmp_mdtest_uniform_{bottleneck_id}_{rotation_id}_{exp_type}_{rotation_servers_num}.out', 'w') as outfile:
        subprocess.run(command, stdout=outfile)


@hydra.main(version_base=None, config_path="configs", config_name="config")
def entrypoint(cfg: DictConfig):
    print("Loading the config")
    max_requests = cfg["experiment"]["max_requests"]
    request_pruning_factor = cfg["experiment"]["request_pruning_factor"]
    depths = cfg["experiment"]["depths"]
    cache_capacity = cfg["experiment"]["cache_capacity"]
    zetas = cfg["experiment"]["zetas"]
    workload_generation_methods = cfg["experiment"]["workload_generation_methods"]
    current_methods = cfg["experiment"]["current_methods"]
    client_thread = cfg["experiment"]["client_thread"]
    server_logical_num_list = cfg["experiment"]["server_logical_num_list"]
    total_files_for_mdtest = cfg["experiment"]["total_files_for_mdtest"]
    total_number_of_requests = cfg["experiment"]["total_number_of_requests"]
    fs_tree_breathes = cfg["experiment"]["fs_tree_breath"]
    workload_ids = cfg["experiment"]["workloads"]
    for idx in workload_ids:
        assert idx < len(cfg["workloads"]), f"workload id {idx} is out of range"
    assert len(depths) == len(fs_tree_breathes), "depths and fs_tree_breathes should have the same length"
    init_connections()
    # # test_sudo()
    # # stop_servers()
    # close_all_connection()
    for server_logical_num in server_logical_num_list:
        round_idx = cfg["program"]["roundidx"]+"_"+str(server_logical_num)
        for depth, fs_tree_breath in zip(depths, fs_tree_breathes):
            if cfg["program"]["generate"]:
                print("generate start")
                # 1. generate file.out: 40 files, 4 depth, 1 breath
                generate_file_out(total_files_for_mdtest, depth, fs_tree_breath, server_logical_num)
                # 2. sort file.out and return total_files
                [per_workload_files, total_files] = sort_file_out_for_workload_generation(
                    f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}", fs_tree_breath
                )
                print("per_workload_files", per_workload_files, "total_files", total_files)
                # 3. generate workload

                print(f"generate workload for uniform")

                # forget workload_generation_method
                full_path = os.path.join(
                    f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}",
                    "file.out",
                )
                new_path = full_path + ".sorted"
                # print(new_path)
                if fs_tree_breath == 1:
                    new_path = full_path
                generate_workload(
                    total_files,
                    depth,
                    total_number_of_requests,
                    fs_tree_breath,
                )
                # generate requests
                # map request idx to filename
                file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
                access_dir = f"{HOME_DIR}/workload/workloads"
                access_out = os.path.join(
                    access_dir,
                    f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out",
                )
                print("skip generate_requests no need for test any more")
                # generate caches for netcache
                # generate_cache_list(total_files, zeta, cache_capacity, current_methods)

                freq_dir = f"{HOME_DIR}/workload/caches"
                freq_out = os.path.join(
                    freq_dir,
                    f"freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out",
                )
                [bottleneck_id, bottleneck_namenode] = dumptrace(
                    new_path, access_out, freq_out, server_logical_num, total_files, total_number_of_requests, depth, fs_tree_breath
                )
                print(
                    "bottleneck_id is",
                    bottleneck_id,
                    "bottleneck namenode is",
                    bottleneck_namenode,
                    "server number is",
                    server_logical_num,
                )

                print("generate done")


            base_results_dir = Path(HOME_DIR) / "results" / f"round_{round_idx}"
            base_results_dir.mkdir(parents=True, exist_ok=True)
            # write hydra config to results dir
            with open(base_results_dir / "config.yaml", "w") as f:
                f.write(OmegaConf.to_yaml(cfg))
            if cfg["program"]["needload"]:
                # 6. pre run
                # 6.1 load. for load ,zeta and method wont affetc it
                # we dont care rotation_id for 6.1 and 6.2
                print("start testbed nocache for loading")
                # stop_all()
                # start_testbed("nocache")
                # time.sleep(5)
                print("loading")

                updates = {
                    "global": {
                        # "hot_paths_filename": f"{HOME_DIR}/workload/caches/netcache_{zeta}.out",
                        # "hot_path_size": number_of_caches,
                        "file_out": f"{HOME_DIR}/workload/file.out",
                        "server_logical_num": server_logical_num,
                        "bottleneck_id": bottleneck_id,
                        "rotation_id": (1 if 0 == bottleneck_id else 0),
                        "current_method": 2,
                        "flag_for_file_out_generation": 0,
                    }
                }
                update_config(f"{HOME_DIR}/config.ini", updates)

                load_hdfs(total_files, depth, zeta, total_files_for_mdtest, fs_tree_breath, is_dir_only=True)
                load_mdtest(
                    total_files,
                    depth, fs_tree_breath, client_thread, total_files_for_mdtest)

                # load_local(total_files, depth, zeta)
                print("loaded\n run")
                time.sleep(30)

            if cfg["program"]["rotation"]:
                workloads = cfg["workloads"]
                for idx, tmp_update_list_mixed in enumerate(workloads):
                    if idx not in workload_ids:
                        continue
                    round_idx = cfg["program"]["roundidx"]+"_"+str(server_logical_num)+ str(idx)
                    results_dir = base_results_dir / f"{idx}"
                    results_dir.mkdir(parents=True, exist_ok=True)
                    for zeta in zetas:
                        # GET namenode
                        for current_method in current_methods:
                            # if idx == 0:
                            # continue
                            for workload_generation_method in workload_generation_methods:
                                file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
                                access_dir = f"{HOME_DIR}/workload/workloads"
                                access_out = os.path.join(
                                    access_dir,
                                    f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out",
                                )
                                full_path = os.path.join(
                                    f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}",
                                    "file.out",
                                )
                                new_path = full_path + ".sorted"
                                # print(new_path)
                                if fs_tree_breath == 1:
                                    new_path = full_path
                                [bottleneck_id, bottleneck_namenode] = dumptrace(
                                    new_path, access_out, freq_out, server_logical_num, total_files, total_number_of_requests, depth, fs_tree_breath
                                )
                                print(
                                    "bottleneck_id is",
                                    bottleneck_id,
                                    "bottleneck namenode is",
                                    bottleneck_namenode,
                                    "server number is",
                                    server_logical_num,
                                    f"for {depth} uniform in {current_method}"
                                )
                                method_name = "error"
                                if current_method <= 7:
                                    method_name = method_list[current_method]
                                else:
                                    method_name = "error"

                                print(
                                    f"start testbed {method_name} for test. \nround: {round_idx} zeta: uniform total_number_of_requests: {total_number_of_requests} \ndepth: {depth} fs_tree_breath: {fs_tree_breath} workload_generation_method: {workload_generation_method}"
                                )
                                stop_all()
                                start_testbed(method_name)

                                run_results_dir = results_dir / f"{method_name}_uniform_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform"
                                run_results_dir.mkdir(parents=True, exist_ok=True)

                                hot_paths_filename = f"{HOME_DIR}/workload/caches/freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_uniform.out"

                                number_of_caches = 0
                                if (
                                    current_method == 0
                                    or current_method == 1
                                    or current_method == 3
                                    or current_method == 4
                                    or current_method == 5 or current_method == 7
                                ):
                                    number_of_caches = count_lines_in_file(hot_paths_filename)
                                    number_of_caches = min(int(number_of_caches), cache_capacity)

                                # TODO
                                if current_method == 2:
                                    number_of_caches = 0

                                # 6.2 warmup
                                print("Warmup")
                                # TODO
                                time.sleep(5)
                                print("start mdtest")
                                update_config(f"{HOME_DIR}/config.ini", tmp_update_list_mixed)
                                rotation_list = [bottleneck_id, 0] + [i for i in range(server_logical_num) if i != 0 and i != bottleneck_id]
                                for rotation_id in rotation_list:
                                    updates = {
                                        "global": {
                                            "mode": 1,
                                            "hot_paths_filename": hot_paths_filename,
                                            "hot_path_size": number_of_caches,
                                            "file_out": f"{HOME_DIR}/workload/file.out",
                                            "server_logical_num": server_logical_num,
                                            "bottleneck_id": bottleneck_id,
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
                                    # 7. run mdtest
                                    # if current_method == 2 or (depth >5 and workload_generation_method !=1 and current_method==0): #nocache
                                    #     request_pruning_factor = int(request_pruning_factor * 2)
                                    tmp_request_pruning_factor = request_pruning_factor
                                    if idx ==0  and (current_method == 7 or current_method == 6):
                                        tmp_request_pruning_factor = 9
                                    elif idx !=0 and (current_method == 7 or current_method == 6):
                                        tmp_request_pruning_factor = 6.5
                                    run_mdtest(
                                        total_files,
                                        depth,
                                        bottleneck_id,
                                        rotation_id,
                                        server_logical_num,
                                        run_results_dir,
                                        request_pruning_factor=tmp_request_pruning_factor,
                                        client_thread=client_thread,
                                        fs_tree_breath=fs_tree_breath,
                                    )
                                    # if current_method == 2 or (depth >5 and workload_generation_method !=1 and current_method==0):
                                    #     request_pruning_factor = int(request_pruning_factor / 2)
                                    time.sleep(2)
                                    # Move all latency files that match the pattern
                                    for file in os.listdir(HOME_DIR):
                                        if file.startswith(f"latency_{bottleneck_id}_{rotation_id}_{bottleneck_id}") or file.startswith(f"latency_{bottleneck_id}_{rotation_id}_{rotation_id}"):
                                            shutil.move(os.path.join(HOME_DIR, file), run_results_dir / file)



    # test end
    close_all_connection()


if __name__ == "__main__":
    entrypoint()