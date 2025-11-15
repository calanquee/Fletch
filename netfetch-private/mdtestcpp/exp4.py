import os
import subprocess
import time
import shutil
from pathlib import Path
import hydra
from omegaconf import DictConfig, OmegaConf
import os
import pandas as pd
import numpy as np
import re
import logging
import socket
import struct
from exp_common import (
    HOME_DIR, 
    dumptrace,
    count_lines_in_file,
    update_config,
    start_testbed,
    stop_all,
    sort_file_out_for_workload_generation,
    method_list,
    get_hits
)
from utils.workload_gen_util.work_generate import file_offset
from utils.remote_util import init_connections, close_all_connection, serverSSHs, switchSSHs, SWITCH_HOME, sync_file_to_server

from exp4_rate_limiter import ccache_list_0, ccache_list_1, nocache_list_0, nocache_list_1, fletch_list_0, fletch_list_1, cfletch_list_0, cfletch_list_1

# socket handler
socket_popserver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
switch_ip = "10.26.43.200"
switch_port = 5006

def run_mdtest_latency(
    files_for_mdtest,
    depth,
    zeta,
    bottleneck_id,
    rotation_id,
    rotation_servers_num,
    run_results_dir,
    request_pruning_factor,
    delay,
    total_files,
    workload_generation_method,
    client_thread,
    fs_tree_breath,
    total_number_of_requests,
    need_compaction = False,
):
    access_dir = f"{HOME_DIR}/workload/workloads"
    access_out = os.path.join(
        access_dir,
        f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
    )
    if zeta == 0:
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
        '-P',str(request_pruning_factor),
        '-p',str(delay),
    ]
    if need_compaction:
        command.append('-Q')
    print(rotation_id, f"exec {' '.join(command)}")
    exp_type ='dynamic' if rotation_servers_num == 2 else 'static'
    file = run_results_dir / f'tmp_mdtest_{zeta}_{bottleneck_id}_{rotation_id}_{exp_type}_{rotation_servers_num}_mixed.out'
    with open(file, 'w') as outfile:
        subprocess.run(command, stdout=outfile)
    return file

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
    max_retry = cfg["experiment"]["max_retry"]
    assert len(depths) == 1, "only one depth is supported"
    assert len(server_logical_num_list) == 1, "only one server logical num is supported"
    assert len(fs_tree_breathes) == 1, "only one fs tree breath is supported"
    for idx in workload_ids:
        assert idx < len(cfg["workloads"]), f"workload id {idx} is out of range"
    depth = depths[0]
    server_logical_num = server_logical_num_list[0]
    fs_tree_breath = fs_tree_breathes[0]


    init_connections()

    round_idx = cfg["program"]["roundidx"]+"_"+str(server_logical_num)
    base_results_dir = Path(HOME_DIR) / "results" / f"round_{round_idx}"
    base_results_dir.mkdir(parents=True, exist_ok=True)
    # write hydra config to results dir
    with open(base_results_dir / "config.yaml", "w") as f:
        f.write(OmegaConf.to_yaml(cfg))

    # setup logger 
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    handler = logging.FileHandler((base_results_dir / 'running_status.log').as_posix(), mode='w')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    [per_workload_files, total_files] = sort_file_out_for_workload_generation(
        f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}", fs_tree_breath=fs_tree_breath
    )


    if cfg["program"]["rotation"]:
        workloads = cfg["workloads"]
        for idx, tmp_update_list_mixed in enumerate(workloads):
            # skip workloads
            if idx not in workload_ids:
                continue
            print("idx: ", idx)
            print("tmp_update_list_mixed: ", tmp_update_list_mixed)

            # round_idx = cfg["program"]["roundidx"]+"_"+str(server_logical_num) +"_"+ str(delay_idx) +"_"+ str(idx)
            workload_results_dir = base_results_dir / f"{idx}"
            workload_results_dir.mkdir(parents=True, exist_ok=True)

            delay_list = []
            for current_method in current_methods:
                # select delay list
                if idx == 0:
                    if current_method == 2:
                        delay_list = nocache_list_0
                    elif current_method == 6:
                        delay_list = ccache_list_0
                    elif current_method == 1:
                        delay_list = fletch_list_0
                    elif current_method == 7:
                        delay_list = cfletch_list_0
                elif idx == 1:
                    if current_method == 2:
                        delay_list = nocache_list_1
                    elif current_method == 6:
                        delay_list = ccache_list_1
                    elif current_method == 1:
                        delay_list = fletch_list_1
                    elif current_method == 7:
                        delay_list = cfletch_list_1
                else:
                    raise ValueError(f"unknown workload {idx}")

                for delay_idx, tmp_delay_list in enumerate(delay_list):
                    results_dir = workload_results_dir / f"{delay_idx}"
                    results_dir.mkdir(parents=True, exist_ok=True)
                    for zeta in zetas:
                    # GET namenode
                        # skip tag
                        # if idx != 0 and current_method == 6 :
                        # continue
                        # if 2000 == tmp_delay and current_method == 6:
                        # continue
                        for workload_generation_method in workload_generation_methods:
                            freq_dir = f"{HOME_DIR}/workload/caches"
                            freq_out = os.path.join(
                                freq_dir,
                                f"freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
                            )
                            file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
                            access_dir = f"{HOME_DIR}/workload/workloads"
                            access_out = os.path.join(
                                access_dir,
                                f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
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
                                zeta, 
                                new_path, 
                                access_out, 
                                freq_out,
                                total_files, 
                                workload_generation_method,
                                server_logical_num,
                                total_number_of_requests,
                                depth,
                                fs_tree_breath
                            )
                            print(
                                "bottleneck_id is",
                                bottleneck_id,
                                "bottleneck namenode is",
                                bottleneck_namenode,
                                "server number is",
                                server_logical_num,
                                f"for {depth} {zeta} {workload_generation_method} in {current_method}"
                            )
                            method_name = "error"
                            if current_method <= 7:
                                method_name = method_list[current_method]
                            else:
                                method_name = "error"

                            # print(
                            #     f"start testbed {method_name} for test. \nround: {round_idx} zeta: {zeta} total_number_of_requests: {total_number_of_requests} \ndepth: {depth} fs_tree_breath: {fs_tree_breath} workload_generation_method: {workload_generation_method}"
                            # )

                            logger.info(
                                f"start testbed {method_name} for worload {idx}. round: {round_idx} zeta: {zeta} \n\n"
                            )

                            stop_all()
                            start_testbed(method_name)

                            run_results_dir = results_dir / f"{method_name}_{zeta}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{workload_generation_method}"
                            run_results_dir.mkdir(parents=True, exist_ok=True)

                            hot_paths_filename = f"{HOME_DIR}/workload/caches/freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out"

                            number_of_caches = 0
                            if current_method == 0 or current_method == 1 or current_method == 3 or current_method == 4 or current_method == 5 or current_method == 7:
                                number_of_caches = count_lines_in_file(hot_paths_filename)
                                number_of_caches = min(int(number_of_caches), cache_capacity)

                            # TODO
                            if current_method == 2:
                                number_of_caches = 0

                            # # 6.2 warmup
                            print("start mdtest")
                            update_config(f"{HOME_DIR}/config.ini", tmp_update_list_mixed)
                            rotation_list= []
                            if bottleneck_id == 0:
                                rotation_list = [0] + [i for i in range(server_logical_num) if i != 0]
                            else:
                                rotation_list = [bottleneck_id, 0] + [i for i in range(server_logical_num) if i != 0 and i != bottleneck_id]

                            def run_one_rotation():
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
                                tmp_request_pruning_factor = request_pruning_factor
                                
                                # 7. run mdtest
                                if idx == 0  and (current_method == 7 or current_method == 6):
                                    tmp_request_pruning_factor = 4
                                elif idx != 0 and (current_method == 7 or current_method == 6):
                                    tmp_request_pruning_factor = 2
                                elif idx != 0 and (current_method == 1):
                                    tmp_request_pruning_factor = 5.5

                                need_compaction = False
                                if idx == 0 and (current_method == 7 or current_method == 6):
                                    need_compaction = True
                                ret = run_mdtest_latency(
                                    total_files,
                                    depth,
                                    zeta,
                                    bottleneck_id,
                                    rotation_id,
                                    server_logical_num,
                                    run_results_dir,
                                    request_pruning_factor=tmp_request_pruning_factor,
                                    delay = tmp_delay_list[rotation_id],
                                    total_files=total_files,
                                    workload_generation_method=workload_generation_method,
                                    client_thread=client_thread,
                                    fs_tree_breath=fs_tree_breath,
                                    total_number_of_requests=total_number_of_requests,
                                    need_compaction=need_compaction,
                                )
                                # if current_method == 2 or (depth >5 and workload_generation_method !=1 and current_method==0):
                                #     request_pruning_factor = int(request_pruning_factor / 2)
                                time.sleep(2)
                                # Move all latency files that match the pattern
                                for file in os.listdir(HOME_DIR):
                                    if file.startswith(f"latency_{bottleneck_id}_{rotation_id}_{bottleneck_id}") or file.startswith(f"latency_{bottleneck_id}_{rotation_id}_{rotation_id}"):
                                        shutil.move(os.path.join(HOME_DIR, file), os.path.join(run_results_dir, file))
                                return ret

                            rotation_idx = 0
                            retry_idx = 0
                            while rotation_idx < len(rotation_list):
                                rotation_id = rotation_list[rotation_idx]
                                is_bottleneck = rotation_id == bottleneck_id
                                result_path = run_one_rotation()
                                if current_method == 7 or current_method == 1:
                                    # lets send some messages to the server
                                    control_type = 25
                                    payload = b"1234"
                                    packet = struct.pack("=i", control_type) + payload
                                    socket_popserver.sendto(packet, (switch_ip, switch_port))
                                    # print(f"Sent UDP packet with control_type {control_type} to {switch_ip}:{switch_port}")

                                bottleneck_hit, rotation_hit = get_hits(result_path, logger)

                                if idx == 0 and current_method == 7: # alibaba only
                                    if rotation_idx < len(rotation_list):
                                        time.sleep(100) # sleep for compaction
                                if idx == 0 and current_method == 6: # alibaba only
                                    if rotation_idx < len(rotation_list):
                                        time.sleep(50) # sleep for compaction
                                
                                if bottleneck_hit is None:
                                    if is_bottleneck:
                                        # no target line, startover
                                        logger.error("bottleneck fail, need to restart switch")
                                        stop_all()
                                        start_testbed(method_name, depth)
                                        rotation_idx = 0
                                        retry_idx = 0
                                        continue
                                    else:
                                        logger.error("rotation fail, need to retry rotation")
                                        # don't change rotation_idx
                                        retry_idx += 1
                                        continue
                                if current_method == 2 or current_method == 6:
                                    # nocache, cscache
                                    # success 
                                    rotation_idx += 1
                                    retry_idx = 0
                                elif current_method == 1 or current_method == 7:
                                    if is_bottleneck:
                                        if retry_idx > max_retry:
                                            logger.fatal(f"Bottleneck {rotation_idx} fail, max retry {max_retry} reached. Next Workload")
                                            # exit while loop and go to next workload
                                            break
                                        if bottleneck_hit > 1e5 and rotation_hit == 0:
                                            logger.info(f"bottleneck {rotation_idx} success with {retry_idx} retry")
                                            rotation_idx += 1
                                            retry_idx = 0
                                        else:
                                            logger.error("Bottleneck fail, need to restart switch")
                                            stop_all()
                                            start_testbed(method_name, depth)
                                            retry_idx += 1
                                    else:
                                        if retry_idx > max_retry:
                                            logger.fatal(f"rotation {rotation_idx} fail, max retry {max_retry} reached. Next rotation")
                                            rotation_idx += 1
                                            retry_idx = 0
                                            # continue while loop and go to next rotation
                                            continue
                                        if bottleneck_hit > 1e5 and rotation_hit > 0:
                                            logger.info(f"rotation {rotation_idx} success with {retry_idx} retry")
                                            rotation_idx += 1
                                            retry_idx = 0
                                        else:
                                            logger.error("rotation fail, need to retry rotation")
                                            if rotation_idx == 1:
                                                logger.error("first rotation fail, need to restart testbed")
                                                stop_all()
                                                start_testbed(method_name, depth)
                                                rotation_idx = 0
                                                retry_idx = 0
                                            retry_idx += 1
                                else:
                                    logger.fatal(f"unknown method {current_method}")


    # test end
    close_all_connection()
    socket_popserver.close()


if __name__ == "__main__":
    entrypoint()