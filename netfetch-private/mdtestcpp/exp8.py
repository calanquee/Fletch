import os
import subprocess
import time
import shutil
from pathlib import Path
import hydra
from omegaconf import DictConfig, OmegaConf
import logging
from utils.workload_gen_util.work_generate import file_offset
from utils.remote_util import init_connections, close_all_connection, serverSSHs, switchSSHs, SWITCH_HOME, sync_file_to_server
from exp_common import (
    HOME_DIR, 
    generate_file_out,
    generate_workload,
    dumptrace,
    count_lines_in_file,
    load_hdfs,
    load_mdtest,
    update_config,
    start_testbed,
    stop_all,
    sort_file_out_for_workload_generation,
    method_list,
    get_hits,
    check_read_write
)

def run_mdtest(
    files_for_mdtest,
    depth,
    zeta,
    bottleneck_id,
    rotation_id,
    rotation_servers_num,
    run_results_dir,
    request_pruning_factor,
    tmp_rule,
    total_files,
    total_number_of_requests,
    fs_tree_breath,
    workload_generation_method,
    client_thread,
    current_method,
    workload_idx,
):
    access_dir = f"{HOME_DIR}/workload/workloads"
    if (current_method == 6 or current_method == 7) and workload_idx not in [0,1,2]:
        access_out = os.path.join(
            access_dir,
            f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.extend.out",
        )
    else:
        access_out = os.path.join(
            access_dir,
            f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
        )
    if not os.path.exists(access_out):
        print(f"access_out {access_out} does not exist")
        return
    if zeta == 0:
        access_out = "uniform"
    adjusted_depth = depth - file_offset
    # fmt: off
    # ./mdtest -a 64 -n 63962892 -m -z 8 -b 4 -d / -Y /home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/workload/workloads/access_file_63962892_64000000_10_4_0.9_3.out -P 300000
    command = [
        'stdbuf', '-o0',
        './mdtest',
        '-a', str(client_thread),
        '-n', str(files_for_mdtest),
        '-m',
        '-z', str(adjusted_depth),
        '-b', str(fs_tree_breath),
        '-d', '/',
        '-Y', access_out,
        '-P',str(request_pruning_factor)
    ]
    print(f"exec {' '.join(command)}")
    exp_type ='dynamic' if rotation_servers_num == 2 else 'static'
    with open(run_results_dir / f'tmp_mdtest_{zeta}_{bottleneck_id}_{rotation_id}_{tmp_rule}_{rotation_servers_num}_mixed.out', 'w') as outfile:
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
    max_retry = cfg["experiment"]["max_retry"]
    rules = cfg["experiment"]["rules"]
    assert len(depths) == len(fs_tree_breathes), "depths and fs_tree_breathes should have the same length"
    for idx in workload_ids:
        assert idx < len(cfg["workloads"]), f"workload id {idx} is out of range"
    server_logical_num = server_logical_num_list[0]
    init_connections()

    round_idx = cfg["program"]["roundidx"]+"_"+"dynamic"
    for depth, fs_tree_breath in zip(depths, fs_tree_breathes):
        if cfg["program"]["generate"]:
            print("generate start")
            # 1. generate file.out: 40 files, 4 depth, 1 breath
            generate_file_out(total_files_for_mdtest=total_files_for_mdtest, depth=depth, fs_tree_breath=fs_tree_breath, server_logical_num=server_logical_num)
            # 2. sort file.out and return total_files
            [per_workload_files, total_files] = sort_file_out_for_workload_generation(
                f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}", fs_tree_breath
            )
            print("per_workload_files", per_workload_files, "total_files", total_files)
            # 3. generate workload
            for zeta in zetas:
                print(f"generate workload for {zeta}")
                for workload_generation_method in workload_generation_methods:
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
                        zeta,
                        total_files,
                        # total_number_of_requests,
                        depth,
                        workload_generation_method,
                        new_path,
                        total_number_of_requests,
                        fs_tree_breath,
                        extend=False
                    )
                    # generate requests
                    # map request idx to filename
                    file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
                    access_dir = f"{HOME_DIR}/workload/workloads"
                    access_out = os.path.join(
                        access_dir,
                        f"access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
                    )
                    print("skip generate_requests no need for test any more")
                    # generate caches for netcache
                    # generate_cache_list(total_files, zeta, cache_capacity, current_methods)

                    freq_dir = f"{HOME_DIR}/workload/caches"
                    freq_out = os.path.join(
                        freq_dir,
                        f"freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
                    )

                    [bottleneck_id, bottleneck_namenode] = dumptrace(
                        zeta, 
                        new_path, 
                        access_out, 
                        freq_out, 
                        total_files, 
                        workload_generation_method,
                        16,
                        total_number_of_requests,
                        depth,
                        fs_tree_breath
                    )


            print("generate done")


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

        if cfg["program"]["needload"]:

            print("start testbed nocache for loading")

            print("loading")
            bottleneck_id = 1
            updates = {
                "global": {
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
                depth,
                zeta,
                fs_tree_breath, 
                client_thread,
                total_files_for_mdtest,
            )

            # load_local(total_files, depth, zeta)
            print("loaded\n run")
            time.sleep(30)


        # data4 = (np.array(data[i*17+12][5:length + 5], dtype=int)) / 1
        if cfg["program"]["dynamic"]:
            workloads = cfg["workloads"]
            for idx, tmp_update_list_mixed in enumerate(workloads):
                # skip workloads
                if idx not in workload_ids:
                    continue
                results_dir = base_results_dir / f"{idx}"
                results_dir.mkdir(parents=True, exist_ok=True)
                for zeta in zetas:
                    # GET namenode
                    for current_method in current_methods:
                        for workload_generation_method in workload_generation_methods:
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

                            method_name = "error"
                            if current_method <= 7:
                                method_name = method_list[current_method]
                            else:
                                method_name = "error"



                            run_results_dir = results_dir / f"{method_name}_{zeta}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{workload_generation_method}"
                            run_results_dir.mkdir(parents=True, exist_ok=True)
                            hot_paths_filename = f"{HOME_DIR}/workload/caches/freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out"

                            number_of_caches = 0
                            if (
                                current_method == 0
                                or current_method == 2
                                or current_method == 1
                                or current_method == 3
                                or current_method == 4
                                or current_method == 5 or current_method == 7
                            ):
                                number_of_caches = count_lines_in_file(hot_paths_filename)
                                number_of_caches = min(int(number_of_caches), cache_capacity)

                            # # TODO
                            # if current_method == 2:
                            #     number_of_caches = min(int(number_of_caches), cache_capacity)

                            # 6.2 warmup
                            print("Warmup")
                            # TODO
                            time.sleep(5)
                            print("start mdtest")
                            update_config(f"{HOME_DIR}/config.ini", tmp_update_list_mixed)

                            for tmp_rule in rules:
                                print(
                                    f"start testbed {method_name} for test. \nround: {round_idx} zeta: {zeta} total_number_of_requests: {total_number_of_requests} \ndepth: {depth} fs_tree_breath: {fs_tree_breath} workload_generation_method: {workload_generation_method}"
                                )

                                # round_idx = cfg["program"]["roundidx"]+"_"+ tmp_rule + str(idx)
                                # round_idx = args.roundidx

                                # run_results_dir = base_results_dir / f"{method_name}_{zeta}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{workload_generation_method}"
                                # run_results_dir.mkdir(parents=True, exist_ok=True)

                                updates = {
                                    "global": {
                                        "mode": 0,
                                        "hot_paths_filename": hot_paths_filename,
                                        "hot_path_size": number_of_caches,
                                        "file_out": f"{HOME_DIR}/workload/file.out",
                                        "server_logical_num": server_logical_num,
                                        "bottleneck_id": 1,
                                        "rotation_id": 0,
                                        "current_method": current_method,
                                        "flag_for_file_out_generation": 0,
                                    },
                                    'dynamic': {
                                        'rule': tmp_rule,
                                        'scale': 100,
                                        'period': 20,
                                    }
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

                                stop_all()
                                start_testbed(method_name)
                                # 7. run mdtest
                                tmp_request_pruning_factor = request_pruning_factor
                                if idx == 0:
                                    tmp_request_pruning_factor = 3.5
                                else:
                                    tmp_request_pruning_factor = 2.9
                                if current_method == 7:
                                    tmp_request_pruning_factor = 1.5
                                    if idx == 0:
                                        tmp_request_pruning_factor = 2
                                elif current_method == 6:
                                    tmp_request_pruning_factor = 2
                                print("results in ",run_results_dir)
                                run_mdtest(
                                    total_files,
                                    depth,
                                    zeta,
                                    1,
                                    0,
                                    server_logical_num,
                                    run_results_dir,
                                    request_pruning_factor=tmp_request_pruning_factor,
                                    tmp_rule=tmp_rule,
                                    total_files=total_files,
                                    total_number_of_requests=total_number_of_requests,
                                    fs_tree_breath=fs_tree_breath,
                                    workload_generation_method=workload_generation_method,
                                    client_thread=client_thread,
                                    current_method=current_method,
                                    workload_idx=idx,
                                )

                                time.sleep(2)
                                # Move all latency files that match the pattern
                                for file in os.listdir(HOME_DIR):
                                    if file.startswith(f"latency_{1}_{0}_{0}") or file.startswith(f"latency_{1}_{0}_{1}"):
                                        shutil.move(os.path.join(HOME_DIR, file), os.path.join(run_results_dir, file))



    # test end
    close_all_connection()

if __name__ == "__main__":
    entrypoint()