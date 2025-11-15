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
    count_lines_in_file,
    load_hdfs,
    update_config,
    start_testbed,
    stop_all,
    sort_file_out_for_workload_generation,
    method_list,
    get_hits,
    check_read_write
)

# rmdir = {
#     'mixed': {
#         'open_close': 0,
#         'stat': 0,
#         'create': 0,
#         'delete': 0,
#         'rename': 0,
#         'chmod': 0.0,
#         'readdir': 0.0,
#         'statdir': 0.0,
#         'mkdir': 0.0,
#         'rmdir': 100.0
#     }
# }


def dumptrace(zeta, filenames_out, access_out, freq_out, total_files, workload_generation_method, server_logical_num, total_number_of_requests, depth, fs_tree_breath):
    updates = {
        "global": {
            "server_logical_num": server_logical_num,
        }
    }
    update_config(f"{HOME_DIR}/config.ini", updates)
    bottleneck_dir = f"{HOME_DIR}/workload/caches"
    bottleneck_file = os.path.join(
        bottleneck_dir,
        f"bottleneck_file_{server_logical_num}_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}_rmdir.out",
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
    command = [f"{HOME_DIR}/dumptrace_fast_rmdir", filenames_out, access_out, freq_out]
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
        )  

    return [bottleneck_id, bottleneck_namenode]

def load_mdtest(total_files, depth, zeta, fs_tree_breath, client_thread, total_files_for_mdtest):
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
        # '-Y',"uniform",
        '-d', '/',
        '-A' #load phase
        # '-V', '3',
        # '-Y', f'{HOME_DIR}/workload/workloads/access_file_{zeta}.out'
    ]
    print(f"exec {' '.join(command)}")
    with open(f'{HOME_DIR}/tmp_mdtest_{zeta}_load.out', 'w') as outfile:
        subprocess.run(command, stdout=outfile)

def run_mdtest(
    files_for_mdtest,
    depth,
    zeta,
    bottleneck_id,
    rotation_id,
    rotation_servers_num,
    run_results_dir,
    request_pruning_factor,
    need_warm,
    total_files,
    total_number_of_requests,
    fs_tree_breath,
    workload_generation_method,
    client_thread
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
        '-r',
        '-z', str(adjusted_depth),
        '-b', str(fs_tree_breath),
        '-d', '/',
        '-Y', access_out,
        '-P',str(request_pruning_factor)
    ]

    print(f"exec {' '.join(command)}")
    exp_type ='dynamic' if rotation_servers_num == 2 else 'static'
    out_file = run_results_dir / f'tmp_mdtest_{zeta}_{bottleneck_id}_{rotation_id}_{exp_type}_{rotation_servers_num}_mixed.out'
    with open(out_file, 'w') as outfile:
        subprocess.run(command, stdout=outfile)
    return out_file


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
    assert len(depths) == len(fs_tree_breathes), "depths and fs_tree_breathes should have the same length"
    for idx in workload_ids:
        assert idx < len(cfg["workloads"]), f"workload id {idx} is out of range"
    workload = cfg["workloads"][workload_ids[0]]
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
                            fs_tree_breath
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
                            f"freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}_rmdir.out",
                        )
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
                            fs_tree_breath,

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
                    depth,
                    zeta,
                    fs_tree_breath,
                    client_thread,
                    total_files_for_mdtest
                )

                # load_local(total_files, depth, zeta)
                print("loaded\n run")
                time.sleep(30)

            if cfg["program"]["rotation"]:
                round_idx = cfg["program"]["roundidx"]+"_"+str(server_logical_num)
                # round_idx = args.roundidx 
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
                            [bottleneck_id, bottleneck_namenode] = dumptrace(
                                zeta, new_path, access_out, freq_out, total_files, workload_generation_method, server_logical_num, total_number_of_requests, depth, fs_tree_breath
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

                            print(
                                f"start testbed {method_name} for test. \nround: {round_idx} zeta: {zeta} total_number_of_requests: {total_number_of_requests} \ndepth: {depth} fs_tree_breath: {fs_tree_breath} workload_generation_method: {workload_generation_method}"
                            )
                            stop_all()
                            start_testbed(method_name)

                            run_results_dir = base_results_dir / f"{method_name}_{zeta}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{workload_generation_method}"
                            run_results_dir.mkdir(parents=True, exist_ok=True)
                            hot_paths_filename = f"{HOME_DIR}/workload/caches/freq_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out"

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
                            update_config(f"{HOME_DIR}/config.ini", workload)
                            rotation_list= []
                            if bottleneck_id == 0:
                                rotation_list = [0] + [i for i in range(server_logical_num) if i != 0]
                                
                            else:
                                rotation_list = [bottleneck_id, 0] + [i for i in range(server_logical_num) if i != 0 and i != bottleneck_id]
                            # rotation_list = [0, bottleneck_id]
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
                                # 7. run mdtest
                                # if current_method == 2 or (depth >5 and workload_generation_method !=1 and current_method==0): #nocache
                                #     request_pruning_factor = int(request_pruning_factor * 2)
                                tmp_request_pruning_factor = 8
                                ret = run_mdtest(
                                    total_files,
                                    depth,
                                    zeta,
                                    bottleneck_id,
                                    rotation_id,
                                    server_logical_num,
                                    run_results_dir,
                                    request_pruning_factor=request_pruning_factor,
                                    need_warm=False,
                                    total_files=total_files,
                                    total_number_of_requests=total_number_of_requests,
                                    fs_tree_breath=fs_tree_breath,
                                    workload_generation_method=workload_generation_method,
                                    client_thread=client_thread
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
                            line_pattern = r"RMDIR\s+:\s*\d+(?:\.\d+)?\s+ms, BOTTLENECK\s+:\s*\d+(?:\.\d+)?ops/sec, ROTATION\s+:\s*\d+(?:\.\d+)?ops/sec, BOTTLENECK HIT\s+:\s*\d+, ROTATION HIT\s+:\s*\d+,"

                            while rotation_idx < len(rotation_list):
                                # result_path = run_one_rotation()

                                rotation_id = rotation_list[rotation_idx]
                                is_bottleneck = rotation_id == bottleneck_id
                                result_path = run_one_rotation()

                                bottleneck_hit, rotation_hit = get_hits(result_path, logger, line_pattern)
                                if bottleneck_hit is None:
                                    # no target line, startover
                                    logger.error("fail, retry")
                                    # stop_all()
                                    # start_testbed(method_name)
                                    # rotation_idx = 0
                                    if retry_idx > max_retry:
                                        logger.fatal(f"Bottleneck {rotation_idx} fail, max retry {max_retry} reached. Next Rotation")
                                        rotation_idx += 1
                                        retry_idx = 0
                                        break
                                    retry_idx += 1
                                    continue
                                rotation_idx += 1
                                retry_idx = 0
                                # if current_method == 2 or current_method == 6:
                                #     # nocache, cscache
                                #     # success 
                                #     rotation_idx += 1
                                #     retry_idx = 0
                                # elif current_method == 1 or current_method == 7:
                                #     # csfletch, csfletch+
                                #     need_retry = check_read_write(workload)
                                #     if need_retry:
                                #         if is_bottleneck:
                                #             if retry_idx > max_retry:
                                #                 logger.fatal(f"Bottleneck {rotation_idx} fail, max retry {max_retry} reached. Next Workload")
                                #                 # exit since there is one workload
                                #                 break
                                #             if bottleneck_hit > 1e5 and rotation_hit == 0:
                                #                 logger.info(f"bottleneck {rotation_idx} success with {retry_idx} retry")
                                #                 rotation_idx += 1
                                #                 retry_idx = 0
                                #             else:
                                #                 logger.error("Bottleneck fail, need to restart switch")
                                #                 stop_all()
                                #                 start_testbed(method_name)
                                #                 retry_idx += 1
                                #         else:
                                #             if retry_idx > max_retry:
                                #                 logger.fatal(f"rotation {rotation_idx} fail, max retry {max_retry} reached. Next rotation")
                                #                 rotation_idx += 1
                                #                 retry_idx = 0
                                #                 # continue while loop and go to next rotation
                                #                 continue
                                #             if bottleneck_hit > 1e5 and rotation_hit > 0:
                                #                 logger.info(f"rotation {rotation_idx} success with {retry_idx} retry")
                                #                 rotation_idx += 1
                                #                 retry_idx = 0
                                #             else:
                                #                 logger.error("rotation fail, need to retry rotation")
                                #                 retry_idx += 1
                                #     else:
                                #         logger.info(f"single write, no need to retry")
                                #         rotation_idx += 1
                                #         retry_idx = 0
                                # else:
                                #     logger.fatal(f"unknown method {current_method}")


    # test end
    close_all_connection()



if __name__ == "__main__":
    entrypoint()