import os
import subprocess
import configparser
import time
import shutil
import threading
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from hdfs3 import HDFileSystem
import re
from utils.workload_gen_util.work_generate import generate_workloads, file_offset
from utils.workload_gen_util.fast_sort_lines import sort_lines
from utils.remote_util import SERVER_IPs, stop_all, start_testbed
from utils.dir_tree import gen_dir_tree
import numpy as np
HOME_DIR = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp"


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

def generate_file_out(total_files_for_mdtest, depth, fs_tree_breath, server_logical_num):
    store_dir = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}"
    # check whether we have generate it before
    if os.path.exists(store_dir + "/file.out.sorted"):
        print(f"[file_out] we have generate {store_dir} skip")
        return
    adjusted_depth = depth - file_offset
    # will not send packets
    updates = {
        "global": {
            "file_out": f"{HOME_DIR}/workload/file.out",
            "flag_for_file_out_generation": 1,
            "mode": 1,
            "current_method": 2,
            "server_logical_num": server_logical_num,
        }
    }
    update_config(f"{HOME_DIR}/config.ini", updates)
    # generate all files
    # fmt: off
    command = [
        'stdbuf', '-o0',
        # 'mpirun', '-n', '1',
        f'{HOME_DIR}/mdtest', '-a','1','-n', str(total_files_for_mdtest), '-S','-C', '-F', '-z', str(adjusted_depth), '-b', str(fs_tree_breath), '-d', '/',
        # modify to fit mdtest_rate
    ]

    print(f"exec {' '.join(command)}")
    with open(f'{HOME_DIR}/tmp_mdtest.out', 'w') as outfile:
        subprocess.run(command, stdout=outfile)

    
    if os.path.exists(store_dir):
        
        for filename in os.listdir(store_dir):
            file_path = os.path.join(store_dir, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                # print(f"del {file_path}")
                os.unlink(file_path)  
        # print(f"All files in the directory {store_dir} have been removed.")
    else:
        
        os.mkdir(store_dir)

    original_file_path = f"{HOME_DIR}/workload/file.out.0"  
    new_file_path = f"{HOME_DIR}/workload/file.out"
    if os.path.exists(original_file_path):
        shutil.move(original_file_path, new_file_path)
    else:
        print(f"[file_out] {original_file_path} does not exist.")

    original_file_path = f"{HOME_DIR}/workload/file.out"
    new_file_path = os.path.join(store_dir, f"file.out")
    # print(f"{original_file_path} to {new_file_path}")
    if os.path.exists(original_file_path):
        shutil.move(original_file_path, new_file_path)
    else:
        print(f"[file_out] {original_file_path} does not exist.")

def count_lines_in_file(filepath):
    # with open(filepath, "r") as file:
    #     line_count = sum(1 for _ in file)
    return subprocess.run(
        ["wc", "-l", filepath], capture_output=True, text=True
    ).stdout.split()[0]

def sort_file_out_for_workload_generation(filenames_path, fs_tree_breath):
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

def generate_workload(
    zeta,
    total_files,
    # number_of_requests,
    depth,
    workload_generation_method,
    sorted_file_path,
    # access_pattern_out,
    total_number_of_requests,
    fs_tree_breath,
    extend=False
):
    destination_dir = f"{HOME_DIR}/workload/workloads/access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out"
    if extend:
        extended_destination = f"{HOME_DIR}/workload/workloads/access_file_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.extend.out"
        if os.path.exists(extended_destination):
            print(
                f"[generate_workload] we have generate extended {extended_destination} skip, you could rm it to re-gen one"
            )
            # should supersede the original one
            return
    else:
        if os.path.exists(destination_dir):
            print(
                f"[generate_workload] we have generate {destination_dir} skip, you could rm it to re-gen one"
            )
            return
    original_path = f"{HOME_DIR}/workload/workloads/access_file.out"
    access_pattern = generate_workloads(
        zeta,
        total_files,
        total_number_of_requests,
        depth,
        workload_generation_method,
        sorted_file_path,
        original_path,
    )
    os.rename(original_path, destination_dir)

    if extend:
        # extended_destination
        print(f"Generate extended workload to {extended_destination}")
        extended_access_pattern = np.concatenate(
            (access_pattern, access_pattern), axis=0
        )
        # print(access_pattern.shape, extended_access_pattern.shape)
        np.savetxt(extended_destination, extended_access_pattern, fmt='%d')


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
        f"bottleneck_file_{server_logical_num}_{total_files}_{total_number_of_requests}_{depth}_{fs_tree_breath}_{zeta}_{workload_generation_method}.out",
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
        )  

    return [bottleneck_id, bottleneck_namenode]

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

def load_local(total_files, depth, zeta, total_files_for_mdtest, fs_tree_breath):
    # hdfs_servers = [{"host": serverip, "port": 9001} for serverip in SERVER_IPs]
    base ="/tmp/"
    file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
    adjusted_depth = depth - file_offset

    def read_file_list(file_path):
        with open(file_path, "r") as f:
            return [line.strip() for line in f.readlines()]

    def create_directories():
        dir_list = gen_dir_tree(
            depth,
            fs_tree_breath,
            f"/#test-dir.0.d.{adjusted_depth}.n.{total_files}.b.{fs_tree_breath}",
        )
        os.mkdir(base + f"/#test-dir.0.d.{adjusted_depth}.n.{total_files}.b.{fs_tree_breath}")
        for dir in dir_list:
            # print(f"mkdir {base + dir}")
            os.mkdir(base + dir)
        print("Directories created")

    def create_hdfs_files(file_list, idx):
        if idx == 0:
            for file_path in tqdm(file_list):
                # hdfs.touch(file_path)
                Path(base + file_path).touch()
        else:
            for file_path in file_list:
                Path(base + file_path).touch()
                # hdfs.touch(file_path)

    file_list = read_file_list(file_out)
    num_clients_per_server = 128

    # create dir
    threads = []
    create_directories()


    chunk_size = len(file_list) // num_clients_per_server
    remainder = len(file_list) % num_clients_per_server

    with ThreadPoolExecutor(max_workers=num_clients_per_server * 2) as executor:
        futures = []
        for i in range(num_clients_per_server):
            start_idx = i * chunk_size + min(i, remainder)
            end_idx = start_idx + chunk_size + (1 if i < remainder else 0)
            futures.append(
                executor.submit(
                    create_hdfs_files, file_list[start_idx:end_idx], i
                )
            )

        for future in as_completed(futures):
            future.result()

def load_hdfs(total_files, depth, zeta,total_files_for_mdtest,fs_tree_breath,  is_dir_only=False):
    hdfs_servers = [{"host": serverip, "port": 9001} for serverip in SERVER_IPs]

    file_out = f"{HOME_DIR}/workload/filenames/filename_{total_files_for_mdtest}_{depth}_{fs_tree_breath}/file.out"
    adjusted_depth = depth - file_offset

    def read_file_list(file_path):
        with open(file_path, "r") as f:
            return [line.strip() for line in f.readlines()]

    def create_directories(hdfs):
        dir_list = gen_dir_tree(
            depth,
            fs_tree_breath,
            f"/#test-dir.0.d.{adjusted_depth}.n.{total_files}.b.{fs_tree_breath}",
        )
        hdfs.mkdir(f"/#test-dir.0.d.{adjusted_depth}.n.{total_files}.b.{fs_tree_breath}")
        for dir in dir_list:
            hdfs.mkdir(dir)
        print("Directories created")

    def create_hdfs_files(hdfs, file_list, idx):
        if idx == 0:
            for file_path in tqdm(file_list):
                hdfs.touch(file_path)
        else:
            for file_path in file_list:
                hdfs.touch(file_path)

    file_list = read_file_list(file_out)
    num_clients_per_server = 128
    hdfs_clients = [
        HDFileSystem(host=server["host"], port=server["port"])
        for server in hdfs_servers
    ]

    # create dir
    threads = []
    for hdfs_client in hdfs_clients:
        thread = threading.Thread(target=create_directories, args=(hdfs_client,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    if is_dir_only == True:
        return
    # create file
    hdfs_clients_lists = [
        [
            HDFileSystem(host=server["host"], port=server["port"])
            for server in hdfs_servers
        ]
        for _ in range(num_clients_per_server)
    ]

    chunk_size = len(file_list) // num_clients_per_server
    remainder = len(file_list) % num_clients_per_server

    with ThreadPoolExecutor(max_workers=num_clients_per_server * 2) as executor:
        futures = []
        for i, hdfs_clients in enumerate(hdfs_clients_lists):
            for j, hdfs_client in enumerate(hdfs_clients):
                start_idx = i * chunk_size + min(i, remainder)
                end_idx = start_idx + chunk_size + (1 if i < remainder else 0)
                futures.append(
                    executor.submit(
                        create_hdfs_files, hdfs_client, file_list[start_idx:end_idx], i
                    )
                )

        for future in as_completed(futures):
            future.result()

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
    client_thread,
    need_compaction = False
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
        './mdtest', # modify to fit mdtest_rate
        '-a', str(client_thread),
        '-n', str(files_for_mdtest), # modify to fit mdtest_rate
        '-m',
        '-z', str(adjusted_depth),
        '-b', str(fs_tree_breath),
        '-d', '/',
        # '-i', '5',
        # '-V', '3',
        '-Y', access_out,
        '-P',str(request_pruning_factor)
    ]
    if need_warm == True:
        command.append('-W')
    if need_compaction:
        command.append('-Q')
    print(f"exec {' '.join(command)}")
    exp_type ='dynamic' if rotation_servers_num == 2 else 'static'
    ret_file_path = run_results_dir / f'tmp_mdtest_{zeta}_{bottleneck_id}_{rotation_id}_{exp_type}_{rotation_servers_num}_mixed.out'
    with open(ret_file_path, 'w') as outfile:
        subprocess.run(command, stdout=outfile)
    return ret_file_path

def generate_requests(file_out, access_file, requests_file, client_thread):
    if os.path.exists(requests_file):
        print(f"[requests] we have generate {requests_file} skip")
        return
    command = [
        f"{HOME_DIR}/gen_requests",
        file_out,
        access_file,
        requests_file,
        str(client_thread),
    ]
    print(f"exec {' '.join(command)}")
    subprocess.run(command)


# New functions 


def get_hits(path, logger:logging.Logger, line_pattern=None):
    """
    Check the status of a given path by looking for specific patterns in the files within that path.
    
    Args:
        path (str): The path to check.
    
    Returns:
        tuple: A tuple containing the bottleneck hit and rotation hit values.
    """

    if line_pattern is None:
        line_pattern = r"Total\s+:\s*\d+(?:\.\d+)?\s+ms, BOTTLENECK\s+:\s*\d+(?:\.\d+)?ops/sec, ROTATION\s+:\s*\d+(?:\.\d+)?ops/sec, BOTTLENECK HIT\s+:\s*\d+, ROTATION HIT\s+:\s*\d+,.*"

    try:
        with open(path, 'r') as file:
            content = file.read()
        # Search for the line with the specified structure
        line_match = re.search(line_pattern, content)
        
        if line_match:
            matched_line = line_match.group(0)
            numbers = re.findall(r'\d+(?:\.\d+)?', matched_line)
            # assert len(numbers) == 6, f"Expected 6 numbers, but found {len(numbers)} in line: {matched_line}"

            bottleneck_hit = int(numbers[3])
            rotation_hit = int(numbers[4])
            logger.info(f"Bottleneck Hit: {bottleneck_hit} | Rotation Hit: {rotation_hit}")
            return bottleneck_hit, rotation_hit
        else:
            logger.error("Target line not found in the file.")
            return None, None
    except FileNotFoundError:
        logger.error(f"Error: File '{path}' not found.")
        return None, None
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return None, None

def check_read_write(workload):
    ret = np.nonzero(np.array(list(workload["mixed"].values()), dtype=float))[0]
    if len(ret) == 1:
        key = list(workload["mixed"].keys())[ret[0]]
        if key in ["open_close", "stat", "statdir"]:
            return True
        else:
            return False
    else:
        return True


if __name__ == "__main__":
    # Example usage
    logger = logging.getLogger(__name__)
    ret = get_hits(
        "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/results/round_rmdir_r5_16/cscaching_0.9_32000000_10_4_3/tmp_mdtest_0.9_11_1_static_16_mixed.out",
        logger,
        r"RMDIR\s+:\s*\d+(?:\.\d+)?\s+ms, BOTTLENECK\s+:\s*\d+(?:\.\d+)?ops/sec, ROTATION\s+:\s*\d+(?:\.\d+)?ops/sec, BOTTLENECK HIT\s+:\s*\d+, ROTATION HIT\s+:\s*\d+,"
    )
    print(ret)