
# Fletch: File-System Metadata Caching in Programmable Switches

- This repo is the source code for our submission `Fletch: File-System Metadata Caching in Programmable Switches`, including how to:
    - deploy Fletch
    - run experiments
    - get results shown in our paper

## Table of Contents

- 1. [Important Folders and Binaries](#1-important-folders-and-binaries)
    - 1.1 [Client](#11-client)
    - 1.2 [Server and Switch](#12-server-and-switch)
- 2. [Environment Preparation](#2-environment-preparation)
    - 2.1 [Boost Library](#21-boost-lib)
    - 2.2 [RocksDB](#22-rocksdb)
    - 2.3 [Other Dependencies](#23-other-dependencies)
    - 2.4 [Run HDFS (for NoCache and Fletch only)](#24-run-hdfs)
- 3. [Compilation](#3-compilation)
    - 3.1 [Client](#31-client)
    - 3.2 [Server, Controller, and SwitchOS](#32-server-controller-and-switchos)
    - 3.3 [Switch](#33-switch)
- 4. [Start Testbed](#4-start-testbed)
    - 4.1 [Configurations Before Running Testbed](#41-configurations-before-running-testbed)
    - 4.2 [Run Testbed with Scripts](#42-run-testbed-with-scripts)
    - 4.3 [Run Testbed Without Scripts](#43-run-testbed-without-scripts)
- 5. [Workload Generation](#5-workload-generation)
    - 5.1 [Skewed Workloads](#51-skewed-workloads)
    - 5.2 [Uniform Workloads](#52-uniform-workloads)
- 6. [Load Dataset to HDFS](#6-load-dataset-to-hdfs)
    - 6.1 [For Two Physical Servers (for NoCache and Fletch only)](#61-for-two-physical-servers)
    - 6.2 [For One Physical Server](#62-for-one-physical-server)
    - 6.3 [Load Dataset to RocksDB (for CCache and Fletch+ only)](#63-load-dataset-to-rocksdb)
- 7. [Experiments](#7-experiments)
    - 7.1 [Run Experiments Automatically](#71-automatically)
    - 7.2 [Experiment Scripts (Replace the Script in 7.2 for Each Experiment)](#72-scripts)
    - 7.3 [Get Numbers Presented in the Paper](#73-get-numbers)
- 8. [Handle Corner Cases During Experiments](#8-corner-cases)


## 1. Important Folders and Binaries

- home directory {$PROJ_HOME}
    - server and client: `/home/jz/In-Switch-FS-Metadata`
    - switch: `/home/cyh/jz/In-Switch-FS-Metadata`
- client directory ($CLIENT_HOME)
    - `$PROJ_HOME/netfetch-private/mdtestcpp`
- method directory
- remark: Fletch's folder name is netfetch (i.e., fletch == netfetch in this documentation)

### 1.1 Client

- In `$CLIENT_HOME/workload`
    - caches
        - freq_{}: top 100K hot files (used for warmup)
        - bottleneck_{}: record which server is bottleneck server (dumptrace results)
    - workloads
        - access_file_{}: generated workload
    - filenames
        - filename_{}: all files' name of dataset

- `$CLIENT_HOME/results`: experiment results

- `$CLIENT_HOME/mdtest`: client binary code

- `$CLIENT_HOME/dumptrace_fast`: binary code to dump workload to get bottleneck server and cache list

### 1.2 Server and Switch

- {method_name} -> nocache, netfetch, cscaching, csfletch
- Nocache:
    - `$PROJ_HOME/netfetch-private/nocache`
- Fletch:
    - `$PROJ_HOME/netfetch-private/netfetch`
- CCache:
    - `$PROJ_HOME/netfetch-private/cscaching`
- Fletch+:
    - `$PROJ_HOME/netfetch-private/csfletch`


## 2. Environment Preparation

### 2.1 Boost Library

- Install libboost 1.81.0 in servers if not
    - Under project directory `{$PROJ_HOME}/netfetch-private`
```bash
    wget https://boostorg.jfrog.io/artifactory/main/release/1.81.0/source/boost_1_81_0.tar.gz
    tar -xzvf boost_1_81_0.tar.gz
    cd boost_1_81_0; ./bootstrap.sh --with-libraries=system,thread --prefix=./install && sudo ./b2 install
```

### 2.2 RocksDB

- Install RocksDB v6.22.1 in servers if not
    - Under project directory `{$PROJ_HOME}/netfetch-private`
```bash
    sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev libjemalloc-dev libsnappy-dev # RocksDB dependencies
    wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.22.1.tar.gz
    tar zxvf v6.22.1.tar.gz
    cd rocksdb-6.22.1
    PORTABLE=1 make static_lib # We use PORTABLE=1 to fix runtime error of illegal instruction when open()
```

### 2.3 Other Dependencies

- Install other dependencies
    - `cd {$PROJ_HOME}/Installation`
    - see `Installation.sh` for details

- Configure HDFS
    - Put HDFS configuration files into `cd {$PROJ_HOME}/hdfs3.2`
    - `cd {$PROJ_HOME}/Installation`
    - run `python3 3_config_hdfs.py`

### 2.4 Run HDFS (for NoCache and Fletch only)

- Start HDFS
    - Format HDFS: in each server, run the following commands
        - `sudo rm -rf $HADOOP_HOME/logs $HADOOP_HOME/tmp`
        - `hdfs namenode -format -clusterId test-hdfs-123`
    - Start namenode and dfsrouter: in each server, run the following commands
        - `hdfs --daemon start namenode`
        - `hdfs --daemon start dfsrouter`
    - Start datanode and zookeeper
        - `hdfs --daemon start datanode`
        - `/home/jz/environments/zookeeper-3.4.6/bin/zkServer.sh start`
    - verify if HDFS starts successfully
        - `hdfs dfs -ls hdfs://{server1_ip}:{port}/`
            - in our work, we use port 8888 for dfsrouter and 9001 for RPC calls
        - `hdfs dfs -ls hdfs://{{server2_ip}}:{port}/`

- Stop HDFS
    - In each server
        - `hdfs --daemon stop namenode`
        - `hdfs --daemon stop dfsrouter`
    - In datanode
        - `hdfs --daemon stop datanode`
        - `/home/jz/environments/zookeeper-3.4.6/bin/zkServer.sh stop`


## 3. Compilation

### 3.1 Client

- in `$CLIENT_HOME`, run `make`

### 3.2 Server, Controller and SwitchOS

- in `$PROJ_HOME/netfetch-private/{method_name}`, run `make`

### 3.3 Switch

- in `$PROJ_HOME/netfetch-private/{method_name}/tofino`, run `bash compile.sh`


## 4. Start Testbed (Server and Switch only)

### 4.1 Configurations before Running Testbed

- ARP table
    - use `arp -s` to configure your NIC with correct ip and mac

- `config.ini`
    - server and switch: `$PROJ_HOME/netfetch-private/{method_name}`
        - modify the corresponding ip and mac address
    - client: `$CLIENT_HOME`
        - please ref `config.ini.details` to modify
    - Make sure each node (i.e., server and client) has the consistent config.ini in **both** {method_name} and mdtestcpp

### 4.2 Run Testbed with Scripts [only for 2 Physical Servers]

- `cd $CLIENT_HOME`
    - `python3 start_server.py`
    - `python3 stop_server.py`

- before running the above scripts, configure the following parameters
    - modify the `method_name` with {method_name} in `start_server.py`
    - configure ip and address for client, server, and switch
        - `XXX_IPs`, `XXX_PWDs`, `XXX_PORTs`, `XXX_USERs` in the following three scripts
            - `$CLIENT_HOME/utils/client_util.py`
            - `$CLIENT_HOME/utils/server_util.py`
            - `$CLIENT_HOME/utils/switch_util.py`

### 4.3 Run Testbed without Scripts [for 1 Physical Server]

- modify `config.ini` for client in `$CLIENT_HOME`
    - configure both `bottleneck_ip` and `rotation_ip` with the same ip if running with only ONE server

- server: `$PROJ_HOME/netfetch-private/{method_name}`
    - `./server {idx}` (i.e., {idx} can be 0 or 1)
    - `./controller 0` (only for fletch)

- switch: `$PROJ_HOME/netfetch-private/{method_name}/tofino`
    - `bash start_switch.sh`
    - `bash configure.sh`
    - `bash ptf_popserver.sh` (only for fletch)
- switchos: `$PROJ_HOME/netfetch-private/netfetch`
    - `./switchos`


## 5. Workload Generation

### 5.1 Skewed Workloads

- configure important parameters for generated skewed workloads
    - each experiment script encodes workload generation code
    - we can use `$CLIENT_HOME/exp_thpt.py` to generate skewed workloads
        1. configure corresponding parameters in `$CLIENT_HOME/configs/skewed_workloads_{depth|generation_methods|zeta}.yaml`
            ```bash
            depths = [4, 6, 8, 10]                          # the max depth include root
            fs_tree_breath: [296, 17, 7, 4]                 # the number of subdirs for each directory in the file-system namespace
            zetas = [0.8, 0.9, 1.0]                         # alpha in the power-law distribution
            workload_generation_methods = [1, 2, 3]         # key popularity generation method: 1. assign from low level; 2. assign from high level; 3. random assign
            total_files_for_mdtest = 32000000               # max files (the tree you generated may have less than 32000000 files)
            total_number_of_requests = 32000000             # total requests
            server_logical_num_list = [16]                  # logical server numbers, 16 and 128 in current experiments (will not care when generating workloads)
            bottleneck_ip = physical server0 data plane ip  # pick one server as the bottleneck server for server rotation
            rotation_ip = physical server1 data plane ip    # pick another server as the rotation server for server rotation
            ```
        2. in `$CLIENT_HOME/`, run `python3 exp_thpt.py --config-name {config_name.yaml}`
            - will also dump trace (i.e., find bottleneck server which will be used for server rotation)

- outputs
    - caches
        - freq_{}: top 100K hot files (used for warmup)
        - bottleneck_{}: record which server is bottleneck server (dumptrace results)
        - fairness index: will be used in exp5 and exp7
    - workloads
        - access_file_{}: generated workload
    - filenames
        - filename_{}: all files' name of dataset

### 5.2 Uniform Workloads

- generate uniform workload
    - in `$CLIENT_HOME/`
    - run `python3 exp6_uniform.py --config-name uniform_workload`

- outputs
    - caches
        - freq_{}: top 100K hot files (used for warmup)
        - bottleneck_{}: record which server is bottleneck server (dumptrace results)
    - workloads
        - access_file_{}: generated workload
    - filenames
        - filename_{}: all files' name of dataset


## 6. Load Dataset to HDFS

### 6.1 For Two Physical Servers (for NoCache and Fletch only)

- should make sure we can start testbed and generate workloads
    - it means that we have correct testbed configurations

- in `$CLIENT_HOME/`
    - run `python3 exp_thpt.py --config-name load-hdfs`

### 6.2 For One Physical Server

- it requires to load dataset manually
    - 1. load directories as directly loading files may have the problem that the dir is not created before file
        - use the function `load_hdfs(total_files, depth, zeta,is_dir_only=True)` in `exp_thpt.py`
        - [NOTE] total_files is the exactly number of files (e.g., for depth 10 the total files is 31981446 if configuring 32M dataset)
            - get this number by `wc -l file.out` in `$CLIENT_HOME/workload/filenames`
    - 2. load files use mdtest
        - start nocache testbed or use control plane IP address of nocache server
        - in `$CLIENT_HOME/`
            - run `stdbuf -o0 ./mdtest -a {client_thread} -n {total_files_for_mdtest} -S -C -z {adjusted_depth} -b {fs_tree_breath} -d / -A`
            - example: `stdbuf -o0 ./mdtest -a 64 -n 32000000 -S -C -z 8 -b 4 -d / -A`

### 6.3 Load Dataset to RocksDB (for CCache and Fletch+ only)

- scp filename file tp servers first
    - cd `$CLIENT_HOME/workload/filenames`, choose one target `file.out` file
    - scp the above `file.out` to servers (target address: `$PROJ_HOME/netfetch-private/cscaching/`, `$PROJ_HOME/netfetch-private/csfletch/`)
- before loadding dataset into RocksDB in each server
    - `mkdir /tmp/nocache` and `mkdir /tmp/index`
    - in server0: `mkdir /tmp/nocache/worker0.db` and `mkdir /tmp/index/worker0.db`
    - in server1: `mkdir /tmp/nocache/worker1.db` and `mkdir /tmp/index/worker1.db`
- in each server, in `$PROJ_HOME/netfetch-private/cscaching/` or `$PROJ_HOME/netfetch-private/csfletch/`
    - run `./load_local {server_physical_idx} file.out`
    - `{server_physical_idx}` cound be 0 or 1 in current testbed
- backup workloads for different depths (i.e., 4, 6, 8, and 10) in `~/exp6_bak/{depth}` in each server, so that we can use the following scripts


## 7. Experiments

### 7.1 Run Experiments Automatically

1. paramter configuration
    - need to manually configure all `config.ini`. It is only needed for the first time, and no reconfiguration is required for subsequent runs of other experiments
        - `bottleneck_ip`, `rotation_ip`, and `dst_ip`
2. configure `exp1.yaml` for the experiment
    - `roundidx`: the target forder name that stores the results
    - `server_logical_num_list`: the number of simulated servers
3. load dataset (optional, if alreadly loaded, no need to do this again)
    - in `$CLIENT_HOME`, run `python3 exp_thpt.py --config-name skewed_workloads_generation`
    - set `needload` as `true` for NoCache and Fletch
    - for CCache and Fletch+, refer to 6.3
4. run experiment (take exp1 as an example)
    - in `$CLIENT_HOME`, run `python3 exp_thpt.py --config-name exp1`
5. results in `$CLIENT_HOME/results`
    - e.g., if we run the script with `roundidx exp1_r1`, we will obtain eight result folders
        - `round_exp1_r1_16` with four sub-folders `0`, `1`, `2` and `3`
            - 16 server experiments for Alibaba, Training, Thumb, and LinkedIn workloads
        - `round_exp1_r1_128` with four sub-folders `0`, `1`, `2` and `3`
            - 128 server experiments for Alibaba, Training, Thumb, and LinkedIn workloads

### 7.2 Experiment Scripts (Replace the Script in 7.1 for Each Experiment)

- remarks
    - all scripts are in the `$CLIENT_HOME` folder
    - refer to 7.1 to see how to configure `roundidx` in experiment configure files

- exp1 (workload performance)
    - `python3 exp_thpt.py --config-name exp1`

- exp2 (single-type operation performance)
    - (stat, open) `python3 exp_thpt.py --config-name exp2-stat.py`
    - (rm, rename) `python3 exp_thpt.py --config-name exp2-rm-mv`
    - (create, mkdir) `python3 exp_thpt.py --config-name exp2-mkdir-create`
    - (rmdir) `python3 exp2_rmdir.py --config-name exp2-rmdir`

- exp3 (chmod ratio)
    - `python3 exp_thpt.py --config-name exp3`
        - for chmod ratio 75%, we should adjust `exp_thpt.py` for triggering retry
            - replace `1e5` with `1e4` 

- exp4 (latency analysis)
    - `python3 exp4.py --config-name exp4`
    - additional parameter configuration in `exp4_rate_limiter.py`
        - `{method}_list`: rate limiters, controlling client sending rate

- exp5 (key popularity assignment)
    - `python3 exp_thpt.py --config-name exp5`

- exp6 (skewness)
    - `python3 exp_thpt.py --config-name exp6-skewed`
        - for Alibaba, we should adjust `exp_thpt.py` for triggering retry
            - replace `1e5` with `1e4`
    - `python3 exp6_uniform.py --config-name exp6-uniform`

- exp7 (depth)
    - [NOTE] need to clean dataset before running this experiment; can do it manually (recommanded)
        - run `hadoop fs -rm -r {test_root_dir}` in each physical server
    - run `python3 exp_thpt.py --config-name exp7` in `$CLIENT_HOME`
        - for NoCache and Fletch, set `needload` as `true`
    - for Alibaba, we should adjust `exp_thpt.py` for triggering retry
        - replace `1e5` with `1e4`

- exp8 (dynamic pattern)
    - `python3 exp8.py --config-name exp8`
    - before experiments, configure `workload_mode` as `0` in `mdtestcpp_ini` in switch

- exp9 (switch resource usage)
    - we compile the methods and use `p4i` to obatin resource usage

### 7.3 Get Numbers Presented in the Paper

- remark: all scripts are in `$CLIENT_HOME/obtain_results/`

- for experiments 1, 3, 5, and 7, we use `res_thpt.py` to calculate numbers shown in the paper
    - parameter configuration in `res_thpt.py`
        - `root_dir`: `$CLIENT_HOME/results`
        - `all_round`: result folder name under `root_dir`
    - run `python3 res_thpt.py`
    - output: numbers shown in the paper

- for experiment 2, we use the following scripts to calculate numbers for different operations
    - for stat, open, mkdir, create, rm and mv, use `res_thpt.py`
    - for rmdir, use `res_exp2_rmdir.py`
    - parameter configuration example: in `res_exp2_rmdir.py`
        - `root_dir`: `$CLIENT_HOME/results`
        - `all_round`: result folder name under `root_dir`

- for experiment 4, we use `python3 res_exp4.py` to obtain latency results
    - use `python3 res_exp4.py` to obtain latency results
        - configure `root_dir` and `round_idx` in `res_exp4.py`
    - running this script once can obtain average, p95, p99 latencies for one method under one fixed throughput

- for experiment 6, we use the following scripts to calculate numbers for different settings
    - for uniform workloads, we use `res_exp6_uniform.py`
        - `root_dir`: `$CLIENT_HOME/results`
        - `round`: result folder name under `root_dir`
    - for skewed workloads, we use `res_thpt.py`

- for experiment 8, we use `res_exp8.py` to calculate numbers shown in the paper
    - parameter configuration refers to `res_thpt.py`


## 8. Handle Corner Cases during Experiments

- when encountering incorrect numbers or corrupted datasets (highly related to unstable HDFS), we should re-load datasets via two ways
    - directly re-load the entire dataset, see 6.1 for details
        - time estimation: directly (1 hour) or delete all dataset and run it (2 hours), depending on the degree of damage to the dataset
    - use our provided script `$CLIENT_HOME/reload_partition.py`
        - in server rotation, we have 16 or 128 servers, each of which is responsible for one partition
        - parameter configuration in `$CLIENT_HOME/reload_partition.py`
            ```bash
            server_logical_num = 128                            # logical server numbers
            id_0 = 84                                           # id_0 is the bottleneck server
            rotation_list = [27,28,29]                          # rotation_list is the rotation server idx
            ```
        - run `python3 reload_partition.py` in `$CLIENT_HOME`
        - time estimation: re-load one partition may take 10 min

- Use bak datasets for CCache and Fletch+
    - in each server
        - cd `/tmp`, run `rm -r nocache/ index/`
        - cd `~/db_bak`, run `cp -r nocache /tmp/` and `cp -r index /tmp/`
