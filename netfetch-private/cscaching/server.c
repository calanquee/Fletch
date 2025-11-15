#include <arpa/inet.h>  // inetaddr conversion
#include <errno.h>
#include <getopt.h>
#include <netinet/in.h>  // struct sockaddr_in
#include <signal.h>      // for signal and raise
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>  // socket API
#include <sys/time.h>    // struct timeval

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

// CPU affinity
#define _GNU_SOURCE
#include <pthread.h>
#include <sched.h>

#include "../common/helper.h"
#include "../common/latency_helper.h"
#include "../common/socket_helper.h"

#define MAX_VERSION 0xFFFFFFFFFFFFFFFF

// #ifdef USE_YCSB
// #include "../common/workloadparser/ycsb_parser.h"
// #elif defined USE_SYNTHETIC
// #include "../common/workloadparser/synthetic_parser.h"
// #endif

#include "common_impl.h"

/* variables */

// transaction phase
bool volatile transaction_running = false;
std::atomic<size_t> transaction_ready_threads(0);
size_t transaction_expected_ready_threads = 0;
bool volatile killed = false;

int server_physical_idx = -1;

cpu_set_t nonserverworker_cpuset;  // [server_cores, total_cores-1] for all other threads

/* functions */

// transaction phase
// #include "reflector_impl.h"
#include "server_impl.h"
void transaction_main();  // transaction phase
void kill(int signum);

int main(int argc, char** argv) {
    parse_ini("config.ini");
    parse_control_ini("control_type.ini");

    if (argc != 2) {
        printf("Usage: ./server server_physical_idx\n");
        exit(-1);
    }
    server_physical_idx = atoi(argv[1]);
    INVARIANT(server_physical_idx >= 0);
    INVARIANT(server_physical_idx < server_physical_num);

    CPU_ZERO(&nonserverworker_cpuset);
    for (int i = server_worker_corenums[server_physical_idx]; i < server_total_corenums[server_physical_idx]; i++) {
        CPU_SET(i, &nonserverworker_cpuset);
    }
    // int ret = sched_setaffinity(0, sizeof(nonserverworker_cpuset), &nonserverworker_cpuset);
    pthread_t main_thread = pthread_self();
    int ret = pthread_setaffinity_np(main_thread, sizeof(nonserverworker_cpuset), &nonserverworker_cpuset);
    if (ret) {
        printf("[Error] fail to set affinity of server.main; errno: %d\n", errno);
        exit(-1);
    }

    /* (1) prepare phase */

    // register signal handler
    signal(SIGTERM, SIG_IGN);  // Ignore SIGTERM for subthreads

    /* (2) transaction phase */
    printf("[main] transaction phase start\n");
    fflush(stdout);

    prepare_server();
    transaction_main();

    /* (3) free phase */

    free_common();
    close_server();

    COUT_THIS("[server.main] Exit successfully")
    exit(0);
}

/*
 * Transaction phase
 */

void transaction_main() {
    uint32_t current_server_logical_num = server_logical_idxes_list[server_physical_idx].size();

    // update transaction_expected_ready_threads
    // reflector: cp2dpserver + dp2cpserver
    // server: server_num * (worker + evictserver + popserver + valueupdateserver)
    if (server_physical_idx == 0) {                                               // deploy reflector in the first physical server
        transaction_expected_ready_threads = 1 * current_server_logical_num + 3;  // plus 3 for invalidation list coordinator
    } else {
        transaction_expected_ready_threads = 1 * current_server_logical_num + 3;
    }

    int ret = 0;

    transaction_running = false;

    cpu_set_t serverworker_cpuset;  // [0, server_cores-1] for each server.worker

    // launch valueupdateservers

    // launch workers (processing normal packets)
    pthread_t worker_threads[current_server_logical_num + 3];
    server_worker_param_t server_worker_params[current_server_logical_num + 3];

    printf("[NETFETCH_DEBUG] current_server_logical_num = %d\n", current_server_logical_num);

    // check if parameters are cacheline aligned
    for (size_t i = 0; i < current_server_logical_num; i++) {
        if ((uint64_t)(&(server_worker_params[i])) % CACHELINE_SIZE != 0) {
            COUT_N_EXIT("wrong parameter address: " << &(server_worker_params[i]));
        }
    }
    for (uint16_t worker_i = 0; worker_i < current_server_logical_num; worker_i++) {
        server_worker_params[worker_i].local_server_logical_idx = worker_i;

        // for server.worker
        ret = pthread_create(&worker_threads[worker_i], nullptr, run_server_worker, (void*)&server_worker_params[worker_i]);
        if (ret) {
            COUT_N_EXIT("Error of launching some server.worker:" << ret);
        }

        CPU_ZERO(&serverworker_cpuset);
        CPU_SET(worker_i % server_worker_corenums[server_physical_idx], &serverworker_cpuset);
        ret = pthread_setaffinity_np(worker_threads[worker_i], sizeof(serverworker_cpuset), &serverworker_cpuset);
        if (ret) {
            printf("Error of setaffinity for server.worker; errno: %d\n", errno);
            exit(-1);
        }
    }
    for (uint16_t worker_i = current_server_logical_num; worker_i < current_server_logical_num + 3; worker_i++) {
        server_worker_params[worker_i].local_server_logical_idx = 0;
    }

    ret = pthread_create(&worker_threads[current_server_logical_num], nullptr, run_server_coordinator, (void*)&server_worker_params[current_server_logical_num]);
    if (ret) {
        COUT_N_EXIT("Error of launching server.coordinator:" << ret);
    }

    ret = pthread_create(&worker_threads[current_server_logical_num + 1], nullptr, run_client_coordinator, (void*)&server_worker_params[current_server_logical_num + 1]);
    if (ret) {
        COUT_N_EXIT("Error of launching client.coordinator:" << ret);
    }

    ret = pthread_create(&worker_threads[current_server_logical_num + 2], nullptr, run_client_notifier, (void*)&server_worker_params[current_server_logical_num + 2]);
    if (ret) {
        COUT_N_EXIT("Error of launching client.notifier:" << ret);
    }

    while (transaction_ready_threads < transaction_expected_ready_threads)
        sleep(1);

#ifdef SERVER_ROTATION
    std::vector<int> valid_global_server_logical_idxes;
    for (int tmp_server_physical_idx = 0; tmp_server_physical_idx < server_physical_num; tmp_server_physical_idx++) {
        for (int i = 0; i < server_logical_idxes_list[tmp_server_physical_idx].size(); i++) {
            valid_global_server_logical_idxes.push_back(server_logical_idxes_list[tmp_server_physical_idx][i]);
        }
    }

    // Resume cached keyset in DistCache for server-issued in-switch value update (aka cache coherence phase 2)
    if (valid_global_server_logical_idxes.size() == 1 || valid_global_server_logical_idxes.size() == 2) {  // server rotation transaction phase
        printf("Resume cached keyset from %s\n", raw_warmup_workload_filename);

        // must ONLY have one logical server per physical server
        INVARIANT(server_logical_idxes_list[server_physical_idx].size() == 1);

        ParserIterator* iter = NULL;
#ifdef USE_YCSB
        iter = new YcsbParserIterator(raw_warmup_workload_filename);
#elif defined USE_SYNTHETIC
        iter = new SyntheticParserIterator(raw_warmup_workload_filename);
#endif
        INVARIANT(iter != NULL);

        netreach_key_t tmpkey;
        while (true) {
            if (!iter->next()) {
                break;
            }

            INVARIANT(iter->type() == optype_t(packet_type_t::WARMUPREQ));
            tmpkey = iter->key();
#ifdef USE_HASH
            uint32_t tmp_global_server_logical_idx = tmpkey.get_hashpartition_idx(switch_partition_count, max_server_total_logical_num);
#elif defined(USE_RANGE)
            uint32_t tmp_global_server_logical_idx = tmpkey.get_rangepartition_idx(max_server_total_logical_num);
#endif

            if (tmp_global_server_logical_idx == server_logical_idxes_list[server_physical_idx][0]) {
                server_cached_keyset_list[0].insert(tmpkey);
            }
        }
    }
#endif

    transaction_running = true;
    COUT_THIS("[transaction.main] all threads ready");
    fflush(stdout);

    signal(SIGTERM, kill);  // Set for main thread (kill -15)

    while (!killed) {
        sleep(1);
    }

    transaction_running = false;

    void* status;
    printf("wait for server.workers\n");
    for (size_t i = 0; i < current_server_logical_num; i++) {
        int rc = pthread_join(worker_threads[i], &status);
        if (rc) {
            COUT_N_EXIT("Error: unable to join server.worker " << rc);
        }
    }
    printf("server.workers finish\n");

    printf("wait for server.coordinator\n");
    int rc = pthread_join(worker_threads[current_server_logical_num], &status);
    if (rc) {
        COUT_N_EXIT("Error: unable to join server.coordinator " << rc);
    }
    printf("server.coordinator finish\n");

    printf("wait for client.coordinator\n");
    rc = pthread_join(worker_threads[current_server_logical_num + 1], &status);
    if (rc) {
        COUT_N_EXIT("Error: unable to join client.coordinator " << rc);
    }
    printf("client.coordinator finish\n");

    printf("wait for client.notifier\n");
    rc = pthread_join(worker_threads[current_server_logical_num + 2], &status);
    if (rc) {
        COUT_N_EXIT("Error: unable to join client.notifier " << rc);
    }
    printf("client.notifier finish\n");

    printf("[transaction.main] all threads end\n");
}

void kill(int signum) {
    COUT_THIS("[transaction phase] receive SIGKILL!")
    killed = true;
}
