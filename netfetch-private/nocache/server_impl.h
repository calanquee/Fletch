#ifndef SERVER_IMPL_H
#define SERVER_IMPL_H

// Transaction phase for ycsb_server

#include <openssl/md5.h>
#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <map>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "../common/dynamic_array.h"
#include "../common/helper.h"
#include "../common/key.h"
#include "../common/metadata.h"
#include "../common/pkt_ring_buffer.h"
#include "../common/snapshot_record.h"
#include "../common/val.h"
// #include "hdfs.h"
#include "message_queue_impl.h"

// #define server_thread_pool_size 20

#define ENABLE_HDFS
// #define DISABLE_HDFS

#define MD5_DIGEST_LENGTH_INSWITCH 8
// typedef ConcurrentSet<netreach_key_t> concurrent_set_t;

struct alignas(CACHELINE_SIZE) ServerWorkerParam {
    uint16_t local_server_logical_idx;
#ifdef DEBUG_SERVER
    std::vector<double> process_latency_list;
    std::vector<double> wait_latency_list;
    std::vector<double> wait_beforerecv_latency_list;
    std::vector<double> udprecv_latency_list;
    std::vector<double> rocksdb_latency_list;
    std::vector<double> beingcached_latency_list;
    std::vector<double> beingupdated_latency_list;
#endif
};
typedef ServerWorkerParam server_worker_param_t;

// Qingxiu: comment the following line
// RocksdbWrapper *db_wrappers = NULL;
int* server_worker_udpsock_list = NULL;
int* server_worker_lwpid_list = NULL;
pkt_ring_buffer_t* server_worker_pkt_ring_buffer_list = NULL;

#ifdef ENABLE_HDFS
// hdfs handler
hdfsFS fs = NULL;
hdfsFS ns1 = NULL;
hdfsFS ns2 = NULL;
#endif
std::vector<long long> durations;
std::atomic<int> batchidx(0);
std::atomic<long long> batchTotalDuration(0);
std::atomic<long long> batchMaxDuration(0);

const int batchSize = 1000;
// server.evictservers <-> controller.evictserver.evictclients
// int * server_evictserver_tcpsock_list = NULL;
// server.evictserver <-> controller.evictserver.evictclient
constexpr int SHARD_COUNT = 2048;

std::vector<std::mutex> shard_mutexes(SHARD_COUNT);

inline int compute_hash_index(const netreach_key_t& key) {
#ifdef LARGE_KEY
    return (key.keylolo ^ key.keylohi ^ key.keyhilo ^ key.keyhihi) % SHARD_COUNT;
#else
    return (key.keylo ^ key.keyhi) % SHARD_COUNT;
#endif
}
// data plane <-> per-server.valueupdateserver
MessagePtrQueue<netcache_valueupdate_t>* server_netcache_valueupdate_ptr_queue_list = NULL;
int* server_valueupdateserver_udpsock_list = NULL;

void prepare_server();
// server.workers for processing pkts
void* run_server_worker(void* param);

void close_server();

// for consistent hashing
// std::map<std::string, std::string> hashRing;

std::vector<unsigned char> computeMD5(const std::string& input);
void constructHashRing(const std::vector<std::string>& namespaces);
// void initHashRing(int server_logical_num);
std::string mapToNameNode(const std::vector<unsigned char>& hashValue);
uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue);
void fetch_metadata_from_hdfs(metadata_t& tmp_val, char* queried_path, int operation);
int extract_mode_from_chmod_command(const char* chmod_command);
int fetch_metadata_from_hdfs_for_listdir(val_t& tmp_val, const char* queried_path, int* numEntries);

// load balance ratio
int ns1_accesses = 0;
int ns2_accesses = 0;

// for network order conversion
// htonll may need to be defined if not already available
// uint64_t htonll(uint64_t val) {
//     if (htonl(1) == 1) {
//         return val;
//     } else {
//         return ((uint64_t)htonl(val & 0xFFFFFFFF) << 32) | htonl(val >> 32);
//     }
// }

std::map<std::vector<unsigned char>, std::pair<std::string, uint16_t>> hashRing;

// Function to compute MD5 hash of a string
inline std::vector<unsigned char> computeMD5(const std::string& input) {
    std::vector<unsigned char> digest(MD5_DIGEST_LENGTH);
    MD5(reinterpret_cast<const unsigned char*>(input.c_str()), input.length(), digest.data());
    return digest;
}
// Function to compare MD5 hash vectors lexicographically
bool compareMD5(const std::vector<unsigned char>& a, const std::vector<unsigned char>& b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}
void constructHashRing(const std::vector<std::string>& namespaces) {
    uint16_t logical_idx = 0;
    for (const auto& ns : namespaces) {
        for (int i = 0; i < 100; ++i) {
            std::string key = ns + "/" + std::to_string(i);
            std::vector<unsigned char> value = computeMD5(key);
            hashRing[value] = std::make_pair(ns, logical_idx);
        }
        logical_idx++;
    }
}

inline std::string mapToNameNode(const std::vector<unsigned char>& hashValue) {
    auto it = std::find_if(hashRing.begin(), hashRing.end(),
                           [&](const auto& pair) { return !compareMD5(pair.first, hashValue); });

    if (it != hashRing.end()) {
        return it->second.first;
    } else {
        return hashRing.begin()->second.first;  // Wrap around to the first node
    }
}
// Function to map a hash value to a name node index
inline uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue) {
    auto it = std::find_if(hashRing.begin(), hashRing.end(),
                           [&](const auto& pair) { return !compareMD5(pair.first, hashValue); });

    if (it != hashRing.end()) {
        return it->second.second;
    } else {
        return hashRing.begin()->second.second;  // Wrap around to the first node
    }
}
// operation 1: GETREQ
// operation 2: POPSERVER
// operation 3: PUTREQ_CACHED
inline void fetch_metadata_from_hdfs(metadata_t& tmp_val, char* queried_path, int operation) {
#ifdef DEBUG_NETFETCH
    auto start = std::chrono::high_resolution_clock::now();
#endif
    // operation is for printing debug information
#ifdef DEBUG_NETFETCH
    char print_log_header[100];
    memset(print_log_header, 0, 100);
    if (operation == 1) {
        memcpy(print_log_header, "[NETFETCH_GETREQ]", 18);
    } else if (operation == 2) {
        memcpy(print_log_header, "[NETFETCH_POPSERVER]", 20);
    } else if (operation == 3) {
        memcpy(print_log_header, "[NETFETCH_PUTREQ_CACHED]", 24);
    } else {
        printf("[NETFETCH_ERROR] Invalid operation!\n");
    }
#endif
    // fetch metadata from hdfs router
    hdfsFileInfo* queried_path_metadata;
    if (operation == 2 || operation == 3) {
        queried_path_metadata = hdfsGetPathInfo(ns1, queried_path);
#ifdef DEBUG_NETFETCH
        printf("%s fetch metadata from router!\n", print_log_header);
#endif
    }
    // fetch metadata from hdfs servers
    else if (operation == 1) {
#ifdef DEBUG_NETFETCH
        auto digest_str = computeMD5(queried_path);
        std::string namenode = mapToNameNode(digest_str);
        printf("%s namenode: %s\n", print_log_header, namenode.c_str());
#endif
        queried_path_metadata = hdfsGetPathInfo(ns1, queried_path);
    }
    // printf("debug\n");
    // fflush(stdout);
    if (queried_path_metadata == NULL) {
#ifdef DEBUG_NETFETCH
        printf("[line %d] %s Memory allocation failed!\n", __LINE__, print_log_header);
#endif
        tmp_val.metadata_length = 0;
        // tmp_val.metadata_data = NULL;
    }
    if (queried_path_metadata) {
        // printf("debug\n");
        // fflush(stdout);
        tmp_val = metadata_t(queried_path_metadata);

        // tmp_val.dump();
#ifdef DEBUG_NETFETCH
        printf("%s Kind: %s\n", print_log_header, (queried_path_metadata->mKind == kObjectKindFile) ? "File" : "Directory");  // 1B
        printf("%s Name: %s\n", print_log_header, queried_path_metadata->mName);                                              // no need
        printf("%s Last Modified: %lld\n", print_log_header, (long long)queried_path_metadata->mLastMod);                     // 8B
        printf("%s Size: %lld\n", print_log_header, (long long)queried_path_metadata->mSize);                                 // 8B
        printf("%s Replication: %d\n", print_log_header, queried_path_metadata->mReplication);                                // 2B
        printf("%s Block Size: %lld\n", print_log_header, (long long)queried_path_metadata->mBlockSize);                      // 8B
        printf("%s Owner: %s\n", print_log_header, queried_path_metadata->mOwner);                                            // no need
        printf("%s Group: %s\n", print_log_header, queried_path_metadata->mGroup);                                            // no need
        printf("%s Permissions: %o\n", print_log_header, queried_path_metadata->mPermissions);                                // 2B
        printf("%s Last Access: %lld\n", print_log_header, (long long)queried_path_metadata->mLastAccess);                    // 8B
#endif
        INVARIANT(tmp_val.metadata_data != NULL);

        hdfsFreeFileInfo(queried_path_metadata, 1);
        queried_path_metadata = NULL;
    }
#ifdef DEBUG_NETFETCH
    auto end = std::chrono::high_resolution_clock::now();
    long long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    // printf("[hdfs] fetch metadata duration: %lld us\n", duration);

    // durations.push_back(duration);

    batchTotalDuration += duration;
    if (duration > batchMaxDuration) {
        batchMaxDuration = duration;
    }

    if ((batchidx++) % batchSize == 0) {
        double batchAvgLatency = batchTotalDuration / static_cast<double>(batchSize);  // 当前批次的平均延迟
        std::cout << "Batch " << (batchidx + 1) / batchSize << " - Average latency: " << batchAvgLatency
                  << " microseconds, Max latency: " << batchMaxDuration << " microseconds" << std::endl;
        // 重置统计数据
        batchTotalDuration = 0;
        batchMaxDuration = 0;
    }
#endif
}

int extract_mode_from_chmod_command(const char* chmod_command) {
    int mode;
    if (sscanf(chmod_command, "chmod %o", &mode) == 1) {
        return mode;
    }
    return -1;  // Return -1 if the command does not match expected format
}

int fetch_metadata_from_hdfs_for_listdir(val_t& tmp_val, const char* queried_path, int* numEntries) {
    tmp_val.val_length = 0;

    // fetch metadata from hdfs router
    hdfsFileInfo* queried_path_metadata = hdfsListDirectory(ns1, queried_path, numEntries);
    if (queried_path_metadata == NULL) {
#ifdef DEBUG_NETFETCH_HDFS
        printf("Memory allocation failed!\n");
#endif
        tmp_val.val_data = NULL;
        tmp_val.val_length = 0;
        return 1;  // report failure
    } else {
        // 32B: mLastMod + mSize + mBlockSize + mLastAccess
        // 8B: + mKind + mPermissions + mReplication + padding(0)
        // 2B owner length + mOwner + 2B group length + mGroup
        int return_entry_num = 10;
        if (*numEntries < 10) {
            return_entry_num = *numEntries;
        }
        tmp_val.val_data = new char[FILE_META_SIZE * (return_entry_num)];
        for (int i = 0; i < return_entry_num; i++) {
            metadata_t tmp_metadata(queried_path_metadata[i]);
            memcpy(tmp_val.val_data + tmp_val.val_length, tmp_metadata.metadata_data + sizeof(uint16_t), tmp_metadata.metadata_length);
            tmp_val.val_length += tmp_metadata.metadata_length;
            INVARIANT(tmp_val.val_data != NULL);
        }
        // free metadata structure
        hdfsFreeFileInfo(queried_path_metadata, *numEntries);
        queried_path_metadata = NULL;
        *numEntries = std::min(*numEntries, return_entry_num);
        return -1;  // use -1 to identify list operation
    }
}

void prepare_server() {
    // printf(" sizeof(netreach_key_t) %d\n", sizeof(netreach_key_t));
    init_GroupMapping("groupmapping");
    init_OwnerMapping("ownermapping");
    printf("[server] prepare start\n");

    // Qingxiu: comment the following line

    uint32_t current_server_logical_num = server_logical_idxes_list[server_physical_idx].size();

    server_worker_udpsock_list = new int[current_server_logical_num];
    for (size_t tmp_local_server_logical_idx = 0; tmp_local_server_logical_idx < current_server_logical_num; tmp_local_server_logical_idx++) {
        uint16_t tmp_global_server_logical_idx = server_logical_idxes_list[server_physical_idx][tmp_local_server_logical_idx];
        // short tmp_server_worker_port = server_worker_port_start + tmp_global_server_logical_idx;
#ifndef SERVER_ROTATION
        short tmp_server_worker_port = server_worker_port_start + tmp_local_server_logical_idx;
#else
        short tmp_server_worker_port = 0;
        if (tmp_global_server_logical_idx == bottleneck_serveridx_for_rotation) {
            INVARIANT(tmp_local_server_logical_idx == 0);
            tmp_server_worker_port = server_worker_port_start;
        } else {
            tmp_server_worker_port = server_worker_port_start + tmp_global_server_logical_idx;
            if (tmp_global_server_logical_idx > bottleneck_serveridx_for_rotation) {
                tmp_server_worker_port -= 1;
            }
        }
#endif
        // prepare_udpserver(server_worker_udpsock_list[tmp_local_server_logical_idx], true, tmp_server_worker_port, "server.worker", SOCKET_TIMEOUT, 0, UDP_LARGE_RCVBUFSIZE);
        prepare_udpserver(server_worker_udpsock_list[tmp_local_server_logical_idx], true, tmp_server_worker_port, "server.worker", 0, SERVER_SOCKET_TIMEOUT_USECS, UDP_LARGE_RCVBUFSIZE);
        printf("prepare udp socket for server.worker %d-%d on port %d\n", tmp_local_server_logical_idx, tmp_global_server_logical_idx, server_worker_port_start + tmp_local_server_logical_idx);
    }
    server_worker_lwpid_list = new int[current_server_logical_num];
    memset(server_worker_lwpid_list, 0, current_server_logical_num);

    // for large value
    server_worker_pkt_ring_buffer_list = new pkt_ring_buffer_t[current_server_logical_num];
    for (size_t tmp_local_server_logical_idx = 0; tmp_local_server_logical_idx < current_server_logical_num; tmp_local_server_logical_idx++) {
        server_worker_pkt_ring_buffer_list[tmp_local_server_logical_idx].init(PKT_RING_BUFFER_SIZE);
    }

    // Prepare for cache population

    // prepare for inswitch value update

    memory_fence();

    printf("[server] prepare end\n");
}

void close_server() {
#ifdef ENABLE_HDFS
    // disconnect with hdfs server
    int return_value = hdfsDisconnect(fs);
    if (return_value == 0) {
        printf("[DEBUG] disconnect with hdfs router sucessfully!\n");
    } else {
        printf("[ERROR] NOT disconnect with hdfs router!\n");
    }

    return_value = hdfsDisconnect(ns1);
    if (return_value == 0) {
        printf("[DEBUG] disconnect with hdfs namenode ns1 sucessfully!\n");
    } else {
        printf("[ERROR] NOT disconnect with hdfs namenode ns1!\n");
    }

    return_value = hdfsDisconnect(ns2);
    if (return_value == 0) {
        printf("[DEBUG] disconnect with hdfs namenode ns2 sucessfully!\n");
    } else {
        printf("[ERROR] NOT disconnect with hdfs namenode ns2!\n");
    }

    // print load balance ratio
    printf("-----------------------------------------------------\n");
    printf("[NETFETCH_DEBUG] overall access: %d\n", ns1_accesses + ns2_accesses);
    printf("[NETFETCH_DEBUG] ns1_accesses: %d\n", ns1_accesses);
    printf("[NETFETCH_DEBUG] ns2_accesses: %d\n", ns2_accesses);
    printf("-----------------------------------------------------\n");
#endif
    // // rocksdb code
    // if (db_wrappers != NULL) {
    // 	printf("Close rocksdb databases\n");
    // 	delete [] db_wrappers;
    // 	db_wrappers = NULL;
    // }

    if (server_worker_udpsock_list != NULL) {
        delete[] server_worker_udpsock_list;
        server_worker_udpsock_list = NULL;
    }
    if (server_worker_lwpid_list != NULL) {
        delete[] server_worker_lwpid_list;
        server_worker_lwpid_list = NULL;
    }
    if (server_worker_pkt_ring_buffer_list != NULL) {
        delete[] server_worker_pkt_ring_buffer_list;
        server_worker_pkt_ring_buffer_list = NULL;
    }

    if (server_netcache_valueupdate_ptr_queue_list != NULL) {
        delete[] server_netcache_valueupdate_ptr_queue_list;
        server_netcache_valueupdate_ptr_queue_list = NULL;
    }
    if (server_valueupdateserver_udpsock_list != NULL) {
        delete[] server_valueupdateserver_udpsock_list;
        server_valueupdateserver_udpsock_list = NULL;
    }
}

/*
thread pool for server worker
*/
struct Task {
    // (MAX_BUFSIZE, MAX_LARGE_BUFSIZE)
    Task(const char* recvbuf, int _recv_size, struct sockaddr_in& _client_addr, socklen_t _client_addrlen)
        : recv_size(_recv_size), client_addrlen(_client_addrlen) {
        dynamicbuf = new char[_recv_size + 5];
        memcpy(dynamicbuf, recvbuf, _recv_size);
        // printf("new task %d\n", _recv_size);
        // fflush(stdout);
        memcpy(&client_addr, &_client_addr, sizeof(client_addr));
    }
    ~Task() {
        if (dynamicbuf != NULL) {
            delete dynamicbuf;
        }
    }
    char* dynamicbuf;
    struct sockaddr_in client_addr;
    socklen_t client_addrlen;
    int recv_size;
};

// Define a thread-safe queue to store tasks
class TaskQueue {
public:
    void push(Task* task) {
        pthread_mutex_lock(&mutex);
        queue.push(task);
        pthread_cond_signal(&cond);
        pthread_mutex_unlock(&mutex);
    }

    bool pop(Task*& task) {
        pthread_mutex_lock(&mutex);
        while (queue.empty() && !stop) {
            pthread_cond_wait(&cond, &mutex);
        }
        if (!queue.empty()) {
            task = queue.front();
            queue.pop();
            pthread_mutex_unlock(&mutex);
            return true;
        } else {
            pthread_mutex_unlock(&mutex);
            return false;
        }
    }

    void shutdown() {
        pthread_mutex_lock(&mutex);
        stop = true;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&mutex);
    }

private:
    std::queue<Task*> queue;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool stop = false;
};

// Define the thread pool
class ThreadPool {
public:
    ThreadPool(int size, uint16_t _local_server_logical_idx, uint16_t _global_server_logical_idx);
    ~ThreadPool();
    void add_task(Task* task) {
        task_queue->push(task);
    }

    struct sockaddr_in controller_popserver_addr;
    socklen_t controller_popserver_addrlen;
    uint16_t local_server_logical_idx;
    uint16_t global_server_logical_idx;
    short tmp_server_worker_port;

private:
    void handle_task(Task* task, int thread_server_fd, int thread_idx);
    void bind_thread_to_core(int thread_idx);
    std::vector<std::thread> threads;
    TaskQueue* task_queue;
    bool stop;
};

void ThreadPool::bind_thread_to_core(int thread_idx) {
    // Set CPU Affinity for the current thread
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);             // Clear the CPU set
    CPU_SET(thread_idx, &cpuset);  // Bind to the specified core (thread_idx)

    // Get the current thread's handle
    pthread_t current_thread = pthread_self();

    // Apply the CPU affinity settings
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to set CPU affinity for thread " << thread_idx << std::endl;
    } else {
        // std::cout << "Thread " << std::this_thread::get_id() << " bound to core " << thread_idx << std::endl;
    }
}
ThreadPool::ThreadPool(int size, uint16_t _local_server_logical_idx, uint16_t _global_server_logical_idx)
    : task_queue(new TaskQueue()), stop(false), local_server_logical_idx(_local_server_logical_idx), global_server_logical_idx(_global_server_logical_idx) {
    // init var
    set_sockaddr(controller_popserver_addr, inet_addr(controller_ip_for_server), controller_popserver_port_start + global_server_logical_idx);
    controller_popserver_addrlen = sizeof(struct sockaddr_in);
    tmp_server_worker_port = server_worker_port_start + local_server_logical_idx;
    size_t num_cores = std::thread::hardware_concurrency();  // Get the number of CPU cores
    // print
    printf("[CPU] num_cores: %d\n", num_cores);
    for (int i = 0; i < size; ++i) {
        threads.emplace_back([this, i, num_cores]() {
            // bind_thread_to_core(num_cores - i % num_cores - 1);
            std::string thread_name = "server.worker.thread" + std::to_string(i);
            int thread_server_fd;
            prepare_udpserver(thread_server_fd, true, tmp_server_worker_port + 128 + i, thread_name.c_str(), 0, SERVER_SOCKET_TIMEOUT_USECS, UDP_LARGE_RCVBUFSIZE);

            while (!stop) {
                Task* task;
                if (task_queue->pop(task)) {
                    // printf("start to handle task\n");
                    fflush(stdout);
                    if (task == NULL) {
                        printf("pop NULL task:error\n");
                        continue;
                    }
                    handle_task(task, thread_server_fd, i);
                    delete task;
                }
            }
            close(thread_server_fd);
        });
    }
}
ThreadPool::~ThreadPool() {
    stop = true;
    task_queue->shutdown();
    for (auto& thread : threads) {
        thread.join();
    }
    delete task_queue;
}
char pktbufs[128][4096];
char ackbufs[128][4096];
char bufs[128][MAX_BUFSIZE];
char buf_getresponses[128][MAX_BUFSIZE];
char recvbufs[128][MAX_BUFSIZE];
void ThreadPool::handle_task(Task* task, int thread_server_fd, int thread_idx) {
    // refer to buffers
    char* pktbuf = pktbufs[thread_idx];
    char* ackbuf = ackbufs[thread_idx];
    char* buf = bufs[thread_idx];
    char* buf_getresponse = buf_getresponses[thread_idx];
    char* recvbuf = recvbufs[thread_idx];
    // Original task handling logic from the while loop in the provided code
    if (task == NULL) {
        printf("NULL task:error\n");
        return;
    }
    // char buf[MAX_BUFSIZE];
    // dynamic_array_t dynamicbuf(MAX_BUFSIZE, MAX_LARGE_BUFSIZE);
    int recv_size = 0;
    int rsp_size = 0;
    // char recvbuf[MAX_BUFSIZE];

    // Determine packet type
    packet_type_t pkt_type = get_packet_type(task->dynamicbuf, task->recv_size);

#ifdef DEBUG_NETFETCH
    printf("[NETFETCH_DEBUG] pkt_type: %d\n", pkt_type);
#endif

    // printf("[debug]pkt_type %d is_timeout %d\n",pkt_type,is_timeout);
    switch (pkt_type) {
    case packet_type_t::GETREQ: {
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] start serving GETREQ!\n");
        fflush(stdout);
#endif

        get_request_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] [line %d] recv_buf: ", __LINE__);
        dump_buf(task->dynamicbuf, task->recv_size);
        for (int i = 0; i < task->recv_size; ++i) {
            printf("%02x ", (unsigned char)task->dynamicbuf[i]);
        }
        printf("\n");
#endif
        netreach_key_t tmp_key = req.key();

        // get the real path length
        // first get depth that will affect offset to path length
        uint16_t path_depth;
        memcpy(&path_depth, &task->dynamicbuf[2], 2);
        path_depth = ntohs(path_depth);
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] [line %d] path_depth: %d\n", __LINE__, path_depth);
#endif
        uint16_t path_length;
        memcpy(&path_length, &task->dynamicbuf[sizeof(optype_t) + sizeof(keydepth_t) + path_depth * sizeof(netreach_key_t)], 2);
        path_length = ntohs(path_length);
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
        // get the path
        char queried_path[path_length + 1];
        // layout: op + depth + hash for each depth + path_length + path
        memcpy(queried_path, &task->dynamicbuf[sizeof(optype_t) + sizeof(keydepth_t) + path_depth * sizeof(netreach_key_t) + 2], path_length);
        queried_path[path_length] = '\0';
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] [line %d] queried_path: %s\n", __LINE__, queried_path);
#endif
        // test the hashed key
        unsigned char digest[MD5_DIGEST_LENGTH];

        MD5((unsigned char*)&queried_path, strlen(queried_path), digest);

        for (int i = 0; i < sizeof(netreach_key_t); ++i) {
            if (digest[i] != (unsigned char)(task->dynamicbuf[i + sizeof(optype_t) + sizeof(keydepth_t) + (path_depth - 1) * sizeof(netreach_key_t)])) {
                printf("[NETFETCH_ERROR] MD5 digest error!\n");
                // dump path and path length
                printf("[NETFETCH_ERROR] queried_path: %s\n", queried_path);
                printf("[NETFETCH_ERROR] path_length: %d\n", path_length);
                // dump pkt
                dump_buf(task->dynamicbuf, task->recv_size);
                // printf("[NETFETCH_ERROR] digest[%d] = %02x, dynamicbuf.array()[%d] = %02x\n", i, digest[i], i + sizeof(optype_t) + sizeof(keydepth_t) + (path_depth - 1) * sizeof(netreach_key_t), (unsigned char)(task->dynamicbuf[i + sizeof(optype_t) + (path_depth - 1) * sizeof(netreach_key_t)]));
            }
        }

#ifdef ENABLE_HDFS

        metadata_t tmp_val;  // send to client
        uint32_t tmp_seq = 0;
        bool tmp_stat = true;

        fetch_metadata_from_hdfs(tmp_val, queried_path, 1);

#endif
        // tmp_val.dump();
        if (tmp_val.metadata_length == FILE_META_SIZE or tmp_val.metadata_length == DIR_META_SIZE) {
            get_response_t rsp(CURMETHOD_ID, req.key(), val_t(tmp_val.metadata_data + sizeof(uint16_t), tmp_val.metadata_length), tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);

#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] length = %ld tmp_val: ", __LINE__, tmp_val.metadata_length);
            tmp_val.dump();
            printf("\n");
            printf("[NETFETCH_DEBUG] [line %d] print the buf (length: %ld) sent to client: ", __LINE__, rsp_size);
            for (int i = 0; i < rsp_size; ++i) {
                printf("%02x ", (unsigned char)buf[i]);
            }
            printf("\n");

#endif
            udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");

        } else if (tmp_val.metadata_length == 0) {
            get_response_t rsp(CURMETHOD_ID, req.key(), val_t(), tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
        } else {
            printf("[NETFETCH_DEBUG] [line %d] length = %ld tmp_val: ", __LINE__, tmp_val.metadata_length);
            printf(" error no large value for fs metadata service\n");
            //                 get_response_largevalue_t rsp(CURMETHOD_ID, req.key(), tmp_val, tmp_stat, global_server_logical_idx);
            //                 task->dynamicbuf->clear();
            //                 rsp_size = rsp.dynamic_serialize(*task->dynamicbuf);
            //                 udpsendlarge_ipfrag(server_worker_udpsock_list[local_server_logical_idx], task->dynamicbuf->array(), rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker", get_response_largevalue_t::get_frag_hdrsize(CURMETHOD_ID));
            // #ifdef DUMP_BUF
            //                 dump_buf(task->dynamicbuf->array(), rsp_size);
            // #endif
        }
        break;
    }
    case packet_type_t::PUTREQ:
    case packet_type_t::DELREQ: {
        // dump_buf(task->dynamicbuf, task->recv_size);

        // failure flag
        int8_t failure_flag = 0;
        netreach_key_t tmp_key;
        val_t tmp_val;
        if (pkt_type == packet_type_t::PUTREQ) {
            // Qingxiu: NetFetch only considers this operation and PUT_CACHED_SEQ for PUT
            // Qingxiu: key-->hash(path), value-->operation, payload-->real path
            put_request_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);

#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] value_length: %d\n", __LINE__, req.val().val_length);
#endif
            tmp_key = req.key();
            tmp_val = req.val();
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] tmp_val: %s\n", __LINE__, tmp_val.val_data);
#endif
            // get path length
            uint16_t path_depth;
            memcpy(&path_depth, &task->dynamicbuf[2], 2);
            path_depth = ntohs(path_depth);
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] path_depth: %d\n", __LINE__, path_depth);
#endif
            uint16_t path_length;
            uint16_t path2_length;
            // 2B op_type + 2B keydepth + 16B*keydepth key + 2B value_length + value + 6B + 2B path_length + path
            memcpy(&path_length, &task->dynamicbuf[8 + path_depth * sizeof(netreach_key_t) + req.val().val_length], 2);
            path_length = ntohs(path_length);
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            char queried_path[path_length + 1];
            char new_path[MAX_PATH_LENGTH];
            uint16_t tmp_offset = 10 + path_depth * sizeof(netreach_key_t) + req.val().val_length;
            memcpy(queried_path, &task->dynamicbuf[10 + path_depth * sizeof(netreach_key_t) + req.val().val_length], path_length);
            queried_path[path_length] = '\0';
            // get new path for mv only
            if (task->recv_size > tmp_offset + path_length) {
                tmp_offset += path_length;
                memcpy(&path2_length, &task->dynamicbuf[tmp_offset], 2);
                path2_length = ntohs(path2_length);
                tmp_offset += sizeof(uint16_t);
                memcpy(new_path, &task->dynamicbuf[tmp_offset], path2_length);
                new_path[path2_length] = '\0';
            }
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_DEBUG] [line %d] queried_path: %s\n", __LINE__, queried_path);
#endif
            // test the hashed key
            unsigned char digest[MD5_DIGEST_LENGTH];
            MD5((unsigned char*)&queried_path, strlen(queried_path), digest);
            for (int i = 0; i < sizeof(netreach_key_t); ++i) {
                if (digest[i] != (unsigned char)(task->dynamicbuf[i + 4 + (path_depth - 1) * sizeof(netreach_key_t)])) {
                    printf("[NETFETCH_ERROR] MD5 digest error!\n");
                    // dump path and path length
                    printf("[NETFETCH_ERROR] queried_path: %s\n", queried_path);
                    printf("[NETFETCH_ERROR] path_length: %d\n", path_length);
                    // dump pkt
                    dump_buf(task->dynamicbuf, task->recv_size);

                    // printf("[NETFETCH_ERROR] digest[%d] = %02x, dynamicbuf.array()[%d] = %02x\n", i, digest[i], i + 4 + (path_depth - 1) * sizeof(netreach_key_t), (unsigned char)(task->dynamicbuf[i + 4 + (path_depth - 1) * sizeof(netreach_key_t)]));
                }
            }
            int shard_index = compute_hash_index(tmp_key);
            bool is_create = strcmp(tmp_val.val_data, "touch") == 0;
            bool is_delete = strcmp(tmp_val.val_data, "rm") == 0;
            bool is_mkdir = strcmp(tmp_val.val_data, "mkdir") == 0;
            bool is_rmdir = strcmp(tmp_val.val_data, "rmdir") == 0;
            if (is_create || is_delete) {
                shard_mutexes[shard_index].lock();
            }
            // perform metadata operation
#ifdef ENABLE_HDFS
            // create file
            if (strcmp(tmp_val.val_data, "touch") == 0) {
                hdfsFile result_pointer = NULL;
                if (hdfsExists(ns1, queried_path) != 0) {
                    // file exists
                    result_pointer = hdfsOpenFile(ns1, queried_path, O_WRONLY, 0, 0, 0);
                }
                if (result_pointer) {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_DEBUG] hdfsOpenFile successfully!\n");
#endif
                    hdfsCloseFile(ns1, result_pointer);
                    result_pointer = NULL;
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] hdfsOpenFile failed!\n");
#endif
                    // change keydepth to 0xFFFF and copy it to the third and forth bytes of tml_val
                    failure_flag = 1;
                }
            }

            // delete file
            else if (strcmp(tmp_val.val_data, "rm") == 0) {
                int result = hdfsDelete(ns1, queried_path, 0);
                if (result == 0) {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_DEBUG] hdfsDelete successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] hdfsDelete failed!\n");
#endif
                    failure_flag = 1;
                }
            }

            // create directory
            else if (strcmp(tmp_val.val_data, "mkdir") == 0) {
                int result = hdfsCreateDirectory(ns1, queried_path);
                if (result == 0) {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_DEBUG] hdfsCreateDirectory successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] hdfsCreateDirectory failed!\n");
#endif
                    failure_flag = 1;
                }
            }

            // list directory
            else if (strcmp(tmp_val.val_data, "ls") == 0) {
#ifdef DEBUG_NETFETCH
                printf("[NETFETCH_DEBUG] [line %d] start hdfsListDir!\n", __LINE__);
#endif
                int numEntries = 10;
                failure_flag = fetch_metadata_from_hdfs_for_listdir(tmp_val, queried_path, &numEntries);
#ifdef DEBUG_NETFETCH
                printf("[NETFETCH_DEBUG] [line %d] hdfsListDir successfully!\n", __LINE__);
#endif
            }

            // delete directory
            else if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                int result = hdfsDelete(ns1, queried_path, 0);
                if (result == 0) {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_DEBUG] hdfsDelete successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] hdfsDelete failed!\n");
#endif
                    failure_flag = 1;
                }
            }

            // chmod for file or directory
            else if (strncmp(tmp_val.val_data, "chmod", 5) == 0) {  // Check if the operation starts with "chmod"
                int mode = extract_mode_from_chmod_command(tmp_val.val_data);
                if (mode != -1) {  // Ensure the mode was correctly extracted
                    int result = hdfsChmod(ns1, queried_path, mode);
                    if (result == 0) {
#ifdef DEBUG_NETFETCH
                        printf("[NETFETCH_DEBUG] hdfsChmod successfully applied permissions %o on %s\n", mode, queried_path);
#endif
                    } else {
#ifdef DEBUG_NETFETCH
                        printf("[NETFETCH_ERROR] hdfsChmod failed on %s with mode %o\n", queried_path, mode);
#endif
                        failure_flag = 1;
                    }
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] Invalid chmod command format: %s\n", tmp_val.val_data);
#endif
                    failure_flag = 1;
                }
            }

            // mv
            else if (strcmp(tmp_val.val_data, "mv") == 0) {
                int result = hdfsRename(ns1, queried_path, new_path);
                if (result == 0) {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_DEBUG] [line %d] hdfsRename successfully!\n", __LINE__);
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[NETFETCH_ERROR] [line %d] hdfsRename failed!\n", __LINE__);
#endif
                    failure_flag = 1;
                }
            }

            // finsh the benchmark
            else if (strcmp(tmp_val.val_data, "finish") == 0) {
                // print load balance ratio
                printf("-----------------------------------------------------\n");
                printf("[NETFETCH_DEBUG] overall access: %d\n", ns1_accesses + ns2_accesses);
                printf("[NETFETCH_DEBUG] ns1_accesses: %d\n", ns1_accesses);
                printf("[NETFETCH_DEBUG] ns2_accesses: %d\n", ns2_accesses);
                printf("-----------------------------------------------------\n");
                // set ns1_accesses and ns2_accesses to 0
                ns1_accesses = 0;
                ns2_accesses = 0;
                // set failure_flag to 0
                failure_flag = 0;
            }

#endif
            if (is_create || is_delete) {
                shard_mutexes[shard_index].unlock();
            }
        } else {
            printf("[server.worker] invalid pkttype: %x which should be PUTREQ_SEQ/DELREQ_SEQ\n", optype_t(pkt_type));
            exit(-1);
        }

        if (pkt_type == packet_type_t::PUTREQ || pkt_type == packet_type_t::PUTREQ_LARGEVALUE) {
            put_response_t rsp(CURMETHOD_ID, tmp_key, true, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            // Qingxiu: report failure to client
            if (failure_flag == 1) {
                // modify the third and forth bytes of buf to 0xFFFF
                buf[2] = 0xFF;
                buf[3] = 0xFF;
            }
            if (failure_flag == -1) {  // this is for listdir
#ifdef DEBUG_NETFETCH
                printf("[NETFETCH_DEBUG] [line %d] append value for listdir\n", __LINE__);
#endif
                uint16_t tmp_vallen = htons(tmp_val.val_length);
                // print val_length
                // printf("[NETFETCH_DEBUG] [line %d] val_length: %d\n", __LINE__, tmp_val.val_length);
                memcpy(&buf[rsp_size], &tmp_vallen, sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
                memcpy(&buf[rsp_size], tmp_val.val_data, tmp_val.val_length);
                rsp_size += tmp_val.val_length;
            }
        }
        udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
// print the buf sent to client for test
#ifdef DEBUG_NETFETCH
        printf("print the buf sent to client for test\n");
        for (int i = 0; i < rsp_size; ++i) {
            printf("%02x ", (unsigned char)buf[i]);
        }
        printf("\n");
#endif
        break;
    }
    default: {
        COUT_THIS("[server.worker] Invalid packet type: " << int(pkt_type))
        std::cout << std::flush;
    }
    }
}
/*
 * Worker for server-side processing
 */

void* run_server_worker(void* param) {
    // Parse param
    server_worker_param_t& thread_param = *(server_worker_param_t*)param;
    uint16_t local_server_logical_idx = thread_param.local_server_logical_idx;  // [0, current_server_logical_num - 1]
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];
    ThreadPool thread_pool(server_thread_pool_size, local_server_logical_idx, global_server_logical_idx);
    // NOTE: pthread id != LWP id (linux thread id)
    server_worker_lwpid_list[local_server_logical_idx] = CUR_LWPID();

    pkt_ring_buffer_t& cur_worker_pkt_ring_buffer = server_worker_pkt_ring_buffer_list[local_server_logical_idx];

#ifdef ENABLE_HDFS
    // connect to hdfs server
    struct hdfsBuilder* fsBuilder1 = hdfsNewBuilder();
    struct hdfsBuilder* fsBuilder2 = hdfsNewBuilder();
    // struct hdfsBuilder* fsBuilder3 = hdfsNewBuilder();
    struct hdfsBuilder* fsBuilder_thread_ns1[server_thread_pool_size];
    // struct hdfsBuilder* fsBuilder_thread_ns2[server_thread_pool_size];
    // router
    // Qingxiu: modify the following line for server thpt test: server_physical_idx --> 1
    hdfsBuilderSetNameNode(fsBuilder1, server_ip_for_controller_list[server_physical_idx]);
    // printf("[NETFETCH_DEBUG] server_ip_for_controller_list[0]: %s\n", server_ip_for_controller_list[0]);
    // Qingxiu: 8888 -> 9001
    hdfsBuilderSetNameNodePort(fsBuilder1, 8888);  // router port
    fs = hdfsBuilderConnect(fsBuilder1);
    // ns1
    hdfsBuilderSetNameNode(fsBuilder2, server_ip_for_controller_list[server_physical_idx]);
    hdfsBuilderSetNameNodePort(fsBuilder2, 9001);  // ns port
    ns1 = hdfsBuilderConnect(fsBuilder2);
    // ns2
    // hdfsBuilderSetNameNode(fsBuilder3, server_ip_for_controller_list[server_physical_idx]);
    // hdfsBuilderSetNameNodePort(fsBuilder3, 9001);  // ns port
    // ns2 = hdfsBuilderConnect(fsBuilder3);
#endif

    // #ifdef DEBUG_NETFETCH
    if (fs == NULL) {
        printf("[NETFETCH_ERROR] NOT connect to fs\n");
    } else if (ns1 == NULL) {
        printf("[NETFETCH_ERROR] NOT connect to ns1\n");
        // } else if (ns2 == NULL) {
        //     printf("[NETFETCH_ERROR] NOT connect to ns2\n");
    } else {
        printf("[NETFETCH_DEBUG] connect to hdfs namenode successfully!\n");
    }
    // #endif
    // hdfsFreeBuilder(fsBuilder1);  // 连接后释放构建器
    // hdfsFreeBuilder(fsBuilder2);  // 连接后释放构建器

#ifdef ENABLE_HDFS
    // init consistent hashing
    std::vector<std::string> namespaces;
    namespaces.push_back("ns1");
    namespaces.push_back("ns2");
    constructHashRing(namespaces);
#endif

    // client address (only needed by udp socket)
    // NOTE: udp socket uses binded port as server.srcport; if we use raw socket, we need to judge client.dstport in every worker, which is time-consumine; -> we resort switch to hide server.srcport
    struct sockaddr_in client_addr;
    socklen_t client_addrlen = sizeof(struct sockaddr_in);

    // NOTE: controller and switchos should have been launched before servers

    char buf[MAX_BUFSIZE];
    dynamic_array_t dynamicbuf(MAX_BUFSIZE, MAX_LARGE_BUFSIZE);
    int recv_size = 0;
    int rsp_size = 0;
    char recvbuf[MAX_BUFSIZE];

    struct timespec polling_interrupt_for_blocking;
    polling_interrupt_for_blocking.tv_sec = 0;
    polling_interrupt_for_blocking.tv_nsec = 1000;  // 1ms = 1000ns

    printf("[server.worker %d-%d] ready\n", local_server_logical_idx, global_server_logical_idx);
    fflush(stdout);
    transaction_ready_threads++;

    while (!transaction_running) {
    }

    bool is_timeout = false;
    while (transaction_running) {
        dynamicbuf.clear();
        is_timeout = udprecvlarge_ipfrag(CURMETHOD_ID, server_worker_udpsock_list[local_server_logical_idx], dynamicbuf, 0, &client_addr, &client_addrlen, "server.worker", &cur_worker_pkt_ring_buffer);
        // printf("[debug]running%d\n",is_timeout);
        recv_size = dynamicbuf.size();
        if (is_timeout) {
            // printf("timeout\n");
            continue;  // continue to check transaction_running
        }
        packet_type_t pkt_type = get_packet_type(dynamicbuf.array(), recv_size);
#ifdef DEBUG_NETFETCH
        printf("[NETFETCH_DEBUG] pkt_type: %d\n", pkt_type);
#endif
        // printf("recv req2 %d\n", recv_size);
        // fflush(stdout);
        Task* task = new Task(dynamicbuf.array(), recv_size, client_addr, client_addrlen);
        // task->recv_size = task->dynamicbuf->size();
        thread_pool.add_task(task);
    }
    close(server_worker_udpsock_list[local_server_logical_idx]);
    printf("[server.worker %d-%d] exits", local_server_logical_idx, global_server_logical_idx);

    pthread_exit(nullptr);
}

#endif