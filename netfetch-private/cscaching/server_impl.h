#ifndef SERVER_IMPL_H
#define SERVER_IMPL_H

// Transaction phase for ycsb_server

#include <openssl/md5.h>
#include <pthread.h>

#include "../common/dynamic_array.h"
#include "../common/helper.h"
#include "../common/key.h"
#include "../common/metadata.h"
#include "../common/pkt_ring_buffer.h"
#include "../common/rocksdb_wrapper.h"
#include "../common/snapshot_record.h"
#include "../common/val.h"
#include "common_impl.h"
// #include "hdfs.h"
#include "hdfs_helper.h"
#include "../common/kvs_helper.h"
#include "message_queue_impl.h"

// #define server_thread_pool_size 20

#define ENABLE_HDFS

#define MD5_DIGEST_LENGTH_INSWITCH 8
// typedef ConcurrentSet<netreach_key_t> concurrent_set_t;

struct alignas(CACHELINE_SIZE) ServerWorkerParam {
    uint16_t local_server_logical_idx;
};
typedef ServerWorkerParam server_worker_param_t;

// Qingxiu: comment the following line
// RocksdbWrapper* db_wrappers = NULL;

int* server_worker_udpsock_list = NULL;
int* server_worker_lwpid_list = NULL;
pkt_ring_buffer_t* server_worker_pkt_ring_buffer_list = NULL;

// server.evictservers <-> controller.evictserver.evictclients
// int * server_evictserver_tcpsock_list = NULL;
// server.evictserver <-> controller.evictserver.evictclient
constexpr int SHARD_COUNT = 2048;

std::vector<std::mutex> shard_mutexes(SHARD_COUNT);

// for invalidation list
struct length_with_path_t {
    uint16_t length;
    std::string path;
};

MessagePtrQueue<length_with_path_t> invalidation_requests_ptr(MQ_SIZE * 1000);
MessagePtrQueue<length_with_path_t> invalidation_notification_ptr(MQ_SIZE * 1000);
pthread_mutex_t notification_mutex;

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

// std::atomic<long long> batchTotalDuration(0);
// std::atomic<long long> batchMaxDuration(0);
// const int batchSize = 1000;

std::atomic<long long> batchTotalDuration_rocksdb(0);
std::atomic<long long> batchMaxDuration_rocksdb(0);
const int batchSize_rocksdb = 10000;
std::atomic<int> batchidx_rocksdb(0);

void prepare_server();
// server.workers for processing pkts
void* run_server_worker(void* param);

void close_server();

void prepare_server() {
    // printf(" sizeof(netreach_key_t) %d\n", sizeof(netreach_key_t));
    init_GroupMapping("groupmapping");
    init_OwnerMapping("ownermapping");
    printf("[server] prepare start\n");

    // Qingxiu: comment the following line

    // RocksDB
    RocksdbWrapper::prepare_rocksdb();

    uint32_t current_server_logical_num = server_logical_idxes_list[server_physical_idx].size();

    db_wrappers = new RocksdbWrapper[current_server_logical_num];
    INVARIANT(db_wrappers != NULL);
    for (int i = 0; i < current_server_logical_num; i++) {
        db_wrappers[i].init(CURMETHOD_ID);
    }
    index_db_wrappers = new RocksdbWrapper[current_server_logical_num];
    INVARIANT(index_db_wrappers != NULL);
    for (int i = 0; i < current_server_logical_num; i++) {
        index_db_wrappers[i].init(CURMETHOD_ID);
    }

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
#endif
    // rocksdb code
    if (db_wrappers != NULL) {
        printf("Close rocksdb databases\n");
        delete[] db_wrappers;
        db_wrappers = NULL;
    }
    if (index_db_wrappers != NULL) {
        printf("Close rocksdb databases\n");
        delete[] index_db_wrappers;
        index_db_wrappers = NULL;
    }
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
             bind_thread_to_core(num_cores - i % num_cores - 1);
            std::string thread_name = "server.worker.thread" + std::to_string(i);
            int thread_server_fd;
            prepare_udpserver(thread_server_fd, true, tmp_server_worker_port + 128 + i, thread_name.c_str(), 0, SERVER_SOCKET_TIMEOUT_USECS, UDP_LARGE_RCVBUFSIZE);

            // auto start = std::chrono::high_resolution_clock::now();
            while (!stop) {
                Task* task;
                if (task_queue->pop(task)) {
                    // printf("start to handle task\n");
                    // fflush(stdout);
                    if (task == NULL) {
                        printf("pop NULL task:error\n");
                        continue;
                    }
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_DEBUG] [line %d] recv_buf: ", __LINE__);
                    for (int i = 0; i < task->recv_size; ++i) {
                        printf("%02x ", (unsigned char)task->dynamicbuf[i]);
                    }
                    printf("\n");
#endif
                    // auto start_handle_task = std::chrono::high_resolution_clock::now();
                    handle_task(task, thread_server_fd, i);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // auto duration_handle_task = std::chrono::duration_cast<std::chrono::microseconds>(end - start_handle_task);
                    // printf("[CSCACHE_DEBUG] [line %d] duration of two tasks is %ld microseconds\n", __LINE__, duration.count());
                    // printf("[CSCACHE_DEBUG] [line %d] handle_task took %ld microseconds\n", __LINE__, duration_handle_task.count());
                    // start = end;
                    delete task;
                    task == NULL;
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
#ifdef DEBUG_NETFETCH
    printf("[CSCACHE_DEBUG] [line %d] recv_buf: ", __LINE__);
    for (int i = 0; i < task->recv_size; ++i) {
        printf("%02x ", (unsigned char)task->dynamicbuf[i]);
    }
    printf("\n");
#endif

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
    printf("[CSCACHE_DEBUG] pkt_type: %d\n", pkt_type);
#endif

    // printf("[debug]pkt_type %d is_timeout %d\n",pkt_type,is_timeout);
    switch (pkt_type) {
    case packet_type_t::GETREQ: {
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] start serving GETREQ!\n");
        fflush(stdout);
#endif

        get_request_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
        netreach_key_t tmp_key = req.key();

        // get the real path length
        // first get depth that will affect offset to path length
        uint16_t path_depth;
        memcpy(&path_depth, &task->dynamicbuf[2], 2);
        path_depth = ntohs(path_depth);
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] path_depth: %d\n", __LINE__, path_depth);
#endif
        uint16_t path_length;
        uint16_t tmp_offset;
        tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + path_depth * sizeof(netreach_key_t);
        memcpy(&path_length, &task->dynamicbuf[tmp_offset], 2);
        path_length = ntohs(path_length);

#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif

        // get the path
        char queried_path[path_length + 1];
        tmp_offset += sizeof(uint16_t);
        // layout: op + depth + hash for each depth + path_length + path
        memcpy(queried_path, &task->dynamicbuf[tmp_offset], path_length);
        queried_path[path_length] = '\0';

#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] queried_path: %s\n", __LINE__, queried_path);
        // test the hashed key
        unsigned char digest[MD5_DIGEST_LENGTH];
        MD5((unsigned char*)&queried_path, strlen(queried_path), digest);
        for (int i = 0; i < sizeof(netreach_key_t); ++i) {
            if (digest[i] != (unsigned char)(task->dynamicbuf[i + sizeof(optype_t) + sizeof(keydepth_t) + (path_depth - 1) * sizeof(netreach_key_t)])) {
                printf("[CSCACHE_ERROR] MD5 digest error!\n");
                // dump path and path length
                printf("[CSCACHE_ERROR] queried_path: %s\n", queried_path);
                printf("[CSCACHE_ERROR] path_length: %d\n", path_length);
                // dump pkt
                // dump_buf(task->dynamicbuf, task->recv_size);
                // printf("[CSCACHE_ERROR] digest[%d] = %02x, dynamicbuf.array()[%d] = %02x\n", i, digest[i], i + sizeof(optype_t) + sizeof(keydepth_t) + (path_depth - 1) * sizeof(netreach_key_t), (unsigned char)(task->dynamicbuf[i + sizeof(optype_t) + (path_depth - 1) * sizeof(netreach_key_t)]));
            }
        }
#endif

        // get the bool value for is_cached
        bool is_cached = false;
        tmp_offset += path_length;
        memcpy(&is_cached, &task->dynamicbuf[tmp_offset], sizeof(bool));

#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] client-side is_cached: %s\n", __LINE__, is_cached ? "true" : "false");
#endif

        // if cached, get version numbers for each internal path
        std::vector<uint16_t> internal_versions;
        if (is_cached) {
            tmp_offset += sizeof(bool);
            for (int i = 0; i < path_depth - 1; ++i) {
                uint16_t tmp_version;
                memcpy(&tmp_version, &task->dynamicbuf[tmp_offset], sizeof(uint16_t));
                internal_versions.push_back(ntohs(tmp_version));
                tmp_offset += sizeof(uint16_t);
            }
        }

        val_t tmp_val;  // send to client
        uint32_t tmp_seq = 0;
        bool tmp_stat = true;

        std::string queried_path_string(queried_path);
        std::vector<std::string> tmp_levels = extractPathLevels(queried_path_string, file_parser_offset);

        // auto start = std::chrono::high_resolution_clock::now();
        auto get_results = kvs_get_path(queried_path_string, tmp_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
        // auto end = std::chrono::high_resolution_clock::now();
        // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        // printf("[CSCACHE_DEBUG] [line %d] kvs_get_path took %ld microseconds\n", __LINE__, duration.count());


        bool is_permission_check_success = get_results.first;
        std::vector<uint16_t> latest_versions = get_results.second;
#ifdef DEBUG_NETFETCH
        printf("[DEBUG][%d] is_permission_check_success %d\n", __LINE__, is_permission_check_success);
        printf("[DEBUG][%d] latest_versions %d\n", __LINE__, latest_versions.size());
#endif
        if (tmp_val.val_length == FILE_META_SIZE or tmp_val.val_length == DIR_META_SIZE) {
            // print debug

#ifdef DEBUG_NETFETCH
            printf("[DEBUG][%d] resp last version size %d tmp_levels %d\n", __LINE__, latest_versions.size(), tmp_levels.size());
#endif
            get_response_t rsp(CURMETHOD_ID, req.key(), tmp_val, tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            // attach latest permissions for internal paths
            for (int i = 0; i < tmp_levels.size() - 1; i++) {
                // bigendian
                latest_versions[i] = htons(latest_versions[i]);
                memcpy(&buf[rsp_size], &latest_versions[i], sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
            }
#ifdef DEBUG_NETFETCH
            printf("[DEBUG][%d] attach version rsp_size %d\n", __LINE__, rsp_size);
#endif
            // append the internal_paths size
            uint16_t internal_paths_size = htons(tmp_levels.size() - 1);
            memcpy(&buf[rsp_size], &internal_paths_size, sizeof(uint16_t));
            rsp_size += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] length = %ld\n", __LINE__, tmp_val.val_length);
            printf("[CSCACHE_DEBUG] [line %d] print the buf (length: %ld) sent to client: ", __LINE__, rsp_size);
            for (int i = 0; i < rsp_size; ++i) {
                printf("%02x ", (unsigned char)buf[i]);
            }
            printf("\n");
#endif
            udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] after sending response to client\n", __LINE__);
#endif

        } else if (tmp_val.val_length == 0) {
            get_response_t rsp(CURMETHOD_ID, req.key(), val_t(), tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
        } else {  // will not reach here
            printf("[CSCACHE_DEBUG] [line %d] length = %ld tmp_val: ", __LINE__, tmp_val.val_length);
            printf("error no large value for fs metadata service\n");
        }
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] after serving the request\n", __LINE__);
#endif
        break;
    }
    case packet_type_t::PUTREQ:
    case packet_type_t::DELREQ: {
        // dump_buf(task->dynamicbuf, task->recv_size);

        // failure flag
        int8_t failure_flag = 0;
        netreach_key_t tmp_key;
        val_t tmp_val;
        std::vector<std::string> tmp_levels;
        std::vector<uint16_t> latest_versions;
        std::string queried_path_string;

        if (pkt_type == packet_type_t::PUTREQ) {
            // Qingxiu: NetFetch only considers this operation and PUT_CACHED_SEQ for PUT
            // Qingxiu: key-->hash(path), value-->operation, payload-->real path
            put_request_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);

#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] value_length: %d\n", __LINE__, req.val().val_length);
#endif
            tmp_key = req.key();
            tmp_val = req.val();
#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] tmp_val: %s\n", __LINE__, tmp_val.val_data);
#endif
            // get path length
            uint16_t path_depth;
            memcpy(&path_depth, &task->dynamicbuf[2], 2);
            path_depth = ntohs(path_depth);
#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] path_depth: %d\n", __LINE__, path_depth);
#endif
            uint16_t path_length;
            uint16_t path2_length;
            // 2B op_type + 2B keydepth + 16B*keydepth key + 2B value_length + value + 6B + 2B path_length + path
            memcpy(&path_length, &task->dynamicbuf[8 + path_depth * sizeof(netreach_key_t) + req.val().val_length], 2);
            path_length = ntohs(path_length);
#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            char queried_path[path_length + 1];
            char new_path[MAX_PATH_LENGTH];
            uint16_t tmp_offset = 10 + path_depth * sizeof(netreach_key_t) + req.val().val_length;
            memcpy(queried_path, &task->dynamicbuf[tmp_offset], path_length);
            queried_path[path_length] = '\0';
            queried_path_string = std::string(queried_path);
            bool is_cached = false;

            tmp_offset += path_length;
            std::vector<uint16_t> internal_versions;
            tmp_levels = extractPathLevels(queried_path_string, file_parser_offset);
            bool is_mv = strcmp(tmp_val.val_data, "mv") == 0;
            if (tmp_offset + 1 == task->recv_size) {  // no new path , no versions
                memcpy(&is_cached, &task->dynamicbuf[tmp_offset], 1);
                // if cached, get version numbers for each internal path
                // INVARIANT(is_cached == 0);
                is_cached = 0;
            } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size && !is_mv) {  // no new path
                memcpy(&is_cached, &task->dynamicbuf[tmp_offset], 1);
                // if cached, get version numbers for each internal path
                // INVARIANT(is_cached == 1);
                if (is_cached) {
                    tmp_offset += sizeof(bool);
                    for (int i = 0; i < tmp_levels.size() - 1; ++i) {
                        uint16_t tmp_version;
                        memcpy(&tmp_version, &task->dynamicbuf[tmp_offset], sizeof(uint16_t));
                        internal_versions.push_back(ntohs(tmp_version));
                        tmp_offset += sizeof(uint16_t);
                    }
                }
            } else if (is_mv) {  // has new path
#ifdef DEBUG_NETFETCH
                printf("[CSCACHE_DEBUG] [line %d] has new path\n", __LINE__);
#endif
                memcpy(&path2_length, &task->dynamicbuf[tmp_offset], 2);
                path2_length = ntohs(path2_length);
                tmp_offset += sizeof(uint16_t);
                memcpy(new_path, &task->dynamicbuf[tmp_offset], path2_length);
                new_path[path2_length] = '\0';
                tmp_offset += path2_length;

                if (tmp_offset + 1 == task->recv_size) {  // has new path , no versions
                    memcpy(&is_cached, &task->dynamicbuf[tmp_offset], 1);
                    is_cached = 0;
                } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size) {  // has new path , has versions
                    memcpy(&is_cached, &task->dynamicbuf[tmp_offset], 1);                      // if cached, get version numbers for each internal path
                    INVARIANT(is_cached == 1);
                    if (is_cached) {
                        tmp_offset += sizeof(bool);
                        for (int i = 0; i < tmp_levels.size() - 1; ++i) {
                            uint16_t tmp_version;
                            memcpy(&tmp_version, &task->dynamicbuf[tmp_offset], sizeof(uint16_t));
                            internal_versions.push_back(ntohs(tmp_version));
                            tmp_offset += sizeof(uint16_t);
                        }
                    }
                } else {
                    printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
                }
            } else {
                printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
            }

#ifdef DEBUG_NETFETCH
            printf("[CSCACHE_DEBUG] [line %d] is_cached: %s\n", __LINE__, is_cached ? "true" : "false");
#endif

            // use string for queried path and new path
            queried_path_string = std::string(queried_path);
            std::string new_path_string = std::string(new_path);

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
                val_t tmp_touch_val;
                tmp_touch_val.val_length = FILE_META_SIZE;
                tmp_touch_val.val_data = new char[FILE_META_SIZE];
                memset(tmp_touch_val.val_data, 0, FILE_META_SIZE);
                auto touch_res = kvs_create_path(queried_path_string, tmp_touch_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                bool is_touch_successful = touch_res.first;
                latest_versions = touch_res.second;
                if (is_touch_successful) {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_DEBUG] hdfsOpenFile successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_ERROR] hdfsOpenFile failed!\n");
#endif
                    // change keydepth to 0xFFFF and copy it to the third and forth bytes of tml_val
                    failure_flag = 1;
                }
            }

            // delete file
            else if (strcmp(tmp_val.val_data, "rm") == 0) {
                val_t tmp_rm_val;
                auto rm_res = kvs_delete_path(queried_path_string, tmp_rm_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                bool is_rm_successful = rm_res.first;
                latest_versions = rm_res.second;
                if (is_rm_successful) {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_DEBUG] hdfsDelete successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_ERROR] hdfsDelete failed!\n");
#endif
                    failure_flag = 1;
                }
            }

            // create directory
            else if (strcmp(tmp_val.val_data, "mkdir") == 0) {
                val_t tmp_mkdir_val;
                tmp_mkdir_val.val_length = DIR_META_SIZE;
                tmp_mkdir_val.val_data = new char[DIR_META_SIZE];
                memset(tmp_mkdir_val.val_data, 0, DIR_META_SIZE);
                // auto start = std::chrono::high_resolution_clock::now();
                auto mkdir_res = kvs_mkdir(queried_path_string, tmp_mkdir_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                // auto end = std::chrono::high_resolution_clock::now();
                // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                // printf("[CSCACHE_DEBUG] [line %d] kvs_mkdir took %ld microseconds\n", __LINE__, duration.count());
                bool is_mkdir_successful = mkdir_res.first;
                latest_versions = mkdir_res.second;
                if (is_mkdir_successful) {

#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_DEBUG] hdfsCreateDirectory successfully!\n");
#endif
                    if (true) {
                        // notify other servers to invalidate the cache
                        length_with_path_t* tmp_length_with_path = new length_with_path_t();
                        tmp_length_with_path->length = path_length;
                        tmp_length_with_path->path = queried_path_string;
                        pthread_mutex_lock(&notification_mutex);
                        bool res = invalidation_notification_ptr.write(tmp_length_with_path);
                        pthread_mutex_unlock(&notification_mutex);
                        if (!res) {
                            printf("[CSCACHE_ERROR] [line %d] invalidation_notification_ptr.write failed!\n", __LINE__);
                        } else {
#ifdef DEBUG_NETFETCH
                            printf("[CSCACHE_DEBUG] [line %d] invalidation_notification_ptr.write successfully!\n", __LINE__);
#endif
                        }
                    }
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_ERROR] hdfsCreateDirectory failed!\n");
#endif

                    failure_flag = 1;
                }
            }

            // list directory
            else if (strcmp(tmp_val.val_data, "ls") == 0) {
                // auto start = std::chrono::high_resolution_clock::now();
                auto ls_res = kvs_readdir(queried_path_string, tmp_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                // auto end = std::chrono::high_resolution_clock::now();
                // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                // printf("[CSCACHE_DEBUG] [line %d] kvs_readdir took %ld microseconds\n", __LINE__, duration.count());
                failure_flag = ls_res.first == (size_t)-1 ? 1 : -1;
#ifdef DEBUG_NETFETCH
                printf("[CSCACHE_DEBUG] [line %d] ls_res.first = %d, failure_flag = %d\n", __LINE__, ls_res.first, failure_flag);
#endif
                latest_versions = ls_res.second;
            }

            // delete directory
            else if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                val_t tmp_rmdir_val;
                // auto start = std::chrono::high_resolution_clock::now();
                auto rmdir_res = kvs_rmdir(queried_path_string, tmp_rmdir_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                // auto end = std::chrono::high_resolution_clock::now();
                // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                // printf("[CSCACHE_DEBUG] [line %d] kvs_rmdir took %ld microseconds\n", __LINE__, duration.count());
                bool is_rmdir_successful = rmdir_res.first;
                latest_versions = rmdir_res.second;
                if (is_rmdir_successful) {
                } else {
                    failure_flag = 1;
                }
            }

            // chmod for file or directory
            else if (strncmp(tmp_val.val_data, "chmod", 5) == 0) {  // Check if the operation starts with "chmod"
                int mode = extract_mode_from_chmod_command(tmp_val.val_data);
                if (mode != -1) {  // Ensure the mode was correctly extracted
                    val_t tmp_chmod_val;
                    tmp_chmod_val.val_length = FILE_META_SIZE;
                    tmp_chmod_val.val_data = new char[FILE_META_SIZE];
                    memset(tmp_chmod_val.val_data, 0, FILE_META_SIZE);
                    // auto start = std::chrono::high_resolution_clock::now();
                    auto chmod_res = kvs_update_path(queried_path_string, tmp_chmod_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSCACHE_DEBUG] [line %d] kvs_update_path took %ld microseconds\n", __LINE__, duration.count());
                    bool result = chmod_res.first;
                    latest_versions = chmod_res.second;
                    if (result) {
#ifdef DEBUG_NETFETCH
                        printf("[CSCACHE_DEBUG] hdfsChmod successfully applied permissions %o on %s\n", mode, queried_path);
#endif
                    } else {
#ifdef DEBUG_NETFETCH
                        printf("[CSCACHE_ERROR] hdfsChmod failed on %s with mode %o\n", queried_path, mode);
#endif
                        failure_flag = 1;
                    }
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_ERROR] Invalid chmod command format: %s\n", tmp_val.val_data);
#endif
                    failure_flag = 1;
                }
            }

            // mv
            else if (strcmp(tmp_val.val_data, "mv") == 0) {
                val_t tmp_mv_val;
                auto start = std::chrono::high_resolution_clock::now();
                auto rm_res = kvs_mv_file(queried_path_string, new_path_string, tmp_mv_val, tmp_levels, is_cached, internal_versions, local_server_logical_idx);
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                // printf("[CSCACHE_DEBUG] [line %d] kvs_mv_file took %ld microseconds\n", __LINE__, duration.count());
                bool is_mv_successful = rm_res.first;
                latest_versions = rm_res.second;
                if (is_mv_successful) {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_DEBUG] rename successfully!\n");
#endif
                } else {
#ifdef DEBUG_NETFETCH
                    printf("[CSCACHE_ERROR] rename failed!\n");
#endif
                    failure_flag = 1;
                }
            }

            // finsh the benchmark
            if (strcmp(tmp_val.val_data, "finish") == 0) {
                // print finish message in each iteration and do flush and compaction
                printf("[CSCACHE_DEBUG] [line %d] finish message received\n", __LINE__);
                // do flush and compaction
                auto start_time = std::chrono::high_resolution_clock::now();
                bool res = kvs_flush_and_compact(local_server_logical_idx);
                auto end_time = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                printf("[CSCACHE_DEBUG] [line %d] kvs_flush_and_compact took %ld microseconds\n", __LINE__, duration.count());
                if (!res) {
                    printf("[CSCACHE_ERROR] [line %d] kvs_flush_and_compact failed!\n", __LINE__);
                } else {
                }
                failure_flag = 1;
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
                printf("[CSCACHE_DEBUG] [line %d] append value for listdir\n", __LINE__);
#endif
                uint16_t tmp_vallen = htons(tmp_val.val_length);
                // print val_length
                // printf("[CSCACHE_DEBUG] [line %d] val_length: %d\n", __LINE__, tmp_val.val_length);
                memcpy(&buf[rsp_size], &tmp_vallen, sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
                memcpy(&buf[rsp_size], tmp_val.val_data, tmp_val.val_length);
                rsp_size += tmp_val.val_length;
                if ((tmp_levels.size() - 1) != latest_versions.size()) {
                    printf("[CSCACHE_ERROR] [line %d] tmp_levels.size = %d, latest_versions.size = %d\n", __LINE__, tmp_levels.size(), latest_versions.size());
                }
                // attach latest permissions for internal paths
                for (int i = 0; i < tmp_levels.size() - 1; i++) {
                    // bigendian
                    latest_versions[i] = htons(latest_versions[i]);
                    memcpy(&buf[rsp_size], &latest_versions[i], sizeof(uint16_t));
                    rsp_size += sizeof(uint16_t);
                }
                // append the internal_paths size
                uint16_t internal_paths_size = htons(latest_versions.size());
                memcpy(&buf[rsp_size], &internal_paths_size, sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
            }
            if (failure_flag == 0) {
                if ((tmp_levels.size() - 1) != latest_versions.size()) {
                    printf("[CSCACHE_ERROR] [line %d] tmp_levels.size = %d, latest_versions.size = %d\n", __LINE__, tmp_levels.size(), latest_versions.size());
                }
                // printf("[DEBUG][%d] queried_path_string %s, tmp_levels.size = %d\n", __LINE__, queried_path_string.c_str(), tmp_levels.size());
                // attach latest permissions for internal paths
                for (int i = 0; i < tmp_levels.size() - 1; i++) {
                    // bigendian
                    latest_versions[i] = htons(latest_versions[i]);
                    memcpy(&buf[rsp_size], &latest_versions[i], sizeof(uint16_t));
                    rsp_size += sizeof(uint16_t);
                }
                // append the internal_paths size
                uint16_t internal_paths_size = htons(latest_versions.size());
                memcpy(&buf[rsp_size], &internal_paths_size, sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
            }
        }
        udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
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

    // init_HDFS();

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

    // rocksdb
    // open rocksdb
    bool is_existing = db_wrappers[local_server_logical_idx].open(global_server_logical_idx);
    if (!is_existing) {
        printf("[server.worker %d-%d] you need to run loader before server\n", local_server_logical_idx, global_server_logical_idx);
        // exit(-1);
    }
    printf("[server.worker %d-%d] data db ready\n", local_server_logical_idx, global_server_logical_idx);
    is_existing = index_db_wrappers[local_server_logical_idx].open_index(global_server_logical_idx);
    if (!is_existing) {
        printf("[server.worker %d-%d] you need to run loader before server\n", local_server_logical_idx, global_server_logical_idx);
        // exit(-1);
    }
    init_dir_id_mapping(local_server_logical_idx);
    printf("[server.worker %d-%d] index db ready, has %d dirs mapping\n", local_server_logical_idx, global_server_logical_idx, dir_id_map.size());
    fflush(stdout);
    transaction_ready_threads++;

    while (!transaction_running) {
    }

    bool is_timeout = false;
    // auto start = std::chrono::high_resolution_clock::now();
    while (transaction_running) {
        dynamicbuf.clear();
        is_timeout = udprecvlarge_ipfrag(CURMETHOD_ID, server_worker_udpsock_list[local_server_logical_idx], dynamicbuf, 0, &client_addr, &client_addrlen, "server.worker", &cur_worker_pkt_ring_buffer);
        recv_size = dynamicbuf.size();
        if (is_timeout) {
            continue;  // continue to check transaction_running
        }
        packet_type_t pkt_type = get_packet_type(dynamicbuf.array(), recv_size);
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] recv_buf: ", __LINE__);
        for (int i = 0; i < recv_size; ++i) {
            printf("%02x ", (unsigned char)dynamicbuf.array()[i]);
        }
        printf("\n");
        printf("[CSCACHE_DEBUG] pkt_type: %d\n", pkt_type);
#endif
        Task* task = new Task(dynamicbuf.array(), recv_size, client_addr, client_addrlen);
        thread_pool.add_task(task);
        // auto end = std::chrono::high_resolution_clock::now();
        // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        // printf("[server.worker %d-%d] [line %d] task added to thread pool, duration: %ld microseconds\n", local_server_logical_idx, global_server_logical_idx, __LINE__, duration.count());
        // start = end;  // reset start time for next task
    }
    close(server_worker_udpsock_list[local_server_logical_idx]);
    printf("[server.worker %d-%d] exits", local_server_logical_idx, global_server_logical_idx);

    pthread_exit(nullptr);
}

void* run_server_coordinator(void* param) {
    printf("[server.coordinator] ready\n");
    fflush(stdout);

    server_worker_param_t& thread_param = *(server_worker_param_t*)param;
    uint16_t local_server_logical_idx = thread_param.local_server_logical_idx;  // [0, current_server_logical_num - 1]
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    transaction_ready_threads++;

    int coordinator_server_udpsock = -1;
    prepare_udpserver(coordinator_server_udpsock, false, atoi(server_ports_for_invalidation_list[server_physical_idx]), "server.coordinatorserver");
    char recvbuf[MAX_BUFSIZE];
    int recvsize = 0;
    sockaddr_in sender_addr;
    socklen_t sender_addr_len = sizeof(sender_addr);

    while (!transaction_running) {
    }

    while (transaction_running) {
        udprecvfrom(coordinator_server_udpsock, recvbuf, MAX_BUFSIZE, 0, &sender_addr, &sender_addr_len, recvsize, "server.coordinatorserver");
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE] [line %d] recvsize: %d\n", __LINE__, recvsize);
        printf("[CSCACHE] recvbuf: ");
        for (int i = 0; i < recvsize; i++) {
            printf("%02x ", (unsigned char)recvbuf[i]);
        }
        printf("\n");
#endif
        if (recvsize < 2) {
            printf("[server.coordinator] invalid recvsize: %d\n", recvsize);
            continue;
        }
        // send ack to another server
        udpsendto(coordinator_server_udpsock, recvbuf, recvsize, 0, &sender_addr, sender_addr_len, "server.coordinatorserver");
        // 2B length + path
        uint16_t path_length;
        memcpy(&path_length, recvbuf, 2);
        path_length = ntohs(path_length);
        if (recvsize != 2 + path_length) {
            printf("[server.coordinator] invalid recvsize: %d\n", recvsize);
            continue;
        }
        std::string path(recvbuf + 2, path_length);
        length_with_path_t* tmp_length_with_path_ptr = new length_with_path_t();
        tmp_length_with_path_ptr->length = path_length;
        tmp_length_with_path_ptr->path = path;

        // add updates into messagequeue
        bool res = invalidation_requests_ptr.write(tmp_length_with_path_ptr);
        if (!res) {
            printf("[CSCACHE_ERROR] message queue overflow of invalidation_requests_ptr!\n");
        } else {
#ifdef NETFETCH_DEBUG
            printf("[CSCACHE_DEBUG] [line %d] write to invalidation_requests_ptr\n", __LINE__);
#endif
        }
    }

    // close socket
    close(coordinator_server_udpsock);
    pthread_exit(nullptr);
}

void* run_client_coordinator(void* param) {
    printf("[client.coordinator] ready\n");
    fflush(stdout);

    server_worker_param_t& thread_param = *(server_worker_param_t*)param;
    uint16_t local_server_logical_idx = thread_param.local_server_logical_idx;  // [0, current_server_logical_num - 1]
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    transaction_ready_threads++;

    while (!transaction_running) {
    }

    while (transaction_running) {
        length_with_path_t* tmp_length_with_path_ptr = invalidation_requests_ptr.read();
        if (tmp_length_with_path_ptr == NULL) {
            continue;
        } else {  // read updates from messagequeue
            std::string path = tmp_length_with_path_ptr->path;
            uint16_t path_length = tmp_length_with_path_ptr->length;
            // delete useless pointer
            delete tmp_length_with_path_ptr;
            tmp_length_with_path_ptr = NULL;
#ifdef NETFETCH_DEBUG
            printf("[CSCACHE_DEBUG] [line %d] read %s invalidation_requests_ptr\n", __LINE__, path.c_str());
#endif
            // add updates into invalidation list
            mutex_for_invalidation_list.lock();
            auto it = invalidation_list.find(path);
            if (it == invalidation_list.end()) {
                invalidation_list.insert({path, 1});
            } else {
                it->second += 1;
            }
            mutex_for_invalidation_list.unlock();

            // perform metadata retrieval from HDFS
            val_t tmp_val;
            tmp_val.val_length = DIR_META_SIZE;
            tmp_val.val_data = new char[DIR_META_SIZE];
            memset(tmp_val.val_data, 0, DIR_META_SIZE);
            // fetch_metadata_from_hdfs_for_get(tmp_val, path.c_str());

            // update rocksdb
            db_wrappers[local_server_logical_idx].put_path(path, tmp_val);
#ifdef NETFETCH_DEBUG
            // read the metadata from rocksdb
            val_t tmp_rocksdb_val;
            db_wrappers[local_server_logical_idx].get_path(path, tmp_rocksdb_val);
            for (int i = 0; i < tmp_rocksdb_val.val_length; i++) {
                printf("%02x ", (unsigned char)tmp_rocksdb_val.val_data[i]);
            }
            printf("\n");
#endif
        }
    }

    pthread_exit(nullptr);
}

void* run_client_notifier(void* param) {
    printf("[client.notifier] ready\n");
    fflush(stdout);

    server_worker_param_t& thread_param = *(server_worker_param_t*)param;
    uint16_t local_server_logical_idx = thread_param.local_server_logical_idx;  // [0, current_server_logical_num - 1]
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    // prepare socket
    int client_notifier_socket = -1;
    create_udpsock(client_notifier_socket, false, "client.notifier");
    struct sockaddr_in client_notifier_addr;
    memset(&client_notifier_addr, 0, sizeof(client_notifier_addr));
    set_sockaddr(client_notifier_addr, inet_addr(another_server_ips_for_invalidation_list[global_server_logical_idx]), atoi(server_ports_for_invalidation_list[server_physical_idx]));
    int client_notifier_addr_len = sizeof(struct sockaddr);

    char sendbuf[MAX_BUFSIZE];
    char recvbuf[MAX_BUFSIZE];
    int sendsize = 0;
    int recvsize = 0;

    transaction_ready_threads++;

    while (!transaction_running) {
    }

    while (transaction_running) {
        length_with_path_t* tmp_length_with_path_ptr = invalidation_notification_ptr.read();
        if (tmp_length_with_path_ptr == NULL) {
            continue;
        } else {  // read updates from messagequeue
#ifdef NETFETCH_DEBUG
            printf("[CSCACHE_DEBUG] [line %d] read invalidation_notification_ptr\n", __LINE__);
#endif
            std::string path = tmp_length_with_path_ptr->path;
            uint16_t path_length = tmp_length_with_path_ptr->length;
            // delete useless pointer
            delete tmp_length_with_path_ptr;
            tmp_length_with_path_ptr = NULL;
#ifdef NETFETCH_DEBUG
            printf("[CSCACHE_DEBUG] [line %d] read %s invalidation_requests_ptr\n", __LINE__, path.c_str());
#endif
            // prepare sendbuf
            uint16_t tmp_path_length = htons(path_length);
            memcpy(sendbuf, &tmp_path_length, 2);
            memcpy(sendbuf + 2, path.c_str(), path_length);
            sendsize = 2 + path_length;

            while (true) {
                // send invalidation to another server
            udpsendto(client_notifier_socket, sendbuf, sendsize, 0, &client_notifier_addr, client_notifier_addr_len, "client.notifier");
                bool is_timeout = false;
                // recv ack from another server
                is_timeout = udprecvfrom(client_notifier_socket, recvbuf, MAX_BUFSIZE, 0, NULL, NULL, recvsize, "client.notifier");
                if (unlikely(is_timeout)) {
                    continue;
                }
                // get path and length
                uint16_t recv_path_length;
                memcpy(&recv_path_length, recvbuf, 2);
                recv_path_length = ntohs(recv_path_length);
                if (recv_path_length != path_length) {
                    continue;
                }
                char recv_path[MAX_PATH_LENGTH];
                memcpy(recv_path, recvbuf + 2, recv_path_length);
                recv_path[recv_path_length] = '\0';
                if (strcmp(path.c_str(), recv_path) != 0) {
                    continue;
                } else {
#ifdef NETFETCH_DEBUG
                    printf("[CSCACHE_DEBUG] [line %d] recv_path: %s\n", __LINE__, recv_path);
#endif
                }
                break;
            }
        }
    }

    // close socket
    close(client_notifier_socket);
    pthread_exit(nullptr);
}

#endif