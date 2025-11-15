#ifndef SERVER_IMPL_H
#define SERVER_IMPL_H

// Transaction phase for ycsb_server

#include <pthread.h>

#include <queue>
#include <thread>
#include <vector>

#include "../common/helper.h"
#include "../common/key.h"
#include "../common/metadata.h"
#include "../common/rocksdb_wrapper.h"
#include "../common/snapshot_record.h"
#include "../common/val.h"
#include "concurrent_map_impl.h"
#include "concurrent_set_impl.h"
#include "message_queue_impl.h"
// #include "../common/rocksdb_wrapper.h"
#include <openssl/md5.h>
#include <tbb/concurrent_unordered_map.h>

#include <cstring>
#include <map>
#include <unordered_map>
#include <unordered_set>

#include "../common/dynamic_array.h"
#include "../common/pkt_ring_buffer.h"
#include "hashring.h"
#include "hdfs_helper.h"
#include "../common/kvs_helper.h"
// #include "hdfs.h"

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

//

#define ENABLE_HDFS
// #define DISABLE_HDFS

typedef ConcurrentSet<netreach_key_t> concurrent_set_t;
#define MD5_DIGEST_LENGTH_INSWITCH 8
struct alignas(CACHELINE_SIZE) ServerWorkerParam {
    uint16_t local_server_logical_idx;
};
typedef ServerWorkerParam server_worker_param_t;
typedef uint8_t token_t;

// Qingxiu: comment the following line
// RocksdbWrapper* db_wrappers = NULL;
int* server_worker_udpsock_list = NULL;
int* server_worker_lwpid_list = NULL;
pkt_ring_buffer_t* server_worker_pkt_ring_buffer_list = NULL;

// Per-server popserver <-> controller.popserver.popclient
int* server_popserver_udpsock_list = NULL;
// access all of beingcached/cached/beingupdated keysets should be atomic
std::mutex* server_mutex_for_keyset_list = NULL;
// std::mutex* server_mutex_for_setinvalid = NULL;
// std::set<std::pair<netreach_key_t, std::string>>* server_beingcached_keyset_list = NULL;
// std::set<std::pair<netreach_key_t, std::string>>* server_cached_keyset_list = NULL;
// std::set<std::pair<netreach_key_t, std::string>>* server_beingupdated_keyset_list = NULL;
// tbb::concurrent_unordered_map<std::string, token_t> server_cached_path_token_map_list;
std::unordered_map<std::string, token_t> server_cached_path_token_map_list;
constexpr int SHARD_COUNT = 2048;
struct PairHash {
    std::size_t operator()(const std::pair<netreach_key_t, std::string>& p) const {
        size_t hash1 = (p.first.keylo ^ p.first.keyhi);
        size_t hash2 = std::hash<std::string>{}(p.second);
        return hash1 ^ (hash2 << 1);  // 合并两个哈希值
    }
};
std::vector<std::unordered_set<std::pair<netreach_key_t, std::string>, PairHash>> beingcached_shards(SHARD_COUNT);
std::vector<std::unordered_set<std::pair<netreach_key_t, std::string>, PairHash>> cached_shards(SHARD_COUNT);
std::vector<std::unordered_set<std::pair<netreach_key_t, std::string>, PairHash>> beingupdated_shards(SHARD_COUNT);
std::vector<std::mutex> shard_mutexes(SHARD_COUNT);

// for invalidation list
struct length_with_path_t {
    uint16_t length;
    std::string path;
};
// std::unordered_set<std::string> invalidation_list;
// std::mutex mutex_for_invalidation_list;
MessagePtrQueue<length_with_path_t> invalidation_requests_ptr(MQ_SIZE * 1000);
MessagePtrQueue<length_with_path_t> invalidation_notification_ptr(MQ_SIZE * 1000);
pthread_mutex_t notification_mutex;
pthread_mutex_t delete_server_mutex;
bool volatile is_loading_phase_end = false;

inline int compute_hash_index(const netreach_key_t& key) {
    return (key.keylo ^ key.keyhi) % SHARD_COUNT;
}
// server.evictservers <-> controller.evictserver.evictclients
// int * server_evictserver_tcpsock_list = NULL;
// server.evictserver <-> controller.evictserver.evictclient
int* server_evictserver_udpsock_list = NULL;

// data plane <-> per-server.valueupdateserver
MessagePtrQueue<std::tuple<netcache_valueupdate_t*, std::string, int8_t>>* server_netcache_valueupdate_ptr_queue_list = NULL;
MessagePtrQueue<std::pair<key_op_path_t*, std::string>>* server_netcache_valuedelete_ptr_queue_list = NULL;

int* server_valueupdateserver_udpsock_list = NULL;
int* server_valuedeleteserver_udpsock_list = NULL;

void prepare_server();
// server.workers for processing pkts
void* run_server_worker(void* param);
void* run_server_popserver(void* param);
void* run_server_evictserver(void* param);
void* run_server_valueupdateserver(void* param);
void* run_server_valuedeleteserver(void* param);
void close_server();

// for hdfs partition
namespace Patch {
// jz add a patch
int patch_mode = -1;
int patch_server_logical_num = -1;
int patch_bottleneck_id = -1;
int patch_rotation_id = -1;

bool trim(std::string& str) {
    size_t first = str.find_first_not_of(' ');
    size_t last = str.find_last_not_of(' ');
    if (first == std::string::npos || last == std::string::npos) {
        return false;
    }
    str = str.substr(first, (last - first + 1));
    return true;
}

std::map<std::string, std::string> parseIniFile(const std::string& filename) {
    std::ifstream file(filename);
    std::string line;
    std::map<std::string, std::string> globalSection;
    bool inGlobalSection = false;
    while (std::getline(file, line)) {
        // Trim leading and trailing spaces
        trim(line);
        // Skip empty lines and comments
        if (line.empty() || line[0] == ';' || line[0] == '#') {
            continue;
        }
        // Check if the line is a section header
        if (line.front() == '[' && line.back() == ']') {
            std::string section = line.substr(1, line.size() - 2);
            trim(section);
            // Check if we are in the [global] section
            inGlobalSection = (section == "global");
            continue;
        }
        // If we are in the [global] section, parse key-value pairs
        if (inGlobalSection) {
            size_t pos = line.find('=');
            if (pos != std::string::npos) {
                std::string key = line.substr(0, pos);
                std::string value = line.substr(pos + 1);
                trim(key);
                trim(value);
                globalSection[key] = value;
            }
        }
    }
    return globalSection;
}
int patch_parse_ini() {
    std::string filename = "/home/jz/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/config.ini";
    std::map<std::string, std::string> globalConfig = parseIniFile(filename);
    if (globalConfig.find("mode") != globalConfig.end()) {
        // std::cout << "Mode: " << globalConfig["mode"] << std::endl;
        patch_mode = std::stoi(globalConfig["mode"]);
    }
    if (globalConfig.find("server_logical_num") != globalConfig.end()) {
        // std::cout << "Server Logical Num: " << globalConfig["server_logical_num"] << std::endl;
        patch_server_logical_num = std::stoi(globalConfig["server_logical_num"]);
    }
    if (globalConfig.find("bottleneck_id") != globalConfig.end()) {
        // std::cout << "Bottleneck ID: " << globalConfig["bottleneck_id"] << std::endl;
        patch_bottleneck_id = std::stoi(globalConfig["bottleneck_id"]);
    }
    if (globalConfig.find("rotation_id") != globalConfig.end()) {
        // std::cout << "Rotation ID: " << globalConfig["rotation_id"] << std::endl;
        patch_rotation_id = std::stoi(globalConfig["rotation_id"]);
    }
    return 0;
}
}  // namespace Patch
void prepare_server() {
    init_GroupMapping("groupmapping");
    init_OwnerMapping("ownermapping");
    printf("[server] prepare start %d servers\n", server_logical_idxes_list[server_physical_idx].size());
    // RocksDB
    RocksdbWrapper::prepare_rocksdb();

    uint32_t current_server_logical_num = server_logical_idxes_list[server_physical_idx].size();

    db_wrappers = new RocksdbWrapper[current_server_logical_num];
    INVARIANT(db_wrappers != NULL);
    for (int i = 0; i < current_server_logical_num; i++) {
        db_wrappers[i].init(NOCACHE_ID);
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
    server_popserver_udpsock_list = new int[current_server_logical_num];
    // server_mutex_for_keyset_list = new std::mutex[current_server_logical_num];
    // server_mutex_for_setinvalid = new std::mutex[current_server_logical_num];
    // server_beingcached_keyset_list = new std::set<std::pair<netreach_key_t, std::string>>[current_server_logical_num];
    // server_cached_keyset_list = new std::set<std::pair<netreach_key_t, std::string>>[current_server_logical_num];
    // server_beingupdated_keyset_list = new std::set<std::pair<netreach_key_t, std::string>>[current_server_logical_num];
    // server_cached_path_token_map_list = new std::unordered_map<std::string, token_t>[current_server_logical_num];

    for (size_t i = 0; i < current_server_logical_num; i++) {
        prepare_udpserver(server_popserver_udpsock_list[i], true, server_popserver_port_start + server_logical_idxes_list[server_physical_idx][i], "server.popserver");
        // server_beingcached_keyset_list[i].clear();
        // server_cached_keyset_list[i].clear();
        // server_cached_path_token_map_list[i].clear();
        // server_beingupdated_keyset_list[i].clear();
        // server_cached_path_token_map_list[i].reserve(100000);
    }

    // prepare for cache eviction
    server_evictserver_udpsock_list = new int[current_server_logical_num];
    for (size_t i = 0; i < current_server_logical_num; i++) {
        uint16_t tmp_global_server_logical_idx = server_logical_idxes_list[server_physical_idx][i];
        prepare_udpserver(server_evictserver_udpsock_list[i], true, server_evictserver_port_start + tmp_global_server_logical_idx, "server.evictserver");
    }

    // prepare for inswitch value update
    server_netcache_valueupdate_ptr_queue_list = new MessagePtrQueue<std::tuple<netcache_valueupdate_t*, std::string, int8_t>>[current_server_logical_num];
    server_valueupdateserver_udpsock_list = new int[current_server_logical_num];
    for (size_t i = 0; i < current_server_logical_num; i++) {
        server_netcache_valueupdate_ptr_queue_list[i].init(MQ_SIZE);
        uint16_t tmp_global_server_logical_idx = server_logical_idxes_list[server_physical_idx][i];
        prepare_udpserver(server_valueupdateserver_udpsock_list[i], true, server_valueupdateserver_port_start + delete_server_number + i, "server.valueupdateserver");
    }

    // prepare for inswitch value delete
    server_netcache_valuedelete_ptr_queue_list = new MessagePtrQueue<std::pair<key_op_path_t*, std::string>>[current_server_logical_num];
    server_valuedeleteserver_udpsock_list = new int[delete_server_number];
    for (size_t i = 0; i < current_server_logical_num; i++) {
        server_netcache_valuedelete_ptr_queue_list[i].init(MQ_SIZE);
        uint16_t tmp_global_server_logical_idx = server_logical_idxes_list[server_physical_idx][i];
    }
    for (size_t i = 0; i < delete_server_number; i++) {
        prepare_udpserver(server_valuedeleteserver_udpsock_list[i], true, server_valueupdateserver_port_start + i, "server.valuedeleteserver");
    }

    memory_fence();

    printf("[server] prepare end\n");
}

void close_server() {
#ifdef ENABLE_HDFS
    // disconnect_with_hdfs_namenodes();
    // print load balance ratio
    printf("-----------------------------------------------------\n");
    printf("[CSFETCH_DEBUG] overall access: %d\n", ns1_accesses + ns2_accesses);
    printf("[CSFETCH_DEBUG] ns1_accesses: %d\n", ns1_accesses);
    printf("[CSFETCH_DEBUG] ns2_accesses: %d\n", ns2_accesses);
    printf("-----------------------------------------------------\n");
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
    if (server_popserver_udpsock_list != NULL) {
        delete[] server_popserver_udpsock_list;
        server_popserver_udpsock_list = NULL;
    }
    // if (server_mutex_for_keyset_list != NULL) {
    //     delete[] server_mutex_for_keyset_list;
    //     server_mutex_for_keyset_list = NULL;
    // }
    // if (server_mutex_for_setinvalid != NULL) {
    //     delete[] server_mutex_for_setinvalid;
    //     server_mutex_for_setinvalid = NULL;
    // }
    // if (server_cached_path_token_map_list != NULL) {
    //     delete[] server_cached_path_token_map_list;
    //     server_cached_path_token_map_list = NULL;
    // }
    // if (server_beingcached_keyset_list != NULL) {
    //     delete[] server_beingcached_keyset_list;
    //     server_beingcached_keyset_list = NULL;
    // }
    // if (server_cached_keyset_list != NULL) {
    //     delete[] server_cached_keyset_list;
    //     server_cached_keyset_list = NULL;
    // }
    // if (server_beingupdated_keyset_list != NULL) {
    //     delete[] server_beingupdated_keyset_list;
    //     server_beingupdated_keyset_list = NULL;
    // }
    if (server_evictserver_udpsock_list != NULL) {
        delete[] server_evictserver_udpsock_list;
        server_evictserver_udpsock_list = NULL;
    }
    /*if (server_evictserver_tcpsock_list != NULL) {
            delete [] server_evictserver_tcpsock_list;
            server_evictserver_tcpsock_list = NULL;
    }*/
    if (server_netcache_valueupdate_ptr_queue_list != NULL) {
        delete[] server_netcache_valueupdate_ptr_queue_list;
        server_netcache_valueupdate_ptr_queue_list = NULL;
    }
    if (server_valueupdateserver_udpsock_list != NULL) {
        delete[] server_valueupdateserver_udpsock_list;
        server_valueupdateserver_udpsock_list = NULL;
    }
    if (server_netcache_valuedelete_ptr_queue_list != NULL) {
        delete[] server_netcache_valuedelete_ptr_queue_list;
        server_netcache_valuedelete_ptr_queue_list = NULL;
    }
    if (server_valuedeleteserver_udpsock_list != NULL) {
        delete[] server_valuedeleteserver_udpsock_list;
        server_valuedeleteserver_udpsock_list = NULL;
    }
}

void* run_server_popserver(void* param) {
    // Parse param
    uint16_t local_server_logical_idx = *((uint16_t*)param);
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    // NOTE: controller and switchos should have been launched before servers
    struct sockaddr_in controller_popserver_popclient_addr;
    socklen_t controller_popserver_popclient_addrlen = sizeof(struct sockaddr_in);

    printf("[server.popserver%d] ready\n", int(local_server_logical_idx));
    fflush(stdout);

    transaction_ready_threads++;
    while (!transaction_running) {
    }
    char buf[MAX_BUFSIZE];
    char recvbuf[MAX_BUFSIZE];
    int recvsize = 0;
    while (transaction_running) {
        // printf("[debug]running\n");
        bool is_timeout = udprecvfrom(server_popserver_udpsock_list[local_server_logical_idx], recvbuf, MAX_BUFSIZE, 0, &controller_popserver_popclient_addr, &controller_popserver_popclient_addrlen, recvsize, "server.popserver");
        // printf("[debug]running\n");
        if (is_timeout) {
            controller_popserver_popclient_addrlen = sizeof(struct sockaddr_in);
            continue;
        }
        if (recvsize < sizeof(optype_t)) {
            // print error and dump buf
            printf("[popserver] recv_size < sizeof(optype_t): %d\n", recvsize);
            dump_buf(recvbuf, recvsize);
            // print src ip and port
            continue;
        }
        packet_type_t tmp_optype = get_packet_type(recvbuf, recvsize);
        // printf("[debug]tmp_optype %d is_timeout %d\n",tmp_optype,is_timeout);
        if (tmp_optype == packet_type_t::NETCACHE_CACHE_POP) {
            // receive NETCACHE_CACHE_POP from controller
            netcache_cache_pop_t tmp_netcache_cache_pop(CURMETHOD_ID, recvbuf, recvsize);
            int shard_index = compute_hash_index(tmp_netcache_cache_pop.key());
            auto& beingcached_set = beingcached_shards[shard_index];
            auto& cached_set = cached_shards[shard_index];
            auto& beingupdated_set = beingupdated_shards[shard_index];
            INVARIANT(tmp_netcache_cache_pop.serveridx() == global_server_logical_idx);
#ifdef DEBUG_NETFETCH_POPSERVER
            // Qingxiu: print file_parse_offset for test
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] file_parser_offset: %d\n", __LINE__, file_parser_offset);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] recvsize: %d\n", __LINE__, recvsize);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] CACHE_POP recv_buf: ", __LINE__);
            for (int i = 0; i < recvsize; ++i) {
                printf("%02x ", (unsigned char)(recvbuf[i]));
            }
            printf("\n");
#endif
            uint16_t tmp_keydepth = get_keydepth(recvbuf, recvsize);
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] tmp_keydepth: %d\n", __LINE__, tmp_keydepth);
#endif
            uint16_t path_length;
            // layout: op+keydepth+key+shadowtype+path_length
            uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(optype_t);
            memcpy(&path_length, &(recvbuf[tmp_offset]), 2);
            path_length = ntohs(path_length);
            tmp_offset += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            char queried_path[path_length + 1];
            // layout: op+keydepth+key+shadowtype+path_length+path
            memcpy(queried_path, &(recvbuf[tmp_offset]), path_length);
            queried_path[path_length] = '\0';
            std::string queried_path_for_admission(queried_path);
            // test the hashed key
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] queried_path: %s\n", queried_path);
#endif
            unsigned char digest[MD5_DIGEST_LENGTH];
            MD5((unsigned char*)&queried_path, strlen(queried_path), digest);
            // compare the first 8B
            tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + (tmp_keydepth - 1) * sizeof(netreach_key_t);
            for (int i = 0; i < sizeof(netreach_key_t); ++i) {
                if (digest[i] != (unsigned char)(recvbuf[i + tmp_offset])) {
                    printf("[NETFETCH_POPSERVER_ERROR] MD5 digest error!\n");
                    printf("[NETFETCH_POPSERVER_ERROR] digest[%d] = %02x, recvbuf[%d] = %02x\n", i, digest[i],
                           i + tmp_offset, (unsigned char)(recvbuf[i + tmp_offset]));
                }
            }
            // keep atomicity
            shard_mutexes[shard_index].lock();
            // NETCACHE_CACHE_POP's key must NOT in beingcached/cached/beingupdated keyset
            // INVARIANT((beingcached_set.find(tmp_netcache_cache_pop.key()) == beingcached_set.end()) && \
				(cached_set.find(tmp_netcache_cache_pop.key()) == cached_set.end()) && \
				(beingupdated_set.find(tmp_netcache_cache_pop.key()) == beingupdated_set.end()));
            // add key into beingcached keyset
            beingcached_set.insert(std::make_pair(tmp_netcache_cache_pop.key(), std::string(queried_path)));
            shard_mutexes[shard_index].unlock();
#ifdef DEBUG_NETFETCH_POPSERVER
            std::cout << "[" << __LINE__ << "]" << "admit path" << std::string(queried_path) << " in shard" << shard_index << std::endl;
#endif
            // get latest value from hdfs server
#ifdef ENABLE_HDFS
            val_t tmp_val;  // send to client
            uint32_t tmp_seq = 0;
            bool tmp_stat = true;
            // fetch_metadata_from_hdfs_for_admission(tmp_val, queried_path_for_admission);
            auto levels = extractPathLevels(queried_path_for_admission, file_parser_offset);
            kvs_admission(tmp_val, levels, queried_path_for_admission, local_server_logical_idx);
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] tmp_val.val_length: %d\n", __LINE__, tmp_val.val_length);
#endif
#endif

            // send NETCACHE_CACHE_POP_ACK to controller
            uint32_t pktsize = 0;
            if (tmp_val.val_length <= val_t::SWITCH_MAX_VALLEN) {
                netcache_cache_pop_ack_t tmp_netcache_cache_pop_ack(CURMETHOD_ID, tmp_netcache_cache_pop.key(), tmp_val, tmp_seq, tmp_stat, global_server_logical_idx);
                pktsize = tmp_netcache_cache_pop_ack.serialize(buf, MAX_BUFSIZE);

#ifdef DEBUG_NETFETCH_POPSERVER
                printf("[REAL FETCH] [line %d] shard %d pktsize: %d for %s\n", __LINE__, shard_index, pktsize, queried_path_for_admission.c_str());
                printf("[NETFETCH_POPSERVER_DEBUG] [line %d] pktsize: %d\n", __LINE__, pktsize);
                printf("[NETFETCH_POPSERVER_DEBUG] [line %d] send_buf: ", __LINE__);
                for (int i = 0; i < pktsize; ++i) {
                    printf("%02x ", (unsigned char)(buf[i]));
                }
                printf("\n");
#endif
            } else {                                                                                                                                                            // large value
                netcache_cache_pop_ack_nlatest_t tmp_netcache_cache_pop_ack_nlatest(CURMETHOD_ID, tmp_netcache_cache_pop.key(), tmp_seq, tmp_stat, global_server_logical_idx);  // use default value to invalidate inswitch cache
                pktsize = tmp_netcache_cache_pop_ack_nlatest.serialize(buf, MAX_BUFSIZE);
            }
            udpsendto(server_popserver_udpsock_list[local_server_logical_idx], buf, pktsize, 0, &controller_popserver_popclient_addr, controller_popserver_popclient_addrlen, "server.popserver");
        } else if (tmp_optype == packet_type_t::NETCACHE_PRE_CACHE_POP) {
            netcache_cache_pop_t tmp_netcache_cache_pop(CURMETHOD_ID, recvbuf, recvsize);
            INVARIANT(tmp_netcache_cache_pop.serveridx() == global_server_logical_idx);
#ifdef DEBUG_NETFETCH_POPSERVER

            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] file_parser_offset: %d\n", __LINE__, file_parser_offset);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] recvsize: %d\n", __LINE__, recvsize);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] PRE POP recv_buf: ", __LINE__);
            for (int i = 0; i < recvsize; ++i) {
                printf("%02x ", (unsigned char)(recvbuf[i]));
            }
            printf("\n");
#endif
            uint16_t tmp_keydepth = get_keydepth(recvbuf, recvsize);
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] tmp_keydepth: %d\n", __LINE__, tmp_keydepth);
#endif
            uint16_t path_length;
            // layout: op+keydepth+key+shadowtype+path_length
            uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(optype_t);
            memcpy(&path_length, &(recvbuf[tmp_offset]), 2);
            path_length = ntohs(path_length);
            tmp_offset += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            char queried_path[path_length + 1];
            // layout: op+keydepth+key+shadowtype+path_length+path
            memcpy(queried_path, &(recvbuf[tmp_offset]), path_length);
            queried_path[path_length] = '\0';
            std::string queried_path_for_admission(queried_path);
// test the hashed key
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] queried_path: %s\n", queried_path);
#endif
            unsigned char digest[MD5_DIGEST_LENGTH];
            MD5((unsigned char*)&queried_path, strlen(queried_path), digest);
            // compare the first 8B
            tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + (tmp_keydepth - 1) * sizeof(netreach_key_t);
            for (int i = 0; i < sizeof(netreach_key_t); ++i) {
                if (digest[i] != (unsigned char)(recvbuf[i + tmp_offset])) {
                    printf("[NETFETCH_POPSERVER_ERROR] MD5 digest error!\n");
                    printf("[NETFETCH_POPSERVER_ERROR] digest[%d] = %02x, recvbuf[%d] = %02x\n", i, digest[i],
                           i + tmp_offset, (unsigned char)(recvbuf[i + tmp_offset]));
                }
            }
            // no need to keep atomic just try to fetch

            // get latest value from hdfs server
#ifdef ENABLE_HDFS
            val_t tmp_val;  // send to client
            uint32_t tmp_seq = 0;
            bool tmp_stat = true;
            auto levels = extractPathLevels(queried_path_for_admission, file_parser_offset);
            kvs_admission(tmp_val, levels, queried_path_for_admission, local_server_logical_idx);
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] tmp_val.val_length: %d\n", __LINE__, tmp_val.val_length);
#endif
#endif

            // send NETCACHE_CACHE_POP_ACK to controller
            uint32_t pktsize = 0;

            netcache_cache_pop_ack_t tmp_netcache_cache_pop_ack(CURMETHOD_ID, tmp_netcache_cache_pop.key(), tmp_val, tmp_seq, tmp_stat, global_server_logical_idx);
            pktsize = tmp_netcache_cache_pop_ack.serialize(buf, MAX_BUFSIZE);

#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[PRE FETCH] [line %d] pktsize: %d for %s\n", __LINE__, pktsize, queried_path_for_admission.c_str());
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] send_buf: ", __LINE__);
            for (int i = 0; i < pktsize; ++i) {
                printf("%02x ", (unsigned char)(buf[i]));
            }
            printf("\n");
#endif

            udpsendto(server_popserver_udpsock_list[local_server_logical_idx], buf, pktsize, 0, &controller_popserver_popclient_addr, controller_popserver_popclient_addrlen, "server.popserver");

        } else if (tmp_optype == packet_type_t::NETCACHE_CACHE_POP_FINISH) {
            // receive NETCACHE_CACHE_POP_FINISH from controller
            netcache_cache_pop_finish_t tmp_netcache_cache_pop_finish(CURMETHOD_ID, recvbuf, recvsize);
            // INVARIANT(tmp_netcache_cache_pop_finish.serveridx() == global_server_logical_idx);

            // get token path_length path
            uint16_t path_length = 0;
            token_t token = 0;
            // layout: op + keydepth + key + 4 + token + path_length + path
            uint16_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + 4;
            memcpy(&token, &(recvbuf[tmp_offset]), sizeof(token_t));
            tmp_offset += sizeof(token_t);
            memcpy(&path_length, &(recvbuf[tmp_offset]), sizeof(uint16_t));
            char queried_path[path_length + 1];
            tmp_offset += sizeof(uint16_t);
            memcpy(queried_path, &(recvbuf[tmp_offset]), path_length);
            queried_path[path_length] = '\0';

            // keep atomicity
#ifdef DEBUG_NETFETCH_POPSERVER
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] POP FINISH queried_path: %s\n", __LINE__, queried_path);
#endif
            auto check_key = std::make_pair(tmp_netcache_cache_pop_finish.key(), std::string(queried_path));
            int shard_index = compute_hash_index(tmp_netcache_cache_pop_finish.key());
            auto& beingcached_set = beingcached_shards[shard_index];
            auto& cached_set = cached_shards[shard_index];
            auto& beingupdated_set = beingupdated_shards[shard_index];
            shard_mutexes[shard_index].lock();
            // NETCACHE_CACHE_POP's key must in beingcached or cached keyset
            if (beingcached_set.find(check_key) != beingcached_set.end()) {  // in beingcached keyset
                INVARIANT(cached_set.find(check_key) == cached_set.end());   // must no in cached keyset
                // move key from beingcached keyset into cached keyset
                beingcached_set.erase(check_key);
                cached_set.insert(check_key);
                server_cached_path_token_map_list[std::string(queried_path)] = token;
            } else {
#ifdef DEBUG_NETFETCH_POPSERVER
                printf("[DEBUG] recive cache finish for %s\n", queried_path);
#endif
                server_cached_path_token_map_list[std::string(queried_path)] = token;  // not in beingcached keyset
                // INVARIANT(cached_set.find(check_key) != cached_set.end());  // must in cached keyset
            }
            shard_mutexes[shard_index].unlock();

            // send NETCACHE_CACHE_POP_FINISH_ACK to controller
            netcache_cache_pop_finish_ack_t tmp_netcache_cache_pop_finish_ack(CURMETHOD_ID, tmp_netcache_cache_pop_finish.key(), global_server_logical_idx);
            uint32_t pktsize = tmp_netcache_cache_pop_finish_ack.serialize(buf, MAX_BUFSIZE);
#ifdef DEBUG_NETFETCH_POPSERVER
            // Qingxiu: debug this packet
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] pktsize: %d\n", __LINE__, pktsize);
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] [pop_finish_ack] send_buf: ", __LINE__);
            for (int i = 0; i < pktsize; ++i) {
                printf("%02x ", (unsigned char)(buf[i]));
            }
            printf("\n");
#endif
            udpsendto(server_popserver_udpsock_list[local_server_logical_idx], buf, pktsize, 0, &controller_popserver_popclient_addr, controller_popserver_popclient_addrlen, "server.popserver");
        } else {
            printf("[server.popserver] invalid optype: %x\n", optype_t(tmp_optype));
            exit(-1);
        }
    }

    close(server_popserver_udpsock_list[local_server_logical_idx]);
    pthread_exit(nullptr);
    return 0;
}

// jzcai: thread pool
// Define the task for the thread pool
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
    void handle_task(Task* task, int thread_server_fd, int thread_server_invalid_fd, int thread_idx);
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
            int thread_server_fd, thread_server_invalid_fd;
            prepare_udpserver(thread_server_fd, true, tmp_server_worker_port + 128 + i, thread_name.c_str(), 0, SERVER_SOCKET_TIMEOUT_USECS, UDP_LARGE_RCVBUFSIZE);
            prepare_udpserver(thread_server_invalid_fd, true, tmp_server_worker_port + 127 - i, thread_name.c_str(), 0, SERVER_SOCKET_TIMEOUT_USECS, UDP_LARGE_RCVBUFSIZE);

            while (!stop) {
                Task* task;
                if (task_queue->pop(task)) {
                    // printf("start to handle task\n");
                    // fflush(stdout);
                    if (task == NULL) {
                        printf("pop NULL task:error\n");
                        continue;
                    }
                    // auto start = std::chrono::high_resolution_clock::now();
                    handle_task(task, thread_server_fd, thread_server_invalid_fd, i);

                    delete task;
                    task == NULL;
                }
            }
            close(thread_server_fd);
            close(thread_server_invalid_fd);
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

inline std::vector<token_t> quicksearchTokensforPaths(const std::vector<std::string>& paths, int local_server_logical_idx, bool need_update = false) {
    std::vector<token_t> tokens(paths.size(), 0);
    int len = paths.size();

    if (Patch::patch_mode == 1) {
        for (int i = 0; i < len; ++i) {
            tokens[i] = 1;
        }
        return tokens;
    }
    if (need_update == false) {
        for (int i = 0; i < len; ++i) {
            tokens[i] = 0;
        }
        return tokens;
    }

#ifdef DEBUG_NETFETCH
    printf("[CSFETCH_DEBUG] [line %d] paths.size(): %d\n", __LINE__, paths.size());
#endif

    // Iterate from the end to the beginning of the paths
    for (int i = len - 1; i >= 0; --i) {
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] path: %s\n", __LINE__, paths[i].c_str());
#endif
        auto cached_it = server_cached_path_token_map_list.find(paths[i]);
        if (cached_it != server_cached_path_token_map_list.end()) {
            tokens[i] = cached_it->second;
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, cached_it->second);
#endif
        } else {
            tokens[i] = 0;
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, 0);
#endif
            // Fill remaining tokens with 0 and break
            for (int j = i - 1; j >= 0; --j) {
                tokens[j] = 0;
            }
            break;
        }
    }

    return tokens;
}

inline std::vector<token_t> searchTokensforPaths(const std::vector<std::string>& paths, int local_server_logical_idx) {
    std::vector<token_t> tokens(paths.size(), 0);
    int len = paths.size();

    if (Patch::patch_mode == 1) {
        for (int i = 0; i < len; ++i) {
            tokens[i] = 1;
        }
        return tokens;
    }

#ifdef DEBUG_NETFETCH
    printf("[CSFETCH_DEBUG] [line %d] paths.size(): %d\n", __LINE__, paths.size());
#endif

    // Iterate from the end to the beginning of the paths
    for (int i = len - 1; i >= 0; --i) {
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] path: %s\n", __LINE__, paths[i].c_str());
#endif
        auto cached_it = server_cached_path_token_map_list.find(paths[i]);
        if (cached_it != server_cached_path_token_map_list.end()) {
            tokens[i] = cached_it->second;
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, cached_it->second);
#endif
        } else {
            tokens[i] = 0;
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, 0);
#endif
            // Fill remaining tokens with 0 and break
            for (int j = i - 1; j >= 0; --j) {
                tokens[j] = 0;
            }
            break;
        }
    }

    return tokens;
}
inline std::vector<token_t> slowsearchTokensforPaths(const std::vector<std::string>& paths, int local_server_logical_idx) {
    std::vector<token_t> tokens;
    int len = paths.size();

    if (Patch::patch_mode == 1) {
        for (auto& path : paths) {
            tokens.push_back(1);
        }
        return tokens;
    }

#ifdef DEBUG_NETFETCH
    printf("[CSFETCH_DEBUG] [line %d] paths.size(): %d\n", __LINE__, paths.size());
#endif
    int i = 0;

    for (auto& path : paths) {
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] path: %s\n", __LINE__, path.c_str());
#endif
        i++;
        auto it = server_cached_path_token_map_list.find(path);
        if (it != server_cached_path_token_map_list.end()) {
            tokens.push_back(it->second);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, it->second);
#endif
        } else {
            tokens.push_back(0);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] token: %d\n", __LINE__, 0);
#endif
            break;
        }
    }
    for (; i < len; i++) {
        tokens.push_back(0);
    }
    return tokens;
}
char pktbufs[128][MAX_BUFSIZE];
char ackbufs[128][MAX_BUFSIZE];
char bufs[128][MAX_BUFSIZE];
char buf_getresponses[128][MAX_BUFSIZE];
char recvbufs[128][MAX_BUFSIZE];

void ThreadPool::handle_task(Task* task, int thread_server_fd, int thread_server_invalid_fd, int thread_idx) {
    // refer to buffers
    char* pktbuf = pktbufs[thread_idx];
    char* ackbuf = ackbufs[thread_idx];
    char* buf = bufs[thread_idx];
    char* buf_getresponse = buf_getresponses[thread_idx];
    char* recvbuf = recvbufs[thread_idx];

    // for set valid
    // char pktbuf[MAX_BUFSIZE];
    uint32_t pktsize = 0;
    // char ackbuf[MAX_BUFSIZE];
    int ack_recvsize = 0;
    struct sockaddr_in fake_clientaddr;
    set_sockaddr(fake_clientaddr, inet_addr(client_ips[0]), 123);  // client ip and client port are not important
    socklen_t fake_clientaddrlen = sizeof(struct sockaddr_in);

    // Original task handling logic from the while loop in the provided code

    // memset(buf, 0, MAX_BUFSIZE);
    // memset(buf_getresponse, 0, MAX_BUFSIZE);
    // dynamic_array_t dynamicbuf(MAX_BUFSIZE, MAX_LARGE_BUFSIZE);
    int recv_size = 0;
    int rsp_size = 0;
    if (task == NULL) {
        printf("NULL task:error\n");
        return;
    }
    struct timespec polling_interrupt_for_blocking;
    polling_interrupt_for_blocking.tv_sec = 0;
    polling_interrupt_for_blocking.tv_nsec = 1000;  // 1ms = 1000ns
    if (task->recv_size < sizeof(optype_t)) {
        // print error and dump buf
        printf("[task] recv_size < sizeof(optype_t): %d\n", local_server_logical_idx, global_server_logical_idx, recv_size);
        dump_buf(task->dynamicbuf, task->recv_size);
        // print src ip and port
        // continue;
        return;
    }
    packet_type_t pkt_type = get_packet_type(task->dynamicbuf, task->recv_size);
#ifdef DEBUG_NETFETCH
    printf("[CSFETCH_DEBUG] [line %d] pkt_type: %d\n", __LINE__, pkt_type);
#endif
    // printf("[debug]pkt_type %d is_timeout %d\n",pkt_type,is_timeout);
    switch (pkt_type) {
    case packet_type_t::GETREQ: {
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] start serving GETREQ!\n");
#endif

        get_request_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] recv_buf: ", __LINE__);
        // dump_buf(task->dynamicbuf, task->recv_size);
        for (int i = 0; i < task->recv_size; ++i) {
            printf("%02x ", (unsigned char)task->dynamicbuf[i]);
        }
        printf("\n");
#endif
        netreach_key_t tmp_key = req.key();
        uint16_t tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);
        uint16_t tmp_real_keydepth = get_real_keydepth(task->dynamicbuf, task->recv_size);
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] tmp_keydepth: %d, tmp_real_keydepth: %d\n", __LINE__, tmp_keydepth, tmp_real_keydepth);
#endif
        uint16_t path_length;
        token_t last_token = 0;
        // layout: op + keydepth + keys + path_length + tokens
        uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + tmp_keydepth * sizeof(token_t);
        memcpy(&last_token, &task->dynamicbuf[tmp_offset - sizeof(token_t)], sizeof(token_t));
        memcpy(&path_length, &task->dynamicbuf[tmp_offset], 2);
        path_length = ntohs(path_length);
        tmp_offset += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
        // get the path
        char queried_path[path_length + 1];
        // layout: op + keydepth + keys + path_length + path
        memcpy(queried_path, &task->dynamicbuf[tmp_offset], path_length);
        queried_path[path_length] = '\0';
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] GET queried_path: %s\n", __LINE__, queried_path);
#endif
        auto queried_path_str = std::string(queried_path);
        // test the hashed key
        // unsigned char digest[MD5_DIGEST_LENGTH];
        // MD5((unsigned char*)&queried_path, strlen(queried_path), digest);
        // tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + (tmp_keydepth - 1) * sizeof(netreach_key_t);
        // get the bool value for is_cached
        unsigned char tmp_is_cached = 0;
        // bool is_client_cached = false;
        // memcpy(&tmp_is_cached, &task->dynamicbuf[task->recv_size - 1], sizeof(unsigned char));
        bool is_client_cached = false;
        tmp_offset += path_length;
        memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], sizeof(bool));
        // is_client_cached = tmp_is_cached == 0 ? false : true;

#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] %s client-side is_cached %d: %s\n", __LINE__, queried_path_str.c_str(), is_client_cached, is_client_cached ? "true" : "false");
#endif

        // if cached, get version numbers for each internal path
        std::vector<uint16_t> internal_versions;
        if (is_client_cached) {
            tmp_offset += sizeof(bool);
            for (int i = 0; i < tmp_real_keydepth - 1; ++i) {
                uint16_t tmp_version;
                memcpy(&tmp_version, &task->dynamicbuf[tmp_offset], sizeof(uint16_t));
                internal_versions.push_back(ntohs(tmp_version));
                tmp_offset += sizeof(uint16_t);
            }
        }

        val_t tmp_val;
        uint32_t tmp_seq = 0;
        bool tmp_stat = true;
        auto split_paths = extractPathLevels(queried_path_str, file_parser_offset);

        // auto start = std::chrono::high_resolution_clock::now();
        
        auto get_results = kvs_get_path(queried_path_str, tmp_val, split_paths, is_client_cached, internal_versions, local_server_logical_idx);
        
        // auto end = std::chrono::high_resolution_clock::now();
        // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        // printf("[CSFETCH_DEBUG] [line %d] kvs_get_path took %ld microseconds\n", __LINE__, duration.count());

        bool is_permission_check_success = get_results.first;
        std::vector<uint16_t> latest_versions = get_results.second;

        char tmp_internal_permission[2 * split_paths.size()];

        auto tokens = searchTokensforPaths(split_paths, local_server_logical_idx);

        // auto tokens = quicksearchTokensforPaths(split_paths, local_server_logical_idx, last_token == 0);
#ifdef DEBUG_NETFETCH
        printf("[DEBUG][%d] is_permission_check_success %d\n", __LINE__, is_permission_check_success);
        printf("[DEBUG][%d] latest_versions %d\n", __LINE__, latest_versions.size());
#endif

#ifdef DEBUG_NETFETCH
        // dump tokens
        printf("[CSFETCH_DEBUG] [line %d] tokens: ", __LINE__);
        for (auto& token : tokens) {
            printf("%d ", token);
        }
        printf("\n");
#endif
        if (tmp_val.val_length == FILE_META_SIZE or tmp_val.val_length == DIR_META_SIZE) {
            // get_response_t rsp(CURMETHOD_ID, req.key(), tmp_val, tmp_stat, global_server_logical_idx);
            get_response_t rsp(CURMETHOD_ID, req.key(), tmp_val, tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            // write real keydepth into the response
            buf[sizeof(optype_t)] = tmp_real_keydepth;
            // write keydepth into the response
            buf[sizeof(optype_t) + 1] = tmp_keydepth;
            // copy optype and keydepth intp the response
            memcpy(buf_getresponse, buf, sizeof(optype_t) + sizeof(keydepth_t));
            // reverse the keys and write reversed keys into the response
            std::vector<char> reverse_keys = reverse_key_for_getres(task->dynamicbuf, task->recv_size, tmp_keydepth);
            memcpy(buf_getresponse + sizeof(optype_t) + sizeof(keydepth_t), reverse_keys.data(), reverse_keys.size());

            // copy the rest of the response
            memcpy(buf_getresponse + sizeof(optype_t) + sizeof(keydepth_t) + reverse_keys.size(), buf + sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t), rsp_size - sizeof(optype_t) - sizeof(keydepth_t) - sizeof(netreach_key_t));
            rsp_size = rsp_size - sizeof(netreach_key_t) + reverse_keys.size();

            // add tokens at tail of buf send back to server
            char* tail = buf_getresponse + rsp_size;
            std::memcpy(tail, tokens.data(), tokens.size());
            rsp_size += tokens.size();
            // attach latest permissions for internal paths
            for (int i = 0; i < split_paths.size() - 1; i++) {
                // bigendian
                latest_versions[i] = htons(latest_versions[i]);
                memcpy(&buf_getresponse[rsp_size], &latest_versions[i], sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
            }
            // append the internal_paths size
            uint16_t internal_paths_size = htons(split_paths.size() - 1);
            memcpy(&buf_getresponse[rsp_size], &internal_paths_size, sizeof(uint16_t));
            rsp_size += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH
            printf("[CSFLETCH_DEBUG] [line %d] sent response to client, size: %zu\n", __LINE__, rsp_size);
#endif
#ifdef DEBUG_NETFETCH
            // write it into database, for further warm up
            printf("[CSFETCH_DEBUG] [line %d] print the buf (length: %ld) sent to client: ", __LINE__, rsp_size);
            for (int i = 0; i < rsp_size; ++i) {
                printf("%02x ", (unsigned char)buf_getresponse[i]);
            }
            printf("\n");
#endif

            udpsendto(thread_server_fd, buf_getresponse, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");

        } else if (tmp_val.val_length == 0) {
            get_response_t rsp(CURMETHOD_ID, req.key(), val_t(), tmp_stat, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            // printf("[CSFETCH_DEBUG] [line %d] no value for %s, rsp_size: %d\n", __LINE__, queried_path_str.c_str(), rsp_size);
            udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
            // printf("[CSFETCH_DEBUG] [line %d] DONE\n", __LINE__);
        } else {  // will not reach here
            printf("[CSCACHE_DEBUG] [line %d] length = %ld tmp_val: ", __LINE__, tmp_val.val_length);
            printf("error no large value for fs metadata service\n");
        }
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] after serving the request\n", __LINE__);
#endif
        break;
    }
    case packet_type_t::PUTREQ_SEQ:
    case packet_type_t::DELREQ_SEQ:
    case packet_type_t::PUTREQ_LARGEVALUE_SEQ: {
        // failure flag
        int8_t failure_flag = 0;
        metadata_t tmp_metadata;
        netreach_key_t tmp_key;
        val_t tmp_val;
        val_t ls_val;
        token_t real_token = 0, bring_token = 0;
        uint32_t tmp_seq;
        uint16_t path_length = 0;
        uint16_t path2_length = 0;  // for mv only
        // path for metadata operation
        char queried_path[MAX_PATH_LENGTH];
        std::string queried_path_str;
        std::vector<uint16_t> internal_versions;
        std::vector<std::string> tmp_levels;
        std::vector<uint16_t> latest_versions;
        bool is_client_cached = false;
        bool need_token_update = false;
        char new_path[MAX_PATH_LENGTH];  // for mv only
        if (pkt_type == packet_type_t::PUTREQ_SEQ) {
            // Qingxiu: NetFetch only considers this operation and PUT_CACHED_SEQ for PUT
            // Qingxiu: key-->hash(path), value-->operation, payload-->real path
            uint16_t tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] tmp_keydepth: %d\n", __LINE__, tmp_keydepth);
#endif
            if (tmp_keydepth == 2) {
                uint32_t token_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(uint16_t) + 8 + sizeof(optype_t) + sizeof(uint32_t);
                // modify dynamicbuf to remove the first key
                uint32_t current_offset = sizeof(optype_t) + 1; // get the offset for keydepth
                task->dynamicbuf[current_offset] = 1;
                current_offset += 1; // get the offset of key1
                for (int i=current_offset;i<task->recv_size-sizeof(netreach_key_t)-1;++i){
                    if (i + sizeof(netreach_key_t) < token_offset){
                        task->dynamicbuf[i] = task->dynamicbuf[i+sizeof(netreach_key_t)];
                    } else {
                        task->dynamicbuf[i] = task->dynamicbuf[i+sizeof(netreach_key_t)+1];
                    }
                }
                task->recv_size -= 9; // one key + one token
                tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);  
            }

#ifdef DEBUG_NETFETCH
            for (int i=0;i<task->recv_size;++i){
                printf("%02x", (unsigned char)task->dynamicbuf[i]);
            }
#endif

            put_request_seq_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_val = req.val();
            tmp_seq = req.seq();
            
            // layout: op_type + keydepth + keys + value_length + value + shadowtype + seq + tokens + path_length
            uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(uint16_t) + req.val().val_length + sizeof(optype_t) + sizeof(uint32_t);
            // get token
            bring_token = task->dynamicbuf[tmp_offset + (tmp_keydepth - 1) * sizeof(token_t)];
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] PUTREQ bring_token: %d\n", __LINE__, bring_token);
#endif
            tmp_offset += tmp_keydepth * sizeof(token_t);
            memcpy(&path_length, &task->dynamicbuf[tmp_offset], 2);
            path_length = ntohs(path_length);
            tmp_offset += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            memcpy(queried_path, &task->dynamicbuf[tmp_offset], path_length);
            queried_path[path_length] = '\0';
            queried_path_str = std::string(queried_path);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] PUT queried_path: %s\n", __LINE__, queried_path);
#endif
            // if (task->recv_size > tmp_offset + path_length) {  // get path2 if existing
            //     tmp_offset += path_length;
            //     memcpy(&path2_length, &task->dynamicbuf[tmp_offset], 2);
            //     path2_length = ntohs(path2_length);
            //     tmp_offset += sizeof(uint16_t);
            //     memcpy(new_path, &task->dynamicbuf[tmp_offset], path2_length);
            //     new_path[path2_length] = '\0';
            // }
            tmp_offset += path_length;
            tmp_levels = extractPathLevels(queried_path_str, file_parser_offset);
            bool is_mv = strcmp(tmp_val.val_data, "mv") == 0;
            if (tmp_offset + 1 == task->recv_size) {  // no new path , no versions
                memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                // if cached, get version numbers for each internal path
                is_client_cached = 0;
            } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size && !is_mv) {  // no new path
                memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                // print detail info
                if (is_client_cached != 1) {
                    printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
                    // print the buffer
                    dump_buf(task->dynamicbuf, task->recv_size);
                    // print path
                    printf("[CSCACHE_ERROR] [line %d] queried_path: %s\n", __LINE__, queried_path);
                    // print val
                    printf("[CSCACHE_ERROR] [line %d] val: %s\n", __LINE__, tmp_val.val_data);
                }
                // if cached, get version numbers for each internal path
                INVARIANT(is_client_cached == 1);
                if (is_client_cached) {
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
                    memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                    is_client_cached = 0;
                } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size) {  // has new path , has versions
                    memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);               // if cached, get version numbers for each internal path
                    if (is_client_cached != 1) {
                        printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
                        // print the buffer
                        dump_buf(task->dynamicbuf, task->recv_size);
                        // print path
                        printf("[CSCACHE_ERROR] [line %d] queried_path: %s\n", __LINE__, queried_path);
                        // print val
                        printf("[CSCACHE_ERROR] [line %d] val: %s\n", __LINE__, tmp_val.val_data);
                    }
                    INVARIANT(is_client_cached == 1);
                    if (is_client_cached) {
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

        } else if (pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ) {  // not used in netfetch
            put_request_largevalue_seq_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_val = req.val();
            tmp_seq = req.seq();
        } else if (pkt_type == packet_type_t::DELREQ_SEQ) {
            del_request_seq_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_seq = req.seq();
        } else {
            printf("[server.worker] invalid pkttype: %x which should be PUTREQ_SEQ/DELREQ_SEQ\n", optype_t(pkt_type));
            exit(-1);
        }
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] start check key set\n", __LINE__);
#endif
        bool tmp_stat = false;
        auto check_key = std::make_pair(tmp_key, queried_path_str);
        int shard_index = compute_hash_index(tmp_key);
        auto& beingcached_set = beingcached_shards[shard_index];
        auto& cached_set = cached_shards[shard_index];
        auto& beingupdated_set = beingupdated_shards[shard_index];
        bool is_being_cached = false, is_cached = false, is_being_updated = false;
        bool is_skip_invalid = (strcmp(tmp_val.val_data, "touch") == 0 || strcmp(tmp_val.val_data, "ls") == 0 || strcmp(tmp_val.val_data, "mkdir") == 0 || strcmp(tmp_val.val_data, "finish") == 0);
        bool is_create = (strcmp(tmp_val.val_data, "touch") == 0);
        bool is_mkdir = (strcmp(tmp_val.val_data, "mkdir") == 0);
        if (is_skip_invalid && !is_create) {
        } else if (is_create) {
            shard_mutexes[shard_index].lock();
        } else {
            shard_mutexes[shard_index].lock();

            is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
            is_cached = (cached_set.find(check_key) != cached_set.end());
            // is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
        }
        // auto start = std::chrono::high_resolution_clock::now();
        if (likely(!is_being_cached && !is_cached)) {  // uncached
#ifdef DEBUG_NETFETCH
            // printf("[CSFETCH_DEBUG] [line %d] check key set -> uncached\n", __LINE__);
            printf("[LINE %d] opration: %s\n", __LINE__, tmp_val.val_data);

#endif
            if (pkt_type == packet_type_t::PUTREQ_SEQ || pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ) {
                // perform metadata operation
                // create file
                if (strcmp(tmp_val.val_data, "touch") == 0) {
                    val_t tmp_rm_val;
                    tmp_rm_val.val_length = FILE_META_SIZE;
                    tmp_rm_val.val_data = new char[FILE_META_SIZE];
                    memset(tmp_rm_val.val_data, 0, FILE_META_SIZE);
                    // auto start = std::chrono::high_resolution_clock::now();
                    auto rm_res = kvs_create_path(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_create_path took %ld microseconds\n", __LINE__, duration.count());
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                }
                // list directory
                else if (strcmp(tmp_val.val_data, "ls") == 0) {
                    int numEntries = 10;
                    // auto start = std::chrono::high_resolution_clock::now();
                    auto ls_res = kvs_readdir(queried_path_str, ls_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_readdir took %ld microseconds\n", __LINE__, duration.count());
                    failure_flag = ls_res.first == (size_t)-1 ? 1 : -1;
                    latest_versions = ls_res.second;
                }
                // delete file
                else if (strcmp(tmp_val.val_data, "rm") == 0) {
                    val_t tmp_rm_val;
                    // auto start = std::chrono::high_resolution_clock::now();
                    auto rm_res = kvs_delete_path(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_delete_path took %ld microseconds\n", __LINE__, duration.count());
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
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
                    auto mkdir_res = kvs_mkdir(queried_path_str, tmp_mkdir_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_mkdir took %ld microseconds\n", __LINE__, duration.count());
                    bool is_mkdir_successful = mkdir_res.first;
                    latest_versions = mkdir_res.second;
                    if (is_mkdir_successful) {
                        failure_flag = 0;
                        // notify other servers to invalidate the cache
                        length_with_path_t* tmp_length_with_path = new length_with_path_t();
                        tmp_length_with_path->length = path_length;
                        tmp_length_with_path->path = queried_path_str;
                        pthread_mutex_lock(&notification_mutex);
                        bool res = invalidation_notification_ptr.write(tmp_length_with_path);
                        pthread_mutex_unlock(&notification_mutex);
                        if (!res) {
                            printf("[CSCACHE_ERROR] [line %d] invalidation_notification_ptr.write failed!\n", __LINE__);
                        }
                    } else {
                        failure_flag = 1;
                    }
                }
                // delete directory
                else if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                    val_t tmp_rm_val;
                    // auto start = std::chrono::high_resolution_clock::now();
                    auto rm_res = kvs_rmdir(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_rmdir took %ld microseconds\n", __LINE__, duration.count());
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;

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
                        auto chmod_res = kvs_update_path(queried_path_str, tmp_chmod_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                        // auto end = std::chrono::high_resolution_clock::now();
                        // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                        // printf("[CSFETCH_DEBUG] [line %d] kvs_update_path took %ld microseconds\n", __LINE__, duration.count());
                        bool result = chmod_res.first;
                        latest_versions = chmod_res.second;
                        if (result) {
                            failure_flag = 0;
                        } else {
                            failure_flag = 1;
                        }
                    } else {
                        failure_flag = 1;
                    }
                }
                // mv for file or directory
                else if (strcmp(tmp_val.val_data, "mv") == 0) {
                    auto new_queried_path_str = std::string(new_path);
                    val_t tmp_mv_val;
                    // auto start = std::chrono::high_resolution_clock::now();
                    // FIXME: disable kvs
                    // std::pair<bool, std::vector<uint16_t>> rm_res = {false, {}};
                    auto rm_res = kvs_mv_file(queried_path_str, new_queried_path_str, tmp_mv_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    // auto end = std::chrono::high_resolution_clock::now();
                    // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                    // printf("[CSFETCH_DEBUG] [line %d] kvs_mv_file took %ld microseconds\n", __LINE__, duration.count());
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                }
                // finsh the benchmark
                else if (strcmp(tmp_val.val_data, "finish") == 0) {
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

            } else {  // perform DEL operation, not use in netfetch
            }
        } else if (unlikely(is_being_cached)) {  // being cached
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] check key set -> being cached\n", __LINE__);
#endif
            INVARIANT(!is_cached);
            while (is_being_cached) {
                shard_mutexes[shard_index].unlock();
                // usleep(1); // wait for cache population finish
                nanosleep(&polling_interrupt_for_blocking, NULL);  // wait for cache population finish
                shard_mutexes[shard_index].lock();
                is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
            }
            is_cached = (cached_set.find(check_key) != cached_set.end());
            INVARIANT(is_cached);
        }
        if (unlikely(is_cached)) {  // already cached
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] check key set -> already cached\n", __LINE__);
#endif
            // if (pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ) {
            //     while (is_being_updated) {  // being updated
            //         shard_mutexes[shard_index].unlock();
            //         // usleep(1); // wait for inswitch value update finish
            //         nanosleep(&polling_interrupt_for_blocking, NULL);  // wait for cache population finish
            //         shard_mutexes[shard_index].lock();
            //         is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
            //     }
            //     INVARIANT(!is_being_updated);
            // }

            // perform mkdir/touch/ls to avoid to add them to beingupdatedcache set
            // create file
            if (strcmp(tmp_val.val_data, "touch") == 0) {
                // error info
                printf("[CSCACHE_ERROR] [line %d] touch operation on cached file\n", __LINE__);
            }
            // create directory
            else if (strcmp(tmp_val.val_data, "mkdir") == 0) {
                val_t tmp_mkdir_val;
                tmp_mkdir_val.val_length = DIR_META_SIZE;
                tmp_mkdir_val.val_data = new char[DIR_META_SIZE];
                memset(tmp_mkdir_val.val_data, 0, DIR_META_SIZE);
                auto mkdir_res = kvs_mkdir(queried_path_str, tmp_mkdir_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                bool is_mkdir_successful = mkdir_res.first;
                latest_versions = mkdir_res.second;
                if (is_mkdir_successful) {
                    failure_flag = 0;
                    // notify other servers to invalidate the cache
                    length_with_path_t* tmp_length_with_path = new length_with_path_t();
                    tmp_length_with_path->length = path_length;
                    tmp_length_with_path->path = queried_path_str;
                    pthread_mutex_lock(&notification_mutex);
                    bool res = invalidation_notification_ptr.write(tmp_length_with_path);
                    pthread_mutex_unlock(&notification_mutex);
                    if (!res) {
                        printf("[CSCACHE_ERROR] [line %d] invalidation_notification_ptr.write failed!\n", __LINE__);
                    }  
                } else {
                    failure_flag = 1;
                }
            }
            // list directory
            else if (strcmp(tmp_val.val_data, "ls") == 0) {
                int numEntries = 10;
                auto ls_res = kvs_readdir(queried_path_str, ls_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                failure_flag = ls_res.first == (size_t)-1 ? 1 : -1;
                latest_versions = ls_res.second;
                // failure_flag = fetch_metadata_from_hdfs_for_listdir(tmp_val, queried_path, &numEntries);
            }
#ifdef DEBUG_NETFETCH
            printf("[OP] %s\n", is_skip_invalid ? "skip invalid" : "not skip invalid");
#endif

            if (pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ &&
                is_skip_invalid == false) {  // skip invalid for touch, ls, mkdir, finish
                // if we bring wrong token invalid cache first
                real_token = server_cached_path_token_map_list[queried_path_str];

                if (real_token != bring_token) {
                    need_token_update = true;
#if 1
                    printf("[CSFETCH_DEBUG] [line %d] invalid the cache. client take wrong token\n", __LINE__);
#endif
                    // server_mutex_for_setinvalid[local_server_logical_idx].lock();
                    // invalid the cache
                    // set is_latest = 0
                    // NOTE: client have help us partition into right server
                    while (true) {
                        // 0x0054
                        setvalid_inswitch_t tmp_setvalid_req(CURMETHOD_ID, tmp_key, 0, 0);
                        pktsize = tmp_setvalid_req.serialize(pktbuf, MAX_BUFSIZE);

                        memcpy(pktbuf + pktsize, &real_token, sizeof(token_t));
                        pktsize += sizeof(token_t);
                        udpsendto(thread_server_invalid_fd, pktbuf, pktsize, 0, &fake_clientaddr, fake_clientaddrlen, "switchos.popworker.popclient_for_reflector");

                        bool is_timeout = false;
                        is_timeout = udprecvfrom(thread_server_invalid_fd, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.evictclient");
                        if (unlikely(is_timeout)) {
                            continue;
                        }

                        setvalid_inswitch_ack_t tmp_setvalid_rsp(CURMETHOD_ID, ackbuf, ack_recvsize);
                        if (unlikely(!tmp_setvalid_rsp.is_valid_ || (tmp_setvalid_rsp.key() != tmp_key))) {
#ifdef DEBUG_NETFETCH  // may recv other guys' ack
                            printf("invalid key of SETVALID_INSWITCH_ACK %x which should be %x\n", tmp_setvalid_rsp.key().keyhi, tmp_key.keyhi);
#endif
                            continue;
                        }
                        break;
                    }
                    // server_mutex_for_setinvalid[local_server_logical_idx].unlock();
#ifdef DEBUG_NETFETCH
                    printf("[CSFETCH_DEBUG] [line %d] invalided the cache.\n", __LINE__);
#endif
                }
                // Double-check due to potential cache eviction
                is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
                INVARIANT(!is_being_cached);  // key must NOT in beingcached keyset
                is_cached = (cached_set.find(check_key) != cached_set.end());
                if (is_cached) {                         // key is removed from beingupdated keyset by server.valueupdateserver
                    // TODO: if real_keydepth is 10, we need to double-check if the path is in the cache
                    // beingupdated_set.insert(check_key);  // mark it as being updated
                    INVARIANT(pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ);
                    if (pkt_type == packet_type_t::PUTREQ_SEQ) {
                        if (strncmp(tmp_val.val_data, "chmod", 5) == 0) {
                            // perform chmod
                            int mode = extract_mode_from_chmod_command(tmp_val.val_data);
                            if (mode != -1) {  // Ensure the mode was correctly extracted
                                val_t tmp_chmod_val;
                                tmp_chmod_val.val_length = FILE_META_SIZE;
                                tmp_chmod_val.val_data = new char[FILE_META_SIZE];
                                memset(tmp_chmod_val.val_data, 0, FILE_META_SIZE);
                                auto chmod_res = kvs_update_path(queried_path_str, tmp_chmod_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                                bool result = chmod_res.first;
                                latest_versions = chmod_res.second;
                                if (result) {
                                    failure_flag = 0;
                                } else {
                                    failure_flag = 1;
                                }
                            } else {
                                failure_flag = 1;
                            }
                            if (failure_flag == 0 || failure_flag == 1) {
                                if (failure_flag == 0) {  // success

                                    tmp_val.val_length = FILE_META_SIZE;
                                    tmp_val.val_data = new char[FILE_META_SIZE];
                                    memset(tmp_val.val_data, 0, FILE_META_SIZE);
                                } else {  // failure
                                    tmp_val = val_t();
                                }
                                netcache_valueupdate_t* tmp_netcache_valueupdate_ptr = NULL;  // freed by server.valueupdateserver
                                tmp_netcache_valueupdate_ptr = new netcache_valueupdate_t(CURMETHOD_ID, tmp_key, tmp_val, tmp_seq, true);
                                // write updated metadata into value update message queue
                                std::tuple<netcache_valueupdate_t*, std::string, int8_t>* msg_que_ptr = new std::tuple<netcache_valueupdate_t*, std::string, int8_t>(tmp_netcache_valueupdate_ptr, queried_path_str, failure_flag);
                                bool res = server_netcache_valueupdate_ptr_queue_list[local_server_logical_idx].write(msg_que_ptr);
                                if (!res) {
                                    printf("[server.worker %d-%d] message queue overflow of NETCACHE_VALUEUPDATE\n", local_server_logical_idx, global_server_logical_idx);
                                    fflush(stdout);
                                }
                                // key_op_path_t* tmp_netcache_valuedelete_ptr = NULL;  // freed by server.valuedeleteserver
                                // tmp_netcache_valuedelete_ptr = new key_op_path_t(tmp_key, failure_flag, path_length, queried_path);
                                // std::pair<key_op_path_t*, std::string>* msg_que_ptr = new std::pair<key_op_path_t*, std::string>(tmp_netcache_valuedelete_ptr, queried_path_str);
                                // bool res = server_netcache_valuedelete_ptr_queue_list[local_server_logical_idx].write(msg_que_ptr);
                                // if (!res) {
                                //     printf("[server.worker %d-%d] message queue overflow of NETCACHE_VALUEDELETE\n", local_server_logical_idx, global_server_logical_idx);
                                //     fflush(stdout);
                                // }
                            }
                        } else {
                            // perform hdfs metadata operations
                            // delete file
                            if (strcmp(tmp_val.val_data, "rm") == 0) {
                                val_t tmp_rm_val;
                                auto rm_res = kvs_delete_path(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                                bool result = rm_res.first;
                                latest_versions = rm_res.second;
                                if (result) {
                                    failure_flag = 0;
                                } else {
                                    failure_flag = 1;
                                }
                            }
                            // delete directory
                            if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                                val_t tmp_rm_val;
                                auto rm_res = kvs_rmdir(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                                bool result = rm_res.first;
                                latest_versions = rm_res.second;
                                if (result) {
                                    failure_flag = 0;
                                } else {
                                    failure_flag = 1;
                                }
                            }
                            // mv file/dir
                            if (strcmp(tmp_val.val_data, "mv") == 0) {
                                auto new_queried_path_str = std::string(new_path);
                                val_t tmp_mv_val;
                                auto rm_res = kvs_mv_file(queried_path_str, new_queried_path_str, tmp_mv_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                                bool result = rm_res.first;
                                latest_versions = rm_res.second;
                                if (result) {
                                    failure_flag = 0;
                                } else {
                                    failure_flag = 1;
                                }
                            }
                            if (failure_flag == 0 || failure_flag == 1) {
                                key_op_path_t* tmp_netcache_valuedelete_ptr = NULL;  // freed by server.valuedeleteserver
                                tmp_netcache_valuedelete_ptr = new key_op_path_t(tmp_key, failure_flag, path_length, queried_path);
                                std::pair<key_op_path_t*, std::string>* msg_que_ptr = new std::pair<key_op_path_t*, std::string>(tmp_netcache_valuedelete_ptr, queried_path_str);
                                bool res = server_netcache_valuedelete_ptr_queue_list[local_server_logical_idx].write(msg_que_ptr);
                                if (!res) {
                                    printf("[server.worker %d-%d] message queue overflow of NETCACHE_VALUEDELETE\n", local_server_logical_idx, global_server_logical_idx);
                                    fflush(stdout);
                                }
                            }
                        }
                    } else {
                    }  // DEL, not use in netfetch
                }
            }
        }
        if (is_skip_invalid && !is_create) {
        } else if (is_create) {
            shard_mutexes[shard_index].unlock();
        } else {
            shard_mutexes[shard_index].unlock();
        }
        UNUSED(tmp_stat);
        std::vector<token_t> tokens;
        // if (is_create || is_mkdir) {
        //     // prepare token for the new file/dir all 0
        //     // fill tokens with 0
        //     for (int i = 0; i < tmp_levels.size(); i++) {
        //         tokens.push_back((token_t)0);
        //     }
        // } else {

        tokens = quicksearchTokensforPaths(tmp_levels, local_server_logical_idx, need_token_update);

        // tokens = quicksearchTokensforPaths(tmp_levels, local_server_logical_idx, need_token_update);
        // append token

        // PUT response
        if (pkt_type == packet_type_t::PUTREQ_SEQ || pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ) {
            // op+keydepth+key+shadowtype+stat+nodeidx
            put_response_t rsp(CURMETHOD_ID, tmp_key, true, global_server_logical_idx);
            rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
            // Qingxiu: report failure to client
            if (failure_flag == 1) {
                size_t tmp_offset = sizeof(optype_t);
                uint8_t failure_flag_for_response = 0xFF;
                memcpy(&buf[tmp_offset], &failure_flag_for_response, sizeof(uint8_t));
            }
            if (failure_flag == -1) {  // this is for listdir
#ifdef DEBUG_NETFETCH
                printf("[CSFETCH_DEBUG] [line %d] append value for listdir\n", __LINE__);
#endif
                uint16_t tmp_vallen = htons(ls_val.val_length);
                memcpy(&buf[rsp_size], &tmp_vallen, sizeof(uint16_t));
                rsp_size += sizeof(uint16_t);
                memcpy(&buf[rsp_size], ls_val.val_data, ls_val.val_length);
                rsp_size += ls_val.val_length;
                if ((tmp_levels.size() - 1) != latest_versions.size()) {
                    printf("[CSCACHE_ERROR] [line %d] tmp_levels.size = %d, latest_versions.size = %d\n", __LINE__, tmp_levels.size(), latest_versions.size());
                }
                char* tail = buf + rsp_size;
                std::memcpy(tail, tokens.data(), tokens.size());
                rsp_size += tokens.size();
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
                char* tail = buf + rsp_size;
                std::memcpy(tail, tokens.data(), tokens.size());
                rsp_size += tokens.size();
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
        } else {
        }
        // for tokens
        // auto split_paths = extractPathLevels(queried_path_str, file_parser_offset);
        // std::cout << "[CSCACHE_DEBUG] [line " << __LINE__ << "] tokens: " << rsp_size << "\n";
        // printf("[CSFETCH_DEBUG] [line %d] rsp_size: %zu\n", __LINE__, rsp_size);
        udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
#if 0
        printf("[CSFLETCH_DEBUG] [line %d] sent response to client, size: %zu\n", __LINE__, rsp_size);
#endif
        break;
    }  // end of case packet_type_t::PUTREQ_SEQ

    case packet_type_t::NETCACHE_PUTREQ_SEQ_CACHED:
    case packet_type_t::NETCACHE_DELREQ_SEQ_CACHED:
    case packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED: {
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] NETCACHE_PUTREQ_SEQ_CACHED\n", __LINE__);
#endif
        netreach_key_t tmp_key;
        val_t tmp_val;
        val_t ls_val;
        metadata_t tmp_metadata;
        uint32_t tmp_seq;
        int8_t failure_flag = 0;
        uint16_t path_length = 0;
        uint16_t path2_length = 0;  // for mv only
        char queried_path[MAX_PATH_LENGTH];
        std::string queried_path_str;
        char new_path[MAX_PATH_LENGTH];  // for mv only

        std::vector<uint16_t> internal_versions;
        std::vector<std::string> tmp_levels;
        std::vector<uint16_t> latest_versions;
        bool is_client_cached = false;

        if (pkt_type == packet_type_t::NETCACHE_PUTREQ_SEQ_CACHED) {
            uint16_t tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);
            if (tmp_keydepth == 2) {
                uint32_t token_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(uint16_t) + 8 + sizeof(optype_t) + sizeof(uint32_t);
                // modify dynamicbuf to remove the first key
                uint32_t current_offset = sizeof(optype_t) + 1; // get the offset for keydepth
                task->dynamicbuf[current_offset] = 1;
                current_offset += 1; // get the offset of key1
                for (int i=current_offset;i<task->recv_size-sizeof(netreach_key_t)-1;++i){
                    if (i + sizeof(netreach_key_t) < token_offset){
                        task->dynamicbuf[i] = task->dynamicbuf[i+sizeof(netreach_key_t)];
                    } else {
                        task->dynamicbuf[i] = task->dynamicbuf[i+sizeof(netreach_key_t)+1];
                    }
                }
                task->recv_size -= 9; // one key + one token
                tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);  
            }

            netcache_put_request_seq_cached_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_val = req.val();
            tmp_seq = req.seq();
            
            // layout: op_type + keydepth + keys + value_length + value + shadowtype + seq + tokens + path_length
            tmp_keydepth = get_keydepth(task->dynamicbuf, task->recv_size);
            
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] tmp_keydepth: %d\n", tmp_keydepth);
#endif
            // layout: 2B op_type + 2B keydepth + 16B*keydepth key + 2B value_length + value + 2B shadow_type + 4B sequence number + 2B path_length + path
            uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + sizeof(uint16_t) + req.val().val_length + sizeof(optype_t) + sizeof(uint32_t) + tmp_keydepth * sizeof(token_t);
            memcpy(&path_length, &task->dynamicbuf[tmp_offset], 2);
            path_length = ntohs(path_length);
            tmp_offset += sizeof(uint16_t);
#ifdef DEBUG_NETFETCH
            // dump_buf(task->dynamicbuf, task->recv_size);
            printf("[CSFETCH_DEBUG] [line %d] path_length: %d\n", __LINE__, path_length);
#endif
            // get the path
            memcpy(queried_path, &task->dynamicbuf[tmp_offset], path_length);
            queried_path[path_length] = '\0';

            queried_path_str = std::string(queried_path);
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] queried_path: %s\n", __LINE__, queried_path);
#endif
            tmp_offset += path_length;

            tmp_levels = extractPathLevels(queried_path_str, file_parser_offset);
            bool is_mv = strcmp(tmp_val.val_data, "mv") == 0;
            if (tmp_offset + 1 == task->recv_size) {  // no new path , no versions
                memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                // if cached, get version numbers for each internal path
                is_client_cached = 0;
            } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size && !is_mv) {  // no new path
                memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                // if cached, get version numbers for each internal path
                if (is_client_cached != 1) {
                    printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
                    // print the buffer
                    dump_buf(task->dynamicbuf, task->recv_size);
                    // print path
                    printf("[CSCACHE_ERROR] [line %d] queried_path: %s\n", __LINE__, queried_path);
                    // print val
                    printf("[CSCACHE_ERROR] [line %d] val: %s\n", __LINE__, tmp_val.val_data);
                }
                INVARIANT(is_client_cached == 1);
                if (is_client_cached) {
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
                    memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);
                    is_client_cached = 0;
                } else if (tmp_offset + 1 + (tmp_levels.size() - 1) * 2 == task->recv_size) {  // has new path , has versions
                    memcpy(&is_client_cached, &task->dynamicbuf[tmp_offset], 1);               // if cached, get version numbers for each internal path
                    if (is_client_cached != 1) {
                        printf("[CSCACHE_ERROR] [line %d] error in parsing put request\n", __LINE__);
                        // print the buffer
                        dump_buf(task->dynamicbuf, task->recv_size);
                        // print path
                        printf("[CSCACHE_ERROR] [line %d] queried_path: %s\n", __LINE__, queried_path);
                        // print val
                        printf("[CSCACHE_ERROR] [line %d] val: %s\n", __LINE__, tmp_val.val_data);
                    }
                    INVARIANT(is_client_cached == 1);
                    if (is_client_cached) {
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
            // chmod for file or directory
            if (strncmp(tmp_val.val_data, "chmod", 5) == 0) {  // Check if the operation starts with "chmod"
                int mode = extract_mode_from_chmod_command(tmp_val.val_data);
                if (mode != -1) {  // Ensure the mode was correctly extracted
                    val_t tmp_chmod_val;
                    tmp_chmod_val.val_length = FILE_META_SIZE;
                    tmp_chmod_val.val_data = new char[FILE_META_SIZE];
                    memset(tmp_chmod_val.val_data, 0, FILE_META_SIZE);
                    auto chmod_res = kvs_update_path(queried_path_str, tmp_chmod_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    bool result = chmod_res.first;
                    latest_versions = chmod_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                } else {
                    failure_flag = 1;
                }
            }
        } else if (pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED) {
            put_request_largevalue_seq_cached_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_val = req.val();
            tmp_seq = req.seq();
        } else if (pkt_type == packet_type_t::NETCACHE_DELREQ_SEQ_CACHED) {
            netcache_del_request_seq_cached_t req(CURMETHOD_ID, task->dynamicbuf, task->recv_size);
            tmp_key = req.key();
            tmp_seq = req.seq();
        } else {
            printf("[server.worker] invalid pkttype: %x which should be NETCACHED_PUTREQ_SEQ_CACHED/NETCACHE_DELREQ_SEQ_CACHED\n", optype_t(pkt_type));
            exit(-1);
        }

        bool tmp_stat = false;
        queried_path_str = std::string(queried_path);

        auto check_key = std::make_pair(tmp_key, queried_path_str);
        int shard_index = compute_hash_index(tmp_key);
        auto& beingcached_set = beingcached_shards[shard_index];
        auto& cached_set = cached_shards[shard_index];
        auto& beingupdated_set = beingupdated_shards[shard_index];
        bool is_being_cached = false, is_cached = false, is_being_updated = false;
        bool is_skip_invalid = (strcmp(tmp_val.val_data, "touch") == 0 || strcmp(tmp_val.val_data, "ls") == 0 || strcmp(tmp_val.val_data, "mkdir") == 0 || strcmp(tmp_val.val_data, "finish") == 0);
        bool is_create = (strcmp(tmp_val.val_data, "touch") == 0);
        bool is_mkdir = (strcmp(tmp_val.val_data, "mkdir") == 0);
        if (is_skip_invalid && !is_create) {
        } else if (is_create) {
            shard_mutexes[shard_index].lock();
        } else {
            shard_mutexes[shard_index].lock();

            is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
            is_cached = (cached_set.find(check_key) != cached_set.end());
            is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
        }
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] [line %d] is_being_cached: %d, is_cached: %d, is_being_updated: %d\n", __LINE__, is_being_cached, is_cached, is_being_updated);
#endif
        if (unlikely(!is_being_cached && !is_cached)) {  // uncached
            if (pkt_type == packet_type_t::NETCACHE_PUTREQ_SEQ_CACHED || pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED) {
                // perform hdfs metadata operations
                // delete file
                if (strcmp(tmp_val.val_data, "rm") == 0) {
                    val_t tmp_rm_val;
                    auto rm_res = kvs_delete_path(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                }
                // delete directory
                if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                    val_t tmp_rm_val;
                    auto rm_res = kvs_rmdir(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                }
                // mv file/dir
                if (strcmp(tmp_val.val_data, "mv") == 0) {
                    auto new_queried_path_str = std::string(new_path);
                    val_t tmp_mv_val;
                    auto rm_res = kvs_mv_file(queried_path_str, new_queried_path_str, tmp_mv_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                    bool result = rm_res.first;
                    latest_versions = rm_res.second;
                    if (result) {
                        failure_flag = 0;
                    } else {
                        failure_flag = 1;
                    }
                }
            } else {
            }  // DEL, never use in netfetch
        } else if (unlikely(is_being_cached)) {  // being cached
            INVARIANT(!is_cached);
            // NOTE: NETCACHE_XXXREQ_SEQ_CACHED does not need to wait for cache population finish; instead, it directly moves key from beingcached keyset into cached keyset
            beingcached_set.erase(check_key);
            cached_set.insert(check_key);

            is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
            INVARIANT(!is_being_cached);
            is_cached = (cached_set.find(check_key) != cached_set.end());
            INVARIANT(is_cached);
        }

        if (likely(is_cached)) {  // already cached
            // if (pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED) {
            //     while (is_being_updated) {  // being updated
            //         shard_mutexes[shard_index].unlock();
            //         // usleep(1); // wait for inswitch value update finish
            //         nanosleep(&polling_interrupt_for_blocking, NULL);  // wait for cache population finish
            //         shard_mutexes[shard_index].lock();
            //         is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
            //     }
            //     INVARIANT(!is_being_updated);
            // }
            if (pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED) {
                // Double-check due to potential cache eviction
                is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
                INVARIANT(!is_being_cached);  // key must NOT in beingcached keyset
                is_cached = (cached_set.find(check_key) != cached_set.end());
                if (is_cached) {                         // key is removed from beingupdated keyset by server.valueupdateserver
                    // beingupdated_set.insert(check_key);  // mark it as being updated
                    INVARIANT(pkt_type != packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED);
                    if (pkt_type == packet_type_t::NETCACHE_PUTREQ_SEQ_CACHED) {
                        // perform hdfs metadata operations
                        // delete file
                        if (strcmp(tmp_val.val_data, "rm") == 0) {
                            val_t tmp_rm_val;
                            auto rm_res = kvs_delete_path(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                            bool result = rm_res.first;
                            latest_versions = rm_res.second;
                            if (result) {
                                failure_flag = 0;
                            } else {
                                failure_flag = 1;
                            }
                        }
                        // delete directory
                        if (strcmp(tmp_val.val_data, "rmdir") == 0) {
                            val_t tmp_rm_val;
                            auto rm_res = kvs_rmdir(queried_path_str, tmp_rm_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                            bool result = rm_res.first;
                            latest_versions = rm_res.second;
                            if (result) {
                                failure_flag = 0;
                            } else {
                                failure_flag = 1;
                            }
                        }
                        // mv file/dir
                        if (strcmp(tmp_val.val_data, "mv") == 0) {
                            auto new_queried_path_str = std::string(new_path);
                            val_t tmp_mv_val;
                            auto rm_res = kvs_mv_file(queried_path_str, new_queried_path_str, tmp_mv_val, tmp_levels, is_client_cached, internal_versions, local_server_logical_idx);
                            bool result = rm_res.first;
                            latest_versions = rm_res.second;
                            if (result) {
                                failure_flag = 0;
                            } else {
                                failure_flag = 1;
                            }
                        }

                        if (strncmp(tmp_val.val_data, "chmod", 5) == 0) {  // chmod --> update cached
                            if (failure_flag == 0 || failure_flag == 1) {  // whether success or failure, we need to remove the key from beingupdated keyset
                                if (failure_flag == 0) {                   // success
                                                                           // kvs_admission(tmp_val, tmp_levels, queried_path_str, local_server_logical_idx);

                                    tmp_val.val_length = FILE_META_SIZE;
                                    tmp_val.val_data = new char[FILE_META_SIZE];
                                    memset(tmp_val.val_data, 0, FILE_META_SIZE);
                                } else {  // failure
                                    tmp_val = val_t();
                                }
                                netcache_valueupdate_t* tmp_netcache_valueupdate_ptr = NULL;  // freed by server.valueupdateserver
                                tmp_netcache_valueupdate_ptr = new netcache_valueupdate_t(CURMETHOD_ID, tmp_key, tmp_val, tmp_seq, true);
                                std::tuple<netcache_valueupdate_t*, std::string, int8_t>* msg_que_ptr = new std::tuple<netcache_valueupdate_t*, std::string, int8_t>(tmp_netcache_valueupdate_ptr, queried_path_str, 1);
                                bool res = server_netcache_valueupdate_ptr_queue_list[local_server_logical_idx].write(msg_que_ptr);
                                if (!res) {
                                    printf("[server.worker %d-%d] message queue overflow of NETCACHE_VALUEUPDATE\n", local_server_logical_idx, global_server_logical_idx);
                                    fflush(stdout);
                                }
                            }
                        } else {  // rm/rmdir/rename --> delete cached
                            if (failure_flag == 0 || failure_flag == 1) {
                                key_op_path_t* tmp_netcache_valuedelete_ptr = NULL;  // freed by server.valuedeleteserver
                                tmp_netcache_valuedelete_ptr = new key_op_path_t(tmp_key, failure_flag, path_length, queried_path);
                                std::pair<key_op_path_t*, std::string>* msg_que_ptr = new std::pair<key_op_path_t*, std::string>(tmp_netcache_valuedelete_ptr, queried_path_str);
                                bool res = server_netcache_valuedelete_ptr_queue_list[local_server_logical_idx].write(msg_que_ptr);
                                if (!res) {
                                    printf("[server.worker %d-%d] message queue overflow of NETCACHE_VALUEDELETE\n", local_server_logical_idx, global_server_logical_idx);
                                    fflush(stdout);
                                }
                            }
                        }
                    } else {
                    }  // DEL, never use in netfetch
                }
                // else: do nothing as key is removed from beingupdated keyset by server.evictserver
            }

            if (pkt_type == packet_type_t::NETCACHE_PUTREQ_SEQ_CACHED || pkt_type == packet_type_t::PUTREQ_LARGEVALUE_SEQ_CACHED) {
                // Qingxiu: no need to perform touch/mkdir/ls here, because these three operation's optype will not be NETCACHE_PUTREQ_SEQ_CACHED
            } else {
            }  // perform DEL operation, never use in netfetch
        }
        if (is_skip_invalid && !is_create) {
        } else if (is_create) {
            shard_mutexes[shard_index].unlock();
        } else {
            shard_mutexes[shard_index].unlock();
        }
        UNUSED(tmp_stat);
        std::vector<token_t> tokens;
        // if (is_create || is_mkdir) {
        //     // prepare token for the new file/dir all 0
        //     // fill tokens with 0
        //     for (int i = 0; i < tmp_levels.size(); i++) {
        //         tokens.push_back((token_t)0);
        //     }
        // } else {
        tokens = searchTokensforPaths(tmp_levels, local_server_logical_idx);
        // }
        // tokens = quicksearchTokensforPaths(tmp_levels, local_server_logical_idx, need_token_update);
        put_response_t rsp(CURMETHOD_ID, tmp_key, true, global_server_logical_idx);
        rsp_size = rsp.serialize(buf, MAX_BUFSIZE);
        // Qingxiu: report failure to client
        if (failure_flag == 1) {
            size_t tmp_offset = sizeof(optype_t);
            uint8_t failure_flag_for_response = 0xFF;
            memcpy(&buf[tmp_offset], &failure_flag_for_response, sizeof(uint8_t));
        }
        if (failure_flag == -1) {  // this is for listdir
#ifdef DEBUG_NETFETCH
            printf("[CSFETCH_DEBUG] [line %d] append value for listdir\n", __LINE__);
#endif
            uint16_t tmp_vallen = htons(ls_val.val_length);
            memcpy(&buf[rsp_size], &tmp_vallen, sizeof(uint16_t));
            rsp_size += sizeof(uint16_t);
            memcpy(&buf[rsp_size], ls_val.val_data, ls_val.val_length);
            rsp_size += ls_val.val_length;
            if ((tmp_levels.size() - 1) != latest_versions.size()) {
                printf("[CSCACHE_ERROR] [line %d] tmp_levels.size = %d, latest_versions.size = %d\n", __LINE__, tmp_levels.size(), latest_versions.size());
            }
            char* tail = buf + rsp_size;
            std::memcpy(tail, tokens.data(), tokens.size());
            rsp_size += tokens.size();
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
            char* tail = buf + rsp_size;
            std::memcpy(tail, tokens.data(), tokens.size());
            rsp_size += tokens.size();
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
        // printf("[CSFETCH_DEBUG] [line %d] rsp_size: %zu\n", __LINE__, rsp_size);
        udpsendto(thread_server_fd, buf, rsp_size, 0, &task->client_addr, task->client_addrlen, "server.worker");
#if 0
        // printf("[CSFLETCH_DEBUG] [line %d] sent response to client, size: %zu\n", __LINE__, rsp_size);
#endif
        break;
    }
    case packet_type_t::SETVALID_INSWITCH_ACK: {
        // do nothing for obsolete pkt
        // filter this type
    }
    default: {
        COUT_THIS("[server.worker] Invalid packet type: " << int(pkt_type))
        dump_buf(task->dynamicbuf, task->recv_size);
        std::cout << std::flush;
        // exit(-1);
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
    // connect_with_hdfs_namenodes();
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
    struct sockaddr_in controller_popserver_addr;

    set_sockaddr(controller_popserver_addr, inet_addr(controller_ip_for_server), controller_popserver_port_start + global_server_logical_idx);
    socklen_t controller_popserver_addrlen = sizeof(struct sockaddr_in);

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
    printf("[server.worker %d-%d] ready\n", local_server_logical_idx, global_server_logical_idx);
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

    bool is_first_pkt = true;
    bool is_timeout = false;
    while (transaction_running) {
        dynamicbuf.clear();
        is_timeout = udprecvlarge_ipfrag(CURMETHOD_ID, server_worker_udpsock_list[local_server_logical_idx], dynamicbuf, 0, &client_addr, &client_addrlen, "server.worker", &cur_worker_pkt_ring_buffer);
        recv_size = dynamicbuf.size();

        if (is_timeout) {
            continue;  // continue to check transaction_running
        }
        if (recv_size < sizeof(optype_t)) {
            // print error and dump buf
            printf("[server.worker %d-%d] recv_size < sizeof(optype_t): %d\n", local_server_logical_idx, global_server_logical_idx, recv_size);
            dump_buf(dynamicbuf.array(), recv_size);
            // print src ip and port
            continue;
        }
        packet_type_t pkt_type = get_packet_type(dynamicbuf.array(), recv_size);
#ifdef DEBUG_NETFETCH
        printf("[CSFETCH_DEBUG] pkt_type: %d\n", pkt_type);
#endif
        Task* task = new Task(dynamicbuf.array(), recv_size, client_addr, client_addrlen);
        thread_pool.add_task(task);
    }

    close(server_worker_udpsock_list[local_server_logical_idx]);
    printf("[server.worker %d-%d] exits", local_server_logical_idx, global_server_logical_idx);

    pthread_exit(nullptr);
}

void* run_server_evictserver(void* param) {
    uint16_t local_server_logical_idx = *((uint16_t*)param);
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    struct sockaddr_in controller_evictclient_addr;
    unsigned int controller_evictclient_addrlen = sizeof(struct sockaddr_in);
    // bool with_controller_evictclient_addr = false;

    printf("[server.evictserver %d-%d] ready\n", local_server_logical_idx, global_server_logical_idx);
    fflush(stdout);
    transaction_ready_threads++;

    while (!transaction_running) {
    }

    // process NETCACHE_CACHE_EVICT packet <optype, key, serveridx>
    char recvbuf[MAX_BUFSIZE];
    int recvsize = 0;
    bool is_timeout = false;
    char sendbuf[MAX_BUFSIZE];  // used to send CACHE_EVICT_ACK to controller
    while (transaction_running) {
        is_timeout = udprecvfrom(server_evictserver_udpsock_list[local_server_logical_idx], recvbuf, MAX_BUFSIZE, 0, &controller_evictclient_addr, &controller_evictclient_addrlen, recvsize, "server.evictserver");
        if (is_timeout) {
            memset(&controller_evictclient_addr, 0, sizeof(struct sockaddr_in));
            controller_evictclient_addrlen = sizeof(struct sockaddr_in);
            continue;  // continue to check transaction_running
        }

        netcache_cache_evict_t tmp_netcache_cache_evict(CURMETHOD_ID, recvbuf, recvsize);

        // layout: op + keydepth + key + 2
        uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + 2;
        uint16_t path_length = 0;
        token_t token = 0;
        memcpy(&token, &(recvbuf[tmp_offset]), sizeof(token_t));
        tmp_offset += sizeof(token_t);
        memcpy(&path_length, &(recvbuf[tmp_offset]), sizeof(uint16_t));
        char queried_path[path_length + 1];
        tmp_offset += sizeof(uint16_t);
        memcpy(queried_path, &(recvbuf[tmp_offset]), path_length);
        queried_path[path_length] = '\0';

        uint16_t tmp_serveridx = tmp_netcache_cache_evict.serveridx();
        INVARIANT(tmp_serveridx == global_server_logical_idx);
        auto check_key = std::make_pair(tmp_netcache_cache_evict.key(), std::string(queried_path));
        int shard_index = compute_hash_index(tmp_netcache_cache_evict.key());
        auto& beingcached_set = beingcached_shards[shard_index];
        auto& cached_set = cached_shards[shard_index];
        auto& beingupdated_set = beingupdated_shards[shard_index];
        // keep atomicity
        shard_mutexes[shard_index].lock();
        // NETCACHE_CACHE_EVICT's key must in beingcached keyset or cached keyset
        // INVARIANT((beingcached_set.find(tmp_netcache_cache_evict.key()) != beingcached_set.end()) || \
		//		(cached_set.find(tmp_netcache_cache_evict.key()) != cached_set.end()));
        if (!((beingcached_set.find(check_key) != beingcached_set.end()) ||
              (cached_set.find(check_key) != cached_set.end()))) {
            // bool is_found = false;
            // // iterate all set to find the path, not the key
            // for (auto it = beingcached_set.begin(); it != beingcached_set.end(); ++it) {
            //     if (it->second == std::string(queried_path)) {
            //         is_found = true;
            //         break;
            //     }
            // }
            // for (auto it = cached_set.begin(); it != cached_set.end(); ++it) {
            //     if (it->second == std::string(queried_path)) {
            //         is_found = true;
            //         break;
            //     }
            // }
            // if (is_found == true) {
            //     std::cout << "key not match" << std::endl;
            // }
            printf("[server.evictserver %d-%d ERROR] shard %d evicted key %x is not in beingcached keyset (size: %d) or cached keyset (size: %d)\n", local_server_logical_idx, global_server_logical_idx, shard_index, tmp_netcache_cache_evict.key().keyhi, beingcached_set.size(), cached_set.size());
            std::cout << "evict path" << check_key.second << "failed" << std::endl;
            // exit(-1);
        }
#if 0
        std::cout << "[" << __LINE__ << "]" << "evict path" << check_key.second << " in shard" << shard_index << std::endl;
#endif
        // remove key from beingcached/cached/beingupdated keyset
        beingcached_set.erase(check_key);
        cached_set.erase(check_key);
        // server_cached_path_token_map_list.erase(std::string(queried_path));
        beingupdated_set.erase(check_key);
        shard_mutexes[shard_index].unlock();

        // send NETCACHE_CACHE_EVICT_ACK to controller.evictserver.evictclient
        netcache_cache_evict_ack_t tmp_netcache_cache_evict_ack(CURMETHOD_ID, tmp_netcache_cache_evict.key(), global_server_logical_idx);
        int sendsize = tmp_netcache_cache_evict_ack.serialize(sendbuf, MAX_BUFSIZE);
        // printf("send NETCACHE_CACHE_EVICT_ACK to controller\n");
        // dump_buf(sendbuf, sendsize);
        udpsendto(server_evictserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &controller_evictclient_addr, controller_evictclient_addrlen, "server.evictserver");
    }

    close(server_evictserver_udpsock_list[local_server_logical_idx]);
    pthread_exit(nullptr);
}

void* run_server_valueupdateserver(void* param) {
    uint16_t local_server_logical_idx = *((uint16_t*)param);
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    // client address (switch will not hide NETCACHE_VALUEUPDATE from clients)
    struct sockaddr_in client_addr;
    set_sockaddr(client_addr, inet_addr(client_ips[0]), 123);  // client ip and client port are not important
    socklen_t client_addrlen = sizeof(struct sockaddr_in);

    printf("[server.valueupdateserver %d-%d] ready\n", local_server_logical_idx, global_server_logical_idx);
    fflush(stdout);
    transaction_ready_threads++;

    while (!transaction_running) {
    }

    char sendbuf[MAX_BUFSIZE];
    char ackbuf[MAX_BUFSIZE];
    int sendsize = 0;
    int ack_recvsize = 0;
    while (transaction_running) {
        auto it = server_netcache_valueupdate_ptr_queue_list[local_server_logical_idx].read();

        if (it != NULL) {
            netcache_valueupdate_t* tmp_netcache_valueupdate_ptr = std::get<0>(*it);
            std::string path = std::get<1>(*it);
            int8_t failure_flag = std::get<2>(*it);
            auto check_key = std::make_pair(tmp_netcache_valueupdate_ptr->key(), path);
            token_t token = server_cached_path_token_map_list[path];
            int shard_index = compute_hash_index(tmp_netcache_valueupdate_ptr->key());
            auto& beingcached_set = beingcached_shards[shard_index];
            auto& cached_set = cached_shards[shard_index];
            auto& beingupdated_set = beingupdated_shards[shard_index];
            if (failure_flag == 0) {  // update cache value
                sendsize = tmp_netcache_valueupdate_ptr->serialize(sendbuf, MAX_BUFSIZE);
#ifdef DEBUG_NETFETCH
                // Qingxiu: print sendbuf for test
                printf("[CSFETCH_DEBUG] [line %d] sendbuf: ", __LINE__);
                dump_buf(sendbuf, sendsize);
#endif
                memcpy(sendbuf + sendsize, &token, sizeof(token_t));
                sendsize += sizeof(token_t);

                udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "server.valueupdateserver");
                // udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "server.valueupdateserver");
                // udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "server.valueupdateserver");

            } else {  // only set valid for cache
                // set valid for rm/rmdir/mv cached path, no matter success or failure
                setvalid_inswitch_t tmp_setvalid_req(CURMETHOD_ID, tmp_netcache_valueupdate_ptr->key(), 0, 0);
                sendsize = tmp_setvalid_req.serialize(sendbuf, MAX_BUFSIZE);
                // use 0x0094 for valid
                sendbuf[0] = 0x00;
                sendbuf[1] = 0x94;
                memcpy(sendbuf + sendsize, &token, sizeof(token_t));
                sendsize += sizeof(token_t);
#ifdef DEBUG_NETFETCH
                printf("[NETFETCH_POPSERVER_DEBUG] [line %d] set valid sendsize %d send_buf: ", __LINE__, sendsize);
                for (int i = 0; i < sendsize; ++i) {
                    printf("%02x ", (unsigned char)(sendbuf[i]));
                }
                printf("\n");
#endif
                udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");
                // udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");
                // udpsendto(server_valueupdateserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");
            }
            delete tmp_netcache_valueupdate_ptr;
            tmp_netcache_valueupdate_ptr = NULL;
            delete it;
            it = NULL;
            // NOTE: valueupdate overhead is too large under write-intensive workload, which is caused by NetCache/DistCache design (write-through policy + server-issued value update) -> we skip timeout-and-retry mechanism here
            // remove key from beingupdated keyset atomically
            // shard_mutexes[shard_index].lock();
            // bool is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
            // if (likely(is_being_updated)) {
            //     beingupdated_set.erase(check_key);
            // } else {  // due to cache eviciton
            //     bool is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
            //     bool is_cached = (cached_set.find(check_key) != cached_set.end());
            //     // INVARIANT(!is_being_cached && !is_cached);  // key must NOT in beingcached/cached keyset
            // }
            // shard_mutexes[shard_index].unlock();
        }
    }

    close(server_valueupdateserver_udpsock_list[local_server_logical_idx]);
    pthread_exit(nullptr);
}

void* run_server_valuedeleteserver(void* param) {
    uint16_t local_server_logical_idx = *((uint16_t*)param);
    uint16_t global_server_logical_idx = server_logical_idxes_list[server_physical_idx][local_server_logical_idx];

    // client address (switch will not hide NETCACHE_VALUEUPDATE from clients)
    struct sockaddr_in client_addr;
    set_sockaddr(client_addr, inet_addr(client_ips[0]), 123);  // client ip and client port are not important
    socklen_t client_addrlen = sizeof(struct sockaddr_in);

    printf("[server.valuedeleteserver %d-%d] ready\n", local_server_logical_idx, global_server_logical_idx);
    fflush(stdout);
    transaction_ready_threads++;

    while (!transaction_running) {
    }

    char sendbuf[MAX_BUFSIZE];
    char ackbuf[MAX_BUFSIZE];
    int sendsize = 0;
    int ack_recvsize = 0;

    while (transaction_running) {
        pthread_mutex_lock(&delete_server_mutex);
        auto it = server_netcache_valuedelete_ptr_queue_list[0].read();
        pthread_mutex_unlock(&delete_server_mutex);
        // printf("server.valuedeleteserver %d get request \n", local_server_logical_idx);
        if (it != NULL) {
            key_op_path_t* tmp_netcache_valuedelete_ptr = it->first;
            std::string path = it->second;
            auto check_key = std::make_pair(tmp_netcache_valuedelete_ptr->key, path);

            int shard_index = compute_hash_index(tmp_netcache_valuedelete_ptr->key);
            auto& beingcached_set = beingcached_shards[shard_index];
            auto& cached_set = cached_shards[shard_index];
            auto& beingupdated_set = beingupdated_shards[shard_index];

            token_t token = server_cached_path_token_map_list[path];

            int8_t failure_flag = tmp_netcache_valuedelete_ptr->failure_flag;

            // set valid for rm/rmdir/mv cached path, no matter success or failure
            setvalid_inswitch_t tmp_setvalid_req(CURMETHOD_ID, tmp_netcache_valuedelete_ptr->key, 0, 0);
            sendsize = tmp_setvalid_req.serialize(sendbuf, MAX_BUFSIZE);
            // use 0x0094 for valid
            sendbuf[0] = 0x00;
            sendbuf[1] = 0x94;
            memcpy(sendbuf + sendsize, &token, sizeof(token_t));
            sendsize += sizeof(token_t);
#ifdef DEBUG_NETFETCH
            printf("[NETFETCH_POPSERVER_DEBUG] [line %d] set valid sendsize %d send_buf: ", __LINE__, sendsize);
            for (int i = 0; i < sendsize; ++i) {
                printf("%02x ", (unsigned char)(sendbuf[i]));
            }
            printf("\n");
#endif
            udpsendto(server_valuedeleteserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");
            // udpsendto(server_valuedeleteserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");
            // udpsendto(server_valuedeleteserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "switchos.popworker.popclient_for_reflector");

#ifdef DEBUG_NETFETCH
            // print op and path for test
            printf("[CSFETCH_DEBUG] [line %d] failure_flag: %d, path1: %s\n", __LINE__, tmp_netcache_valuedelete_ptr->failure_flag, tmp_netcache_valuedelete_ptr->path1);
#endif
            // Qingxiu: comment the following code since we do not need to evict the key from switch
            // // send eviction request to switchos
            // sendsize = netfetch_delete_serialize_to_buffer(tmp_netcache_valuedelete_ptr, sendbuf);
            // udpsendto(server_valuedeleteserver_udpsock_list[local_server_logical_idx], sendbuf, sendsize, 0, &client_addr, client_addrlen, "server.valuedeleteserver");
            delete tmp_netcache_valuedelete_ptr;
            tmp_netcache_valuedelete_ptr = NULL;
            delete it;
            it = NULL;
            // remove key from beingupdated keyset atomically
//             shard_mutexes[shard_index].lock();
//             bool is_being_updated = (beingupdated_set.find(check_key) != beingupdated_set.end());
// #ifdef DEBUG_NETFETCH
//             printf("[CSFETCH_DEBUG] [line %d] is_being_updated: %d\n", __LINE__, is_being_updated);
// #endif
//             if (likely(is_being_updated)) {
//                 beingupdated_set.erase(check_key);
//             } else {  // due to cache eviciton
//                 bool is_being_cached = (beingcached_set.find(check_key) != beingcached_set.end());
//                 bool is_cached = (cached_set.find(check_key) != cached_set.end());
//                 // INVARIANT(!is_being_cached && !is_cached);  // key must NOT in beingcached/cached keyset
//             }
//             shard_mutexes[shard_index].unlock();
        }
    }
    close(server_valuedeleteserver_udpsock_list[local_server_logical_idx]);
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
#ifdef CSFETCH_DEBUG
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
#ifdef CSFETCH_DEBUG
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

            // fetch_metadata_from_hdfs_for_get(tmp_val, path.c_str());
            val_t tmp_val;
            tmp_val.val_length = DIR_META_SIZE;
            tmp_val.val_data = new char[DIR_META_SIZE];
            memset(tmp_val.val_data, 0, DIR_META_SIZE);
            // update rocksdb
            db_wrappers[local_server_logical_idx].put_path(path, tmp_val);
#ifdef CSFETCH_DEBUG
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
    // //debug server_logical_idxes_list info
    // for (int i = 0; i < server_logical_idxes_list.size(); i++) {
    //     for (int j = 0; j < server_logical_idxes_list[i].size(); j++) {
    //         printf("server_logical_idxes_list[%d][%d]: %d\n", i, j, server_logical_idxes_list[i][j]);
    //     }
    // }
    // // debug server_physical_idx local_server_logical_idx
    // printf("server_physical_idx: %d, local_server_logical_idx: %d\n", server_physical_idx, thread_param.local_server_logical_idx);
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
#ifdef CSFETCH_DEBUG
            printf("[CSCACHE_DEBUG] [line %d] read invalidation_notification_ptr\n", __LINE__);
#endif
            std::string path = tmp_length_with_path_ptr->path;
            uint16_t path_length = tmp_length_with_path_ptr->length;
            // delete useless pointer
            delete tmp_length_with_path_ptr;
            tmp_length_with_path_ptr = NULL;
#ifdef CSFETCH_DEBUG
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
#ifdef CSFETCH_DEBUG
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