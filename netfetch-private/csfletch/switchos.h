#ifndef SWITCH_H
#define SWITCH_H

#include <arpa/inet.h>  // inetaddr conversion
#include <getopt.h>
#include <signal.h>  // for signal and raise
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>  // struct timeval
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <bitset>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../common/dynamic_array.h"
#include "../common/helper.h"
#include "../common/iniparser/iniparser_wrapper.h"
#include "../common/io_helper.h"
#include "../common/key.h"
#include "../common/packet_format_impl.h"
#include "../common/socket_helper.h"
#include "../common/val.h"
#include "common_impl.h"
#include "concurrent_map_impl.h"
#include "hashring.h"
#include "message_queue_impl.h"
#include "netfetch_helper.h"

// Qingxiu: for file system tree
#include <openssl/md5.h>

#define KV_BUCKET_SIZE 10000  // debug eviction: 10000 -> 2
#define MAX_DEPTH 10
#define MAX_CACHE_PER_REG 5
#define FILE_SLOTS 5
#define DIR_SLOTS 3
#define REGS_PER_ROW 16
#define REGS_FOR_PERMISSION 5         // the first five stages are reserved for permission
#define FREQUENCY_COUNTER_SIZE 49152  // 32768 + 16384

typedef uint8_t token_t;

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
    std::string filename = "/home/cyh/jzcai/In-Switch-FS-Metadata/netfetch-private/mdtestcpp/config.ini";
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

/*
    1. some important data structure definition
*/

class TreeNode {
public:
    std::string name;
    int weight;
    TreeNode* parent;
    std::unordered_map<std::string, TreeNode*> children;
    double scaled_frequency;

    TreeNode(const std::string& name, int weight, TreeNode* parent = nullptr, double scaled_frequency = 0.0)
        : name(name), weight(weight), parent(parent), scaled_frequency(scaled_frequency) {
#ifdef NETFETCH_POPSERVER_DEBUG
        std::cout << "[FSTREE] TreeNode created: " << name << " with weight " << weight << std::endl;
#endif
    }

    void add_child(TreeNode* child) {
        auto it = children.find(child->name);
        // children.push_back(child);
        if (it == children.end()) {
            children[child->name] = child;
            child->parent = this;
        } else {
            delete child;
        }
    }

    bool matches_path_prefix(const std::string& path) {
        return path.find(name) == 0;
    }

    TreeNode* find_parent(const std::string path) {
        TreeNode* current = this;  // root
        std::string current_path;
        std::istringstream iss(path);
        std::string part;
        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                bool found = false;
                auto it = current->children.find(current_path);
                if (it != current->children.end()) {
                    current = it->second;
                    found = true;
                }
                if (!found) {
                    return nullptr;
                }
            }
        }
        return current->parent;
    }

    TreeNode* find_node(const std::string path) {
        TreeNode* current = this;  // root
        std::string current_path;
        std::istringstream iss(path);
        std::string part;
        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                bool found = false;
                auto it = current->children.find(current_path);
                if (it != current->children.end()) {
                    current = it->second;
                    found = true;
                }
                if (!found) {
                    return nullptr;
                }
            }
        }
        return current;
    }

    ~TreeNode() {
        for (auto& child : children) {
            delete child.second;
        }
        children.clear();
    }
};

// Qingxiu: rewrite this set to store hash(path) and real path
struct KeyWithPath {
    netreach_key_t key;
    uint16_t path_length;
    char path[MAX_PATH_LENGTH];
    uint16_t idx_for_latest_deleted_vallen;  // latest, deteled, cache frequency, vallen
    uint16_t idx_for_regs;                   // val regs
    uint16_t bitmap;

    KeyWithPath()
        : key(), path_length(0), idx_for_latest_deleted_vallen(0), idx_for_regs(0), bitmap(0) {
        path[0] = '\0';
    }

    KeyWithPath(netreach_key_t k, uint16_t len = 0, const char* p = "", uint16_t idx_for_latest_deleted_vallen = 0, uint16_t idx_for_regs = 0, uint16_t bitmap = 0)
        : key(k), path_length(len), idx_for_latest_deleted_vallen(idx_for_latest_deleted_vallen), idx_for_regs(idx_for_regs), bitmap(bitmap) {
        std::strncpy(path, p, len);
        path[len] = '\0';
    }

    bool operator<(const KeyWithPath& other) const {
        if (key != other.key) {
            return key < other.key;
        }
        return std::strcmp(path, other.path) < 0;
    }

    bool operator==(const KeyWithPath& other) const {
        return key == other.key && std::strcmp(path, other.path) == 0;
    }
};

std::ostream& operator<<(std::ostream& os, const KeyWithPath& obj) {
    os << "Path Length: " << obj.path_length << ", Path: " << obj.path << ", Lookup table idx: " << obj.idx_for_latest_deleted_vallen << ", Reg index: " << obj.idx_for_regs;
    return os;
}

namespace std {
template <>
struct hash<KeyWithPath> {
    std::size_t operator()(const KeyWithPath& k) const {
        std::size_t key_hash = std::hash<int>()(k.key.keyhi);
        std::size_t path_hash = std::hash<std::string>()(std::string(k.path));
        return key_hash ^ (path_hash << 1);
    }
};
}  // namespace std

// Qingxiu: create a new data structure that contains hash, path_length, space_consumption, and path for messagequeue
struct key_path_mapping {
    netcache_getreq_pop_t* getreq_pop_ptr;
    uint16_t space_consumption;  // Qingxiu: equal to keydepth; if space_consumption is 0, it means this is a delete request
    uint16_t path_length;
    char path[MAX_PATH_LENGTH];
};

// Qingxiu: define a new structure <id, bitmap> to replace idx
struct idx_t {
    uint16_t idx_for_latest_deleted_vallen;  // lookup table index
    uint16_t idx_for_regs;                   // register index
    uint16_t bitmap;

    idx_t()
        : idx_for_latest_deleted_vallen(0), idx_for_regs(0), bitmap(0) {}

    idx_t(uint16_t idx_for_latest_deleted_vallen, uint16_t idx_for_regs, uint16_t bitmap)
        : idx_for_latest_deleted_vallen(idx_for_latest_deleted_vallen), idx_for_regs(idx_for_regs), bitmap(bitmap) {}

    idx_t& operator=(const idx_t& other) {
        if (this != &other) {
            idx_for_latest_deleted_vallen = other.idx_for_latest_deleted_vallen;
            idx_for_regs = other.idx_for_regs;
            bitmap = other.bitmap;
        }
        return *this;
    }

    bool operator<(const idx_t& other) const {
        if (idx_for_latest_deleted_vallen != other.idx_for_latest_deleted_vallen) {
            return idx_for_latest_deleted_vallen < other.idx_for_latest_deleted_vallen;
        }
        return idx_for_regs < other.idx_for_regs;
    }
};

struct row_t {
    uint16_t bitmap;
    bool is_full;
};

/*
    2. some important global variables
*/

// (1) for thread running control
bool volatile switchos_running = false;
std::atomic<size_t> switchos_ready_threads(0);
const size_t switchos_expected_ready_threads = 3;
bool volatile switchos_popserver_finish = false;
bool volatile is_timer_running = false;  // Qingxiu: for periodically trigger load freq.
bool volatile is_fs_tree_ready = false;  // Qingxiu: for periodically trigger load freq.

// (2) for socket communication
int switchos_popserver_udpsock = -1;                           // reflector.dp2cpserver -> switchos.popserver
int switchos_popworker_popclient_for_controller_udpsock = -1;  // switchos.popworker <-> controller.popserver
int switchos_evictserver_udpsock = -1;                         // switchos.evictserver <-> server.valuedelete
int switchos_popworker_popclient_for_ptf_udpsock = -1;         // switchos.popworker <-> ptf.popserver
int switchos_evictserver_popclient_for_ptf_udpsock = -1;
int switchos_popworker_popclient_for_reflector_udpsock = -1;  // switchos.popworker <-> reflector.cp2dpserver
int switchos_popworker_popclient_for_reflector_ecivtion = -1;
int switchos_popworker_popclient_for_reflector_loadfreq_udpsock = -1;  // Qingxiu: switchos.popworker <-> reflector.cp2dpserver for periodically load freq.
int switchos_popworker_evictclient_for_controller_udpsock = -1;        // switchos.popworker <-> controller.evictserver for cache eviction
int switchos_evictserver_evictclient_for_controller_udpsock = -1;

// (3) for thread communication: message queue between switchos.popserver and switchos.popworker
MessagePtrQueue<key_path_mapping> switchos_netcache_getreq_pop_path_ptr_queue(MQ_SIZE);
std::mutex mutex_for_cached_keyset;                                  // used by switchos.popsever and switchos.popworker -> whether the key has been reported by data plane
MessagePtrQueue<uint16_t> tmp_empty_idx_latest_regs_queue(MQ_SIZE);  // used by switchos.popworker -> switchos.evictserver

// (4) for metadata cache
std::unordered_set<KeyWithPath> switchos_popserver_cached_keyset;
std::unordered_set<KeyWithPath> switchos_popworker_cached_keyset;                       // used by switchos.popworker only
std::map<std::pair<netreach_key_t, std::string>, uint32_t> switchos_cached_keyidx_map;  // key -> idx (of inswitch KV)
std::string** switchos_cache_idxpath_map = NULL;                                        // pipeidx,idx -> path 5 * switch kv
std::map<netreach_key_t, std::set<token_t>> switchos_cached_key_token_mapset;           // hash(path) -> token set
uint16_t volatile** switchos_perpipeline_cached_serveridxarray = NULL;                  // idx (of inswitch KV) -> serveridx of the key
uint32_t volatile* switchos_perpipeline_cached_empty_index = 0;                         // [empty index, kv_bucket_num-1] is empty

// new data structure for cache admission and eviction
std::vector<row_t>** row_state_array = NULL;
TreeNode* root;
std::list<std::tuple<TreeNode*, double, int, bool>> sorted_path_scaled_frequency_space_tuples;

// (5) for cache admission and eviction
uint32_t switchos_frequency_counters[2][FREQUENCY_COUNTER_SIZE];  // Qingxiu: used by periodically load freq. from switch
std::atomic<int> read_index(0);                                   // Qingxiu: used by periodically load freq. and popworker
bool volatile is_cache_empty = true;                              // trigger the loading freq. when the cache is full
size_t number_of_evicted_paths = 0;
idx_t switchos_freeidxes[MAX_DEPTH * 2];
std::vector<std::string> newly_admitted_paths;
std::vector<std::string> newly_evicted_paths;
bool volatile is_file_admission = false;  // if chosen victim is file, we need to admit file
bool volatile is_skip_admission = false;  // if chosen victim is dir but need to admit file, we skip admission
bool volatile is_corner_case = false;     // if chosen victim is file, we need to admit file
// (6) for packet sending and receiving
uint32_t pktsizes[MAX_DEPTH * 2];
char pktbufs[MAX_DEPTH * 2][MAX_BUFSIZE];  // send CACHE_POP_INSWITCH and CACHE_EVICT to controller
char ackbuf[MAX_BUFSIZE];                  // recv CACHE_POP_INSWITCH_ACK and CACHE_EVICT_ACK from controlelr
int ack_recvsize = 0;
char pop_ackbuf[MAX_BUFSIZE];                  // recv CACHE_POP_INSWITCH_ACK and CACHE_EVICT_ACK from controlelr
int pop_ack_recvsize = 0;
char pktbuf[MAX_BUFSIZE];  // communicate with controller.evictserver or reflector.cp2dpserer
uint32_t pktsize = 0;
char ptfbuf[MAX_BUFSIZE];  // communicate with ptf.popserver
uint32_t ptf_sendsize = 0;
int ptf_recvsize = 0;

// (7) for multi-pipeline
std::vector<uint16_t> valid_global_server_logical_idxes;
std::map<std::string, token_t> switchos_cached_path_token_map;          // path -> token
netreach_key_t volatile** switchos_perpipeline_cached_keyarray = NULL;  // idx (of inswitch KV) -> key
token_t volatile** switchos_perpipeline_cached_tokenarray = NULL;       // idx (of inswitch KV) -> token

/*
    3. some important functions
*/

// (1) thread functions
void prepare_switchos();
void* run_switchos_popserver(void* param);  // in switchos.c
void* run_switchos_popworker(void* param);  // in switchos.c
void close_switchos();
void* periodicFunction(void* param);  // Qingxiu: for periodically trigger loading freq.

// (2) packet serialization: switchos <-> ptf.popserver
inline uint32_t serialize_add_cache_lookup(char* buf, netreach_key_t key, idx_t switchos_freeidx, token_t token);
inline uint32_t serialize_remove_cache_lookup(char* buf, netreach_key_t key, idx_t switchos_evictidx, token_t token);

// (3) helper functions
netreach_key_t generate_key_t_for_cachepop(std::string& new_path);
std::string print_bits(uint16_t num);
bool has_five_ones(uint16_t num);
std::pair<uint8_t, uint8_t> findHighestTwoOnes(uint16_t x);
void print_row_state_array(std::vector<uint16_t> tmp_pipeidxes);
bool isBitSet(uint16_t value, uint8_t bitIndex);
std::pair<uint32_t, int> find_logical_physical_server_idx_for_dir(std::string path);
std::pair<uint32_t, int> find_logical_physical_server_idx_for_file(std::string path);
std::vector<std::pair<uint16_t, uint32_t>> get_pipeline_logical_idx_for_admitted_keys(std::vector<netreach_key_t> admitted_keys, std::vector<val_t> tmp_val);
std::pair<uint16_t, uint32_t> get_pipeline_server_idx_for_path(std::string path, uint16_t idx_for_latest_deleted_vallen);
inline std::vector<std::string> extractPathLevels(const std::string& path, int n);

// (4) admission and eviction
std::vector<val_t> fetch_metadata_from_controller(std::vector<netreach_key_t> admitted_keys,
                                                  uint32_t* tmp_seq, uint16_t tmp_global_server_logical_idx,
                                                  bool* tmp_stat, bool* is_nlatest_for_largevalue,
                                                  struct sockaddr_in controller_popserver_addr);
std::vector<netreach_key_t> eviction_with_popserver_controller(int number_of_evicted_paths,
                                                               std::vector<struct idx_t> switchos_evictidx,
                                                               struct sockaddr_in ptf_popserver_addr, struct sockaddr_in controller_evictserver_addr);
void send_cache_pop_inswitch(bool is_nlatest_for_largevalue, std::vector<netreach_key_t> admitted_keys,
                             std::vector<val_t> tmp_val, uint32_t* tmp_seq, bool* tmp_stat, sockaddr_in reflector_cp2dpserver_addr, size_t number_of_paths_for_admission,
                             std::vector<val_t> tmp_val_for_isfile);
void send_cache_pop_popserver(struct sockaddr_in ptf_popserver_addr, struct sockaddr_in controller_popserver_addr,
                              std::vector<netreach_key_t> admitted_keys, std::vector<uint16_t> tmp_pipeidxes, std::vector<uint32_t> tmp_global_server_logical_idxes, size_t number_of_paths_for_admission, std::vector<std::string>& newly_admit_paths);
void load_freq_for_potential_victims(std::vector<std::string> victims,
                                     uint32_t* frequency_counters,
                                     std::vector<struct idx_t> victim_idxes,
                                     struct sockaddr_in reflector_cp2dpserver_addr);
std::vector<val_t> real_fetch_metadata_from_controller(std::vector<netreach_key_t> admitted_keys,
                                                       uint32_t* tmp_seq, std::vector<uint32_t> tmp_global_server_logical_idxes, bool* tmp_stat,
                                                       bool* is_nlatest_for_largevalue, struct sockaddr_in controller_popserver_addr, int number_of_paths_for_admission);

bool set_is_file_admission(std::vector<val_t> tmp_val);
std::vector<idx_t> get_free_idx(uint32_t space_consumption, bool is_file_admission, std::vector<uint16_t> tmp_pipeidxes);
std::vector<std::tuple<std::string, double, int>> findKVictims(
    std::list<std::tuple<TreeNode*, double, int, bool>>& sorted_path_scaled_frequency_space_tuples,
    int k, std::string admitted_path);
std::vector<std::pair<std::string, struct idx_t>> find_true_victims(
    std::vector<std::tuple<std::string, double, int>> potential_victims,
    uint32_t* frequency_counters, int number_of_evicted_paths, bool is_file_admission);
void adjust_bitmap_update_rowstates(std::vector<struct idx_t> switchos_evictidx, std::vector<uint16_t> tmp_pipeidxes, bool is_file_admission, size_t number_of_evicted_paths);
std::vector<val_t> adjust_val_for_inswitch_admission(std::vector<val_t> tmp_val, size_t number_of_paths_for_admission);
void produce(const uint32_t* new_data);
void consume(uint32_t* output_buffer);
bool checkAndSetSkipAdmission(const std::vector<std::tuple<std::string, double, int>>& potential_victims,
                              double tmp_scaled_frequency);

TreeNode* build_tree(const std::vector<std::pair<std::string, int>>& path_access_pairs, int root_weight);
void print_tree(const TreeNode* node, const std::string& prefix = "");
void new_update_scaled_frequency_for_leaf_nodes(
    TreeNode* node,
    std::list<std::tuple<TreeNode*, double, int, bool>>& sorted_path_scaled_frequency_space_tuples,
    uint32_t* frequency_counters);

void remove_node(std::string path, TreeNode* root);
void add_node(std::string path, TreeNode* root);

// (5) for tokens
void generate_tokens_for_newly_admitted_paths(int number_of_paths_for_admission, std::vector<uint16_t> tmp_pipeidxes, std::vector<netreach_key_t> admitted_keys);

std::unordered_set<std::string> inout_paths;
std::unordered_set<std::string> new_admit_paths_set;
#endif
