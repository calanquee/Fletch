
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


// hdfs handler
hdfsFS fs = NULL;
hdfsFS ns1 = NULL;
hdfsFS ns2 = NULL;

std::vector<long long> durations;
std::atomic<int> batchidx(0);
std::atomic<long long> batchTotalDuration(0);
std::atomic<long long> batchMaxDuration(0);
const int batchSize = 10000;

std::vector<unsigned char> computeMD5(const std::string& input);
void constructHashRing(const std::vector<std::string>& namespaces);
// void initHashRing(int server_logical_num);
std::string mapToNameNode(const std::vector<unsigned char>& hashValue);
uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue);
void fetch_metadata_from_hdfs(metadata_t& tmp_val, char* queried_path, int operation);
void fetch_metadata_from_hdfs_for_get(val_t& tmp_val, const char* queried_path);
int extract_mode_from_chmod_command(const char* chmod_command);
int fetch_metadata_from_hdfs_for_listdir(val_t& tmp_val, const char* queried_path, int* numEntries);
void init_HDFS();
std::vector<std::string> get_internal_paths(const std::string& path); // obtain all internal paths

std::map<std::vector<unsigned char>, std::pair<std::string, uint16_t>> hashRing;

std::vector<std::string> get_internal_paths(const std::string& path) {
    std::vector<std::string> internal_paths;
    std::string current_path;
	std::string root_path = "/";
    std::istringstream iss(path);
    std::string part;

	// Qingxiu: start from "/"
	internal_paths.push_back(root_path);

    while (std::getline(iss, part, '/')) {
        if (!part.empty()) {
            current_path += "/" + part;
            internal_paths.push_back(current_path);
        }
    }

	// Remove the last path as it's the full path, not an internal path
    if (!internal_paths.empty()) {
        internal_paths.pop_back();
    }

    return internal_paths;
}

void init_HDFS() {
    // connect to hdfs server
    struct hdfsBuilder* fsBuilder1 = hdfsNewBuilder();
    struct hdfsBuilder* fsBuilder2 = hdfsNewBuilder();
    // struct hdfsBuilder* fsBuilder3 = hdfsNewBuilder();
    struct hdfsBuilder* fsBuilder_thread_ns1[server_thread_pool_size];
    // struct hdfsBuilder* fsBuilder_thread_ns2[server_thread_pool_size];
    // router
    // Qingxiu: modify the following line for server thpt test: server_physical_idx --> 1
    hdfsBuilderSetNameNode(fsBuilder1, server_ip_for_controller_list[server_physical_idx]);
    // hdfsBuilderSetNameNode(fsBuilder1, "10.26.43.164");
    // printf("[NETFETCH_DEBUG] server_ip_for_controller_list[0]: %s\n", server_ip_for_controller_list[0]);
    // Qingxiu: 8888 -> 9001
    hdfsBuilderSetNameNodePort(fsBuilder1, 8888);  // router port
    fs = hdfsBuilderConnect(fsBuilder1);
    // ns1
    hdfsBuilderSetNameNode(fsBuilder2, server_ip_for_controller_list[server_physical_idx]);
    // hdfsBuilderSetNameNode(fsBuilder2, "10.26.43.164");
    hdfsBuilderSetNameNodePort(fsBuilder2, 9001);  // ns port
    ns1 = hdfsBuilderConnect(fsBuilder2);

    if (fs == NULL) {
        printf("[NETFETCH_ERROR] NOT connect to fs\n");
    } else if (ns1 == NULL) {
        printf("[NETFETCH_ERROR] NOT connect to ns1\n");
        // } else if (ns2 == NULL) {
        //     printf("[NETFETCH_ERROR] NOT connect to ns2\n");
    } else {
        printf("[NETFETCH_DEBUG] connect to hdfs namenode successfully!\n");
    }   
}

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

    durations.push_back(duration);

    batchTotalDuration += duration;
    if (duration > batchMaxDuration) {
        batchMaxDuration = duration;
    }

    if ((batchidx++) % batchSize == 0) {
        double batchAvgLatency = batchTotalDuration / static_cast<double>(batchSize);  // 当前批次的平均延迟
        std::cout << "Batch " << (batchidx + 1) / batchSize << " - Average latency: " << batchAvgLatency
                  << " microseconds, Max latency: " << batchMaxDuration << " microseconds" << std::endl;
        batchTotalDuration = 0;
        batchMaxDuration = 0;
    }
#endif
}

inline void fetch_metadata_from_hdfs_for_get(val_t& tmp_val, const char* queried_path) {
    tmp_val.val_length = 0;
    metadata_t tmp_metadata;
    // fetch metadata from hdfs router
    hdfsFileInfo* queried_path_metadata = hdfsGetPathInfo(ns1, queried_path);
    if (queried_path_metadata == NULL) {
#ifdef DEBUG_NETFETCH
        printf("Memory allocation failed!\n");
#endif
        tmp_val.val_data = NULL;
        tmp_val.val_length = 0;
    } else {
        tmp_metadata = metadata_t(queried_path_metadata);
        tmp_val.val_data = new char[tmp_metadata.metadata_length];
        memcpy(tmp_val.val_data, tmp_metadata.metadata_data + sizeof(uint16_t), tmp_metadata.metadata_length);
        tmp_val.val_length = tmp_metadata.metadata_length;
        INVARIANT(tmp_val.val_data != NULL);
        hdfsFreeFileInfo(queried_path_metadata, 1);
        queried_path_metadata = NULL;
    }
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
