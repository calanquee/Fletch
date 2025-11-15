#include <pthread.h>

#include <queue>
#include <thread>
#include <vector>

#include "../common/helper.h"
#include "../common/key.h"
#include "../common/metadata.h"
#include "../common/val.h"
#include "common_impl.h"
#include "concurrent_map_impl.h"
#include "concurrent_set_impl.h"
#include "hashring.h"
#include "message_queue_impl.h"

#ifdef DEBUG_NETFETCH
#define DBGprint(...) printf(__VA_ARGS__)
#else
#define DBGprint(...)
#endif

// hdfs handler
hdfsFS fs = NULL;
hdfsFS ns1 = NULL;
// hdfsFS ns2 = NULL;
// load balance ratio
int ns1_accesses = 0;
int ns2_accesses = 0;
std::vector<long long> durations;
std::atomic<int> batchidx(0);
std::atomic<long long> batchTotalDuration(0);
std::atomic<long long> batchMaxDuration(0);
const int batchSize = 6000;

void fetch_metadata_from_hdfs(metadata_t& tmp_val, const char* queried_path, int operation);
void fetch_metadata_from_hdfs_for_admission(val_t& tmp_val, std::string queried_path_for_admission);
int fetch_metadata_from_hdfs_for_listdir(val_t& tmp_val, const char* queried_path, int* numEntries);
int extract_mode_from_chmod_command(const char* chmod_command);
void disconnect_with_hdfs_namenodes();
void connect_with_hdfs_namenodes();

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

// operation 1: GETREQ; 2: POPSERVER; 3: PUTREQ_CACHED
inline void fetch_metadata_from_hdfs(metadata_t& tmp_val, const char* queried_path, int operation) {
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
        DBGprint("%s fetch metadata from router!\n", print_log_header);

    } else if (operation == 1) {
#ifdef DEBUG_NETFETCH
        auto digest_str = computeMD5(queried_path);
        // printf("%s digest_str: %s\n", print_log_header, digest_str.c_str());
        std::string namenode = mapToNameNode(digest_str);
        printf("%s namenode: %s\n", print_log_header, namenode.c_str());
#endif
        // auto start = std::chrono::high_resolution_clock::now();
        queried_path_metadata = hdfsGetPathInfo(ns1, queried_path);
        // auto end = std::chrono::high_resolution_clock::now();
        // long long duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        // batchTotalDuration += duration;
        // if (duration > batchMaxDuration) {
        //     batchMaxDuration = duration;
        // }

        // if ((batchidx++) % batchSize == 0) {
        //     double batchAvgLatency = batchTotalDuration / static_cast<double>(batchSize);
        //     std::cout << "Batch " << (batchidx + 1) / batchSize << " - Average latency: " << batchAvgLatency
        //               << " microseconds, Max latency: " << batchMaxDuration << " microseconds" << std::endl;

        //     batchTotalDuration = 0;
        //     batchMaxDuration = 0;
        // }
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
}

// Qingxiu: fetch metadata for cache admission
void fetch_metadata_from_hdfs_for_admission(val_t& tmp_val, std::string queried_path_for_admission) {
    hdfsFileInfo* queried_path_metadata;
    tmp_val.val_length = 0;

    // fetch metadata from hdfs router
    queried_path_metadata = hdfsGetPathInfo(ns1, queried_path_for_admission.c_str());
    if (queried_path_metadata == NULL) {
#ifdef DEBUG_NETFETCH_HDFS
        printf("Memory allocation failed!\n");
#endif
        tmp_val.val_data = NULL;
        tmp_val.val_length = 0;
        return;
    } else {
#ifdef DEBUG_NETFETCH_HDFS
        printf("Kind: %s\n", (queried_path_metadata->mKind == kObjectKindFile) ? "File" : "Directory");  // 1B
        printf("Name: %s\n", queried_path_metadata->mName);                                              // no need
        printf("Last Modified: %lld\n", (long long)queried_path_metadata->mLastMod);                     // 8B
        printf("Size: %lld\n", (long long)queried_path_metadata->mSize);                                 // 8B
        printf("Replication: %d\n", queried_path_metadata->mReplication);                                // 2B
        printf("Block Size: %lld\n", (long long)queried_path_metadata->mBlockSize);                      // 8B
        printf("Owner: %s\n", queried_path_metadata->mOwner);                                            // no need
        printf("Group: %s\n", queried_path_metadata->mGroup);                                            // no need
        printf("Permissions: %o\n", queried_path_metadata->mPermissions);                                // 2B
        printf("Last Access: %lld\n", (long long)queried_path_metadata->mLastAccess);                    // 8B
        printf("--------------------------------\n");
#endif
        // 32B: mLastMod + mSize + mBlockSize + mLastAccess
        // 8B: + mKind + mPermissions + mReplication + padding(0)
        // 2B owner length + mOwner + 2B group length + mGroup
        metadata_t tmp_metadata(queried_path_metadata);
        tmp_val.val_length = tmp_metadata.metadata_length;
        tmp_val.val_data = new char[tmp_val.val_length];
        memcpy(tmp_val.val_data, tmp_metadata.metadata_data + sizeof(uint16_t), tmp_metadata.metadata_length);
        INVARIANT(tmp_val.val_data != NULL);
        // free metadata structure
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

void disconnect_with_hdfs_namenodes() {
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
    // return_value = hdfsDisconnect(ns2);
    // if (return_value == 0) {
    //     printf("[DEBUG] disconnect with hdfs namenode ns2 sucessfully!\n");
    // } else {
    //     printf("[ERROR] NOT disconnect with hdfs namenode ns2!\n");
    // }
}

void connect_with_hdfs_namenodes() {
    // connect to hdfs server
    struct hdfsBuilder* fsBuilder1 = hdfsNewBuilder();
    struct hdfsBuilder* fsBuilder2 = hdfsNewBuilder();

    hdfsBuilderSetNameNode(fsBuilder1, server_ip_for_controller_list[server_physical_idx]);
    // hdfsBuilderSetNameNode(fsBuilder1, "10.26.43.163");
    hdfsBuilderSetNameNodePort(fsBuilder1, 8888);  // router port
    fs = hdfsBuilderConnect(fsBuilder1);
    // ns1
    hdfsBuilderSetNameNode(fsBuilder2, server_ip_for_controller_list[server_physical_idx]);
    // hdfsBuilderSetNameNode(fsBuilder2, "10.26.43.163");
    hdfsBuilderSetNameNodePort(fsBuilder2, 9001);  // ns port
    ns1 = hdfsBuilderConnect(fsBuilder2);
    // dump ip
    printf("[DEBUG] connect to hdfs namenode ns1: %s\n", server_ip_for_controller_list[server_physical_idx]);
    
#ifdef DEBUG_NETFETCH_HDFS
    if (fs == NULL || ns1 == NULL) {
        printf("[NETFETCH_ERROR] NOT connect to hdfs namenode!\n");
    } else {
        printf("[NETFETCH_DEBUG] connect to hdfs namenode successfully!\n");
    }
#endif
}