#ifndef METADATA_H
#define METADATA_H

#include <array>
#include <limits>
#include <map>
#include <string>
// #include "rocksdb/slice.h"
#include "dynamic_array.h"
#include "hdfs.h"
#include "helper.h"

#define FILE_META_SIZE 40
#define DIR_META_SIZE 24
static std::map<std::string, uint16_t> OwnerMapping;
static std::map<std::string, uint16_t> GroupMapping;
void init_OwnerMapping(char* src);
void init_GroupMapping(char* src);
uint64_t htonll(uint64_t val);
uint64_t ntohll(uint64_t val);
class Metadata {
   public:
    Metadata();
    ~Metadata();
    Metadata(const char* buf, uint32_t length);
    Metadata(const hdfsFileInfo* queried_path_metadata);
    Metadata(const hdfsFileInfo& queried_path_metadata);
    Metadata& operator=(const Metadata& other);

    // operation on packet buf (2B vallength + valdata)
    uint16_t deserialize(const char* buf, uint32_t buflen);
    uint16_t serialize(char* buf, uint32_t buflen);

    void dump();

    // file 40 B dir 22+2 B
    tObjectKind mKind;                       /* file or directory */
    char* mName; /* the name of the file */  // no need to client
    uint64_t mLastMod;                       /* the last modification time for the file in seconds */
    // no need for dir
    uint64_t mSize;        /* the size of the file in bytes */
    uint16_t mReplication; /* the count of replicas */
    uint64_t mBlockSize;   /* the block size for the file */
    //
    char* mOwner; /* the owner of the file */  // no need to client send uid
    uint16_t uid;
    char* mGroup; /* the group associated with the file */  // no need to client send gid
    uint16_t gid;
    uint16_t mPermissions; /* the permissions associated with the file */
    // drwxrwxrwx
    // uint16_t mKindmPermissions = (permission & 0x01FF) | (kind << 9);
    uint16_t mKindmPermissions; /**/
    uint64_t mLastAccess;       /* the last access time for the file in seconds */
    // mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize

    uint16_t metadata_length;  // val_length (# of bytes)
    char metadata_data[138];   // store metadata_length + metadata_data
} PACKED;

typedef Metadata metadata_t;

#endif

// printf("%s Kind: %s\n", print_log_header, (queried_path_metadata->mKind == kObjectKindFile) ? "File" : "Directory");  // 1B
// printf("%s Name: %s\n", print_log_header, queried_path_metadata->mName);                                              // no need
// printf("%s Last Modified: %lld\n", print_log_header, (long long)queried_path_metadata->mLastMod);                     // 8B
// printf("%s Size: %lld\n", print_log_header, (long long)queried_path_metadata->mSize);                                 // 8B
// printf("%s Replication: %d\n", print_log_header, queried_path_metadata->mReplication);                                // 2B
// printf("%s Block Size: %lld\n", print_log_header, (long long)queried_path_metadata->mBlockSize);                      // 8B
// printf("%s Owner: %s\n", print_log_header, queried_path_metadata->mOwner);                                            // no need
// printf("%s Group: %s\n", print_log_header, queried_path_metadata->mGroup);                                            // no need
// printf("%s Permissions: %o\n", print_log_header, queried_path_metadata->mPermissions);                                // 2B
// printf("%s Last Access: %lld\n", print_log_header, (long long)queried_path_metadata->mLastAccess);                    // 8B
