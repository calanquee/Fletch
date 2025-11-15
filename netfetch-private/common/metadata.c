#include "metadata.h"

#include <arpa/inet.h>

#include <fstream>
#include <sstream>
#define FILE_META_SIZE 40
#define DIR_META_SIZE 24

void init_OwnerMapping(char* src) {
    std::ifstream file(src);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << src << std::endl;
        return;
    }

    std::string line;
    uint16_t lineNumber = 1;

    while (getline(file, line)) {
        OwnerMapping[line] = lineNumber;
        ++lineNumber;
    }

    file.close();
};
void init_GroupMapping(char* src) {
    std::ifstream file(src);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << src << std::endl;
        return;
    }

    std::string line;
    uint16_t lineNumber = 1;

    while (getline(file, line)) {
        GroupMapping[line] = lineNumber;
        ++lineNumber;
    }

    file.close();
};

uint64_t htonll(uint64_t val) {
    if (htonl(1) == 1) {
        return val;
    } else {
        return ((uint64_t)htonl(val & 0xFFFFFFFF) << 32) | htonl(val >> 32);
    }
}
uint64_t ntohll(uint64_t val) {
    if (htonl(1) != 1) {  // Little endian
        return ((uint64_t)ntohl(val & 0xFFFFFFFF) << 32) | ntohl(val >> 32);
    }
    return val;  // Big endian
}

Metadata::Metadata() {
    metadata_length = 0;
    // metadata_data = nullptr;
}

Metadata::~Metadata() {
    // printf("free metadata\n");
    // fflush(stdout);
    // if (metadata_data != nullptr) {
    //     delete[] metadata_data;
    //     metadata_data = nullptr;
    // }
}

Metadata::Metadata(const char* buf, uint32_t length) {
    INVARIANT(buf != nullptr);
    INVARIANT(length == FILE_META_SIZE + sizeof(uint16_t) or length == DIR_META_SIZE + sizeof(uint16_t));
    metadata_length = length - sizeof(uint16_t);
    // Deep copy
    // metadata_data = new char[length];
    // INVARIANT(metadata_data != nullptr);
    memcpy(metadata_data, buf, length);  // length is # of bytes in buf
    this->deserialize(buf, length);
}
Metadata& Metadata::operator=(const Metadata& other) {
    if (other.metadata_data != nullptr && other.metadata_length != 0) {
        // Deep copy
        // metadata_data = new char[other.metadata_length+ sizeof(uint16_t)];
        // INVARIANT(metadata_data != nullptr);
        memcpy(metadata_data, other.metadata_data, other.metadata_length);  // val_length is # of bytes

        metadata_length = other.metadata_length;
        this->deserialize(metadata_data, metadata_length + sizeof(uint16_t));
    } else {
        metadata_length = 0;
        // metadata_data = nullptr;
    }
    return *this;
}
Metadata::Metadata(const hdfsFileInfo* queried_path_metadata) {
    INVARIANT(queried_path_metadata != nullptr);
    mKind = queried_path_metadata->mKind;
    mLastMod = queried_path_metadata->mLastMod;
    mSize = queried_path_metadata->mSize;
    mReplication = queried_path_metadata->mReplication;
    mBlockSize = queried_path_metadata->mBlockSize;
    mOwner = queried_path_metadata->mOwner;
    mGroup = queried_path_metadata->mGroup;
    uid = OwnerMapping[std::string(mOwner)];
    gid = GroupMapping[std::string(mGroup)];
    mLastMod = queried_path_metadata->mLastMod;
    mLastAccess = queried_path_metadata->mLastAccess;
    mSize = queried_path_metadata->mSize;
    mBlockSize = queried_path_metadata->mBlockSize;
    mPermissions = queried_path_metadata->mPermissions;
    mKindmPermissions = (mPermissions & 0x01FF) | ((mKind == kObjectKindDirectory) << 9);
    if (queried_path_metadata->mKind == kObjectKindFile) {
        metadata_length = FILE_META_SIZE;
    } else {
        metadata_length = DIR_META_SIZE;
    }
    // metadata_data = new char[metadata_length + sizeof(uint16_t)];
    // INVARIANT(metadata_data != nullptr);
    this->serialize(metadata_data, metadata_length + sizeof(uint16_t));
}

Metadata::Metadata(const hdfsFileInfo& queried_path_metadata) {
    INVARIANT(&queried_path_metadata != nullptr);
    mKind = queried_path_metadata.mKind;
    mLastMod = queried_path_metadata.mLastMod;
    mSize = queried_path_metadata.mSize;
    mReplication = queried_path_metadata.mReplication;
    mBlockSize = queried_path_metadata.mBlockSize;
    mOwner = queried_path_metadata.mOwner;
    mGroup = queried_path_metadata.mGroup;
    uid = OwnerMapping[std::string(mOwner)];
    gid = GroupMapping[std::string(mGroup)];
    mLastMod = queried_path_metadata.mLastMod;
    mLastAccess = queried_path_metadata.mLastAccess;
    mSize = queried_path_metadata.mSize;
    mBlockSize = queried_path_metadata.mBlockSize;
    mPermissions = queried_path_metadata.mPermissions;
    mKindmPermissions = (mPermissions & 0x01FF) | ((mKind == kObjectKindDirectory) << 9);
    if (queried_path_metadata.mKind == kObjectKindFile) {
        metadata_length = FILE_META_SIZE;
    } else {
        metadata_length = DIR_META_SIZE;
    }
    // metadata_data = new char[metadata_length + sizeof(uint16_t)];
    // INVARIANT(metadata_data != nullptr);
    this->serialize(metadata_data, metadata_length + sizeof(uint16_t));
}

// mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize
uint16_t Metadata::deserialize(const char* buf, uint32_t buflen) {
    INVARIANT(buf != nullptr);
    INVARIANT(buflen == FILE_META_SIZE + sizeof(uint16_t) or buflen == DIR_META_SIZE + sizeof(uint16_t));
    metadata_length = buflen - sizeof(uint16_t);  // # of bytes
    //   mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize + mSize
    buf += sizeof(uint16_t);
    buflen -= sizeof(uint16_t);
    mKindmPermissions = *(const uint16_t*)buf;               // # of bytes
    mKindmPermissions = ntohs(uint16_t(mKindmPermissions));  // Big-endian to little-endian
    mPermissions = mKindmPermissions & 0x01FF;
    mKind = (mKindmPermissions & 0x0200) ? kObjectKindDirectory : kObjectKindFile;
    buf += sizeof(uint16_t);
    buflen -= sizeof(uint16_t);
    mReplication = *(const uint16_t*)buf;
    mReplication = ntohs(uint16_t(mReplication));
    buf += sizeof(uint16_t);
    buflen -= sizeof(uint16_t);
    uid = *(const uint16_t*)buf;
    uid = ntohs(uint16_t(uid));
    buf += sizeof(uint16_t);
    buflen -= sizeof(uint16_t);
    gid = *(const uint16_t*)buf;
    gid = ntohs(uint16_t(gid));
    buf += sizeof(uint16_t);
    buflen -= sizeof(uint16_t);
    mLastMod = *(const uint64_t*)buf;
    mLastMod = ntohll(uint64_t(mLastMod));
    buf += sizeof(uint64_t);
    buflen -= sizeof(uint64_t);
    mLastAccess = *(const uint64_t*)buf;
    mLastAccess = htonll(uint64_t(mLastAccess));
    buf += sizeof(uint64_t);
    buflen -= sizeof(uint64_t);
    if (metadata_length == FILE_META_SIZE) {
        mSize = *(const uint64_t*)buf;
        mSize = htonll(uint64_t(mSize));
        buf += sizeof(uint64_t);
        buflen -= sizeof(uint64_t);
        mBlockSize = *(const uint64_t*)buf;
        mBlockSize = htonll(uint64_t(mBlockSize));
        buf += sizeof(uint64_t);
        buflen -= sizeof(uint64_t);
        return FILE_META_SIZE + sizeof(uint16_t);
    } else
        return DIR_META_SIZE + sizeof(uint16_t);
}
// mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize
uint16_t Metadata::serialize(char* buf, uint32_t buflen) {
    INVARIANT(metadata_length == FILE_META_SIZE or metadata_length == DIR_META_SIZE);
    // INVARIANT(buf != nullptr);
    uint16_t bigendian_vallen = htons(uint16_t(metadata_length));  // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_vallen, sizeof(uint16_t));       // Switch needs to use vallen
    buf += sizeof(uint16_t);
    uint16_t bigendian_mKindmPermissions = htons(uint16_t(mKindmPermissions));  // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_mKindmPermissions, sizeof(uint16_t));         // Switch needs to use vallen
    buf += sizeof(uint16_t);
    uint16_t bigendian_mReplication = htons(uint16_t(mReplication));  // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_mReplication, sizeof(uint16_t));    // Switch needs to use vallen
    buf += sizeof(uint16_t);
    uint16_t bigendian_uid = htons(uint16_t(uid));         // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_uid, sizeof(uint16_t));  // Switch needs to use vallen
    buf += sizeof(uint16_t);
    uint16_t bigendian_gid = htons(uint16_t(gid));         // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_gid, sizeof(uint16_t));  // Switch needs to use vallen
    buf += sizeof(uint16_t);

    uint64_t bigendian_mLastMod = htonll(uint64_t(mLastMod));   // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_mLastMod, sizeof(uint64_t));  // Switch needs to use vallen
    buf += sizeof(uint64_t);
    uint64_t bigendian_mLastAccess = htonll(uint64_t(mLastAccess));  // Little-endian to big-endian
    memcpy(buf, (char*)&bigendian_mLastAccess, sizeof(uint64_t));    // Switch needs to use vallen
    buf += sizeof(uint64_t);

    if (metadata_length == FILE_META_SIZE) {
        uint64_t bigendian_mSize = htonll(uint64_t(mSize));      // Little-endian to big-endian
        memcpy(buf, (char*)&bigendian_mSize, sizeof(uint64_t));  // Switch needs to use vallen
        buf += sizeof(uint64_t);
        uint64_t bigendian_mBlockSize = htonll(uint64_t(mBlockSize));  // Little-endian to big-endian
        memcpy(buf, (char*)&bigendian_mBlockSize, sizeof(uint64_t));   // Switch needs to use vallen
        buf += sizeof(uint64_t);
        return FILE_META_SIZE + sizeof(uint16_t);
    } else
        return DIR_META_SIZE + sizeof(uint16_t);
}

void Metadata::dump() {
    printf("length: %d\n", metadata_length);
    for (int i = 0; i < metadata_length; ++i) {
        printf("%02x ", (unsigned char)metadata_data[i]);
    }

    printf("\nKind: %x\n", mKindmPermissions);     // 2B
    printf("Last Modified: %llx\n", mLastAccess);  // 8B
    printf("Size: %llx\n", mSize);                 // 8B
    printf("Replication: %x\n", mReplication);     // 2B
    printf("Block Size: %llx\n", mBlockSize);      // 8B
    printf("Owner: %x\n", uid);                    // no need
    printf("Group: %x\n", gid);                    // no need
    printf("Permissions: %o\n", mPermissions);     // 2B
    printf("Last Access: %llx", mLastAccess);      // 8B
}