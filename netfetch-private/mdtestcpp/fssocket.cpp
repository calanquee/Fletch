#include "fssocket.h"

// #define DEST_IP "10.0.1.3"
// #define DEST_PORT 1152
#define MAX_LEN 1024

#define MAX_COMPONENTS 20  // max depth
#define MAX_LENGTH 256     // max length of each component
#define TIMEOUT_SEC 5      // 超时时间5秒
#define TIMEOUT_USEC 0     // 微秒部分
// remark: /#test-dir.0/mdtest_tree.0 has depth 1
// remark: /#test-dir.0/mdtest_tree.0/file.mdtest.0.0 has depth 2
// #define MDTEST_PATH_RESOLUTION_OFFSET 2
#define size_of_key 8

// send_socket

// file name: generate for simulation
FILE* file_fd = NULL;
int flag_fprintf = 0;
// buf

// initialize file out
void initialize_file_out() {
    // init file name file
    char rank_file_out[1024];
    sprintf(rank_file_out, "%s", file_out);
    file_fd = fopen(rank_file_out, "w");
    if (file_fd == NULL) {
        printf("[NETFETCH_ERROR] cannot open %s!\n", rank_file_out);
    }
}

int prepare_packet_for_GET_for_netcache(std::string& path, std::vector<token_t>& tokens, char* buf) {
    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    memcpy(buf + 2 + sizeof(short), key.data(), size_of_key);
    // tokens
    if (current_method == NETCACHE_ID) {
        tokens[0] = 1;
        memcpy(buf + 2 + sizeof(short) + size_of_key, &tokens[0], sizeof(unsigned char));
    }
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + size_of_key + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + size_of_key + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + size_of_key + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET_for_netfetch(std::string& path, std::vector<token_t>& tokens, char* buf) {
    // get sub-paths and keydepth
    int keydepth;
    auto levels = extractPathLevels(path, path_resolution_offset);
    // auto tokens = searchTokensforPaths(levels);
    // adjust kepdepth for mdtest; keydepth starts from 1
    keydepth = levels.size();
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    for (int i = 0; i < keydepth; ++i) {
        printf("[NETFETCH_FS] subpath: %s\n", levels[i].c_str());
    }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    for (int i = 0; i < keydepth; ++i) {
        printf("MD5(\"%s\") = ", levels[i].c_str());
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            printf("%02x", (unsigned char)keys[i][j]);
        printf("\n");
    }
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;  // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + i * size_of_key, keys[i].data(), size_of_key);
    }
    // tokens
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + i * sizeof(unsigned char), &tokens[i], sizeof(unsigned char));
    }
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET_for_netfetch_design1(std::string& path, std::vector<token_t>& tokens, char* buf) {
    // get sub-paths and keydepth
    int keydepth;
    std::vector<std::string> levels = extractPathLevels(path, path_resolution_offset);
    // adjust kepdepth for mdtest; keydepth starts from 1
    keydepth = levels.size();
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    for (int i = 0; i < keydepth; ++i) {
        printf("[NETFETCH_FS] subpath: %s\n", levels[i].c_str());
    }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    for (int i = 0; i < keydepth; ++i) {
        printf("MD5(\"%s\") = ", levels[i].c_str());
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            printf("%02x", (unsigned char)keys[i][j]);
        printf("\n");
    }
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + i * size_of_key, keys[i].data(), size_of_key);
    }
    // // tokens
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET_for_netfetch_design2(std::string& path, std::vector<token_t>& tokens, char* buf) {
    // get sub-paths and keydepth
    int keydepth;
    std::vector<std::string> levels = extractPathLevels(path, path_resolution_offset);
    // adjust kepdepth for mdtest; keydepth starts from 1
    keydepth = levels.size();
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    for (int i = 0; i < keydepth; ++i) {
        printf("[NETFETCH_FS] subpath: %s\n", levels[i].c_str());
    }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    for (int i = 0; i < keydepth; ++i) {
        printf("MD5(\"%s\") = ", levels[i].c_str());
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            printf("%02x", (unsigned char)keys[i][j]);
        printf("\n");
    }
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xFF;  // lock key depth
    keydepth_be[1] = keydepth & 0xFF;  // real_keydepth
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + i * size_of_key, keys[i].data(), size_of_key);
    }
    // // tokens
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET_for_netfetch_design3(std::string& path, std::vector<token_t>& tokens, char* buf) {
    // get sub-paths and keydepth
    int keydepth;
    auto levels = extractPathLevels(path, path_resolution_offset);
    // auto tokens = searchTokensforPaths(levels);
    // adjust kepdepth for mdtest; keydepth starts from 1
    keydepth = levels.size();
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    for (int i = 0; i < keydepth; ++i) {
        printf("[NETFETCH_FS] subpath: %s\n", levels[i].c_str());
    }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    for (int i = 0; i < keydepth; ++i) {
        printf("MD5(\"%s\") = ", levels[i].c_str());
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            printf("%02x", (unsigned char)keys[i][j]);
        printf("\n");
    }
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + i * size_of_key, keys[i].data(), size_of_key);
    }
    // tokens
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + i * sizeof(unsigned char), &tokens[i], sizeof(unsigned char));
    }
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET_for_nocache(std::string& path, char* buf) {
    // get sub-paths and keydepth
    int keydepth;
    std::vector<std::string> levels = extractPathLevels(path, path_resolution_offset);
    // nocache keydepth is 1
    keydepth = 1;
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    // for (int i = 0; i < keydepth; ++i) {
    printf("[NETFETCH_FS] subpath: %s\n", levels.back().c_str());
    // }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    printf("MD5(\"%s\") = ", levels.back().c_str());
    for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
        printf("%02x", keys.back()[j]);
    printf("\n");
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    // printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes 1 key
    memcpy(buf + 2 + sizeof(short), keys.back().data(), size_of_key);
    // // tokens
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + sizeof(short) + path_length;
#ifdef NETFETCH_DEBUG
    // printf("buf packet %d: ", packet_length);
    // for (int i = 0; i < packet_length; i++)
    //     printf("%02x ", (unsigned char)buf[i]);
    // printf("\n");
#endif
    return packet_length;
}

// Qingxiu: to modify
int prepare_packet_for_GET_for_cscache(std::string& path, char* buf, bool is_cached) {
    // get sub-paths and keydepth
    int keydepth;
    std::vector<std::string> levels = extractPathLevels(path, path_resolution_offset);
    // nocache keydepth is 1
    keydepth = 1;
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    // for (int i = 0; i < keydepth; ++i) {
    printf("[NETFETCH_FS] subpath: %s\n", levels.back().c_str());
    // }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    printf("MD5(\"%s\") = ", levels.back().c_str());
    for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
        printf("%02x", keys.back()[j]);
    printf("\n");
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    // printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes 1 key
    memcpy(buf + 2 + sizeof(short), keys.back().data(), size_of_key);
    // // tokens
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + sizeof(short), path.c_str(), path_length);
    // is_cached: 1B
    unsigned char is_cached_be[1];
    is_cached_be[0] = is_cached ? 1 : 0;
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + sizeof(short) + path_length, is_cached_be, sizeof(unsigned char));
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + sizeof(short) + path_length + sizeof(unsigned char);
    return packet_length;
}

int prepare_packet_for_GET_for_csfletch(std::string& path, std::vector<token_t>& tokens, char* buf, bool is_client_cached) {
    // get sub-paths and keydepth
    int keydepth;
    auto levels = extractPathLevels(path, path_resolution_offset);
    // auto tokens = searchTokensforPaths(levels);
    // adjust kepdepth for mdtest; keydepth starts from 1
    keydepth = levels.size();
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] path: %s\n", path.c_str());
    printf("[NETFETCH_FS] keydepth: %d\n", keydepth);
    for (int i = 0; i < keydepth; ++i) {
        printf("[NETFETCH_FS] subpath: %s\n", levels[i].c_str());
    }
#endif

    std::vector<std::vector<unsigned char>> keys;
    for (auto& level : levels) {
        keys.push_back(computeMD5(level));
    }

    // prepare for GET buf packet
    memset(buf, 0, MAX_LEN);
#ifdef NETFETCH_DEBUG
    for (int i = 0; i < keydepth; ++i) {
        printf("MD5(\"%s\") = ", levels[i].c_str());
        for (int j = 0; j < MD5_DIGEST_LENGTH; j++)
            printf("%02x", (unsigned char)keys[i][j]);
        printf("\n");
    }
#endif
    // path length
    uint16_t path_length = 0;
    path_length = path.size();

#ifdef NETFETCH_DEBUG
    printf("path length: %d\n", path_length);
#endif
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    // keydepth --> big endian
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;  // low
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x30", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // hashes
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + i * size_of_key, keys[i].data(), size_of_key);
    }
    // tokens
    for (int i = 0; i < keydepth; ++i) {
        memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + i * sizeof(unsigned char), &tokens[i], sizeof(unsigned char));
    }
    // memcpy(buf + 2 + sizeof(short) + size_of_key, tokens, keydepth * sizeof(unsigned char));
    // path_length: 2B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short), path.c_str(), path_length);
    int packet_length = 2 + sizeof(short) + keydepth * size_of_key + keydepth * sizeof(unsigned char) + sizeof(short) + path_length;

    unsigned char is_cached_be;
    is_cached_be = is_client_cached ? 1 : 0;
    memcpy(buf + packet_length, &is_cached_be, sizeof(unsigned char));
    packet_length += sizeof(unsigned char);
#ifdef NETFETCH_DEBUG
    printf("buf packet: ");
    for (int i = 0; i < packet_length; i++)
        printf("%02x ", (unsigned char)buf[i]);
    printf("\n");
#endif
    return packet_length;
}

int prepare_packet_for_GET(std::string& path, std::vector<token_t>& tokens, char* buf, bool is_client_cached) {
    int packet_length = 0;
    if (current_method == NETCACHE_ID) {
        packet_length = prepare_packet_for_GET_for_netcache(path, tokens, buf);
    } else if (current_method == NOCACHE_ID) {
        packet_length = prepare_packet_for_GET_for_nocache(path, buf);
    } else if (current_method == NETFETCH_ID) {
        packet_length = prepare_packet_for_GET_for_netfetch(path, tokens, buf);
    } else if (current_method == NETFETCH_ID_1) {
        packet_length = prepare_packet_for_GET_for_netfetch_design1(path, tokens, buf);
    } else if (current_method == NETFETCH_ID_2) {
        packet_length = prepare_packet_for_GET_for_netfetch_design2(path, tokens, buf);
    } else if (current_method == NETFETCH_ID_3) {
        packet_length = prepare_packet_for_GET_for_netfetch_design3(path, tokens, buf);
    } else if (current_method == CSCACHE_ID) {
        packet_length = prepare_packet_for_GET_for_cscache(path, buf, is_client_cached);
    } else if (current_method == CSFLETCH_ID) {
        packet_length = prepare_packet_for_GET_for_csfletch(path, tokens, buf, is_client_cached);
    }
    return packet_length;
}

int prepare_packet_for_PUT_for_netcache_mv(std::string& path, std::string& operation, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    unsigned char token = 1;
    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    //
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    if (current_method == NETCACHE_ID) {  // ((current_method == 0 || current_method == 1) ? sizeof(unsigned char) : 0)
        token = 1;
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(unsigned char));
        // packet_length += sizeof(unsigned char);
    }
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short), path.c_str(), path_length);
    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short) + path_length + sizeof(short) + new_path_length;

    return packet_length;
}

int prepare_packet_for_PUT_for_netcache(std::string& path, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    unsigned char token = 1;
    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    //
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    if (current_method == NETCACHE_ID) {  // ((current_method == 0 || current_method == 1) ? sizeof(unsigned char) : 0)
        token = 1;
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(unsigned char));
        packet_length += sizeof(unsigned char);
    }
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netcache_touch_mkdir(std::string& path, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    unsigned char token = 1;
    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    //
    memcpy(buf, "\x00\x11", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    if (current_method == NETCACHE_ID) {  // ((current_method == 0 || current_method == 1) ? sizeof(unsigned char) : 0)
        token = 1;
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(unsigned char));
        packet_length += sizeof(unsigned char);
    }
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + ((current_method == NETCACHE_ID) ? sizeof(unsigned char) : 0) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_nocache_mv(std::string& path, std::string& operation, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);

    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);

    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short) + new_path_length;

    return packet_length;
}

int prepare_packet_for_PUT_for_cscache_mv(std::string& path, std::string& operation, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);

    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);

    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short) + new_path_length;

    return packet_length;
}

int prepare_packet_for_PUT_for_nocache(std::string& path, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);

    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_cscache(std::string& path, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);

    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_touch_mkdir(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    // memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = 0;
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 1 & 0xff;  // high
    keydepth_be[1] = 1;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x11", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_rm(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = tokens.back();
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 0;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x21", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_chmod(std::string& path, int keydepth, std::string& _operation, std::vector<token_t>& tokens, char* buf) {
    // memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = tokens.back();
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xff;  // high
    keydepth_be[1] = 1;                // low
    // auto operation = _operation + "777";
    auto operation = _operation;
    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    if (is_single_level_lock == 0) {
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    } else {
        auto levels = extractPathLevels(path, path_resolution_offset);
        if (levels.size() > 1) {
            // caculate the second level's md5 hash and copy the 7th and 8th bytes to the padding
            auto key = computeMD5(levels[1]);
            memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, key.data() + 6, 2);
        } else {
            memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x00\x00", 2);
        }
        // memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    }
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_mv(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = tokens.back();
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 0;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\21", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length + sizeof(short) + new_path_length;

    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design3(std::string& path, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    token_t token = tokens.back();
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design2_touch_mkdir(std::string& path, int keydepth, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    unsigned char keydepth_be[2];
    keydepth_be[0] = 1;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x11", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design2_rm(std::string& path, int keydepth, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    unsigned char keydepth_be[2];
    keydepth_be[0] = 0;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x21", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design2_chmod(std::string& path, int keydepth, std::string& _operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xff;  // high
    keydepth_be[1] = 1;                // low
    // auto operation = _operation + "777";
    auto operation = _operation;
    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design2_mv(std::string& path, int keydepth, std::string& operation, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 1;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length + sizeof(short) + new_path_length;
    return packet_length;
}

int prepare_packet_for_PUT_for_netfetch_design1(std::string& path, std::string& operation, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);

    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_csfletch_touch_mkdir(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    // memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = 0;
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 1 & 0xff;  // high
    keydepth_be[1] = 1;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x11", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_csfletch_rm(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = tokens.back();
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 0;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x21", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT_for_csfletch_chmod(std::string& path, int keydepth, std::string& _operation, std::vector<token_t>& tokens, char* buf) {
    // memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    int token_size = tokens.size();
    token_t token2 = tokens.back();
    token_t token1 = tokens[token_size - 2];
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = keydepth & 0xff;  // high
    if (keydepth == 11) {
        keydepth_be[1] = 2;  // low
    } else {
        keydepth_be[1] = 1;  // low
    }

    std::string prefix_path;
    if (keydepth == 11) {
        // hash the prefix of path (keydepth - 1)
        size_t last_slash_pos = path.find_last_of('/');
        if (last_slash_pos != std::string::npos) {
            prefix_path = path.substr(0, last_slash_pos);
        }
    }
    auto prefix_key = computeMD5(prefix_path);
    // std::vector<unsigned char> prefix_key(size_of_key);

    // auto operation = _operation + "777";
    auto operation = _operation;
    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 0;
    
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    
    if (keydepth == 11) {
        packet_length = 4 + size_of_key*2 + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t)*2 + sizeof(short) + path_length;
        memcpy(buf + 4, prefix_key.data(), size_of_key);
        memcpy(buf + 4 + size_of_key, key.data(), size_of_key);
        memcpy(buf + 4 + size_of_key*2, operation_length_be, sizeof(short));
        memcpy(buf + 4 + size_of_key*2 + sizeof(short), padded_operation, adjust_operation_length);
        memcpy(buf + 4 + size_of_key*2 + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
        memcpy(buf + 4 + size_of_key*2 + sizeof(short) + adjust_operation_length + 2, &token1, sizeof(token_t));
        memcpy(buf + 4 + size_of_key*2 + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), &token2, sizeof(token_t));
        memcpy(buf + 4 + size_of_key*2 + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t)*2, path_length_be, sizeof(short));
        memcpy(buf + 4 + size_of_key*2 + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t)*2 + sizeof(short), path.c_str(), path_length);
    } else {
        packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
        // key: size_of_key B
        memcpy(buf + 4, key.data(), size_of_key);
        // operation_length: 2B
        memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
        // operation: adjust_operation_length B
        memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
        // padding: 2B
        if (is_single_level_lock == 0) {
            memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
        } else {
            auto levels = extractPathLevels(path, path_resolution_offset);
            if (levels.size() > 1) {
                // caculate the second level's md5 hash and copy the 7th and 8th bytes to the padding
                auto key = computeMD5(levels[1]);
                memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, key.data() + 6, 2);
            } else {
                memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x00\x00", 2);
            }
            // memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
        }
        // token: 1B
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token2, sizeof(token_t));
        // path_length: 2B
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
        // path: path_length B
        memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    }
    
    return packet_length;
}

int prepare_packet_for_PUT_for_csfletch_mv(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf, bool is_bak) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    token_t token = tokens.back();
    // keydepth
    // uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = 0;  // high
    keydepth_be[1] = 1;  // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\21", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    auto filename = path;
    if (is_bak == false) {
        filename = path + ".bak";
    } else {
        size_t pos = path.rfind(".bak");
        if (pos != std::string::npos) {
            filename = path.substr(0, pos);
        }
    }
    uint16_t new_path_length = filename.size();
    // path_length --> big endian
    unsigned char new_path_length_be[2];
    new_path_length_be[0] = (new_path_length >> 8) & 0xFF;  // high
    new_path_length_be[1] = new_path_length & 0xFF;         // low

    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length, new_path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length + sizeof(short), filename.c_str(), new_path_length);
    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length + sizeof(short) + new_path_length;

    return packet_length;
}

int prepare_packet_for_PUT_for_csfletch(std::string& path, std::string& operation, std::vector<token_t>& tokens, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);
    // keydepth
    token_t token = tokens.back();
    uint16_t keydepth = 1;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // operation length
    uint16_t operation_length = 0;
    uint16_t adjust_operation_length = 0;
    operation_length = operation.size();
    if (operation_length % 8 != 0) {
        adjust_operation_length = operation_length + 8 - operation_length % 8;
    } else {
        adjust_operation_length = operation_length;
    }
    // operation_length --> big endian
    unsigned char operation_length_be[2];
    operation_length_be[0] = (adjust_operation_length >> 8) & 0xFF;  // high
    operation_length_be[1] = adjust_operation_length & 0xFF;         // low
    // operation
    char padded_operation[adjust_operation_length];
    memset(padded_operation, 0, adjust_operation_length);
    memcpy(padded_operation, operation.c_str(), operation_length);

    // path length
    uint16_t path_length = 0;
    path_length = path.size();
    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low

    int packet_length = 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x01", 2);
    // keydepth: 2B
    memcpy(buf + 2, keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + 4, key.data(), size_of_key);
    // operation_length: 2B
    memcpy(buf + 4 + size_of_key, operation_length_be, sizeof(short));
    // operation: adjust_operation_length B
    memcpy(buf + 4 + size_of_key + sizeof(short), padded_operation, adjust_operation_length);
    // padding: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length, "\x11\x11", 2);
    // token: 1B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2, &token, sizeof(token_t));
    // path_length: 2B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + 4 + size_of_key + sizeof(short) + adjust_operation_length + 2 + sizeof(token_t) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

int prepare_packet_for_PUT(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf, bool is_bak) {
    int packet_length = 0;
    switch (current_method) {
    case NETCACHE_ID:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_netcache_mv(path, operation, buf, is_bak);
        } else if (operation == "touch" || operation == "mkdir" || operation == "ls") {
            packet_length = prepare_packet_for_PUT_for_netcache_touch_mkdir(path, operation, buf);
        } else {
            packet_length = prepare_packet_for_PUT_for_netcache(path, operation, buf);
        }
        break;
    case NOCACHE_ID:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_nocache_mv(path, operation, buf, is_bak);
        } else {
            packet_length = prepare_packet_for_PUT_for_nocache(path, operation, buf);
        }
        break;
    case CSCACHE_ID:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_cscache_mv(path, operation, buf, is_bak);
        } else {
            packet_length = prepare_packet_for_PUT_for_cscache(path, operation, buf);
        }
        break;
    case CSFLETCH_ID:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_csfletch_mv(path, keydepth, operation, tokens, buf, is_bak);
        } else if (operation == "rm" || operation == "rmdir") {
            packet_length = prepare_packet_for_PUT_for_csfletch_rm(path, keydepth, operation, tokens, buf);
        } else if (operation == "touch" || operation == "mkdir" || operation == "ls") {
            packet_length = prepare_packet_for_PUT_for_csfletch_touch_mkdir(path, keydepth, operation, tokens, buf);
        } else if (operation.find("chmod", 0) == 0) {
            packet_length = prepare_packet_for_PUT_for_csfletch_chmod(path, keydepth, operation, tokens, buf);
        } else {
            packet_length = prepare_packet_for_PUT_for_csfletch(path, operation, tokens, buf);
        }
        break;
    case NETFETCH_ID:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_netfetch_mv(path, keydepth, operation, tokens, buf, is_bak);
        } else if (operation == "rm" || operation == "rmdir") {
            packet_length = prepare_packet_for_PUT_for_netfetch_rm(path, keydepth, operation, tokens, buf);
        } else if (operation == "touch" || operation == "mkdir" || operation == "ls") {
            packet_length = prepare_packet_for_PUT_for_netfetch_touch_mkdir(path, keydepth, operation, tokens, buf);
        } else if (operation.find("chmod", 0) == 0) {
            packet_length = prepare_packet_for_PUT_for_netfetch_chmod(path, keydepth, operation, tokens, buf);
        } else {
            packet_length = prepare_packet_for_PUT_for_netfetch_design3(path, operation, tokens, buf);
        }
        break;
    case NETFETCH_ID_1:
        // no put ?
        packet_length = prepare_packet_for_PUT_for_netfetch_design1(path, operation, buf);
        break;
    case NETFETCH_ID_2:
        if (operation == "mv") {
            packet_length = prepare_packet_for_PUT_for_netfetch_design2_mv(path, keydepth, operation, buf, is_bak);
        } else if (operation == "rm" || operation == "rmdir") {
            packet_length = prepare_packet_for_PUT_for_netfetch_design2_rm(path, keydepth, operation, buf);
        } else if (operation == "touch" || operation == "mkdir" || operation == "ls") {
            packet_length = prepare_packet_for_PUT_for_netfetch_design2_touch_mkdir(path, keydepth, operation, buf);
        } else if (operation.find("chmod", 0) == 0) {
            packet_length = prepare_packet_for_PUT_for_netfetch_design2_chmod(path, keydepth, operation, buf);
        } else {
            packet_length = prepare_packet_for_PUT_for_netfetch_design1(path, operation, buf);
        }
        break;
    case NETFETCH_ID_3:
        // no put ?
        packet_length = prepare_packet_for_PUT_for_netfetch_design3(path, operation, tokens, buf);
        break;
    }
    return packet_length;
}

// Function to convert from network byte order to host byte order for uint64_t
uint64_t ntohll(uint64_t val) {
    if (htonl(1) != 1) {  // Little endian
        return ((uint64_t)ntohl(val & 0xFFFFFFFF) << 32) | ntohl(val >> 32);
    }
    return val;  // Big endian
}

int prepare_packet_for_WARMUP(std::string& path, char* buf) {
    memset(buf, 0, MAX_LEN);
    // hash the key, key is path
    auto key = computeMD5(path);

    // keydepth
    uint16_t keydepth = 1;
    unsigned char token = 0;
    unsigned char keydepth_be[2];
    keydepth_be[0] = (keydepth >> 8) & 0xFF;  // high
    keydepth_be[1] = keydepth & 0xFF;         // low

    // path length
    uint16_t path_length = 0;
    path_length = path.size();

    // path_length --> big endian
    unsigned char path_length_be[2];
    path_length_be[0] = (path_length >> 8) & 0xFF;  // high
    path_length_be[1] = path_length & 0xFF;         // low
    bool need_token = (current_method == NETCACHE_ID || current_method == NETFETCH_ID_3 || current_method == NETFETCH_ID || current_method == CSFLETCH_ID);
    int packet_length = sizeof(uint32_t) + size_of_key + (need_token ? sizeof(unsigned char) : 0) + sizeof(short) + path_length;
    // assemble buf packet
    // op type: 2B
    memcpy(buf, "\x00\x00", 2);
    // keydepth: 2B
    memcpy(buf + sizeof(short), keydepth_be, sizeof(short));
    // key: size_of_key B
    memcpy(buf + sizeof(short) + sizeof(short), key.data(), size_of_key);
    // token
    if (current_method == NETCACHE_ID) {  // ((current_method == 0 || current_method == 1) ? sizeof(unsigned char) : 0)
        token = 1;
        memcpy(buf + sizeof(short) + sizeof(short) + size_of_key, &token, sizeof(unsigned char));
    } else if (current_method == NETFETCH_ID_3 || current_method == NETFETCH_ID || current_method == CSFLETCH_ID) {
        token = 1;
        memcpy(buf + sizeof(short) + sizeof(short) + size_of_key, &token, sizeof(unsigned char));
    }
    // path_length: 2B
    memcpy(buf + sizeof(short) + sizeof(short) + size_of_key + (need_token ? sizeof(unsigned char) : 0), path_length_be, sizeof(short));
    // path: path_length B
    memcpy(buf + sizeof(short) + sizeof(short) + size_of_key + (need_token ? sizeof(unsigned char) : 0) + sizeof(short), path.c_str(), path_length);
    return packet_length;
}

void parse_metadata(char* metadata_buffer, struct metadata* queried_path_metadata) {
    int offset = 0;
    // uint64_t fields
    queried_path_metadata->mLastMod = ntohll(*(uint64_t*)(metadata_buffer + offset));
    offset += sizeof(uint64_t);
    queried_path_metadata->mSize = ntohll(*(uint64_t*)(metadata_buffer + offset));
    offset += sizeof(uint64_t);
    queried_path_metadata->mBlockSize = ntohll(*(uint64_t*)(metadata_buffer + offset));
    offset += sizeof(uint64_t);
    queried_path_metadata->mLastAccess = ntohll(*(uint64_t*)(metadata_buffer + offset));
    offset += sizeof(uint64_t);

    // uint16_t fields
    queried_path_metadata->mPermissions = ntohs(*(uint16_t*)(metadata_buffer + offset));
    offset += sizeof(uint16_t);
    queried_path_metadata->mReplication = ntohs(*(uint16_t*)(metadata_buffer + offset));
    offset += sizeof(uint16_t);

    // uint8_t field
    queried_path_metadata->mKind = *(uint8_t*)(metadata_buffer + offset);
    // 3B padding
    offset += sizeof(uint32_t);

    // owner_length and mOwner
    queried_path_metadata->owner_length = ntohs(*(uint16_t*)(metadata_buffer + offset));
#ifdef NETFETCH_DEBUG
    printf("owner_length: %d\n", queried_path_metadata->owner_length);
#endif
    offset += sizeof(uint16_t);
    memcpy(queried_path_metadata->mOwner, metadata_buffer + offset, queried_path_metadata->owner_length);
    queried_path_metadata->mOwner[queried_path_metadata->owner_length] = '\0';  // Ensure null termination
    offset += queried_path_metadata->owner_length;

    // group_length and mGroup
    queried_path_metadata->group_length = ntohs(*(uint16_t*)(metadata_buffer + offset));
    offset += sizeof(uint16_t);
    memcpy(queried_path_metadata->mGroup, metadata_buffer + offset, queried_path_metadata->group_length);
    queried_path_metadata->mGroup[queried_path_metadata->group_length] = '\0';  // Ensure null termination
    offset += queried_path_metadata->group_length;
#ifdef NETFETCH_DEBUG
    // file or directory
    if (queried_path_metadata->mKind == 0x44) {
        printf("mKind: DIRECTORY\n");
    } else {
        printf("mKind: FILE\n");
    }
    printf("mLastMod: %lu\n", queried_path_metadata->mLastMod);
    printf("mSize: %lu\n", queried_path_metadata->mSize);
    printf("mBlockSize: %lu\n", queried_path_metadata->mBlockSize);
    printf("mLastAccess: %lu\n", queried_path_metadata->mLastAccess);
    printf("mPermissions: %o\n", queried_path_metadata->mPermissions);
    printf("mReplication: %d\n", queried_path_metadata->mReplication);
    printf("Owner Length: %d\n", queried_path_metadata->owner_length);
    printf("Owner: %s\n", queried_path_metadata->mOwner);
    printf("Group Length: %d\n", queried_path_metadata->group_length);
    printf("Group: %s\n", queried_path_metadata->mGroup);
#endif
}

void set_sockaddr(sockaddr_in& addr, uint32_t bigendian_saddr, short littleendian_port) {
    memset((void*)&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = bigendian_saddr;
    addr.sin_port = htons(littleendian_port);
}

void create_udpsock(int& sockfd, bool need_timeout, const char* role, int timeout_sec, int timeout_usec, int udp_rcvbufsize) {
    // printf("debug\n");
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    // printf("debug\n");
    if (sockfd == -1) {
        printf("[%s] fail to create udp socket, errno: %d!\n", role, errno);
        exit(-1);
    }
    // printf("debug\n");
    // Disable udp/tcp check
    int disable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_NO_CHECK, (void*)&disable, sizeof(disable)) < 0) {
        printf("[%s] disable checksum failed, errno: %d!\n", role, errno);
        exit(-1);
    }
    // printf("debug\n");
    // Set timeout for recvfrom/accept of udp/tcp
    if (need_timeout) {
        set_recvtimeout(sockfd, timeout_sec, timeout_usec);
    }
    // printf("debug\n");
    // set udp receive buffer size
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &udp_rcvbufsize, sizeof(int)) == -1) {
        printf("[%s] fail to set udp receive bufsize as %d, errno: %d\n", role, udp_rcvbufsize, errno);
        exit(-1);
    }
}

void udpsendto(int sockfd, const void* buf, size_t len, int flags, const struct sockaddr_in* dest_addr, socklen_t addrlen) {
    int res = sendto(sockfd, buf, len, flags, (struct sockaddr*)dest_addr, addrlen);
    if (res < 0) {
        // dump all parameters sockfd, buf, len, flags, dest_addr, addrlen
        printf("dump sockfd %d ", sockfd);
        // dump buf
        printf("dump buf %ld:\n", len);
        for (int i = 0; i < len; i++) {
            printf("%02x ", ((unsigned char*)buf)[i]);
        }
        printf("\n");
        // dump dest_addr
        printf("dump dest_addr %d %d %d %d\n", dest_addr->sin_family, dest_addr->sin_port, dest_addr->sin_addr.s_addr, dest_addr->sin_zero[0]);
        // dump addrlen
        printf("dump addrlen %d\n", addrlen);

        printf("[] sendto of udp socket fails, errno: %d!\n", errno);
        exit(-1);
    }
}

bool udprecvfrom(int sockfd, void* buf, size_t len, int flags, struct sockaddr_in* src_addr, socklen_t* addrlen, int& recvsize) {
    bool need_timeout = false;
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 0;
    socklen_t tvsz = sizeof(tv);
    int res = getsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, &tvsz);
    UNUSED(res);
    if (tv.tv_sec != 0 || tv.tv_usec != 0) {
        need_timeout = true;
    }

    bool is_timeout = false;
    recvsize = recvfrom(sockfd, buf, len, flags, (struct sockaddr*)src_addr, addrlen);
    if (recvsize > MAX_BUFSIZE) {
        printf("recvsize %d\n", recvsize);
        exit(-1);
    }
    // if(recvsize != -1){
    // 	printf("[debug]errno %d need_timeout %d tv.tv_sec %d recvsize %d addrlen %d len %d\n",errno,need_timeout,tv.tv_sec,recvsize,addrlen,len);
    // 	fflush(stdout);
    // }
    if (recvsize < 0) {
        if (need_timeout && (errno == EWOULDBLOCK || errno == EINTR || errno == EAGAIN)) {
            recvsize = 0;
            is_timeout = true;
        } else {
            printf("[] error of recvfrom, errno: %d!\n", errno);
            exit(-1);
        }
    }
    return is_timeout;
}

void set_recvtimeout(int sockfd, int timeout_sec, int timeout_usec) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = timeout_usec;
    int res = setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    INVARIANT(res >= 0);
}