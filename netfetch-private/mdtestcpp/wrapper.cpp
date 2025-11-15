#include "wrapper.h"

short server_port = 1152;

std::mutex path_token_map_mutex;
std::mutex file_out_mutex;
tbb::concurrent_unordered_map<std::string, token_t> path_token_map;
std::vector<std::unordered_map<std::string, token_t>> path_token_maps;

std::unordered_map<std::string, uint16_t> path_permission_map;
std::vector<std::unordered_map<std::string, uint16_t>> path_permission_maps;  // for client-side caching
std::unordered_map<std::string, int> path_frequency_map;
std::vector<std::unordered_map<std::string, int>> path_frequency_maps;  // Frequency tracking maps for client-side caching
size_t cache_capacity = 1000000;                                        // Example capacity for the cache

std::mutex path_cnt_map_mutex;
std::unordered_map<std::string, int> path_cnt_map;
int MAX_RETRIES = 3;

void removeExtraSlashes(std::string &input) {
    int len = input.length();
    int j = 0;
    int i = 0;

    while (i < len) {
        input[j++] = input[i];  // Copy the current character

        // If the current character is a slash, skip all subsequent slashes
        if (input[i] == '/') {
            while (input[i + 1] == '/' && i + 1 < len) {
                i++;
            }
        }
        i++;
    }
    input.resize(j);  // Resize the string to remove the extra characters

#ifdef NETFETCH_DEBUG
    std::cout << "[NETFETCH_FS] output: " << input << std::endl;
#endif
}

int calculate_path_depth(const std::string &path) {
    int depth = 0;
    if (path == "/") {
        return 1;
    }
    for (char c : path) {
        if (c == '/') {
            depth++;
        }
    }
    return depth + 1;
}

void update_local_counter(int thread_id, Operation op, int nodeidx, bool is_cached) {
    SubOperation sub_op;
    if (nodeidx == bottleneck_id && is_cached == true) {
        sub_op = BOTTLENECKHIT;
    } else if (nodeidx == bottleneck_id && is_cached == false) {
        sub_op = BOTTLENECK;
    } else if (nodeidx != bottleneck_id && is_cached == true) {
        sub_op = ROTATIONHIT;
    } else if (nodeidx != bottleneck_id && is_cached == false) {
        sub_op = ROTATION;
    } else {
        printf("[ERROR] wrong operatioin\n");
    }
    local_counts[thread_id][op][sub_op]++;
}

// caculate for server rotation
// -1 skip
// others continue
int prepare_client_addr_for_rotation(struct sockaddr_in &client_addr, socklen_t &client_addr_len, std::string &path) {
    auto hashValue = computeMD5(path);
    uint32_t nodeidx = mapToNameNodeidx(hashValue);

    //  if is not bottleneck_id or rotation_id skip
    if (nodeidx != bottleneck_id && nodeidx != rotation_id && mode == 1) {
#ifdef NETFETCH_DEBUG
        printf("%s Mapped to: %d skip\n", path.c_str(), nodeidx);
#endif
        return -1;
    }
#ifdef NETFETCH_DEBUG
    printf("%s Mapped to: %d\n", path.c_str(), nodeidx);
#endif
    // bottleneck_ip for bottleneck_id
    // rotation_ip for rotation_id static
    // rotation_ip for remain for static load and dynamic
    if (nodeidx == bottleneck_id) {
        set_sockaddr(client_addr, inet_addr(bottleneck_ip), server_port);
    } else {
        set_sockaddr(client_addr, inet_addr(rotation_ip), server_port);
    }
    client_addr_len = sizeof(struct sockaddr_in);

    return nodeidx;
}

int prepare_client_addr_for_rotation(struct sockaddr_in &client_addr, socklen_t &client_addr_len, uint32_t nodeidx) {
    //  if is not bottleneck_id or rotation_id skip
    if (nodeidx != bottleneck_id && nodeidx != rotation_id && mode == 1) {
#ifdef NETFETCH_DEBUG
        printf("Mapped to: %d skip\n", nodeidx);
#endif
        return -1;
    }
#ifdef NETFETCH_DEBUG
    printf("mapped to: %d\n", nodeidx);
#endif
    // bottleneck_ip for bottleneck_id
    // rotation_ip for rotation_id static
    // rotation_ip for remain for static load and dynamic
    if (nodeidx == bottleneck_id) {
        set_sockaddr(client_addr, inet_addr(bottleneck_ip), server_port);
    } else {
        set_sockaddr(client_addr, inet_addr(rotation_ip), server_port);
    }
    client_addr_len = sizeof(struct sockaddr_in);

    return nodeidx;
}

bool netfetch_finish_signal(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "finish";

    auto levels = extractPathLevels(path, 0);
    auto tokens = searchTokensforPaths(levels, 0);
    int keydepth = levels.size();
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    removeExtraSlashes(path);
    unsigned char token = 0;

    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        // add is_cached
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        sendbuf[sendbufsize] = 0;
        sendbufsize += 1;
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
    }

    // if (flag_for_file_out_generation == 0) {
    //     // PUT request
    //     sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
    //     if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
    //         // add is_cached
    //         sendbuf[sendbufsize] = 0;
    //         sendbufsize += 1;
    //     }
    //     // timeout and retry
    //     int retries = 0;
    //     while (retries < MAX_RETRIES) {
    //         // send request
    //         udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
    //         if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
    //             sleep(10);
    //         }
    //         // recv buf
    //         bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

    //         if (is_timeout) {
    //             retries++;
    //         } else {
    //             // ACK
    //             break;
    //         }
    //     }

    //     if (retries == MAX_RETRIES) {
    //         printf("[finish] Max retries for %s reached.\n", path.c_str());
    //         return 0;
    //     }
    // }
    // close file pointer
    if (flag_fprintf) {
        fclose(file_fd);
        file_fd == NULL;
    }
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    // dump buf
    for (int i = 0; i < bufsize; i++) {
        printf("%02x ", ((unsigned char *)buf)[i]);
    }
#endif
//     if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
// #ifdef NETFETCH_DEBUG
//         printf("[NETFETCH_FS] finish benchmark failed!\n");
// #endif
//     } else {
// #ifdef NETFETCH_DEBUG
//         printf("[NETFETCH_FS] finsh benchmark successfully!\n");
// #endif
//     }
    return 1;
}

bool netfetch_write_cache(std::string &path, int sockfd) {
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] netfetch_write_cache %s\n", path.c_str());
#endif
    // preprocess path
    removeExtraSlashes(path);
    // warmup to local token map for each thread
    auto levels = extractPathLevels(path, path_resolution_offset);
    if (current_method == NETCACHE_ID) {
        // if levels size > 3 skip
        if (is_mixed && remove_only && levels.size() > 5) {  // rmdir only
            return 0;
        } else if (levels.size() > 4) {
            return 0;
        }
    }
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;

    struct sockaddr_in client_addr;
    set_sockaddr(client_addr, inet_addr(bottleneck_ip), server_port);
    socklen_t client_addr_len = sizeof(struct sockaddr_in);

    // Qingxiu: comment it for test
    if (flag_for_file_out_generation == 0) {
        sendbufsize = prepare_packet_for_WARMUP(path, sendbuf);
        // timeout and retry
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);

            // recv buf
            bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
            } else {
                // ACK
                break;
            }
        }

        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[netfetch_write_cache] Max retries for %s reached.\n", path.c_str());
            return 0;
        }
    }

    for (auto &level : levels) {
        // print debug info
        // std::cout << "path: " << level << " token: " << 1 << std::endl;
        // set token for each thread
        for (int i = 0; i < client_logical_num; i++) {
            path_token_maps[i][level] = (token_t)1;
            // print debug info
            // std::cout << "path: " << level << " token: " << path_token_maps[i][level] << "for logical client " << i << std::endl;
        }
    }
    if (bufsize > WARMUP_ACK_LEN) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] write %s into switch successfully!\n", path.c_str());
#endif
        return 1;
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] write %s into switch unsuccessfully!\n", path.c_str());
#endif
        return 1;
    }
}

bool netfetch_close_for_netcache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    // prepare client addr
    bool is_cached = false;

    // preprocess path

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] close %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
            return true;
        }

        // printf("metadata_length: %d\n", metadata_length);
        // Qingxiu: first verify if cache hit or server served
        // mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
            // Qingxiu: if cache hit, compute offset and resolve metadata by offset
            int offset = metadata_length / dir_metadata_length - 1;
            // Qingxiu: permission checking for all offset+1 paths
            for (int i = 0; i < offset + 1; ++i) {
                // get kind, permission, uid, gid
                uint16_t kind_permission;
                uint16_t mPermissions;
                bool kind;
                uint16_t req_uid;
                uint16_t req_gid;
                memcpy(&kind_permission, buf + 4 + size_of_key + 2 + i * dir_metadata_length, 2);
                kind_permission = ntohs(kind_permission);
                mPermissions = kind_permission & 0x01FF;
                kind = (kind_permission >> 9) & 0x01;
                memcpy(&req_uid, buf + 4 + size_of_key + 6 + i * dir_metadata_length, 2);
                memcpy(&req_gid, buf + 4 + size_of_key + 8 + i * dir_metadata_length, 2);
                req_uid = ntohs(req_uid);
                req_gid = ntohs(req_gid);
#ifdef NETFETCH_DEBUG
                printf("kind_permission: %o\n", mPermissions);
                printf("kind: %d\n", kind);
                printf("req_uid: %d\n", req_uid);
                printf("req_gid: %d\n", req_gid);
#endif
                if (uid == req_uid) {
                    // check user permission
                    if (mPermissions & 0x100) {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check failed!\n");
#endif
                    }
                } else if (gid == req_gid) {
                    if (mPermissions & 0x20) {
#ifdef NETFETCH_DEBUG
                        printf("group permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("group permission check failed!\n");
#endif
                    }
                } else {
                    if (mPermissions & 0x04) {
#ifdef NETFETCH_DEBUG
                        printf("others permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("others permission check failed!\n");
#endif
                    }
                }
            }

            // Qingxiu: parse metadata
            bool is_dir = false;
            if (metadata_length == dir_metadata_length * (offset + 1)) {
                is_dir = true;
            }
            if (is_dir) {
                // Qingxiu: care about the last metadata
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, dir_metadata_length);
            } else {
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, file_metadata_length);
            }
        } else {
            // cache miss
            is_cached = false;

            // Qingxiu: if server served, resolve metadata directly
            memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
        }
        unsigned char new_token = buf[bufsize - 1];
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] close %s success!\n", path.c_str());
#endif
        if (new_token != 0) {
            if (tokens[0] == 0) {
#ifdef NETFETCH_DEBUG
                // printf("[NETFETCH_FS] update token %x for %s path_length %d\n", new_token, mappath, path_length);
#endif
            } else if (tokens[0] != new_token) {
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_FS] error token %x client's token is %x\n", tokens[0], new_token);
#endif
            }
        }
        // parse_metadata(metadata_buffer, &metadata);
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] close %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
}

bool netfetch_close_for_nocache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    // prepare client addr
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
        return 0;
    }
    char metadata_buffer[MAX_LEN];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    printf("[close] metadata_length: %d\n", metadata_length);
    printf("[NETFETCH_FS] close %s successfully\n", path.c_str());
#endif
    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
}

bool netfetch_get_for_cscache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx, Operation op) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto check_key = computeMD5(path);
    auto tokens = searchTokensforPaths(levels, thread_id);
    // prepare client addr
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    // check client-side cache
    bool is_client_cached = false;
    auto result = searchCacheforPaths(levels, thread_id);
    is_client_cached = result.first;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf, is_client_cached);

    if (is_client_cached) {
        // append version number
        int internal_path_number = levels.size() - 1;
        std::vector<uint16_t> version_numbers = result.second;
        for (int i = 0; i < internal_path_number; i++) {
            uint16_t version_number = version_numbers[i];
            version_number = htons(version_number);
            memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
            sendbufsize += sizeof(uint16_t);
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
                break;
            } else {
                if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d] key not match\n", __LINE__);
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", OperationToString(op).c_str(), path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
        return 0;
    }
    char metadata_buffer[MAX_LEN];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, op, nodeidx, is_cached);
        return true;
    }

    std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
    addInternalDirIntoCache(levels, version_numbers, thread_id);

    update_local_counter(thread_id, op, nodeidx, is_cached);
    return true;
}

bool netfetch_close_for_netfetch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint8_t rsp_keydepth = buf[3];
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump rsp buf
        printf("dump resp %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("rsp_keydepth: %d\n", rsp_keydepth);
        printf("metadata_length: %d\n", metadata_length);
#endif
        // TODO
        if (buf[0] == 0x00 && buf[1] == 0x30) {
            // permission check fails
            is_cached = true;
            // do nothing
        } else if (rsp_keydepth == 1 && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
};

bool netfetch_close_for_netfetch_design1(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
#endif
    if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
};

bool netfetch_close_for_netfetch_design2(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint8_t rsp_keydepth = buf[3];
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
    printf("rsp_keydepth: %d\n", rsp_keydepth);
#endif

    // TODO
    if (buf[0] == 0x00 && buf[1] == 0x30) {
        // permission check fails
        is_cached = true;
        // do nothing
    } else if (rsp_keydepth == 1 & (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + +4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }
    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
};

bool netfetch_close_for_netfetch_design3(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[close] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("metadata_length: %d\n", metadata_length);
#endif
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, CLOSE, nodeidx, is_cached);
    return true;
};

bool netfetch_get_for_csfletch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx, Operation op) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // check client-side cache
    bool is_client_cached = false;
    auto result = searchCacheforPaths(levels, thread_id);
    is_client_cached = result.first;
#ifdef NETFETCH_DEBUG
    if (is_client_cached) {
        printf("client cache hit\n");
    }
#endif
    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf, is_client_cached);

    if (is_client_cached) {
        // append version number
        int internal_path_number = levels.size() - 1;
        std::vector<uint16_t> version_numbers = result.second;
        for (int i = 0; i < internal_path_number; i++) {
            uint16_t version_number = version_numbers[i];
            version_number = htons(version_number);
            memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
            sendbufsize += sizeof(uint16_t);
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            // for(int i = 0; i < bufsize; i++) {
            //     printf("%02x ", static_cast<unsigned char>(buf[i]));
            // }
            // printf("\n");
            if (is_timeout) {
                // printf("recv timeout for %s, retrying...\n", path.c_str());
                retries++;
                break;
            } else {
                // QXLIU: add full path
                if (!compare_key_match(path, check_key, buf, 4)) {
                    update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d]\n", __LINE__);
                    for(int i = 0; i < 8; i++) {
                        printf("%02x ", check_key[i]);
                    }
                    printf("\n");
                    for(int i = 0; i < 8; i++) {
                        printf("%02x ", static_cast<unsigned char>(buf[4 + i]));
                    }
                    printf("\n");
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", OperationToString(op).c_str(), path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint8_t rsp_keydepth = buf[3];
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, op, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump rsp buf
        printf("dump resp %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("rsp_keydepth: %d\n", rsp_keydepth);
        printf("metadata_length: %d\n", metadata_length);
#endif
        // TODO
        if (buf[0] == 0x00 && buf[1] == 0x30) {
            // permission check fails
            is_cached = true;
            // do nothing
        } else if (rsp_keydepth == 1 && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    if (is_cached == false) {
        std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
        addInternalDirIntoCache(levels, version_numbers, thread_id);
    }

    update_local_counter(thread_id, op, nodeidx, is_cached);
    return true;
};

bool netfetch_close(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    if (current_method == NETCACHE_ID) {
        return netfetch_close_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NOCACHE_ID) {
        return netfetch_close_for_nocache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID) {
        return netfetch_close_for_netfetch(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_1) {
        return netfetch_close_for_netfetch_design1(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_2) {
        return netfetch_close_for_netfetch_design2(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_3) {
        return netfetch_close_for_netfetch_design3(path, sockfd, thread_id, nodeidx);
    } else if (current_method == CSCACHE_ID) {
        return netfetch_get_for_cscache(path, sockfd, thread_id, nodeidx, CLOSE);
    } else if (current_method == CSFLETCH_ID) {
        return netfetch_get_for_csfletch(path, sockfd, thread_id, nodeidx, CLOSE);
    }
    return false;
}

bool netfetch_mkdir(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    if (flag_fprintf) {
        return true;
    }
    std::string operation = "mkdir";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);

    int keydepth = levels.size();
    bool is_cached = false;
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;
    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // send request
    if (flag_for_file_out_generation == 0) {
        // fetch metadata from hdfs
        // get tokens first
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            // add is_cached
            sendbuf[sendbufsize] = is_client_cached;
            sendbufsize += 1;
            if (is_client_cached) {
                // append version number
                int internal_path_number = levels.size() - 1;
                std::vector<uint16_t> version_numbers = result.second;
                for (int i = 0; i < internal_path_number; i++) {
                    uint16_t version_number = version_numbers[i];
                    version_number = htons(version_number);
                    memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                    sendbufsize += sizeof(uint16_t);
                }
            } else {
                // error
                // printf("error\n");
            }
        }
        // timeout and retry
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            bool is_timeout = false;
            while (true) {
                // recv buf
                is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
                if (is_timeout) {
                    retries++;
                    break;
                } else {
                    if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                        printf("[LINE %d] key not match\n", __LINE__);
#endif
                        continue;
                    }
                    break;
                }
            }
            if (retries == MAX_RETRIES) {
                rtries++;
                // printf("[%s] Max retries for %s reached.\n", "mkdir", path.c_str());
                return 0;
            }
            if (is_timeout) {
                continue;  // retry
            }
            break;
        }
    }
#ifdef NETFETCH_DEBUG
    printf("bufsize: %d\n", sendbufsize);
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mkdir %s failed!\n", path.c_str());
#endif
    } else {
        // add internal dir into cache if it is not in cache
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mkdir %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, MKDIR, nodeidx, is_cached);
    return true;
}

bool netfetch_readdir(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    // printf("[readdir] path: %s\n", path.c_str());
    std::string operation = "ls";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();
    bool is_cached = false;
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;
    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // send request
    if (flag_for_file_out_generation == 0) {
        // fetch metadata from hdfs
        // get tokens first
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            // add is_client_cached
            sendbuf[sendbufsize] = is_client_cached;
            sendbufsize += 1;
            if (is_client_cached) {
                // append version number
                int internal_path_number = levels.size() - 1;
                std::vector<uint16_t> version_numbers = result.second;
                INVARIANT(version_numbers.size() == internal_path_number);
                for (int i = 0; i < internal_path_number; i++) {
                    uint16_t version_number = version_numbers[i];
                    version_number = htons(version_number);
                    memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                    sendbufsize += sizeof(uint16_t);
                }
            }
        }
        // timeout and retry
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            bool is_timeout = false;
            while (true) {
                // recv buf
                is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
                if (is_timeout) {
                    retries++;
                    break;
                } else {
                    if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                        printf("[LINE %d] key not match\n", __LINE__);
#endif
                        continue;
                    }
                    break;
                }
            }
            if (retries == MAX_RETRIES) {
                rtries++;
                printf("[%s] Max retries for %s reached.\n", "readdir", path.c_str());
                return 0;
            }
            if (is_timeout) {
                continue;  // retry
            }
            break;
        }
    }
#ifdef NETFETCH_DEBUG
    // dump buf
    printf("[readdir] dump buf %d:\n", bufsize);
    for (int i = 0; i < bufsize; i++) {
        printf("%02x ", ((unsigned char *)buf)[i]);
    }
    printf("[readdir]\n");
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] readdir %s failed!\n", path.c_str());
#endif
    } else {
        // add internal dir into cache if it is not in cache
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            // before getting into get_versions, first check the last two bytes of the response
            uint16_t version_size;
            memcpy(&version_size, buf + bufsize - 2, 2);
            version_size = ntohs(version_size);
            if (version_size != levels.size() - 1) {
                printf("[line %d] path: %s, levels: %ld, version_size: %d\n", __LINE__, path.c_str(), levels.size(), version_size);
            }
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] readdir %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, READDIR, nodeidx, is_cached);
    return true;
}

bool netfetch_loaddir(std::string &path, int sockfd, int thread_id) {
    if (flag_fprintf) {
        return true;
    }
    std::string operation = "mkdir";
    removeExtraSlashes(path);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(struct sockaddr_in);
    bool is_cached = false;
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;
    // TODO:get token

    // send request
    if (flag_for_file_out_generation == 0) {
        // print debug info
        // std::cout << "loaddir" << endl;
        set_sockaddr(client_addr, inet_addr(bottleneck_ip), server_port);
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        // timeout and retry
        // for (int i = 0; i < sendbufsize; i++) {
        //     printf("%02x ", ((unsigned char *)sendbuf)[i]);
        // }
        // puts("");
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            // recv buf
            bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

            if (is_timeout) {
                retries++;
            } else {
                // ACK
                break;
            }
        }

        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[loadir] Max retries for %s reached.\n", path.c_str());
            return 0;
        }
        set_sockaddr(client_addr, inet_addr(rotation_ip), server_port);
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        retries = 0;
        while (retries < MAX_RETRIES) {
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
            } else {
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[loaddir] Max retries for %s reached.\n", path.c_str());
            return 0;
        }
    }
#ifdef NETFETCH_DEBUG
    printf("bufsize: %d\n", sendbufsize);
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mkdir %s failed!\n", path.c_str());
#endif
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mkdir %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, MKDIR, 0, is_cached);
    return true;
}

bool netfetch_rmdir(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "rmdir";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();
    bool is_cached = false;
    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;
    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // prepare request
    sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);

    // add is_client_cached
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        sendbuf[sendbufsize] = is_client_cached;
        sendbufsize += 1;
        if (is_client_cached) {
            // append version number
            int internal_path_number = levels.size() - 1;
            std::vector<uint16_t> version_numbers = result.second;
            for (int i = 0; i < internal_path_number; i++) {
                uint16_t version_number = version_numbers[i];
                version_number = htons(version_number);
                memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                sendbufsize += sizeof(uint16_t);
            }
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
                break;
            } else {
                if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d] key not match\n", __LINE__);
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", "rmdir", path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }

#ifdef NETFETCH_DEBUG
    printf("bufsize: %d\n", sendbufsize);
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] rmdir %s failed!\n", path.c_str());
#endif
    } else {
        // add internal dir into cache if it is not in cache
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] rmdir %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, RMDIR, nodeidx, is_cached);
    return true;
}

bool netfetch_load(std::string &path, int sockfd, int thread_id) {
    std::string operation = "touch";
    removeExtraSlashes(path);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(struct sockaddr_in);

    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;

    if (flag_fprintf) {
        file_out_mutex.lock();
        fprintf(file_fd, "%s\n", path.c_str());
        file_out_mutex.unlock();
        return true;
    }

    // send request
    if (flag_for_file_out_generation == 0) {
        set_sockaddr(client_addr, inet_addr(bottleneck_ip), server_port);
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        // timeout and retry
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            // recv buf
            bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

            if (is_timeout) {
                retries++;
            } else {
                // ACK
                break;
            }
        }

        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[load] Max retries for %s reached.\n", path.c_str());
            return 0;
        }
        set_sockaddr(client_addr, inet_addr(rotation_ip), server_port);
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);
        retries = 0;
        while (retries < MAX_RETRIES) {
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
            } else {
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[touch] Max retries for %s reached.\n", path.c_str());
            return 0;
        }
    }
#ifdef NETFETCH_DEBUG
    printf("bufsize: %d\n", sendbufsize);
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] touch %s failed!\n", path.c_str());
#endif
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] touch %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, TOUCH, 0, is_cached);
    return true;
};

bool netfetch_touch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "touch";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();

    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;
    // TODO:get token
    // printf("print to file: %s\n", path.c_str());
    if (flag_fprintf) {
        file_out_mutex.lock();
        fprintf(file_fd, "%s\n", path.c_str());
        file_out_mutex.unlock();
        return true;
    }

    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // send request
    if (flag_for_file_out_generation == 0) {
        sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);

        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            // add is_client_cached
            sendbuf[sendbufsize] = is_client_cached;
            sendbufsize += 1;
            if (is_client_cached) {
                // append version number
                int internal_path_number = levels.size() - 1;
                std::vector<uint16_t> version_numbers = result.second;
                for (int i = 0; i < internal_path_number; i++) {
                    uint16_t version_number = version_numbers[i];
                    version_number = htons(version_number);
                    memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                    sendbufsize += sizeof(uint16_t);
                }
            }
        }
#ifdef NETFETCH_DEBUG
        printf("[TOUCH] bufsize: %d\n", sendbufsize);
        for (int i = 0; i < sendbufsize; ++i) {
            printf("%02x ", (unsigned char)sendbuf[i]);
        }
        printf("\n");
#endif
        // timeout and retry
        int retries = 0;
        while (retries < MAX_RETRIES) {
            // send request
            udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
            bool is_timeout = false;
            while (true) {
                // recv buf
                is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
                if (is_timeout) {
                    retries++;
                    break;
                } else {
                    if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                        printf("[LINE %d] key not match\n", __LINE__);
#endif
                        continue;
                    }
                    break;
                }
            }
            if (retries == MAX_RETRIES) {
                rtries++;
                printf("[%s] Max retries for %s reached.\n", "touch", path.c_str());
                return 0;
            }
            if (is_timeout) {
                continue;  // retry
            }
            break;
        }
    }
#ifdef NETFETCH_DEBUG
    printf("bufsize: %d\n", sendbufsize);
#endif
    if ((unsigned char)buf[2] == 0xFF && (unsigned char)buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] touch %s failed!\n", path.c_str());
#endif
    } else {
        // add internal dir into cache if it is not in cache
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] touch %s success!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, TOUCH, nodeidx, is_cached);
    return true;
};

bool netfetch_rm(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "rm";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    unsigned char token = 0;

    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // prepare request
    sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);

    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        // add is_client_cached
        sendbuf[sendbufsize] = is_client_cached;
        sendbufsize += 1;
        if (is_client_cached) {
            // append version number
            int internal_path_number = levels.size() - 1;
            std::vector<uint16_t> version_numbers = result.second;
            for (int i = 0; i < internal_path_number; i++) {
                uint16_t version_number = version_numbers[i];
                version_number = htons(version_number);
                memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                sendbufsize += sizeof(uint16_t);
            }
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            // if(!is_timeout) {
            //     printf("[NETFETCH_FS] recv buf size %d for %s\n", bufsize, path.c_str());
            // }
            if (is_timeout) {
                retries++;
                break;
            } else {
                if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d] key not match\n", __LINE__);
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", "rm", path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }

#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    for (int i = 0; i < bufsize; ++i) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
#endif
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE && (unsigned char)buf[2] != 0xFF && (unsigned char)buf[3] != 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] rm %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_LEN];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, RM, nodeidx, is_cached);
            return true;
        }
        memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
// parse_metadata(metadata_buffer, &metadata);
#ifdef NETFETCH_DEBUG
        printf("metadata_length: %d\n", metadata_length);
#endif
        // add internal dir into cache if it is not in cache
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] rm %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, RM, nodeidx, is_cached);
    return true;
};

bool netfetch_stat_for_netcache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    // }
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    for (int i = 0; i < bufsize; ++i) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
#endif
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STAT, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        printf("metadata_length: %d\n", metadata_length);
#endif
        // printf("metadata_length: %d\n", metadata_length);
        // Qingxiu: first verify if cache hit or server served
        // mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
            // Qingxiu: if cache hit, compute offset and resolve metadata by offset
            int offset = metadata_length / dir_metadata_length - 1;
            // Qingxiu: permission checking for all offset+1 paths
            for (int i = 0; i < offset + 1; ++i) {
                // get kind, permission, uid, gid
                uint16_t kind_permission;
                uint16_t mPermissions;
                bool kind;
                uint16_t req_uid;
                uint16_t req_gid;
                memcpy(&kind_permission, buf + 4 + size_of_key + 2 + i * dir_metadata_length, 2);
                kind_permission = ntohs(kind_permission);
                mPermissions = kind_permission & 0x01FF;
                kind = (kind_permission >> 9) & 0x01;
                memcpy(&req_uid, buf + 4 + size_of_key + 6 + i * dir_metadata_length, 2);
                memcpy(&req_gid, buf + 4 + size_of_key + 8 + i * dir_metadata_length, 2);
                req_uid = ntohs(req_uid);
                req_gid = ntohs(req_gid);
#ifdef NETFETCH_DEBUG
                printf("kind_permission: %o\n", mPermissions);
                printf("kind: %d\n", kind);
                printf("req_uid: %d\n", req_uid);
                printf("req_gid: %d\n", req_gid);
#endif
                if (uid == req_uid) {
                    // check user permission
                    if (mPermissions & 0x100) {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check failed!\n");
#endif
                    }
                } else if (gid == req_gid) {
                    if (mPermissions & 0x20) {
#ifdef NETFETCH_DEBUG
                        printf("group permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("group permission check failed!\n");
#endif
                    }
                } else {
                    if (mPermissions & 0x04) {
#ifdef NETFETCH_DEBUG
                        printf("others permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("others permission check failed!\n");
#endif
                    }
                }
            }

            // Qingxiu: parse metadata
            bool is_dir = false;
            if (metadata_length == dir_metadata_length * (offset + 1)) {
                is_dir = true;
            }
            if (is_dir) {
                // Qingxiu: care about the last metadata
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, dir_metadata_length);
            } else {
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, file_metadata_length);
            }
        } else {
            // cache miss
            is_cached = false;

            // Qingxiu: if server served, resolve metadata directly
            memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
        }
        unsigned char new_token = buf[bufsize - 1];
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s success!\n", path.c_str());
#endif
        if (new_token != 0) {
            if (tokens[0] == 0) {
#ifdef NETFETCH_DEBUG
                // printf("[NETFETCH_FS] update token %x for %s path_length %d\n", new_token, mappath, path_length);
#endif
            } else if (tokens[0] != new_token) {
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_FS] error token %x client's token is %x\n", tokens[0], new_token);
#endif
            }
        }
        // parse_metadata(metadata_buffer, &metadata);
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, STAT, nodeidx, is_cached);
    return true;
}

bool netfetch_stat_for_netfetch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint8_t rsp_keydepth = buf[3];
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STAT, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump rsp buf
        printf("dump resp %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("rsp_keydepth: %d\n", rsp_keydepth);
        printf("metadata_length: %d\n", metadata_length);
#endif
        // TODO
        if (buf[0] == 0x00 && buf[1] == 0x30) {
            // permission check fails
            is_cached = true;
            // do nothing
        } else if (rsp_keydepth == 1 && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, STAT, nodeidx, is_cached);
    return true;
};

bool netfetch_stat_for_netfetch_design1(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, STAT, nodeidx, is_cached);
        return true;
    }
#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
#endif
    if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, STAT, nodeidx, is_cached);
    return true;
};

bool netfetch_stat_for_netfetch_design2(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint8_t rsp_keydepth = buf[3];
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, STAT, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
    printf("rsp_keydepth: %d\n", rsp_keydepth);
#endif

    // TODO
    if (buf[0] == 0x00 && buf[1] == 0x30) {
        // permission check fails
        is_cached = true;
        // do nothing
    } else if (rsp_keydepth == 1 & (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + +4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, STAT, nodeidx, is_cached);
    return true;
};

bool netfetch_stat_for_netfetch_design3(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    // if (nodeidx == -1)
    //     return 0;
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STAT, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("metadata_length: %d\n", metadata_length);
#endif
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, STAT, nodeidx, is_cached);
    return true;
};

bool netfetch_stat(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    // printf("stat %s\n", path.c_str());
    if (current_method == NETCACHE_ID) {
        return netfetch_stat_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NOCACHE_ID) {
        return netfetch_stat_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID) {
        return netfetch_stat_for_netfetch(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_1) {
        return netfetch_stat_for_netfetch_design1(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_2) {
        return netfetch_stat_for_netfetch_design2(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_3) {
        return netfetch_stat_for_netfetch_design3(path, sockfd, thread_id, nodeidx);
    } else if (current_method == CSCACHE_ID) {
        return netfetch_get_for_cscache(path, sockfd, thread_id, nodeidx, STAT);
    } else if (current_method == CSFLETCH_ID) {
        return netfetch_get_for_csfletch(path, sockfd, thread_id, nodeidx, STAT);
    }
    return 0;
};

bool netfetch_statdir_for_netcache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    // }
#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    for (int i = 0; i < bufsize; ++i) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
#endif
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        printf("metadata_length: %d\n", metadata_length);
#endif
        // printf("metadata_length: %d\n", metadata_length);
        // Qingxiu: first verify if cache hit or server served
        // mKindmPermissions + mReplication + uid + gid + mLastMod + mLastAccess + mSize + mBlockSize
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
            // Qingxiu: if cache hit, compute offset and resolve metadata by offset
            int offset = metadata_length / dir_metadata_length - 1;
            // Qingxiu: permission checking for all offset+1 paths
            for (int i = 0; i < offset + 1; ++i) {
                // get kind, permission, uid, gid
                uint16_t kind_permission;
                uint16_t mPermissions;
                bool kind;
                uint16_t req_uid;
                uint16_t req_gid;
                memcpy(&kind_permission, buf + 4 + size_of_key + 2 + i * dir_metadata_length, 2);
                kind_permission = ntohs(kind_permission);
                mPermissions = kind_permission & 0x01FF;
                kind = (kind_permission >> 9) & 0x01;
                memcpy(&req_uid, buf + 4 + size_of_key + 6 + i * dir_metadata_length, 2);
                memcpy(&req_gid, buf + 4 + size_of_key + 8 + i * dir_metadata_length, 2);
                req_uid = ntohs(req_uid);
                req_gid = ntohs(req_gid);
#ifdef NETFETCH_DEBUG
                printf("kind_permission: %o\n", mPermissions);
                printf("kind: %d\n", kind);
                printf("req_uid: %d\n", req_uid);
                printf("req_gid: %d\n", req_gid);
#endif
                if (uid == req_uid) {
                    // check user permission
                    if (mPermissions & 0x100) {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check failed!\n");
#endif
                    }
                } else if (gid == req_gid) {
                    if (mPermissions & 0x20) {
#ifdef NETFETCH_DEBUG
                        printf("group permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("group permission check failed!\n");
#endif
                    }
                } else {
                    if (mPermissions & 0x04) {
#ifdef NETFETCH_DEBUG
                        printf("others permission check successfully!\n");
#endif
                        continue;
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("others permission check failed!\n");
#endif
                    }
                }
            }

            // Qingxiu: parse metadata
            bool is_dir = false;
            if (metadata_length == dir_metadata_length * (offset + 1)) {
                is_dir = true;
            }
            if (is_dir) {
                // Qingxiu: care about the last metadata
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, dir_metadata_length);
            } else {
                memcpy(metadata_buffer, dir_metadata_length * offset + buf + 6 + size_of_key, file_metadata_length);
            }
        } else {
            // cache miss
            is_cached = false;

            // Qingxiu: if server served, resolve metadata directly
            memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
        }
        unsigned char new_token = buf[bufsize - 1];
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s success!\n", path.c_str());
#endif
        if (new_token != 0) {
            if (tokens[0] == 0) {
#ifdef NETFETCH_DEBUG
                // printf("[NETFETCH_FS] update token %x for %s path_length %d\n", new_token, mappath, path_length);
#endif
            } else if (tokens[0] != new_token) {
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_FS] error token %x client's token is %x\n", tokens[0], new_token);
#endif
            }
        }
        // parse_metadata(metadata_buffer, &metadata);
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] stat %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
    return true;
}

bool netfetch_statdir_for_netfetch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint8_t rsp_keydepth = buf[3];
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump rsp buf
        printf("dump resp %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("rsp_keydepth: %d\n", rsp_keydepth);
        printf("metadata_length: %d\n", metadata_length);
#endif
        // TODO
        if (buf[0] == 0x00 && buf[1] == 0x30) {
            // permission check fails
            is_cached = true;
            // do nothing
        } else if (rsp_keydepth == 1 && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
    return true;
};

bool netfetch_statdir_for_netfetch_design1(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] stat failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
        return true;
    }
#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
#endif
    if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
    return true;
};

bool netfetch_statdir_for_netfetch_design2(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] statdir failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint8_t rsp_keydepth = buf[3];
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
    printf("rsp_keydepth: %d\n", rsp_keydepth);
#endif

    // TODO
    if (buf[0] == 0x00 && buf[1] == 0x30) {
        // permission check fails
        is_cached = true;
        // do nothing
    } else if (rsp_keydepth == 1 & (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + +4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
    return true;
};

bool netfetch_statdir_for_netfetch_design3(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    // if (nodeidx == -1)
    //     return 0;
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[stat] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] close failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("metadata_length: %d\n", metadata_length);
#endif
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, STATDIR, nodeidx, is_cached);
    return true;
};

bool netfetch_statdir(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    // printf("stat %s\n", path.c_str());
    // auto parent_dir = getParentDirectory(path);
    if (current_method == NETCACHE_ID) {
        return netfetch_statdir_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NOCACHE_ID) {
        return netfetch_statdir_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID) {
        return netfetch_statdir_for_netfetch(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_1) {
        return netfetch_statdir_for_netfetch_design1(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_2) {
        return netfetch_statdir_for_netfetch_design2(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_3) {
        return netfetch_statdir_for_netfetch_design3(path, sockfd, thread_id, nodeidx);
    } else if (current_method == CSCACHE_ID) {
        return netfetch_get_for_cscache(path, sockfd, thread_id, nodeidx, STATDIR);
    } else if (current_method == CSFLETCH_ID) {
        return netfetch_get_for_csfletch(path, sockfd, thread_id, nodeidx, STATDIR);
    }
    return 0;
}

bool netfetch_open_for_netcache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] open %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_LEN];
        struct metadata metadata;
        //  memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, OPEN, nodeidx, is_cached);
            return true;
        }
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            // printf("cache hit\n");
            is_cached = true;
            // Qingxiu: if cache hit, compute offset and resolve metadata by offset
            int offset = metadata_length / dir_metadata_length - 1;
            // Qingxiu: permission checking for all offset+1 paths
            for (int i = 0; i < offset + 1; ++i) {
                // get kind, permission, uid, gid
                uint16_t kind_permission;
                uint16_t mPermissions;
                bool kind;
                uint16_t req_uid;
                uint16_t req_gid;
                memcpy(&kind_permission, buf + 4 + size_of_key + 2 + i * dir_metadata_length, 2);
                kind_permission = ntohs(kind_permission);
                mPermissions = kind_permission & 0x01FF;
                kind = (kind_permission >> 9) & 0x01;
                memcpy(&req_uid, buf + 4 + size_of_key + 6 + i * dir_metadata_length, 2);
                memcpy(&req_gid, buf + 4 + size_of_key + 8 + i * dir_metadata_length, 2);
                req_uid = ntohs(req_uid);
                req_gid = ntohs(req_gid);
#ifdef NETFETCH_DEBUG
                printf("kind_permission: %o\n", mPermissions);
                printf("kind: %d\n", kind);
                printf("req_uid: %d\n", req_uid);
                printf("req_gid: %d\n", req_gid);
#endif
                if (uid == req_uid) {
                    // check user permission
                    if (mPermissions & 0x100) {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check successfully!\n");
                        continue;
#endif
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("owner permission check failed!\n");
                        printf("[NETFETCH_FS] open %s failed\n", path.c_str());
#endif
                        break;
                    }
                } else if (gid == req_gid) {
                    if (mPermissions & 0x20) {
#ifdef NETFETCH_DEBUG
                        printf("group permission check successfully!\n");
                        continue;
#endif
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("group permission check failed!\n");
                        printf("[NETFETCH_FS] open %s failed\n", path.c_str());
#endif
                        break;
                    }
                } else {
                    if (mPermissions & 0x04) {
#ifdef NETFETCH_DEBUG
                        printf("others permission check successfully!\n");
                        continue;
#endif
                    } else {
#ifdef NETFETCH_DEBUG
                        printf("others permission check failed!\n");
                        printf("[NETFETCH_FS] open %s failed\n", path.c_str());
#endif
                        break;
                    }
                }
            }
        } else {
            // cache miss
            is_cached = false;
        }
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] open %s failed!\n", path.c_str());
#endif
    }
    // TODO update token

    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open_for_nocache(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] open failed: %s not exist!\n", path.c_str());
#endif
        // return;
    }
    char metadata_buffer[MAX_LEN];
    struct metadata metadata;
    // memset(metadata_buffer, 0, MAX_LEN);
    // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        // printf("[ERROR] metadata_length is too large\n");
        update_local_counter(thread_id, OPEN, nodeidx, is_cached);
        return true;
    }
// print metadata_length
#ifdef NETFETCH_DEBUG
    printf("[open] metadata_length: %d\n", metadata_length);
#endif
    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open_for_netfetch(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return 0;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] open failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        //  packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint8_t rsp_keydepth = buf[3];
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            // printf("[ERROR] metadata_length is too large\n");
            update_local_counter(thread_id, OPEN, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump rsp buf
        printf("dump resp %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("rsp_keydepth: %d\n", rsp_keydepth);
        printf("metadata_length: %d\n", metadata_length);
#endif
        // TODO
        if (buf[0] == 0x00 && buf[1] == 0x30) {
            // permission check fails
            is_cached = true;
            // do nothing
        } else if (rsp_keydepth == 1 && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + rsp_keydepth * size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }
            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open_for_netfetch_design1(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    // fetch metadata from hdfs
    // get tokens first

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return 0;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] open failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    //  memset(metadata_buffer, 0, MAX_LEN);
    // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        update_local_counter(thread_id, OPEN, nodeidx, is_cached);
        return true;
    }
#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
#endif
    if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif

        // return;
    }

    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open_for_netfetch_design2(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // std::vector<uint8_t> tokens;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return false;
    }
    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] open failed: %s not exist!\n", path.c_str());
#endif
    }
    char metadata_buffer[MAX_BUFSIZE];
    struct metadata metadata;
    //  memset(metadata_buffer, 0, MAX_LEN);
    // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
    uint8_t rsp_keydepth = buf[3];
    uint16_t metadata_length;
    memcpy(&metadata_length, buf + 4 + rsp_keydepth * size_of_key, 2);
    // Qingxiu: metadata_length is value length
    metadata_length = ntohs(metadata_length);
    if (metadata_length > MAX_BUFSIZE / 4) {
        update_local_counter(thread_id, OPEN, nodeidx, is_cached);
        return true;
    }

#ifdef NETFETCH_DEBUG
    // dump buf
    printf("metadata_length: %d\n", metadata_length);
    printf("rsp_keydepth: %d\n", rsp_keydepth);
#endif

    // TODO
    if (buf[0] == 0x00 && buf[1] == 0x30) {
        // permission check fails
        is_cached = true;
        // do nothing
    } else if (rsp_keydepth == 1 & (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + rsp_keydepth * size_of_key + 2 + metadata_length + +4] == 0xFF) {
        is_cached = true;
#ifdef NETFETCH_DEBUG
        path_cnt_map_mutex.lock();
        if (path_cnt_map.find(path) == path_cnt_map.end()) {
            path_cnt_map[path] = 1;
        } else {
            path_cnt_map[path]++;
        }
        path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("cache hit\n");
#endif
    }

    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open_for_netfetch_design3(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;

    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);
    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    int keydepth = levels.size();
    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;

    sendbufsize = prepare_packet_for_GET(path, tokens, sendbuf);
    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        // recv buf
        bool is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);

        if (is_timeout) {
            retries++;
        } else {
            // ACK
            break;
        }
    }

    if (retries == MAX_RETRIES) {
        rtries++;
        printf("[open] Max retries for %s reached.\n", path.c_str());
        return false;
    }

    if (bufsize <= PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("dump buf %d:\n", bufsize);
        for (int i = 0; i < bufsize; i++) {
            printf("%02x ", ((unsigned char *)buf)[i]);
        }

        printf("[NETFETCH_FS] open failed: %s not exist!\n", path.c_str());
#endif
    } else {
        char metadata_buffer[MAX_BUFSIZE];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        // Qingxiu: metadata_length is value length
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            update_local_counter(thread_id, OPEN, nodeidx, is_cached);
            return true;
        }
#ifdef NETFETCH_DEBUG
        // dump buf
        printf("metadata_length: %d\n", metadata_length);
#endif
        if ((unsigned char)buf[4 + size_of_key + 2 + metadata_length + 3] == 0xFF && (unsigned char)buf[4 + size_of_key + 2 + metadata_length + 4] == 0xFF) {
            is_cached = true;
#ifdef NETFETCH_DEBUG
            path_cnt_map_mutex.lock();
            if (path_cnt_map.find(path) == path_cnt_map.end()) {
                path_cnt_map[path] = 1;
            } else {
                path_cnt_map[path]++;
            }
            path_cnt_map_mutex.unlock();
#endif
#ifdef NETFETCH_DEBUG
            // dump buf
            printf("cache hit\n");
#endif
            // return;
        } else {
            // get tokens
            int offset = 4 + size_of_key + 2 + metadata_length + 4 + 1;
            tokens.clear();
            for (int i = 0; i < keydepth; i++) {
                tokens.push_back(buf[offset + i]);
#ifdef NETFETCH_DEBUG
                printf("token %d: %x for path %s\n", i, tokens[i], levels[i].c_str());
                // dump response
#endif
            }

            setTokensforPaths(levels, tokens, thread_id);
        }
    }
    update_local_counter(thread_id, OPEN, nodeidx, is_cached);
    return true;
};

bool netfetch_open(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    if (current_method == NETCACHE_ID) {
        return netfetch_open_for_netcache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NOCACHE_ID) {
        return netfetch_open_for_nocache(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID) {
        return netfetch_open_for_netfetch(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_1) {
        return netfetch_open_for_netfetch_design1(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_2) {
        return netfetch_open_for_netfetch_design2(path, sockfd, thread_id, nodeidx);
    } else if (current_method == NETFETCH_ID_3) {
        return netfetch_open_for_netfetch_design3(path, sockfd, thread_id, nodeidx);
    } else if (current_method == CSCACHE_ID) {
        return netfetch_get_for_cscache(path, sockfd, thread_id, nodeidx, OPEN);
    } else if (current_method == CSFLETCH_ID) {
        return netfetch_get_for_csfletch(path, sockfd, thread_id, nodeidx, OPEN);
    }
    return false;
};

// TODO
bool netfetch_chmod(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "chmod775";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();

    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // TODO:get token

    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }

    // send request
    sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf);

    // add is_client_cached flag
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        sendbuf[sendbufsize++] = is_client_cached;
        if (is_client_cached) {
            // append version number
            int internal_path_number = levels.size() - 1;
            std::vector<uint16_t> version_numbers = result.second;
            for (int i = 0; i < internal_path_number; i++) {
                uint16_t version_number = version_numbers[i];
                version_number = htons(version_number);
                memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                sendbufsize += sizeof(uint16_t);
            }
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                // printf("[%s] Timeout for %s, retrying...\n", "chmod", path.c_str());
                retries++;
                break;
            } else {
                if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d] key not match\n", __LINE__);
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", "chmod", path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }

#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    for (int i = 0; i < bufsize; ++i) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
#endif
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE && buf[2] != 0xFF && buf[3] != 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] chmod %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_LEN];
        struct metadata metadata;
        //  memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            update_local_counter(thread_id, CHMOD, nodeidx, is_cached);
            return true;
        }
        memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
        // parse_metadata(metadata_buffer, &metadata);

        // before getting into get_versions, first check the last two bytes of the response
        // uint16_t version_size;
        // memcpy(&version_size, buf + bufsize - 2, 2);
        // version_size = ntohs(version_size);
        // if (version_size != levels.size()-1) {
        //     printf("[ERROR] version size is not equal to the number of internal paths\n");
        //     printf("levels: %d, version_size: %d\n", levels.size(), version_size);
        //     printf("path: %s\n", path.c_str());
        // }
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::vector<uint16_t> version_numbers = get_versions(buf, bufsize, levels.size());
            addInternalDirIntoCache(levels, version_numbers, thread_id);
        }
    } else if (buf[2] == 0xFF && buf[3] == 0xFF) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] chmod %s failed!\n", path.c_str());
#endif
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] chmod %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, CHMOD, nodeidx, is_cached);
    return true;
};

bool netfetch_mv(std::string &path, int sockfd, int thread_id, uint32_t nodeidx) {
    std::string operation = "mv";
    removeExtraSlashes(path);
    struct sockaddr_in client_addr;
    socklen_t client_addr_len;
    prepare_client_addr_for_rotation(client_addr, client_addr_len, nodeidx);

    auto levels = extractPathLevels(path, path_resolution_offset);
    auto tokens = searchTokensforPaths(levels, thread_id);
    auto check_key = computeMD5(path);
    int keydepth = levels.size();

    bool is_cached = false;

    char buf[MAX_BUFSIZE];
    char sendbuf[MAX_BUFSIZE];
    int bufsize = 0;
    int sendbufsize = 0;
    // TODO:get token
    bool is_client_cached = false;
    std::pair<bool, std::vector<uint16_t>> result;
    // check client-side cache
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        result = searchCacheforPaths(levels, thread_id);
        is_client_cached = result.first;
    }
    // send request
    bool is_bak = false;
    sendbufsize = prepare_packet_for_PUT(path, keydepth, operation, tokens, sendbuf, is_bak);
    // add is_client_cached flag
    if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
        sendbuf[sendbufsize++] = is_client_cached;
        if (is_client_cached) {
            // append version number
            int internal_path_number = levels.size() - 1;
            std::vector<uint16_t> version_numbers = result.second;
            for (int i = 0; i < internal_path_number; i++) {
                uint16_t version_number = version_numbers[i];
                version_number = htons(version_number);
                memcpy(sendbuf + sendbufsize, &version_number, sizeof(uint16_t));
                sendbufsize += sizeof(uint16_t);
            }
        }
    }

    // timeout and retry
    int retries = 0;
    while (retries < MAX_RETRIES) {
        // send request
        udpsendto(sockfd, sendbuf, sendbufsize, 0, &client_addr, client_addr_len);
        bool is_timeout = false;
        while (true) {
            // recv buf
            is_timeout = udprecvfrom(sockfd, buf, MAX_BUFSIZE, 0, &client_addr, &client_addr_len, bufsize);
            if (is_timeout) {
                retries++;
                break;
            } else {
                if (!compare_key_match(path, check_key, buf, 4)) {update_local_counter(thread_id, OBSOLETE, nodeidx, false);
#ifdef NETFETCH_DEBUG
                    printf("[LINE %d] key not match\n", __LINE__);
#endif
                    continue;
                }
                break;
            }
        }
        if (retries == MAX_RETRIES) {
            rtries++;
            printf("[%s] Max retries for %s reached.\n", "mv", path.c_str());
            return 0;
        }
        if (is_timeout) {
            continue;  // retry
        }
        break;
    }

#ifdef NETFETCH_DEBUG
    printf("[NETFETCH_FS] bufsize: %d\n", bufsize);
    for (int i = 0; i < bufsize; ++i) {
        printf("%02x ", (unsigned char)buf[i]);
    }
    printf("\n");
#endif
    if (bufsize > PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mv %s success!\n", path.c_str());
#endif
        char metadata_buffer[MAX_LEN];
        struct metadata metadata;
        // memset(metadata_buffer, 0, MAX_LEN);
        // packet format: 2B op + 2B keydepth + 16B hash + 2B metadata_len + metadata
        uint16_t metadata_length;
        memcpy(&metadata_length, buf + 4 + size_of_key, 2);
        metadata_length = ntohs(metadata_length);
        if (metadata_length > MAX_BUFSIZE / 4) {
            update_local_counter(thread_id, MV, nodeidx, is_cached);
            return true;
        }
        memcpy(metadata_buffer, buf + 6 + size_of_key, metadata_length);
        // parse_metadata(metadata_buffer, &metadata);
    } else {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_FS] mv %s failed!\n", path.c_str());
#endif
    }
    update_local_counter(thread_id, MV, nodeidx, is_cached);
    return true;
};

std::vector<token_t> searchTokensforPaths(const std::vector<std::string> &paths) {
    std::vector<token_t> tokens;
    if (current_method == NETCACHE_ID || current_method == NOCACHE_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == CSCACHE_ID) {
        for (const auto &path : paths) {
            tokens.push_back(1);
        }
        return tokens;
    }

    // for netfetch it need to get token from path_token_map
    // current_method == 3

    for (const auto &path : paths) {
        auto it = path_token_map.find(path);
        if (it != path_token_map.end()) {
            // std::cout << "get token(" << it->second << ") for" << path << std::endl;
            tokens.push_back(it->second);
        } else {
            tokens.push_back(0);
        }
    }
    return tokens;
}

void setTokensforPaths(const std::vector<std::string> &paths, const std::vector<token_t> &tokens) {
    if (current_method == NOCACHE_ID || current_method == NETCACHE_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == CSCACHE_ID) {
        return;
    }
    if (mode == 1) {
        return;
    }
    // for netfetch it need to set token to path_token_map
    // current_method == 3

    int i = 0;
    for (const auto &path : paths) {
        // auto it = path_token_map.find(path);

        // std::cout << "set token(" << tokens[i] << ") for" << path << std::endl;
#ifdef NETFETCH_DEBUG
        std::cout << "set token(" << tokens[i] << ") for" << path << std::endl;
#endif
        if (tokens[i] == 0)
            break;
        // path_token_map_mutex.lock();
        path_token_map[path] = tokens[i++];
        // path_token_map_mutex.unlock();
    }

    return;
}

std::vector<token_t> searchTokensforPaths(const std::vector<std::string> &paths, int thread_id) {
    std::vector<token_t> tokens(paths.size(), 0);  // Initialize all tokens to 0
    if (current_method == NETCACHE_ID || current_method == NOCACHE_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == CSCACHE_ID) {
        std::fill(tokens.begin(), tokens.end(), 1);  // Set all tokens to 1 for these methods
        return tokens;
    }

    // for netfetch it need to get token from path_token_map
    // current_method == 3
    // for (const auto &path : paths) {
    //     auto it = path_token_maps[thread_id % client_logical_num].find(path);
    //     if (it != path_token_maps[thread_id % client_logical_num].end()) {
    //         tokens.push_back(it->second);
    //         // std::cout << "get token(" << it->second << ") for" << path << std::endl;
    //     } else {
    //         tokens.push_back(0);
    //     }
    // }
    int len = paths.size();
    // Iterate from the end to the beginning of the paths
    for (int i = len - 1; i >= 0; --i) {
        auto cached_it = path_token_maps[thread_id % client_logical_num].find(paths[i]);
        if (cached_it != path_token_maps[thread_id % client_logical_num].end()) {
            tokens[i] = cached_it->second;
        } else {
            break;
        }
    }
    return tokens;
}

// std::vector<token_t> searchTokensforPaths(const std::vector<std::string> &paths, int thread_id) {
//     std::vector<token_t> tokens;
//     if (current_method == NETCACHE_ID || current_method == NOCACHE_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == CSCACHE_ID) {
//         for (const auto &path : paths) {
//             tokens.push_back(1);
//         }
//         return tokens;
//     }

//     // for netfetch it need to get token from path_token_map
//     // current_method == 3
//     for (const auto &path : paths) {
//         auto it = path_token_maps[thread_id % client_logical_num].find(path);
//         if (it != path_token_maps[thread_id % client_logical_num].end()) {
//             tokens.push_back(it->second);
//             // std::cout << "get token(" << it->second << ") for" << path << std::endl;
//         } else {
//             tokens.push_back(0);
//         }
//     }
//     return tokens;
// }

void addInternalDirIntoCache(const std::vector<std::string> &paths, std::vector<uint16_t> versions, int thread_id) {
    if (paths.empty() || versions.empty())
        return;

    auto &path_map = path_permission_maps[thread_id % client_logical_num];
    auto &frequency_map = path_frequency_maps[thread_id % client_logical_num];

    // skip the last path
    for (size_t i = 0; i < paths.size() - 1; ++i) {
        const auto &path = paths[i];
        auto it = path_map.find(path);

        if (it == path_map.end()) {
            // if cache is full, evict LFU path
            if (path_map.size() >= cache_capacity) {
                // find LFU entry
                auto lfu = std::min_element(frequency_map.begin(), frequency_map.end(),
                                            [](const auto &a, const auto &b) { return a.second < b.second; });

                // remove LFU entry
                path_map.erase(lfu->first);
                frequency_map.erase(lfu->first);
            }
            // add new path with frequency 1
            path_map[path] = versions[i];
            frequency_map[path] = 1;
        } else {
            // update existing path
            it->second = versions[i];  // reset version
            frequency_map[path]++;     // increment usage frequency
        }
    }
}

void addallDirIntoCache(const std::vector<std::string> &paths, std::vector<uint16_t> versions, int thread_id) {
    if (paths.empty())
        return;

    auto &path_map = path_permission_maps[thread_id % client_logical_num];
    auto &frequency_map = path_frequency_maps[thread_id % client_logical_num];

    // skip the last path
    for (size_t i = 0; i < paths.size(); ++i) {
        const auto &path = paths[i];
        auto it = path_map.find(path);

        if (it == path_map.end()) {
            // if cache is full, evict LFU path
            if (path_map.size() >= cache_capacity) {
                // find LFU entry
                auto lfu = std::min_element(frequency_map.begin(), frequency_map.end(),
                                            [](const auto &a, const auto &b) { return a.second < b.second; });

                // remove LFU entry
                path_map.erase(lfu->first);
                frequency_map.erase(lfu->first);
            }
            // add new path with frequency 1
            path_map[path] = versions[i];
            frequency_map[path] = 1;
            // std::cout << "add" << path << " into cache" << std::endl;
        } else {
            // update existing path
            it->second = versions[i];  // reset version
            frequency_map[path]++;     // increment usage frequency
        }
    }
}

std::pair<bool, std::vector<uint16_t>> searchCacheforPaths(const std::vector<std::string> &paths, int thread_id) {
#ifdef NETFETCH_DEBUG
    printf("[line %d] search CacheforPaths\n", __LINE__);
#endif

    bool is_client_cached = true;
    std::vector<uint16_t> path_versions;
    auto &path_map = path_permission_maps[thread_id % client_logical_num];
    auto &frequency_map = path_frequency_maps[thread_id % client_logical_num];

    for (size_t i = 0; i < paths.size() - 1; ++i) {
        const auto &path = paths[i];
        auto it = path_map.find(path);
        if (it == path_map.end()) {
            is_client_cached = false;  // path not found in cache
            // std::cout << "path not found in cache " << path << " in " << thread_id % client_logical_num << std::endl;
            break;
        } else {
            frequency_map[path]++;  // update access frequency
            path_versions.push_back(it->second);
        }
    }
#ifdef NETFETCH_DEBUG
    if (is_client_cached) {
        printf("[line %d] find CacheforPaths\n", __LINE__);
    }

#endif
    return {is_client_cached, path_versions};
}

void setTokensforPaths(const std::vector<std::string> &paths, const std::vector<token_t> &tokens, int thread_id) {
    if (current_method == NOCACHE_ID || current_method == NETCACHE_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == CSCACHE_ID) {
        return;
    }
    if (mode == 1 && current_method == CSFLETCH_ID) {
        return;
    }
    if (tokens.back() == 0) {
        return;
    }
    if (tokens.empty()) {
        return;
    }
    // for netfetch it need to set token to path_token_map
    // current_method == 3

    int i = 0;
    for (const auto &path : paths) {
#ifdef NETFETCH_DEBUG
        std::cout << "set token(" << tokens[i] << ") for" << path << std::endl;
#endif
        if (tokens[i] == 0)
            break;
        path_token_maps[thread_id % client_logical_num][path] = tokens[i++];
    }

    return;
}

std::vector<uint16_t> get_versions(char *recvbuf, int recvsize, int levels) {
    std::vector<uint16_t> versions;
    // get size: the last two bytes
    uint16_t internal_path_size = 0;
    memcpy(&internal_path_size, recvbuf + recvsize - 2, 2);
    internal_path_size = ntohs(internal_path_size);
    if (internal_path_size == 0 || internal_path_size == 1) {
        // printf("[line %d] internal_path_size: %d, levels: %d, recvsize: %d\n", __LINE__, internal_path_size, levels, recvsize);
        // for (int i = 0; i < recvsize; i++) {
        //     printf("%02x ", ((unsigned char *)recvbuf)[i]);
        // }
        // printf("\n");
        return versions;
    }
#if 1
    if (internal_path_size != levels - 1) {
        if (remove_only) {  // bug rmdir fletch +
            return versions;
        }
        if (create_only) {  // bug mkdir fletch +
            return versions;
        }
        printf("[line %d] internal_path_size: %d, levels: %d, recvsize: %d\n", __LINE__, internal_path_size, levels, recvsize);
        for (int i = 0; i < recvsize; i++) {
            printf("%02x ", ((unsigned char *)recvbuf)[i]);
        }
        printf("\n");
        return versions;
    }
#endif
    INVARIANT(internal_path_size == levels - 1);
    uint16_t tmp_offset = recvsize - 2 - (levels - 1) * 2;
    for (int i = 0; i < (levels - 1); i++) {
        uint16_t version = 0;
        memcpy(&version, recvbuf + tmp_offset + i * 2, 2);
        version = ntohs(version);
        versions.push_back(version);
    }
    return versions;
}

bool compare_key_match(std::string &path, const std::vector<unsigned char> &last_key, const char *recvbuf, int offset) {
    // Ensure the offset and size_of_key are within bounds
    if (offset < 0 || offset + size_of_key > 4096) {  // Assuming recvbuf is of size 4096
        return false;
    }
    // return true;
    // check if recvbuf matches the hash value of /#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0
    // std::string root_path = "/#test-dir.0.d.8.n.31981446.b.4/mdtest_tree.0";
    size_t pos = 0;
    int count = 0;
    while (pos != std::string::npos && count < 3) {
        pos = path.find("/", pos);
        if (pos != std::string::npos) {
            count++;
            pos++; // Move past the current slash
        }
    }
        // printf("[line %d] compare_key_match for path: %s pos:%ld \n", __LINE__, path.c_str(), pos);

    if (pos == std::string::npos) {
        return true;
    }
    std::string root_path = path.substr(0, pos);
    auto check_key = computeMD5(root_path);
    int counter = 0;
    for (int i = 0; i < size_of_key; i++) {
        if (static_cast<unsigned char>(recvbuf[offset + i]) != static_cast<unsigned char>(check_key[i])) {
            break;
        }
        counter ++;
    }
    if (counter == size_of_key) {
        return true;  // Keys match
    }
    for (int i = 0; i < size_of_key; i++) {
        if (last_key[i] != static_cast<unsigned char>(recvbuf[offset + i])) {    
            
            return false;
        }
    }
    return true;  // Keys match
}