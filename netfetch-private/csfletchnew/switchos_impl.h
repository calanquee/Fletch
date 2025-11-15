#ifndef NETFETCH_SWITCHOS_IMPL_H
#define NETFETCH_SWITCHOS_IMPL_H

#include "switchos.h"

/*
    4. function implementation
*/

void prepare_switchos() {
    printf("[switchos] prepare start\n");
    srand(0);  // set random seed as 0 for cache eviction

    // prepare popserver socket
    prepare_udpserver(switchos_popserver_udpsock, false, switchos_popserver_port, "switchos.popserver");

    // prepare evictserver socket
    prepare_udpserver(switchos_evictserver_udpsock, false, switchos_server_eviction_port, "switchos.evictserver");

    // prepare for popworker

    // popworker <-> controller.popserver
    create_udpsock(switchos_popworker_popclient_for_controller_udpsock, true, "switchos.popworker.popclient_for_controller");

    // popworker <-> ptf.popserver
    create_udpsock(switchos_popworker_popclient_for_ptf_udpsock, false, "switchos.popworker.popclient_for_ptf");

    // evictserver <-> ptf.popserver
    create_udpsock(switchos_evictserver_popclient_for_ptf_udpsock, false, "switchos.evictserver.popclient_for_ptf");

    // popworker <-> controller.evictserver
    create_udpsock(switchos_popworker_evictclient_for_controller_udpsock, true, "switchos.popworker.evictclient");

    // evictserver <-> controller.evictserver
    create_udpsock(switchos_evictserver_evictclient_for_controller_udpsock, true, "switchos.evictserver.evictclient");
    // switchos_evictvalbytes = new char[val_t::MAX_VALLEN];
    // INVARIANT(switchos_evictvalbytes != NULL);
    // memset(switchos_evictvalbytes, 0, val_t::MAX_VALLEN);

    // popworker <-> reflector.cp2dpserver
    create_udpsock(switchos_popworker_popclient_for_reflector_udpsock, true, "switchos.popworker.popclient_for_reflector", 0, SWITCHOS_POPCLIENT_FOR_REFLECTOR_TIMEOUT_USECS);
    create_udpsock(switchos_popworker_popclient_for_reflector_ecivtion, true, "switchos.popworker.popclient_for_reflector_eviction", 0, SWITCHOS_POPCLIENT_FOR_REFLECTOR_TIMEOUT_USECS);

    // cached metadata
    switchos_cached_keyidx_map.clear();
    switchos_cached_key_token_mapset.clear();
    switchos_cached_path_token_map.clear();
    switchos_perpipeline_cached_keyarray = (netreach_key_t volatile**)(new netreach_key_t*[switch_pipeline_num]);
    switchos_cache_idxpath_map = (std::string**)(new std::string*[switch_pipeline_num]);
    switchos_perpipeline_cached_tokenarray = (token_t volatile**)(new token_t*[switch_pipeline_num]);
    switchos_perpipeline_cached_serveridxarray = (uint16_t volatile**)new uint16_t*[switch_pipeline_num];
    switchos_perpipeline_cached_empty_index = new uint32_t[switch_pipeline_num];
    for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
        switchos_perpipeline_cached_keyarray[tmp_pipeidx] = new netreach_key_t[switch_kv_bucket_num * MAX_CACHE_PER_REG]();
        switchos_perpipeline_cached_serveridxarray[tmp_pipeidx] = new uint16_t[switch_kv_bucket_num * MAX_CACHE_PER_REG];
        memset((void*)switchos_perpipeline_cached_serveridxarray[tmp_pipeidx], 0, sizeof(uint16_t) * switch_kv_bucket_num * MAX_CACHE_PER_REG);
        switchos_perpipeline_cached_empty_index[tmp_pipeidx] = 0;
        switchos_cache_idxpath_map[tmp_pipeidx] = new std::string[switch_kv_bucket_num * MAX_CACHE_PER_REG]();
        switchos_perpipeline_cached_tokenarray[tmp_pipeidx] = new token_t[switch_kv_bucket_num * MAX_CACHE_PER_REG]();
    }

    // for new structure
    row_state_array = new std::vector<row_t>*[switch_pipeline_num];
    for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
        row_state_array[tmp_pipeidx] = new std::vector<row_t>(switch_kv_bucket_num, {0, false});
    }

    memory_fence();

    printf("[switchos] prepare end\n");
}

void close_switchos() {
    if (switchos_perpipeline_cached_keyarray != NULL) {
        for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
            if (switchos_perpipeline_cached_keyarray[tmp_pipeidx] != NULL) {
                delete[] switchos_perpipeline_cached_keyarray[tmp_pipeidx];
                switchos_perpipeline_cached_keyarray[tmp_pipeidx] = NULL;
            }
        }
        delete[] switchos_perpipeline_cached_keyarray;
        switchos_perpipeline_cached_keyarray = NULL;
    }
    if (switchos_perpipeline_cached_serveridxarray != NULL) {
        for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
            if (switchos_perpipeline_cached_serveridxarray[tmp_pipeidx] != NULL) {
                delete[] switchos_perpipeline_cached_serveridxarray[tmp_pipeidx];
                switchos_perpipeline_cached_serveridxarray[tmp_pipeidx] = NULL;
            }
        }
        delete[] switchos_perpipeline_cached_serveridxarray;
        switchos_perpipeline_cached_serveridxarray = NULL;
    }

    // free new structures
    for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
        delete row_state_array[tmp_pipeidx];
        row_state_array[tmp_pipeidx] = NULL;
    }
    delete[] row_state_array;
    row_state_array = NULL;

    if (switchos_cache_idxpath_map != NULL) {
        for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
            if (switchos_cache_idxpath_map[tmp_pipeidx] != NULL) {
                delete[] switchos_cache_idxpath_map[tmp_pipeidx];
                switchos_cache_idxpath_map[tmp_pipeidx] = NULL;
            }
        }
        delete[] switchos_cache_idxpath_map;
        switchos_cache_idxpath_map = NULL;
    }
    if (switchos_perpipeline_cached_tokenarray != NULL) {
        for (uint32_t tmp_pipeidx = 0; tmp_pipeidx < switch_pipeline_num; tmp_pipeidx++) {
            if (switchos_perpipeline_cached_tokenarray[tmp_pipeidx] != NULL) {
                delete[] switchos_perpipeline_cached_tokenarray[tmp_pipeidx];
                switchos_perpipeline_cached_tokenarray[tmp_pipeidx] = NULL;
            }
        }
        delete[] switchos_perpipeline_cached_tokenarray;
        switchos_perpipeline_cached_tokenarray = NULL;
    }
}

inline uint32_t serialize_add_cache_lookup(char* buf, netreach_key_t key, idx_t switchos_freeidx, token_t token = 0) {
    memcpy(buf, &SWITCHOS_ADD_CACHE_LOOKUP, sizeof(int));
    memcpy(buf + sizeof(int), &token, sizeof(token_t));
    uint32_t tmp_keysize = key.serialize(buf + sizeof(token_t) + sizeof(int), MAX_BUFSIZE - sizeof(int) - sizeof(token_t));
    memcpy(buf + sizeof(int) + sizeof(token_t) + tmp_keysize, (void*)&switchos_freeidx.idx_for_regs, sizeof(uint16_t));
    memcpy(buf + sizeof(int) + sizeof(token_t) + tmp_keysize + sizeof(uint16_t), (void*)&switchos_freeidx.bitmap, sizeof(uint16_t));
    memcpy(buf + sizeof(int) + sizeof(token_t) + tmp_keysize + sizeof(uint16_t) + sizeof(uint16_t), (void*)&switchos_freeidx.idx_for_latest_deleted_vallen, sizeof(uint16_t));
    return sizeof(int) + sizeof(token_t) + tmp_keysize + sizeof(uint16_t) + sizeof(uint16_t) + sizeof(uint16_t);
}

inline uint32_t serialize_remove_cache_lookup(char* buf, netreach_key_t key, idx_t switchos_freeidx, token_t token = 0) {
    memcpy(buf, &SWITCHOS_REMOVE_CACHE_LOOKUP, sizeof(int));
    memcpy(buf + sizeof(int), &token, sizeof(token_t));
    uint32_t tmp_keysize = key.serialize(buf + sizeof(token_t) + sizeof(int), MAX_BUFSIZE - sizeof(int) - sizeof(token_t));
    memcpy(buf + sizeof(int) + sizeof(token_t) + tmp_keysize, (void*)&switchos_freeidx.idx_for_latest_deleted_vallen, sizeof(uint16_t));
    return sizeof(int) + sizeof(token_t) + tmp_keysize + sizeof(uint16_t);
}

// for key_t generation
netreach_key_t generate_key_t_for_cachepop(std::string& new_path) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    uint32_t keylolo, keylohi, keyhilo, keyhihi, keyhihi_final;
    MD5(reinterpret_cast<const unsigned char*>(new_path.c_str()), new_path.length(), digest);
    memcpy(&keylolo, digest, sizeof(uint32_t));
    memcpy(&keylohi, digest + sizeof(uint32_t), sizeof(uint32_t));
    memcpy(&keyhilo, digest + 2 * sizeof(uint32_t), sizeof(uint32_t));
    memcpy(&keyhihi, digest + 3 * sizeof(uint32_t), sizeof(uint32_t));
    keylolo = ntohl(keylolo);
    keylohi = swap_bytes(keylohi);
    netreach_key_t new_path_key = netreach_key_t(keylolo, keylohi);
    return new_path_key;
}

// for bitmap print
std::string print_bits(uint16_t num) {
    std::string bits;
    for (int i = 15; i >= 0; i--) {
        // Append '1' or '0' to the string
        bits += (num >> i) & 1 ? '1' : '0';
    }
    return bits;
}

bool isBitSet(uint16_t value, uint8_t bitIndex) {
    if (bitIndex < 16) {
        return (value & (1 << bitIndex)) != 0;
    } else {
        std::cerr << "Bit index out of range" << std::endl;
        return false;
    }
}

bool has_five_ones(uint16_t num) {
    int count = 0;
    for (int i = 0; i < 16; ++i) {
        if (num & (1 << i)) {
            count++;
        }
        if (count > 5) {
            return false;
        }
    }
    return count == 5;
}

// find the highest two ones in a 16-bit number
std::pair<uint8_t, uint8_t> findHighestTwoOnes(uint16_t x) {
    uint8_t first_highest = 16;
    uint8_t second_highest = 16;
    for (uint8_t i = 15; i >= 0; --i) {
        if (x & (1 << i)) {
            if (first_highest == 16) {
                first_highest = i;
            } else {
                second_highest = i;
                break;
            }
        }
    }
    return std::make_pair(first_highest, second_highest);
}

void print_row_state_array(std::vector<uint16_t> tmp_pipeidxes) {
    for (uint16_t tmp_pipeidx : tmp_pipeidxes) {
        printf("row_state_array[%d]:\n", tmp_pipeidx);
        for (size_t i = 0; i < switch_kv_bucket_num; i++) {
            printf("row_state_array[%d][%d]: %s, %d\n", tmp_pipeidx, i, print_bits(row_state_array[tmp_pipeidx]->at(i).bitmap).c_str(), row_state_array[tmp_pipeidx]->at(i).is_full);
        }
    }
}

std::pair<uint32_t, int> find_logical_physical_server_idx_for_file(std::string path) {
    std::pair<uint32_t, uint32_t> tmp_server_idx;
    uint32_t tmp_global_server_logical_idx;
    int tmp_server_physical_idx;

    auto testHash = computeMD5(path);
    tmp_global_server_logical_idx = mapToNameNodeidx(testHash);
    // TMPDEBUG
    if (Patch::patch_mode != 1) {
        bool is_valid = false;
        for (int i = 0; i < valid_global_server_logical_idxes.size(); i++) {
            if (tmp_global_server_logical_idx == valid_global_server_logical_idxes[i]) {
                is_valid = true;
                break;
            }
        }
        INVARIANT(is_valid == true);
    }
    // find physical server idx
    if (Patch::patch_mode != 1) {
        for (int i = 0; i < server_physical_num; i++) {
            for (int j = 0; j < server_logical_idxes_list[i].size(); j++) {
                if (server_logical_idxes_list[i][j] == tmp_global_server_logical_idx) {
                    tmp_server_physical_idx = i;
                    break;
                }
            }
        }
    } else {
        tmp_server_physical_idx = (Patch::patch_bottleneck_id == tmp_global_server_logical_idx ? 0 : 1);
        tmp_global_server_logical_idx = tmp_server_physical_idx;
    }
    INVARIANT(tmp_server_physical_idx != -1);

    tmp_server_idx = std::make_pair(tmp_global_server_logical_idx, tmp_server_physical_idx);
    return tmp_server_idx;
}

std::pair<uint32_t, int> find_logical_physical_server_idx_for_dir(std::string path) {
    std::pair<uint32_t, uint32_t> tmp_server_idx;
    uint32_t tmp_global_server_logical_idx;
    int tmp_server_physical_idx;
    // generate key for path
    netreach_key_t tmpkey = generate_key_t_for_cachepop(path);
    tmp_global_server_logical_idx = tmpkey.get_hashpartition_idx(switch_partition_count, max_server_total_logical_num);
    // TMPDEBUG
    if (Patch::patch_mode != 1) {
        bool is_valid = false;
        for (int i = 0; i < valid_global_server_logical_idxes.size(); i++) {
            if (tmp_global_server_logical_idx == valid_global_server_logical_idxes[i]) {
                is_valid = true;
                break;
            }
        }
        INVARIANT(is_valid == true);
    }
    // find physical server idx
    if (Patch::patch_mode != 1) {
        for (int i = 0; i < server_physical_num; i++) {
            for (int j = 0; j < server_logical_idxes_list[i].size(); j++) {
                if (server_logical_idxes_list[i][j] == tmp_global_server_logical_idx) {
                    tmp_server_physical_idx = i;
                    break;
                }
            }
        }
    } else {
        tmp_server_physical_idx = (Patch::patch_bottleneck_id == tmp_global_server_logical_idx ? 0 : 1);
        tmp_global_server_logical_idx = tmp_server_physical_idx;
    }
    INVARIANT(tmp_server_physical_idx != -1);

    tmp_server_idx = std::make_pair(tmp_global_server_logical_idx, tmp_server_physical_idx);
    return tmp_server_idx;
}

std::vector<std::pair<uint16_t, uint32_t>> get_pipeline_logical_idx_for_admitted_keys(std::vector<netreach_key_t> admitted_keys, std::vector<val_t> tmp_val) {
    std::vector<std::pair<uint16_t, uint32_t>> tmp_pipeidxes_logicalidxes;
    uint32_t tmp_global_server_logical_idx;
    uint32_t tmp_server_physical_idx;
    uint16_t tmp_pipeidx;
    for (size_t i = 0; i < tmp_val.size(); i++) {
        if (tmp_val[i].val_length == file_metadata_length) {
            std::pair<uint32_t, int> tmp_server_idx = find_logical_physical_server_idx_for_file(newly_admitted_paths[i]);
            tmp_global_server_logical_idx = tmp_server_idx.first;
            tmp_server_physical_idx = tmp_server_idx.second;
            tmp_pipeidx = server_pipeidxes[tmp_server_physical_idx];
            tmp_pipeidxes_logicalidxes.push_back(std::make_pair(tmp_pipeidx, tmp_global_server_logical_idx));
        } else if (tmp_val[i].val_length == dir_metadata_length) {
            std::pair<uint32_t, int> tmp_server_idx = find_logical_physical_server_idx_for_dir(newly_admitted_paths[i]);
            tmp_global_server_logical_idx = tmp_server_idx.first;
            tmp_server_physical_idx = tmp_server_idx.second;
            tmp_pipeidx = server_pipeidxes[tmp_server_physical_idx];
            tmp_pipeidxes_logicalidxes.push_back(std::make_pair(tmp_pipeidx, tmp_global_server_logical_idx));
        } else {
            printf("[NETFETCH_SWITCHOS_ERROR] [line %d] val_length %d is not supported, set it 40 bit zero\n", __LINE__, tmp_val[i].val_length);
            std::pair<uint32_t, int> tmp_server_idx = find_logical_physical_server_idx_for_file(newly_admitted_paths[i]);
            tmp_global_server_logical_idx = tmp_server_idx.first;
            tmp_server_physical_idx = tmp_server_idx.second;
            tmp_pipeidx = server_pipeidxes[tmp_server_physical_idx];
            tmp_pipeidxes_logicalidxes.push_back(std::make_pair(tmp_pipeidx, tmp_global_server_logical_idx));
            tmp_val[i].val_length = file_metadata_length;
            tmp_val[i].val_data = new char[tmp_val[i].val_length];
            memset(tmp_val[i].val_data, 0, tmp_val[i].val_length);
            // exit(-1);
        }
    }
    return tmp_pipeidxes_logicalidxes;
}

std::pair<uint16_t, uint32_t> get_pipeline_server_idx_for_path(std::string path, uint16_t idx_for_latest_regs) {
    std::pair<uint32_t, int> tmp_server_idx_dir = find_logical_physical_server_idx_for_dir(path);
    std::pair<uint32_t, int> tmp_server_idx_file = find_logical_physical_server_idx_for_file(path);
    uint32_t tmp_global_server_logical_idx_dir = tmp_server_idx_dir.first;
    uint32_t tmp_global_server_logical_idx_file = tmp_server_idx_file.first;
    uint16_t tmp_pipeline_dir = server_pipeidxes[tmp_server_idx_dir.second];
    uint16_t tmp_pipeline_file = server_pipeidxes[tmp_server_idx_file.second];
    uint16_t tmp_global_server_logical_idx;
    uint16_t tmp_pipeidx;
    if (switchos_perpipeline_cached_serveridxarray[tmp_pipeline_dir][idx_for_latest_regs] == tmp_global_server_logical_idx_dir) {
        tmp_global_server_logical_idx = tmp_global_server_logical_idx_dir;
        tmp_pipeidx = tmp_pipeline_dir;
    } else if (switchos_perpipeline_cached_serveridxarray[tmp_pipeline_file][idx_for_latest_regs] == tmp_global_server_logical_idx_file) {
        tmp_global_server_logical_idx = tmp_global_server_logical_idx_file;
        tmp_pipeidx = tmp_pipeline_file;
    } else {
        printf("Error: logical server idx not match\n");
        std::cout << "Dumping idx: " << switchos_freeidxes[tmp_pipeidx].idx_for_latest_deleted_vallen << std::endl;
        std::cout << "Dir: " << tmp_global_server_logical_idx_dir << ", File: " << tmp_global_server_logical_idx_file << std::endl;
        std::cout << "Dir: " << switchos_perpipeline_cached_serveridxarray[tmp_pipeline_dir][idx_for_latest_regs]
                  << ", File: " << switchos_perpipeline_cached_serveridxarray[tmp_pipeline_file][idx_for_latest_regs] << std::endl;
        exit(-1);
    }
    return std::make_pair(tmp_pipeidx, tmp_global_server_logical_idx);
}

// for periodically trigger loading freq.
void* periodicFunction(void* arg) {
    uint32_t freqsize = 0;
    int freqbuf_recvsize = 0;
    int freqack_recvsize = 0;
    char freqbuf[MAX_BUFSIZE];  // communicate with controller.evictserver or reflector.cp2dpserer

    char freqackqbuf[MAX_BUFSIZE];  // communicate with controller.evictserver or reflector.cp2dpserer
    printf("[switchos] load freq. thread start\n");
    switchos_ready_threads++;
    is_timer_running = false;  // for debug
    while (!is_timer_running) {
    }
    while (is_cache_empty) {  // when recv NETCACHE_GETREQ_POP from switch, is_cache_empty will be set to false
    }
    std::cout << "Tree builded" << std::endl;
    create_udpsock(switchos_popworker_popclient_for_reflector_loadfreq_udpsock, true, "switchos.popworker.popclient_for_reflector_loadfreq", 0, SWITCHOS_POPCLIENT_FOR_REFLECTOR_TIMEOUT_USECS);
    // used by udp socket for cache population
    sockaddr_in reflector_cp2dpserver_addr;
    set_sockaddr(reflector_cp2dpserver_addr, inet_addr(reflector_ip_for_switchos), reflector_cp2dpserver_port);
    // print ip and port for test
    // printf("[POPWORKER] [line %d] reflector_cp2dpserver_addr: %s:%d\n", __LINE__, inet_ntoa(reflector_cp2dpserver_addr.sin_addr), ntohs(reflector_cp2dpserver_addr.sin_port));
    int reflector_cp2dpserver_addr_len = sizeof(struct sockaddr);
    while (is_cache_empty) {
    }
    // debug for periodicFunction
    uint32_t tmp_pipeidx = 1;
    if (Patch::patch_mode == 1) {
        is_timer_running = false;
        printf("[switchos] stop load freq.\n");
    }
    while (is_timer_running) {
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[LOADFREQ] [line %d] start loading freq.\n", __LINE__);
#endif
        int counts = FREQUENCY_COUNTER_SIZE;
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[LOADFREQ] [line %d] counts: %d\n", __LINE__, counts);
#endif
        // load frequency counters from data plane
        uint32_t tmp_frequency_counters[FREQUENCY_COUNTER_SIZE];  // 32768 + 16384
        std::string emptypath = "";
        netreach_key_t tmp_emptykey = generate_key_t_for_cachepop(emptypath);
        // std::cout << "Count: " << counts << std::endl;
        for (int i = 0; i < counts; i++) {
            cache_evict_loadfreq_inswitch_t tmp_cache_evict_loadfreq_inswitch_req(CURMETHOD_ID, tmp_emptykey, i);  // key, idx
            freqsize = tmp_cache_evict_loadfreq_inswitch_req.serialize(freqbuf, MAX_BUFSIZE);
            uint16_t tmp_offset = 0;
            packet_type_t tmp_type = packet_type_t::CACHE_EVICT_LOADFREQ_INSWITCH_PERIOD;
            optype_t bigendian_type = htons(static_cast<uint16_t>(tmp_type));
            memcpy(freqbuf + tmp_offset, (void*)&bigendian_type, sizeof(optype_t));
            // layout: optype + keydepth + key + shadowtype
            tmp_offset += sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t);
            memcpy(freqbuf + tmp_offset, (void*)&bigendian_type, sizeof(optype_t));
            // append token
            tmp_offset += sizeof(optype_t) + 25;  // 25 is in-switch header size
            freqsize += sizeof(token_t);
            udpsendto(switchos_popworker_popclient_for_reflector_loadfreq_udpsock, freqbuf, freqsize, 0, &reflector_cp2dpserver_addr, reflector_cp2dpserver_addr_len, "switchos.popworker.popclient_for_reflector_loadfreq");
        }

        int tmp_acknum = 0;
        bool is_timeout = false;
        while (true) {
            is_timeout = udprecvfrom(switchos_popworker_popclient_for_reflector_loadfreq_udpsock, freqackqbuf, MAX_BUFSIZE, 0, NULL, NULL, freqack_recvsize, "switchos.popworker.popclient_for_reflector_loadfreq");
            if (unlikely(is_timeout)) {
                break;
            }
            cache_evict_loadfreq_inswitch_ack_t tmp_cache_evict_loadfreq_inswitch_ack(CURMETHOD_ID, freqackqbuf, freqack_recvsize);
            size_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) - 2;
            uint16_t count_index;
            memcpy(&count_index, freqackqbuf + tmp_offset, sizeof(count_index));
            count_index = ntohs(count_index);
            tmp_acknum += 1;
            tmp_frequency_counters[count_index] = tmp_cache_evict_loadfreq_inswitch_ack.frequency();
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("Receive CACHE_EVICT_LOADFREQ_ACK of index %d, frequency: %d\n", count_index, tmp_frequency_counters[count_index]);
#endif
            if (tmp_acknum >= counts) {
                break;
            }
        }

        // copy frequency counters to shared memory
        produce(tmp_frequency_counters);

        sleep(2);
    }
    return nullptr;
}

std::vector<val_t> fetch_metadata_from_controller(std::vector<netreach_key_t> admitted_keys, uint32_t* tmp_seq, uint16_t tmp_global_server_logical_idx, bool* tmp_stat, bool* is_nlatest_for_largevalue, struct sockaddr_in controller_popserver_addr) {
    std::vector<val_t> tmp_val(admitted_keys.size());
    socklen_t controller_popserver_addrlen = sizeof(struct sockaddr_in);
    netcache_cache_pop_t* tmp_netcache_cache_pop[newly_admitted_paths.size()];
    packet_type_t tmp_type = packet_type_t::NETCACHE_PRE_CACHE_POP;
    optype_t bigendian_type = htons(static_cast<uint16_t>(tmp_type));
    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
        tmp_netcache_cache_pop[i] = new netcache_cache_pop_t(CURMETHOD_ID, admitted_keys[i], uint16_t(tmp_global_server_logical_idx));
        pktsizes[i] = tmp_netcache_cache_pop[i]->serialize(pktbufs[i], MAX_BUFSIZE);
        memcpy(pktbufs[i], (void*)&bigendian_type, sizeof(optype_t));
    }
    uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + sizeof(optype_t);
    // generate CACHE_POP_INSWITCH packets
    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
        uint16_t net_path_length_for_netfetch = htons(newly_admitted_paths[i].length());
        // layout: op + depth + key + shadowtype
        memcpy(&(pktbufs[i][tmp_offset]), &net_path_length_for_netfetch, 2);
        memcpy(&(pktbufs[i][tmp_offset + sizeof(uint16_t)]), newly_admitted_paths[i].c_str(), newly_admitted_paths[i].length());
        // layout plus path_length and path
        pktsizes[i] += sizeof(uint16_t) + newly_admitted_paths[i].length();
#ifdef NETFETCH_POPSERVER_DEBUG
        // print path from tmp_key_path_mapping_ptr
        printf("[POPWORKER] [line %d] path_length_for_netfetch: %d\n", __LINE__, newly_admitted_paths[i].length());
        printf("[POPWORKER] store_path_for_netfetch: %s\n", newly_admitted_paths[i].c_str());
#endif
    }
    // send packets and recv CACHE_POP_INSWITCH_ACK
    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
        while (true) {
            udpsendto(switchos_popworker_popclient_for_controller_udpsock, pktbufs[i], pktsizes[i], 0, &controller_popserver_addr, controller_popserver_addrlen, "switchos.popworker.popclient_for_controller");
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPWORKER] [line %d] sending pktsize: %d\n", __LINE__, pktsizes[i]);
            printf("[POPWORKER] sending pktbuf: ");
            for (int j = 0; j < pktsizes[i]; j++) {
                printf("%02x ", (unsigned char)pktbufs[i][j]);
            }
            printf("\n");
#endif
            bool is_timeout = udprecvfrom(switchos_popworker_popclient_for_controller_udpsock, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.popclient_for_controller");
            if (unlikely(is_timeout)) {
                continue;
            } else {
                packet_type_t tmp_acktype = get_packet_type(ackbuf, ack_recvsize);
                if (tmp_acktype == packet_type_t::NETCACHE_CACHE_POP_ACK) {
                    netcache_cache_pop_ack_t tmp_netcache_cache_pop_ack(CURMETHOD_ID, ackbuf, ack_recvsize);
                    //
                    if (unlikely(tmp_netcache_cache_pop_ack.key() != tmp_netcache_cache_pop[i]->key())) {
                        printf("unmatched key of NETCACHE_CACHE_POP_ACK!\n");
                        continue;
                    } else {
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] ack_recvsize: %d\n", __LINE__, ack_recvsize);
                        printf("[POPWORKER] recv_ackbuf: ");
                        for (int j = 0; j < ack_recvsize; j++) {
                            printf("%02x ", (unsigned char)ackbuf[j]);
                        }
                        printf("\n");
#endif
                        tmp_val[i] = tmp_netcache_cache_pop_ack.val();
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] tmp_val[%d].val_length: %d\n", __LINE__, i, tmp_val[i].val_length);  // print the length of value
#endif
                        if (tmp_val[i].val_length == 0) {
                            // set it
                            tmp_val[i].val_length = dir_metadata_length;
                            tmp_val[i].val_data = new char[tmp_val[i].val_length];
                            memset(tmp_val[i].val_data, 0, tmp_val[i].val_length);
                        }

                        *tmp_seq = tmp_netcache_cache_pop_ack.seq();
                        *tmp_stat = tmp_netcache_cache_pop_ack.stat();
                        *is_nlatest_for_largevalue = false;
                        break;
                    }
                } else if (tmp_acktype == packet_type_t::NETCACHE_CACHE_POP_ACK_NLATEST) {  // for large value
                    netcache_cache_pop_ack_nlatest_t tmp_netcache_cache_pop_ack_nlatest(CURMETHOD_ID, ackbuf, ack_recvsize);
                    if (unlikely(tmp_netcache_cache_pop_ack_nlatest.key() != tmp_netcache_cache_pop[i]->key())) {
                        printf("unmatched key of NETCACHE_CACHE_POP_ACK_NLATEST!\n");
                        continue;
                    } else {
                        tmp_val[i] = tmp_netcache_cache_pop_ack_nlatest.val();  // default value w/ vallen = 0
                        // INVARIANT(tmp_val[i].val_length == 0);
                        if (tmp_val[i].val_length == 0) {
                            // set it
                            tmp_val[i].val_length = dir_metadata_length;
                            tmp_val[i].val_data = new char[tmp_val[i].val_length];
                            memset(tmp_val[i].val_data, 0, tmp_val[i].val_length);
                        }
                        *tmp_seq = tmp_netcache_cache_pop_ack_nlatest.seq();
                        *tmp_stat = tmp_netcache_cache_pop_ack_nlatest.stat();
                        *is_nlatest_for_largevalue = true;
                        break;
                    }
                } else {
                    printf("[ERROR] invalid packet type %x\n", int(tmp_acktype));
                    // exit(-1);
                    continue;
                }
            }
        }
    }
    return tmp_val;
}

std::vector<val_t> real_fetch_metadata_from_controller(std::vector<netreach_key_t> admitted_keys, uint32_t* tmp_seq, std::vector<uint32_t> tmp_global_server_logical_idxes, bool* tmp_stat, bool* is_nlatest_for_largevalue, struct sockaddr_in controller_popserver_addr, int number_of_paths_for_admission) {
    std::vector<val_t> tmp_val(number_of_paths_for_admission);
    socklen_t controller_popserver_addrlen = sizeof(struct sockaddr_in);
    netcache_cache_pop_t* tmp_netcache_cache_pop[newly_admitted_paths.size()];
    packet_type_t tmp_type = packet_type_t::NETCACHE_PRE_CACHE_POP;
    optype_t bigendian_type = htons(static_cast<uint16_t>(tmp_type));
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        tmp_netcache_cache_pop[i] = new netcache_cache_pop_t(CURMETHOD_ID, admitted_keys[i], uint16_t(tmp_global_server_logical_idxes[i]));
        pktsizes[i] = tmp_netcache_cache_pop[i]->serialize(pktbufs[i], MAX_BUFSIZE);
        // memcpy(pktbufs[i], (void*)&bigendian_type, sizeof(optype_t));
    }
    uint32_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + sizeof(optype_t);
    // generate CACHE_POP_INSWITCH packets
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        uint16_t net_path_length_for_netfetch = htons(newly_admitted_paths[i].length());
        // layout: op + depth + key + shadowtype
        memcpy(&(pktbufs[i][tmp_offset]), &net_path_length_for_netfetch, 2);
        memcpy(&(pktbufs[i][tmp_offset + sizeof(uint16_t)]), newly_admitted_paths[i].c_str(), newly_admitted_paths[i].length());
        // layout plus path_length and path
        pktsizes[i] += sizeof(uint16_t) + newly_admitted_paths[i].length();
#ifdef NETFETCH_POPSERVER_DEBUG
        // print path from tmp_key_path_mapping_ptr
        printf("[POPWORKER] [line %d] path_length_for_netfetch: %d\n", __LINE__, newly_admitted_paths[i].length());
        printf("[POPWORKER] store_path_for_netfetch: %s\n", newly_admitted_paths[i].c_str());
#endif
    }
    // send packets and recv CACHE_POP_INSWITCH_ACK
    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
        while (true) {
            udpsendto(switchos_popworker_popclient_for_controller_udpsock, pktbufs[i], pktsizes[i], 0, &controller_popserver_addr, controller_popserver_addrlen, "switchos.popworker.popclient_for_controller");
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPWORKER] [line %d] sending pktsize: %d\n", __LINE__, pktsizes[i]);
            printf("[POPWORKER] sending pktbuf: ");
            for (int j = 0; j < pktsizes[i]; j++) {
                printf("%02x ", (unsigned char)pktbufs[i][j]);
            }
            printf("\n");
#endif
            bool is_timeout = udprecvfrom(switchos_popworker_popclient_for_controller_udpsock, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.popclient_for_controller");
            if (unlikely(is_timeout)) {
                continue;
            } else {
                packet_type_t tmp_acktype = get_packet_type(ackbuf, ack_recvsize);
                if (tmp_acktype == packet_type_t::NETCACHE_CACHE_POP_ACK) {
                    netcache_cache_pop_ack_t tmp_netcache_cache_pop_ack(CURMETHOD_ID, ackbuf, ack_recvsize);
                    if (unlikely(tmp_netcache_cache_pop_ack.key() != tmp_netcache_cache_pop[i]->key())) {
                        printf("unmatched key of NETCACHE_CACHE_POP_ACK!\n");
                        continue;
                    } else {
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] ack_recvsize: %d\n", __LINE__, ack_recvsize);
                        printf("[POPWORKER] recv_ackbuf: ");
                        for (int j = 0; j < ack_recvsize; j++) {
                            printf("%02x ", (unsigned char)ackbuf[j]);
                        }
                        printf("\n");
#endif
                        tmp_val[i] = tmp_netcache_cache_pop_ack.val();
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] tmp_val[%d].val_length: %d\n", __LINE__, i, tmp_val[i].val_length);  // print the length of value
#endif
                        if (tmp_val[i].val_length == 0) {
                            // set it
                            tmp_val[i].val_length = dir_metadata_length;
                            tmp_val[i].val_data = new char[tmp_val[i].val_length];
                            memset(tmp_val[i].val_data, 0, tmp_val[i].val_length);
                        }
                        *tmp_seq = tmp_netcache_cache_pop_ack.seq();
                        *tmp_stat = tmp_netcache_cache_pop_ack.stat();
                        *is_nlatest_for_largevalue = false;
                        break;
                    }
                } else if (tmp_acktype == packet_type_t::NETCACHE_CACHE_POP_ACK_NLATEST) {  // for large value
                    netcache_cache_pop_ack_nlatest_t tmp_netcache_cache_pop_ack_nlatest(CURMETHOD_ID, ackbuf, ack_recvsize);
                    if (unlikely(tmp_netcache_cache_pop_ack_nlatest.key() != tmp_netcache_cache_pop[i]->key())) {
                        printf("unmatched key of NETCACHE_CACHE_POP_ACK_NLATEST!\n");
                        continue;
                    } else {
                        tmp_val[i] = tmp_netcache_cache_pop_ack_nlatest.val();  // default value w/ vallen = 0
                        // INVARIANT(tmp_val[i].val_length == 0);
                        if (tmp_val[i].val_length == 0) {
                            // set it
                            tmp_val[i].val_length = dir_metadata_length;
                            tmp_val[i].val_data = new char[tmp_val[i].val_length];
                            memset(tmp_val[i].val_data, 0, tmp_val[i].val_length);
                        }
                        *tmp_seq = tmp_netcache_cache_pop_ack_nlatest.seq();
                        *tmp_stat = tmp_netcache_cache_pop_ack_nlatest.stat();
                        *is_nlatest_for_largevalue = true;
                        break;
                    }
                } else {
                    printf("[ERROR] invalid packet type %x\n", int(tmp_acktype));
                    // exit(-1);
                    continue;
                }
            }
        }
    }
    return tmp_val;
}

bool set_is_file_admission(std::vector<val_t> tmp_val) {
    bool is_file_admission = false;
    for (int i = 0; i < tmp_val.size(); i++) {
        if (tmp_val[i].val_length == file_metadata_length) {
            is_file_admission = true;
            break;
        }
    }
    return is_file_admission;
}

std::vector<idx_t> get_free_idx(uint32_t space_consumption, bool is_file_admission, std::vector<uint16_t> tmp_pipeidxes) {
    std::vector<idx_t> results;
    // process dir first
    uint16_t internal_dir_number = space_consumption - 1;
    int pipe_idx = 0;
    while (internal_dir_number) {
        uint16_t free_slots = 0;
        for (uint16_t row = 0; row < switch_kv_bucket_num; row++) {
            // start from row[0]
            free_slots = 0;
            row_t& current_row = (*row_state_array[tmp_pipeidxes[pipe_idx]])[row];
            bool is_current_row_full = current_row.is_full;
            if (is_current_row_full) {
                continue;
            } else {
                uint16_t row_bitmap = current_row.bitmap;
                uint16_t combined_bitmap = 0;
                uint16_t initial_bitmap = row_bitmap;  // reverse if allocate failed
                // 1. find a slot for permission data
                bool found_first_slot = false;
                for (uint16_t i = 0; i < REGS_FOR_PERMISSION; i++) {
                    if (((1 << i) & row_bitmap) == 0) {
                        combined_bitmap |= (1 << i);
                        row_bitmap |= (1 << i);
                        free_slots++;
                        found_first_slot = true;
                        break;
                    }
                }
                if (found_first_slot) {
                    current_row.bitmap |= combined_bitmap;
                }
                // 2. find other flots in the rest REGS
                if (found_first_slot) {
                    for (uint16_t i = REGS_FOR_PERMISSION; i < REGS_PER_ROW; i++) {
                        if (((1 << i) & row_bitmap) == 0) {
                            combined_bitmap |= (1 << i);
                            free_slots++;
                            if (free_slots == DIR_SLOTS) {
                                break;
                            }
                        }
                    }
                }
                // 3. if not find enough slots in step2, return back to check in permission regs
                if (free_slots < DIR_SLOTS && found_first_slot) {
                    for (uint16_t i = 0; i < REGS_FOR_PERMISSION; i++) {
                        if (((1 << i) & row_bitmap) == 0) {
                            combined_bitmap |= (1 << i);
                            free_slots++;
                            if (free_slots == DIR_SLOTS) {
                                break;
                            }
                        }
                    }
                }
                // cannot find enough free slots in the last row, return
                if (free_slots < DIR_SLOTS && row == switch_kv_bucket_num - 1) {
                    current_row.bitmap = initial_bitmap;
                    current_row.is_full = true;
                    return results;
                }
                // if not find enough free slots, continue to next row
                if (free_slots < DIR_SLOTS) {
                    // set is_full for current row
                    current_row.bitmap = initial_bitmap;
                    current_row.is_full = true;
                    continue;
                }
                // update row status
                current_row.bitmap |= combined_bitmap;
                idx_t tmp_idx(0, row, combined_bitmap);
                results.push_back(tmp_idx);
                break;
            }
        }
        internal_dir_number--;
        pipe_idx++;
    }

    // process target file or dir
    for (uint16_t row = 0; row < switch_kv_bucket_num; row++) {
        row_t& current_row = (*row_state_array[tmp_pipeidxes[pipe_idx]])[row];
        bool is_current_row_full = current_row.is_full;
        // printf("is_current_row_full = %d\n", is_current_row_full);
        if (is_current_row_full) {
            continue;
        }
        uint16_t row_bitmap = current_row.bitmap;
        uint16_t combined_bitmap = 0;
        uint16_t free_slots = 0;
        uint16_t initial_bitmap = row_bitmap;
        // 1. find a slot for permission data
        bool found_first_slot = false;
        for (uint16_t i = 0; i < REGS_FOR_PERMISSION; i++) {
            if (((1 << i) & row_bitmap) == 0) {
                combined_bitmap |= (1 << i);
                row_bitmap |= (1 << i);
                free_slots++;
                found_first_slot = true;
                break;
            }
        }
        if (found_first_slot) {
            current_row.bitmap |= combined_bitmap;
        }
        // 2. find other flots in the rest REGS
        if (found_first_slot) {
            for (uint16_t i = REGS_FOR_PERMISSION; i < REGS_PER_ROW; i++) {
                if (((1 << i) & row_bitmap) == 0) {
                    combined_bitmap |= (1 << i);
                    free_slots++;
                    if (is_file_admission && free_slots == FILE_SLOTS) {
                        break;
                    } else if (!is_file_admission && free_slots == DIR_SLOTS) {
                        break;
                    }
                }
            }
        }
        // 3. if not find enough slots in step2, return back to check in permission regs
        if (free_slots < (is_file_admission ? FILE_SLOTS : DIR_SLOTS) && found_first_slot) {
            for (uint16_t i = 0; i < REGS_FOR_PERMISSION; i++) {
                if (((1 << i) & row_bitmap) == 0) {
                    combined_bitmap |= (1 << i);
                    free_slots++;
                    if (is_file_admission && free_slots == FILE_SLOTS) {
                        break;
                    } else if (!is_file_admission && free_slots == DIR_SLOTS) {
                        break;
                    }
                }
            }
        }
        if (is_file_admission) {
            // cannot find enough free slots in the last row, return
            if (free_slots < FILE_SLOTS && row == switch_kv_bucket_num - 1) {
                current_row.bitmap = initial_bitmap;
                return results;
            }
            if (free_slots < FILE_SLOTS) {
                current_row.bitmap = initial_bitmap;
                continue;
            }
        } else {
            // cannot find enough free slots in the last row, return
            if (free_slots < DIR_SLOTS && row == switch_kv_bucket_num - 1) {
                current_row.bitmap = initial_bitmap;
                current_row.is_full = true;
                return results;
            }
            if (free_slots < DIR_SLOTS) {
                // set is_full flag
                current_row.bitmap = initial_bitmap;
                current_row.is_full = true;
                continue;
            }
        }
        current_row.bitmap |= combined_bitmap;
        idx_t tmp_idx(0, row, combined_bitmap);
        results.push_back(tmp_idx);
        break;
    }
    return results;
}

void load_freq_for_potential_victims(std::vector<std::string> victims, uint32_t* frequency_counters, std::vector<struct idx_t> victim_idxes, struct sockaddr_in reflector_cp2dpserver_addr) {
    int reflector_cp2dpserver_addr_len = sizeof(struct sockaddr);
    while (true) {
        for (size_t i = 0; i < victims.size(); i++) {
            cache_evict_loadfreq_inswitch_t tmp_cache_evict_loadfreq_inswitch_req(CURMETHOD_ID, generate_key_t_for_cachepop(victims[i]), victim_idxes[i].idx_for_latest_deleted_vallen);
            pktsize = tmp_cache_evict_loadfreq_inswitch_req.serialize(pktbuf, MAX_BUFSIZE);
            uint16_t tmp_offset = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + 25 + sizeof(optype_t);  // 25 is in-switch header size
            pktbuf[tmp_offset] = switchos_cached_path_token_map[victims[i]];
            pktsize += sizeof(token_t);
            udpsendto(switchos_popworker_popclient_for_reflector_ecivtion, pktbuf, pktsize, 0, &reflector_cp2dpserver_addr, reflector_cp2dpserver_addr_len, "switchos.popworker.popclient_for_reflector");
        }
        // loop until receiving corresponding ACK (ignore unmatched ACKs which are duplicate ACKs of previous cache population)
        bool is_timeout = false;
        int tmp_acknum = 0;
        while (true) {
            is_timeout = udprecvfrom(switchos_popworker_popclient_for_reflector_ecivtion, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.popclient_for_reflector");
            if (unlikely(is_timeout)) {
                break;
            }
            cache_evict_loadfreq_inswitch_ack_t tmp_cache_evict_loadfreq_inswitch_ack(CURMETHOD_ID, ackbuf, ack_recvsize);
            for (size_t i = 0; i < victims.size(); i++) {
                if (static_cast<netreach_key_t>(generate_key_t_for_cachepop(victims[i])) == tmp_cache_evict_loadfreq_inswitch_ack.key()) {
                    frequency_counters[i] = tmp_cache_evict_loadfreq_inswitch_ack.frequency();
                    tmp_acknum += 1;
                    break;
                }
            }
            if (tmp_acknum >= victims.size()) {
                break;
            }
        }
        if (unlikely(is_timeout)) {
            continue;
        } else {
            break;
        }
    }
#ifdef NETFETCH_POPSERVER_DEBUG
    // print frequency counters for test
    printf("[POPSERVER] [line %d] frequency counters:\n", __LINE__);
    for (size_t i = 0; i < victims.size(); i++) {
        printf("[POPSERVER] [line %d] path: %s, frequency: %d\n", __LINE__, victims[i].c_str(), frequency_counters[i]);
    }
#endif
}

std::vector<size_t> sort_indexes(const uint32_t* frequency_counters, size_t size) {
    std::vector<size_t> indexes(size);
    for (size_t i = 0; i < size; ++i) {
        indexes[i] = i;
    }
    std::sort(indexes.begin(), indexes.end(),
              [&frequency_counters](size_t a, size_t b) {
                  return frequency_counters[a] < frequency_counters[b];
              });
    return indexes;
}

std::vector<std::pair<std::string, struct idx_t>> find_true_victims(
    std::vector<std::tuple<std::string, double, int>> potential_victims, uint32_t* frequency_counters,
    int number_of_evicted_paths, bool is_file_admission) {
    std::vector<std::pair<std::string, struct idx_t>> newly_evicted_idx;
    std::vector<size_t> sorted_indexes = sort_indexes(frequency_counters, potential_victims.size());

#ifdef NETFETCH_POPSERVER_DEBUG
    printf("[POPSERVER] [line %d] sorted indexes:\n", __LINE__);
    for (size_t i = 0; i < sorted_indexes.size(); i++) {
        printf("[POPSERVER] [line %d] index: %d, frequency: %d\n",
               __LINE__, sorted_indexes[i], frequency_counters[sorted_indexes[i]]);
    }
#endif

    int evicted_count = 0;
    bool has_file_evicted = false;

    for (size_t i = 0; i < sorted_indexes.size(); i++) {
        if (evicted_count >= number_of_evicted_paths) {
            break;
        }
        KeyWithPath candidate_key(generate_key_t_for_cachepop(std::get<0>(potential_victims[sorted_indexes[i]])),
                                  std::get<0>(potential_victims[sorted_indexes[i]]).length(), std::get<0>(potential_victims[sorted_indexes[i]]).c_str());
        struct idx_t victim_idx;
        auto it = switchos_popworker_cached_keyset.find(candidate_key);
        if (it != switchos_popworker_cached_keyset.end()) {
            victim_idx.idx_for_latest_deleted_vallen = it->idx_for_latest_deleted_vallen;
            victim_idx.idx_for_regs = it->idx_for_regs;
            victim_idx.bitmap = it->bitmap;
        }
        // #if 0
        if (is_file_admission) {
            if (!has_five_ones(victim_idx.bitmap)) {
                continue;
            } else {
                has_file_evicted = true;
            }
        }
        // #endif

        if (std::get<2>(potential_victims[sorted_indexes[i]]) > 1 && (number_of_evicted_paths - evicted_count) > 1) {
            if (!has_file_evicted && is_file_admission) {
                continue;
            }
            std::vector<std::string> internal_paths = get_internal_paths(std::get<0>(potential_victims[sorted_indexes[i]]));
            size_t number_of_evicted_internal_path = std::min(number_of_evicted_paths - evicted_count - 1, std::get<2>(potential_victims[sorted_indexes[i]]) - 1);
            for (size_t k = 0; k < number_of_evicted_internal_path; ++k) {
                size_t j = internal_paths.size() - 1 - k;
                KeyWithPath internal_candidate_key(generate_key_t_for_cachepop(internal_paths[j]), internal_paths[j].length(), internal_paths[j].c_str());
                struct idx_t internal_victim_idx;
                auto internal_it = switchos_popworker_cached_keyset.find(internal_candidate_key);
                if (internal_it != switchos_popworker_cached_keyset.end()) {
                    internal_victim_idx.idx_for_latest_deleted_vallen = internal_it->idx_for_latest_deleted_vallen;
                    internal_victim_idx.idx_for_regs = internal_it->idx_for_regs;
                    internal_victim_idx.bitmap = internal_it->bitmap;
                }
                newly_evicted_idx.push_back(std::make_pair(internal_paths[j], internal_victim_idx));
                evicted_count++;
            }
        }

        newly_evicted_idx.push_back(std::make_pair(std::get<0>(potential_victims[sorted_indexes[i]]), victim_idx));
        evicted_count++;
    }

    if (!has_file_evicted && is_file_admission) {
        is_skip_admission = true;
        // return newly_evicted_idx;
    }
    assert(is_skip_admission == false);
    if (evicted_count < number_of_evicted_paths) {
        for (size_t i = 0; i < sorted_indexes.size() && evicted_count < number_of_evicted_paths; i++) {
            // if already evicted, skip
            std::string candidate_str = std::get<0>(potential_victims[sorted_indexes[i]]);
            if (std::any_of(newly_evicted_idx.begin(), newly_evicted_idx.end(),
                            [&candidate_str](const std::pair<std::string, idx_t>& item) {
                                return item.first == candidate_str;
                            })) {
                continue;
            }
            struct idx_t victim_idx;
            KeyWithPath candidate_key(generate_key_t_for_cachepop(std::get<0>(potential_victims[sorted_indexes[i]])),
                                      std::get<0>(potential_victims[sorted_indexes[i]]).length(), std::get<0>(potential_victims[sorted_indexes[i]]).c_str());
            auto it = switchos_popworker_cached_keyset.find(candidate_key);
            if (it != switchos_popworker_cached_keyset.end()) {
                victim_idx.idx_for_latest_deleted_vallen = it->idx_for_latest_deleted_vallen;
                victim_idx.idx_for_regs = it->idx_for_regs;
                victim_idx.bitmap = it->bitmap;
            }
            auto candidate = std::make_pair(std::get<0>(potential_victims[sorted_indexes[i]]), victim_idx);
            if (std::get<2>(potential_victims[sorted_indexes[i]]) > 1 && (number_of_evicted_paths - evicted_count) > 1) {
                std::vector<std::string> internal_paths = get_internal_paths(std::get<0>(potential_victims[sorted_indexes[i]]));
                size_t number_of_evicted_internal_path = std::min(number_of_evicted_paths - evicted_count - 1, std::get<2>(potential_victims[sorted_indexes[i]]) - 1);
                for (size_t k = 0; k < number_of_evicted_internal_path; ++k) {
                    size_t j = internal_paths.size() - 1 - k;
                    KeyWithPath internal_candidate_key(generate_key_t_for_cachepop(internal_paths[j]), internal_paths[j].length(), internal_paths[j].c_str());
                    struct idx_t internal_victim_idx;
                    auto internal_it = switchos_popworker_cached_keyset.find(internal_candidate_key);
                    if (internal_it != switchos_popworker_cached_keyset.end()) {
                        internal_victim_idx.idx_for_latest_deleted_vallen = internal_it->idx_for_latest_deleted_vallen;
                        internal_victim_idx.idx_for_regs = internal_it->idx_for_regs;
                        internal_victim_idx.bitmap = internal_it->bitmap;
                    }
                    newly_evicted_idx.push_back(std::make_pair(internal_paths[j], internal_victim_idx));
                    evicted_count++;
                }
            }
            newly_evicted_idx.push_back(candidate);
            evicted_count++;
        }
    }

    return newly_evicted_idx;
}

bool checkAndSetSkipAdmission(const std::vector<std::tuple<std::string, double, int>>& potential_victims,
                              double tmp_scaled_frequency) {
    bool tmp_skip_admission = true;
    for (const auto& victim : potential_victims) {
        double frequency = std::get<1>(victim);
        if (tmp_scaled_frequency >= frequency) {
            tmp_skip_admission = false;
            break;
        }
    }
    return tmp_skip_admission;
}

void adjust_bitmap_update_rowstates(std::vector<struct idx_t> switchos_evictidx, std::vector<uint16_t> tmp_pipeidxes, bool is_file_admission, size_t number_of_freespace) {
    if (is_file_admission && is_corner_case == false) {
        uint16_t file_idx = 0;
        // find the first row that has five free slots
        for (int i = 0; i < switchos_evictidx.size(); ++i) {
            if (has_five_ones(switchos_evictidx[i].bitmap)) {
                // the last newly file must be file
                switchos_freeidxes[newly_admitted_paths.size() - 1] = switchos_evictidx[i];
                file_idx = i;
                break;
            }
        }
        // for the rest of the newly admitted paths
        int tmp_pt = 0;
        for (int i = 0; i < switchos_evictidx.size(); ++i) {
            if (i == file_idx) {
                continue;
            }
            switchos_freeidxes[tmp_pt + number_of_freespace] = switchos_evictidx[i];
            if (has_five_ones(switchos_freeidxes[tmp_pt + number_of_freespace].bitmap)) {
                // find the highest two ones in the bitmap
                std::pair<uint8_t, uint8_t> result = findHighestTwoOnes(switchos_freeidxes[tmp_pt + number_of_freespace].bitmap);
                uint8_t first_highest = result.first;
                uint8_t second_highest = result.second;
                // update the bitmap
                switchos_freeidxes[tmp_pt + number_of_freespace].bitmap &= ~((1 << first_highest) | (1 << second_highest));
                // update the row state
                row_t& current_row = (*row_state_array[tmp_pipeidxes[i]])[switchos_freeidxes[tmp_pt + number_of_freespace].idx_for_regs];
                current_row.bitmap &= ~((1 << first_highest) | (1 << second_highest));
                current_row.is_full = false;
            }
            tmp_pt++;
        }
    } else {
        for (int i = 0; i < switchos_evictidx.size(); ++i) {
            switchos_freeidxes[i + number_of_freespace] = switchos_evictidx[i];
            if (has_five_ones(switchos_freeidxes[i + number_of_freespace].bitmap)) {
                // find the highest two ones in the bitmap
                std::pair<uint8_t, uint8_t> result = findHighestTwoOnes(switchos_freeidxes[i + number_of_freespace].bitmap);
                uint8_t first_highest = result.first;
                uint8_t second_highest = result.second;
                // update the bitmap
                switchos_freeidxes[i + number_of_freespace].bitmap &= ~((1 << first_highest) | (1 << second_highest));
                // update the row state
                row_t& current_row = (*row_state_array[tmp_pipeidxes[i]])[switchos_freeidxes[i + number_of_freespace].idx_for_regs];
                current_row.bitmap &= ~((1 << first_highest) | (1 << second_highest));
                current_row.is_full = false;
            }
        }
    }
}

std::vector<netreach_key_t> eviction_with_popserver_controller(int number_of_evicted_paths, std::vector<struct idx_t> switchos_evictidx, struct sockaddr_in ptf_popserver_addr, struct sockaddr_in controller_evictserver_addr) {
    // find pipeline and server idx for newly evicted paths
    std::vector<std::pair<uint16_t, uint32_t>> evicted_keys_pipeidxes;
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        INVARIANT(switchos_evictidx[i].idx_for_latest_deleted_vallen >= 0);
        evicted_keys_pipeidxes.push_back(get_pipeline_server_idx_for_path(newly_evicted_paths[i], switchos_evictidx[i].idx_for_latest_deleted_vallen));
    }
#ifdef NETFETCH_POPSERVER_DEBUG
    // dump path and dest server
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        printf("[POPWORKER] [line %d] path: %s, pipeline: %d, server: %d\n", __LINE__, newly_evicted_paths[i].c_str(), evicted_keys_pipeidxes[i].first, evicted_keys_pipeidxes[i].second);
    }

    printf("[POPWORKER] [line %d] evicted_keys_pipeidxes:\n", __LINE__);
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        printf("[POPWORKER] [line %d] path: %s, pipeline: %d, server: %d\n", __LINE__, newly_evicted_paths[i].c_str(), evicted_keys_pipeidxes[i].first, evicted_keys_pipeidxes[i].second);
    }
#endif

    // with popserver
    std::vector<netreach_key_t> cur_evictkey(number_of_evicted_paths);
    int ptf_popserver_addr_len = sizeof(struct sockaddr);
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        cur_evictkey[i] = generate_key_t_for_cachepop(newly_evicted_paths[i]);
        if (switchos_cached_keyidx_map.find(std::make_pair(cur_evictkey[i], switchos_cache_idxpath_map[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen])) == switchos_cached_keyidx_map.end()) {
            printf("[POPWORKER] [line %d] Evicted key %x at kvidx %d is not cached\n", __LINE__, switchos_perpipeline_cached_keyarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen].keyhi, switchos_evictidx[i].idx_for_latest_deleted_vallen);
            exit(-1);
        }
        token_t tmp_token = switchos_perpipeline_cached_tokenarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen];
        ptf_sendsize = serialize_remove_cache_lookup(ptfbuf, cur_evictkey[i], switchos_evictidx[i], tmp_token);
        udpsendto(switchos_popworker_popclient_for_ptf_udpsock, ptfbuf, ptf_sendsize, 0, &ptf_popserver_addr, ptf_popserver_addr_len, "switchos.popworker.popclient_for_ptf");
    }
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        udprecvfrom(switchos_popworker_popclient_for_ptf_udpsock, ptfbuf, MAX_BUFSIZE, 0, NULL, NULL, ptf_recvsize, "switchos.popworker.popclient_for_ptf");
        INVARIANT(*((int*)ptfbuf) == SWITCHOS_REMOVE_CACHE_LOOKUP_ACK);  // wait for SWITCHOS_REMOVE_CACHE_LOOKUP_ACK
    }

    // with controller: sends CACHE_EVICT to controller.evictserver
    socklen_t controller_evictserver_addrlen = sizeof(struct sockaddr_in);
    for (int i = 0; i < number_of_evicted_paths; ++i) {
        netcache_cache_evict_t tmp_netcache_cache_evict(CURMETHOD_ID, cur_evictkey[i], evicted_keys_pipeidxes[i].second);
        pktsize = tmp_netcache_cache_evict.serialize(pktbuf, MAX_BUFSIZE);
        token_t cur_token = switchos_perpipeline_cached_tokenarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen];
        auto evict_path = switchos_cache_idxpath_map[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen];
        memcpy(pktbuf + pktsize, &cur_token, sizeof(token_t));
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPWORKER] [line %d] evicting %s %d\n", __LINE__, evict_path.c_str(), switchos_evictidx[i].idx_for_latest_deleted_vallen);
#endif
        uint16_t length = evict_path.size();
        memcpy(pktbuf + pktsize + sizeof(token_t), &length, sizeof(uint16_t));
        std::copy(evict_path.begin(), evict_path.end(), pktbuf + pktsize + sizeof(uint16_t) + sizeof(token_t));
        pktsize = pktsize + sizeof(token_t) + sizeof(uint16_t) + length;
        while (true) {
            // printf("send NETCACHE_CACHE_EVICT to controller.evictserver\n");
            udpsendto(switchos_popworker_evictclient_for_controller_udpsock, pktbuf, pktsize, 0, &controller_evictserver_addr, controller_evictserver_addrlen, "switchos.popworker.evictclient_for_controller");
            // wait for NETCACHE_CACHE_EVICT_ACK from controller.evictserver
            // NOTE: no concurrent CACHE_EVICTs -> use request-and-reply manner to wait for entire eviction workflow
            bool is_timeout = udprecvfrom(switchos_popworker_evictclient_for_controller_udpsock, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.evictclient_for_controller");
            if (unlikely(is_timeout)) {
                continue;
            } else {
                netcache_cache_evict_ack_t tmp_cache_evict_ack(CURMETHOD_ID, ackbuf, ack_recvsize);
                INVARIANT(tmp_cache_evict_ack.key() == cur_evictkey[i]);
                break;
            }
        }
        // reset keyarray and serveridxarray at evictidx
        auto cur_evictpath = switchos_cache_idxpath_map[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen];
        switchos_cached_keyidx_map.erase(std::make_pair(cur_evictkey[i], cur_evictpath));
        switchos_perpipeline_cached_keyarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen] = netreach_key_t();
        switchos_perpipeline_cached_serveridxarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen] = -1;
        switchos_perpipeline_cached_tokenarray[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen] = 0;
        switchos_cache_idxpath_map[evicted_keys_pipeidxes[i].first][switchos_evictidx[i].idx_for_latest_deleted_vallen] = std::string();
    }

    return cur_evictkey;
}

std::vector<val_t> adjust_val_for_inswitch_admission(std::vector<val_t> tmp_val, size_t number_of_paths_for_admission) {
    std::vector<val_t> adjusted_val(number_of_paths_for_admission);
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        adjusted_val[i].val_length = 128;
        adjusted_val[i].val_data = new char[adjusted_val[i].val_length];
        memset(adjusted_val[i].val_data, 0, adjusted_val[i].val_length);
        uint16_t tmp_offset_for_adjusted_val = 0;
        uint16_t tmp_offset_for_tmp_val = 0;
        for (int j = 0; j < REGS_PER_ROW; ++j) {
            if (isBitSet(switchos_freeidxes[i].bitmap, j)) {
                memcpy(adjusted_val[i].val_data + tmp_offset_for_adjusted_val, tmp_val[i].val_data + tmp_offset_for_tmp_val, 8);
                tmp_offset_for_adjusted_val += 8;
                tmp_offset_for_tmp_val += 8;
            } else {
                tmp_offset_for_adjusted_val += 8;
            }
        }
    }
    return adjusted_val;
}

void send_cache_pop_inswitch(bool is_nlatest_for_largevalue, std::vector<netreach_key_t> admitted_keys, std::vector<val_t> tmp_val, uint32_t* tmp_seq, bool* tmp_stat, sockaddr_in reflector_cp2dpserver_addr, size_t number_of_paths_for_admission, std::vector<val_t> tmp_val_for_isfile) {
    size_t reflector_cp2dpserver_addr_len = sizeof(struct sockaddr);
    cache_pop_inswitch_t* tmp_cache_pop_inswitch[number_of_paths_for_admission];
    bool is_file[number_of_paths_for_admission];
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        // memset(pktbufs[i], 0, MAX_BUFSIZE);
        pktsizes[i] = 0;
        is_file[i] = tmp_val_for_isfile[i].val_length == file_metadata_length;
        // if (is_debug_without_free && is_file[i]) {
        //     std::cout << "[POPWORKER] [line " << __LINE__ << "] is_file[" << i << "] = " << is_file[i] << "for path" << std::endl;
        // }
    }
    if (is_nlatest_for_largevalue == false) {
        for (int i = 0; i < number_of_paths_for_admission; ++i) {
            tmp_cache_pop_inswitch[i] = new cache_pop_inswitch_t(CURMETHOD_ID, admitted_keys[i], tmp_val[i], *tmp_seq, switchos_freeidxes[i].idx_for_latest_deleted_vallen, *tmp_stat, switchos_freeidxes[i].bitmap, switchos_freeidxes[i].idx_for_regs, is_file[i]);
            pktsizes[i] = tmp_cache_pop_inswitch[i]->serialize(pktbufs[i], MAX_BUFSIZE);
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPWORKER] [line %d] switchos_freeidx = %d\n", __LINE__, switchos_freeidxes[i].idx_for_latest_deleted_vallen);
            printf("[POPWORKER] [line %d] switchos_regidx = %d\n", __LINE__, switchos_freeidxes[i].idx_for_regs);
            printf("[POPWORKER] [line %d] pktsize: %d\n", __LINE__, pktsizes[i]);
            printf("[POPWORKER] [line %d] send_pktbuf: ", __LINE__);
            for (int j = 0; j < pktsizes[i]; j++) {
                printf("%02x ", (unsigned char)pktbufs[i][j]);
            }
            printf("\n");
#endif
        }
    }
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        while (true) {
            udpsendto(switchos_popworker_popclient_for_reflector_udpsock, pktbufs[i], pktsizes[i], 0, &reflector_cp2dpserver_addr, reflector_cp2dpserver_addr_len, "switchos.popworker.popclient");
            // loop until receiving corresponding ACK (ignore unmatched ACKs which are duplicate ACKs of previous cache population)
            bool is_timeout = false;
            bool with_correctack = false;
            while (true) {
                is_timeout = udprecvfrom(switchos_popworker_popclient_for_reflector_udpsock, ackbuf, MAX_BUFSIZE, 0, NULL, NULL, ack_recvsize, "switchos.popworker.popclient");
                if (unlikely(is_timeout)) {
                    break;
                }
                cache_pop_inswitch_ack_t tmp_cache_pop_inswitch_ack(CURMETHOD_ID, ackbuf, ack_recvsize);
#ifdef NETFETCH_POPSERVER_DEBUG
                printf("[POPWORKER] [line %d] ack_recvsize: %d\n", __LINE__, ack_recvsize);
                printf("[POPWORKER] [line %d] recv_ackbuf: ", __LINE__);
                for (int j = 0; j < ack_recvsize; j++) {
                    printf("%02x ", (unsigned char)ackbuf[j]);
                }
                printf("\n");
#endif
                if (tmp_cache_pop_inswitch_ack.key() == admitted_keys[i]) {
                    with_correctack = true;
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] recv cache pop inswitch ack\n", __LINE__);
#endif
                    break;
                }
            }

            if (unlikely(is_timeout)) {
                continue;
            }
            if (with_correctack) {
                break;
            }
        }
    }
}

void send_cache_pop_popserver(struct sockaddr_in ptf_popserver_addr, struct sockaddr_in controller_popserver_addr, std::vector<netreach_key_t> admitted_keys, std::vector<uint16_t> tmp_pipeidxes, std::vector<uint32_t> tmp_global_server_logical_idxes, size_t number_of_paths_for_admission, std::vector<std::string>& newly_admit_paths) {
    size_t ptf_popserver_addr_len = sizeof(struct sockaddr);
    size_t controller_popserver_addrlen = sizeof(struct sockaddr);
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        token_t tmp_token = switchos_perpipeline_cached_tokenarray[tmp_pipeidxes[i]][switchos_freeidxes[i].idx_for_latest_deleted_vallen];
        ptf_sendsize = serialize_add_cache_lookup(ptfbuf, admitted_keys[i], switchos_freeidxes[i], tmp_token);
#if 0
        printf("[POPWORKER] [line %d] popserver add key %x %x, %d %d %x\n", __LINE__,
               admitted_keys[i].keyhi, admitted_keys[i].keylo,
               switchos_freeidxes[i].idx_for_latest_deleted_vallen,
               switchos_freeidxes[i].idx_for_regs,
               switchos_freeidxes[i].bitmap);
        // printf("[POPWORKER] [line %d] send_buf  %d: ", __LINE__, ptf_sendsize);
        // for (int j = 0; j < ptf_sendsize; j++) {
        //     printf("%02x ", (unsigned char)ptfbuf[j]);
        // }
        // printf("\n");
#endif
        udpsendto(switchos_popworker_popclient_for_ptf_udpsock, ptfbuf, ptf_sendsize, 0, &ptf_popserver_addr, ptf_popserver_addr_len, "switchos.popworker.popclient_for_ptf");
        udprecvfrom(switchos_popworker_popclient_for_ptf_udpsock, ptfbuf, MAX_BUFSIZE, 0, NULL, NULL, ptf_recvsize, "switchos.popworker.popclient_for_ptf");
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPWORKER] [line %d] recv from ptf popserver; ptf_recvsize: %d\n", __LINE__, ptf_sendsize);
        printf("[POPWORKER] [line %d] recv_buf: ", __LINE__);
        for (int j = 0; j < ptf_sendsize; j++) {
            printf("%02x ", (unsigned char)ptfbuf[j]);
        }
        printf("\n");
#endif
        // INVARIANT(*((int *)ptfbuf) == SWITCHOS_ADD_CACHE_LOOKUP_SETVALID1_ACK); // wait for SWITCHOS_ADD_CACHE_LOOKUP_SETVALID1_ACK
        INVARIANT(*((int*)ptfbuf) == SWITCHOS_ADD_CACHE_LOOKUP_ACK);  // wait for SWITCHOS_ADD_CACHE_LOOKUP_ACK

        // send NETCACHE_CACHE_POP_FINISH to server and wait for ACK
        netcache_cache_pop_finish_t tmp_netcache_cache_pop_finish(CURMETHOD_ID, admitted_keys[i], tmp_global_server_logical_idxes[i]);
        pktsize = tmp_netcache_cache_pop_finish.serialize(pktbuf, MAX_BUFSIZE);
        token_t token = switchos_perpipeline_cached_tokenarray[tmp_pipeidxes[i]][switchos_freeidxes[i].idx_for_latest_deleted_vallen];
// print token and path in a line
#if 0
        printf("[POPWORKER] [line %d] token: %d, path: %s\n", __LINE__, token, newly_admit_paths[i].c_str());
#endif
        auto pop_finish_path = switchos_cache_idxpath_map[tmp_pipeidxes[i]][switchos_freeidxes[i].idx_for_latest_deleted_vallen];
        uint16_t pop_finish_path_length = pop_finish_path.size();
        // jz mark tmp add
        // add token pathlength path
        // notify server token
        memcpy(pktbuf + pktsize, &token, sizeof(token_t));
        memcpy(pktbuf + pktsize + sizeof(token_t), &pop_finish_path_length, sizeof(uint16_t));
        std::copy(pop_finish_path.begin(), pop_finish_path.end(), pktbuf + pktsize + sizeof(token_t) + sizeof(uint16_t));
        // memcpy(pktbuf + pktsize + sizeof(token_t) + sizeof(uint16_t), &tmp_key_path_mapping_ptr->path, tmp_key_path_mapping_ptr->path_length);
        pktsize = pktsize + sizeof(token_t) + sizeof(uint16_t) + pop_finish_path_length;
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPWORKER] [line %d] pktsize: %d\n", __LINE__, pktsize);
        printf("[POPWORKER] [line %d] send_pktbuf: ", __LINE__);
        for (int j = 0; j < pktsize; j++) {
            printf("%02x ", (unsigned char)pktbuf[j]);
        }
        printf("\n");
#endif
        while (true) {
            udpsendto(switchos_popworker_popclient_for_controller_udpsock, pktbuf, pktsize, 0, &controller_popserver_addr, controller_popserver_addrlen, "switchos.popworker.popclient_for_controller");
            bool is_timeout = udprecvfrom(switchos_popworker_popclient_for_controller_udpsock, pop_ackbuf, MAX_BUFSIZE, 0, NULL, NULL, pop_ack_recvsize, "switchos.popworker.popclient_for_controller");
            // get pkt type
            packet_type_t tmp_optype = get_packet_type(pop_ackbuf, pop_ack_recvsize);
            if (unlikely(is_timeout)) {
                continue;
            } else if(tmp_optype != packet_type_t::NETCACHE_CACHE_POP_FINISH_ACK) {
                continue;
            } 
            else {
// #if 1 
//                 printf("[POPWORKER] [line %d] pktsize: %d\n", __LINE__, pop_ack_recvsize);
//                 printf("[POPWORKER] [line %d] recv_pktbuf: ", __LINE__);
//                 for (int j = 0; j < pop_ack_recvsize; j++) {
//                     printf("%02x ", (unsigned char)pop_ackbuf[j]);
//                 }
//                 printf("\n");
// #endif
                netcache_cache_pop_finish_ack_t tmp_netcache_cache_pop_finish_ack(CURMETHOD_ID, pop_ackbuf, pop_ack_recvsize);
                // INVARIANT(tmp_netcache_cache_pop_finish_ack.key() == tmp_netcache_cache_pop_finish.key());
                if (unlikely(!(tmp_netcache_cache_pop_finish_ack.key() == tmp_netcache_cache_pop_finish.key()))) {
                    printf("Unmatched key of NETCACHE_CACHE_POP_FINISH_ACK\n");
                    continue;
                }
#ifdef NETFETCH_POPSERVER_DEBUG
                printf("[POPWORKER] [line %d] recv NETCACHE_CACHE_POP_FINISH_ACK\n", __LINE__);
#endif
                break;
            }
        }
    }
}

void generate_tokens_for_newly_admitted_paths(int number_of_paths_for_admission, std::vector<uint16_t> tmp_pipeidxes, std::vector<netreach_key_t> admitted_keys) {
    // gen token for admit paths
    for (int i = 0; i < number_of_paths_for_admission; ++i) {
        uint16_t token_idx = switchos_freeidxes[i].idx_for_latest_deleted_vallen;
        token_t assign_token = 0;
        // newly_admitted_paths[i] admitted_keys[i] switchos_freeidxes[i]
        switchos_cache_idxpath_map[tmp_pipeidxes[i]][token_idx] = newly_admitted_paths[i];  // pipeidx,idx -> path 5 * switch kv
        if (switchos_cached_path_token_map.find(newly_admitted_paths[i]) != switchos_cached_path_token_map.end()) {
            assign_token = switchos_cached_path_token_map[newly_admitted_paths[i]];
            switchos_perpipeline_cached_tokenarray[tmp_pipeidxes[i]][token_idx] = assign_token;
            continue;
        }
        auto it = switchos_cached_key_token_mapset.find(admitted_keys[i]);
        if (it == switchos_cached_key_token_mapset.end()) {
            // key not exist
            switchos_cached_key_token_mapset[admitted_keys[i]].insert((token_t)1);
            switchos_cached_path_token_map[newly_admitted_paths[i]] = 1;
            assign_token = 1;
        } else {
            // key exist add max +1
            std::set<token_t>& tokens = it->second;
            token_t max_token = *tokens.rbegin();
            tokens.insert(max_token + 1);
            switchos_cached_path_token_map[newly_admitted_paths[i]] = max_token + 1;
            assign_token = max_token + 1;
        }
        switchos_perpipeline_cached_tokenarray[tmp_pipeidxes[i]][token_idx] = assign_token;  // idx (of inswitch KV) -> token (TODO: different switches for distributed case)
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPWORKER] [line %d] assigned token: %d\n", __LINE__, assign_token);
#endif
    }
}

void produce(const uint32_t* new_data) {
    int write_index = 1 - read_index.load();
    std::memcpy(switchos_frequency_counters[write_index], new_data, FREQUENCY_COUNTER_SIZE * sizeof(uint32_t));
    read_index.store(write_index);
}

void consume(uint32_t* output_buffer) {
    int current_read_index = read_index.load();
    std::memcpy(output_buffer, switchos_frequency_counters[current_read_index], FREQUENCY_COUNTER_SIZE * sizeof(uint32_t));
}

TreeNode* build_tree(const std::vector<std::pair<std::string, int>>& path_access_pairs, int root_weight) {
    TreeNode* root = new TreeNode(root_path_for_fs_tree, root_weight, nullptr, 0.0);  // fix bug in iterator
    for (std::vector<std::pair<std::string, int>>::const_iterator it = path_access_pairs.begin();
         it != path_access_pairs.end(); ++it) {
        const std::pair<std::string, int>& pair = *it;
        std::string path = pair.first;
        int weight = pair.second;
        std::istringstream iss(path);
        std::string part;
        TreeNode* current_node = root;
        std::string current_path;
        // printf("processing path: %s\n", path.c_str());
        while (std::getline(iss, part, '/')) {
            if (!part.empty()) {
                current_path += "/" + part;
                // printf("current_path: %s\n", current_path.c_str());
                // printf("current_node->name: %s\n", current_node->name.c_str());
                // Qingxiu: check if the child exists
                bool found = false;
                auto it = current_node->children.find(current_path);
                if (it != current_node->children.end()) {
                    current_node = it->second;
                    found = true;
                }
                // for (auto& child : current_node->children) {
                //     if (child->name == current_path) {
                //         current_node = child;
                //         found = true;
                //         break;
                //     }
                // }
                if (!found) {
                    TreeNode* new_node = new TreeNode(current_path, 0, nullptr, 0.0);
                    current_node->add_child(new_node);
                    current_node = new_node;
                }
            }
        }
        current_node->weight = weight;
    }
    return root;
}

void print_tree(const TreeNode* node, const std::string& prefix) {
    if (!node)
        return;
    std::cout << prefix << node->name << " (weight: " << node->weight << ", scaled_freq: " << node->scaled_frequency << ")" << std::endl;
    // for (const auto& child : node->children) {
    //     print_tree(child, prefix + "    ");
    // }
    for (auto& child : node->children) {
        print_tree(child.second, prefix + "    ");
    }
}

void new_update_scaled_frequency_for_leaf_nodes(
    TreeNode* node,
    std::list<std::tuple<TreeNode*, double, int, bool>>& sorted_path_scaled_frequency_space_tuples,
    uint32_t* frequency_counters) {
    if (!node)
        return;
    if (node->children.empty()) {
        int free_space = 1;
        TreeNode* current = node;
        while (current->parent != nullptr) {
            if (current->parent->children.size() == 1) {
                free_space += 1;
                current = current->parent;
            } else {
                break;
            }
        }
        // obtain weight of current node
        int tmp_idx_for_frequency_counter = 0;
        netreach_key_t key = generate_key_t_for_cachepop(node->name);
        auto it = switchos_popworker_cached_keyset.find(KeyWithPath(key, node->name.length(), node->name.c_str()));
        bool is_file = false;
        if (it != switchos_popworker_cached_keyset.end()) {
            tmp_idx_for_frequency_counter = it->idx_for_latest_deleted_vallen;

            int tmp_frequency = frequency_counters[tmp_idx_for_frequency_counter];
            // file or dir have_five_ones
            // if have 5  free_space 0.6*free_space + 0.4
            if (has_five_ones(it->bitmap)) {
                is_file = true;
                node->scaled_frequency = tmp_frequency;
            } else {
                node->scaled_frequency = tmp_frequency;
            }  // if !have 5  free_space 0.6*free_space
        } else {
            node->scaled_frequency = 0;
        }
        sorted_path_scaled_frequency_space_tuples.emplace(
            sorted_path_scaled_frequency_space_tuples.end(), node, node->scaled_frequency, free_space, is_file);
    } else {
        for (const auto& child : node->children) {
            new_update_scaled_frequency_for_leaf_nodes(child.second, sorted_path_scaled_frequency_space_tuples, frequency_counters);
        }
    }
}

std::vector<std::tuple<std::string, double, int>> findKVictims(
    std::list<std::tuple<TreeNode*, double, int, bool>>& sorted_path_scaled_frequency_space_tuples,
    int k, bool is_file, std::string admitted_path) {
    auto cmp = [](const std::tuple<TreeNode*, double, int, bool>& a,
                  const std::tuple<TreeNode*, double, int, bool>& b) {
        return std::get<1>(a) > std::get<1>(b);
    };

    std::priority_queue<std::tuple<TreeNode*, double, int, bool>, std::vector<std::tuple<TreeNode*, double, int, bool>>, decltype(cmp)> maxHeap(cmp);
    bool found_file = false;
    int heap_size = 0;
    for (const auto& elem : sorted_path_scaled_frequency_space_tuples) {
        if (std::get<0>(elem)->matches_path_prefix(admitted_path)) {
            continue;
        }
        if (new_admit_paths_set.find(std::get<0>(elem)->name) != new_admit_paths_set.end()) {
            continue;
        }
        maxHeap.push(elem);
    }

    std::vector<std::tuple<std::string, double, int>> result;
    while (true) {
        if (maxHeap.empty())
            break;
        if (heap_size >= k)
            break;
        auto elem = maxHeap.top();
        result.push_back(std::make_tuple(std::get<0>(elem)->name, std::get<1>(elem), std::get<2>(elem)));
        heap_size += std::get<2>(elem);
        maxHeap.pop();
        if (std::get<3>(elem)) {
            found_file = true;
        }
    }
    // is_file and not_found_file
    // find a file
    if (found_file == false && is_file == true) {
        while (true) {
            if (maxHeap.empty())
                break;
            if (found_file == true)
                break;
            auto elem = maxHeap.top();

            if (std::get<3>(elem)) {
                result.push_back(std::make_tuple(std::get<0>(elem)->name, std::get<1>(elem), std::get<2>(elem)));
                found_file = true;
            }
            maxHeap.pop();
        }
    }
    return result;
}

void remove_node(std::string path, TreeNode* root) {
    TreeNode* parent = root->find_parent(path);
#if 0
    printf("[FSTREE] [line %d] parent: %s\n", __LINE__, parent->name.c_str());
    printf("[FSTREE] [line %d] removed_node: %s\n", __LINE__, path.c_str());
#endif
    if (parent) {
        auto it = parent->children.find(path);
        if (it != parent->children.end()) {
            parent->children.erase(it);
        }
        delete it->second;
        it->second = nullptr;
    }
}

void add_node(std::string path, TreeNode* root) {
    size_t pos = path.find_last_of('/');
    std::string parent_path = (pos == std::string::npos) ? "/" : path.substr(0, pos);
#ifdef NETFETCH_POPSERVER_DEBUG
    printf("[FSTREE] [line %d] parent: %s\n", __LINE__, parent_path.c_str());
#endif

    TreeNode* parent;
    if (parent_path == "/") {
        parent = root;
    } else {
        parent = root->find_node(parent_path);
    }

    if (parent) {
        if (parent->children.find(path) == parent->children.end()) {
            TreeNode* new_node = new TreeNode(path, 0, parent, 0.0);
            parent->add_child(new_node);
#if 0
            printf("[FSTREE] [line %d] Node '%s' added under parent '%s'\n", __LINE__, path.c_str(), parent_path.c_str());
#endif
        }
    }
}

#endif