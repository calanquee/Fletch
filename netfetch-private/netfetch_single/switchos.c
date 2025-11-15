
#include "switchos_impl.h"

void* run_switchos_popserver(void* param) {
    printf("[switchos.popserver] ready\n");
    switchos_ready_threads++;

    // store path for NetFetch
    char store_path_for_netfetch[MAX_BUFSIZE];
    uint16_t path_length_for_netfetch = 0;

    while (!switchos_running) {
    }

    // Process NETCACHE_GETREQ_POP packet <optype, key, clone_hdr>
    char buf[MAX_BUFSIZE];
    int recvsize = 0;
    while (switchos_running) {
        udprecvfrom(switchos_popserver_udpsock, buf, MAX_BUFSIZE, 0, NULL, NULL, recvsize, "switchos.popserver");
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPSERVER] [line %d] recvsize: %d\n", __LINE__, recvsize);
        printf("[POPSERVER] recvbuf: ");
        for (int i = 0; i < recvsize; i++) {
            printf("%02x ", (unsigned char)buf[i]);
        }
        printf("\n");
#endif
        netcache_getreq_pop_t* tmp_netcache_getreq_pop_ptr = NULL;  // freed by switchos.popworker if the key is not cached
        key_op_path_t* tmp_delete_req = NULL;                       // freed by switchos.popworker after the key is deleted
        packet_type_t tmp_optype = get_packet_type(buf, recvsize);
        keydepth_t tmp_keydepth = get_keydepth(buf, recvsize);
#ifdef NETFETCH_POPSERVER_DEBUG
        printf("[POPSERVER] tmp_keydepth: %d\n", tmp_keydepth);
#endif
        // build key_path_mapping struct
        key_path_mapping* tmp_key_path_mapping_ptr = new key_path_mapping();
        tmp_key_path_mapping_ptr->space_consumption = tmp_keydepth;  // this is useless since for hot report path, its keydepth may be equal to its real keydepth
        if (tmp_optype == packet_type_t::NETCACHE_WARMUPREQ_INSWITCH_POP) {
            netcache_warmupreq_inswitch_pop_t tmp_netcache_warmupreq_inswitch_pop(CURMETHOD_ID, buf, recvsize);
            tmp_netcache_getreq_pop_ptr = new netcache_getreq_pop_t(CURMETHOD_ID, tmp_netcache_warmupreq_inswitch_pop.key());
            // op + depth + key + in-switch header + shadowtype + clone_hdr
            // inswitch_header: 25B right now
            // clone_hdr for netfetch: 8B
            uint32_t offset_for_path_length = sizeof(optype_t) + sizeof(keydepth_t) + sizeof(netreach_key_t) + 25 + sizeof(optype_t) + 8 + sizeof(token_t);
#ifdef NETFETCH_POPSERVER_DEBUG
            // should be  right now
            printf("[POPSERVER] [line %d] offset_for_path_length: %d\n", __LINE__, offset_for_path_length);
#endif
            memcpy(&path_length_for_netfetch, &(buf[offset_for_path_length]), 2);
            path_length_for_netfetch = ntohs(path_length_for_netfetch);
            memcpy(store_path_for_netfetch, &(buf[offset_for_path_length + sizeof(uint16_t)]), path_length_for_netfetch);
            store_path_for_netfetch[path_length_for_netfetch] = '\0';
            tmp_key_path_mapping_ptr->path_length = path_length_for_netfetch;
            memcpy(tmp_key_path_mapping_ptr->path, store_path_for_netfetch, path_length_for_netfetch);
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPSERVER] [line %d] warm_up_path: %s\n", __LINE__, store_path_for_netfetch);
#endif
            // if path is /afterwarmup, we will build filesystem tree since warmup is done
            if (strcmp(store_path_for_netfetch, "/afterwarmup/afterwarmup") == 0) {
                if (!is_fs_tree_ready) {
                    // build file system tree
                    mutex_for_cached_keyset.lock();
                    std::vector<std::pair<std::string, int>> path_access_pairs;
                    for (auto it = switchos_popserver_cached_keyset.begin(); it != switchos_popserver_cached_keyset.end(); ++it) {
                        std::string path(it->path, it->path_length);
                        path_access_pairs.push_back(std::make_pair(path, 0));
                    }
                    mutex_for_cached_keyset.unlock();
                    root = build_tree(path_access_pairs, 0);
                    is_fs_tree_ready = true;
#ifdef NETFETCH_POPSERVER_DEBUG
                    // print file system tree for test
                    printf("[NETFETCH_SWITCHOS_POPSERVER_DEBUG] [line %d] printf FS tree: \n", __LINE__);
                    print_tree(root);
#endif
                }
                // free the memory
                delete tmp_key_path_mapping_ptr;
                tmp_key_path_mapping_ptr = NULL;
                continue;
            }
        } else if (tmp_optype == packet_type_t::NETCACHE_GETREQ_POP) {
            // print
            // continue;
            if (workload_mode == 0) {
                // static pattern does not need change cache for NETCACHE_GETREQ_POP
                continue;
            }
            is_cache_empty = false;
            // modify the buf to delete unrelated key
            char new_buf_without_unrelated_key[MAX_BUFSIZE];
            memcpy(new_buf_without_unrelated_key, buf, sizeof(optype_t) + sizeof(keydepth_t));
            memcpy(new_buf_without_unrelated_key + sizeof(optype_t) + sizeof(keydepth_t),
                   buf + sizeof(optype_t) + sizeof(keydepth_t) + (tmp_keydepth - 1) * sizeof(netreach_key_t),
                   recvsize - (sizeof(optype_t) + sizeof(keydepth_t) + (tmp_keydepth - 1) * sizeof(netreach_key_t)));
            // freed by switchos.popworker if the key is not cached
            tmp_netcache_getreq_pop_ptr = new netcache_getreq_pop_t(CURMETHOD_ID, new_buf_without_unrelated_key, recvsize - (tmp_keydepth - 1) * sizeof(netreach_key_t));
            // get the path length: op + depth + key + clone_hdr(8B)
            uint32_t offset_for_path_length = sizeof(optype_t) + sizeof(keydepth_t) + tmp_keydepth * sizeof(netreach_key_t) + 8 + sizeof(token_t) * tmp_keydepth;
            memcpy(&path_length_for_netfetch, &(buf[offset_for_path_length]), 2);
            path_length_for_netfetch = ntohs(path_length_for_netfetch);
            memcpy(store_path_for_netfetch, &(buf[offset_for_path_length + 2]), path_length_for_netfetch);
            store_path_for_netfetch[path_length_for_netfetch] = '\0';
            // need the full path for NetFetch
            tmp_key_path_mapping_ptr->path_length = path_length_for_netfetch;
            memcpy(tmp_key_path_mapping_ptr->path, store_path_for_netfetch, path_length_for_netfetch);
            // debug tmp
            new_admit_paths_set.insert(std::string(store_path_for_netfetch));

            // printf("[POPSERVER] hot_reported_path: %s\n", store_path_for_netfetch);
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPSERVER] hot_reported_path: %s\n", store_path_for_netfetch);
#endif
        } else if (tmp_optype == packet_type_t::DELETE_SWITCHOS) {  // handle delete requests; will not use this part
            // freed by switchos.popworker after the key is deleted
            tmp_delete_req = netfetch_delete_deserialize_from_buffer(buf, recvsize);
            tmp_netcache_getreq_pop_ptr = new netcache_getreq_pop_t(CURMETHOD_ID, tmp_delete_req->key);
            tmp_key_path_mapping_ptr->path_length = tmp_delete_req->path1_length;
            memcpy(tmp_key_path_mapping_ptr->path, tmp_delete_req->path1, tmp_delete_req->path1_length);
#ifdef NETFETCH_POPSERVER_DEBUG
            printf("[POPSERVER] target path for deletion: %s\n", tmp_delete_req->path1);
#endif
            delete tmp_delete_req;
            tmp_delete_req = NULL;
        } else {
            printf("[switchos.popserver] invalid pkttype: %x\n", optype_t(tmp_optype));
            exit(-1);
        }

        INVARIANT(tmp_netcache_getreq_pop_ptr != NULL);

        // write tmp_key_path_mapping_ptr to message queue
        tmp_key_path_mapping_ptr->getreq_pop_ptr = tmp_netcache_getreq_pop_ptr;
        bool is_cached = false;

        // rewrite the set to store hash(path) and real path
        // set the cached_idx as -1 to avoid periodically load freq. before admission
        KeyWithPath tmp_key_with_path(tmp_key_path_mapping_ptr->getreq_pop_ptr->key(),
                                      tmp_key_path_mapping_ptr->path_length, tmp_key_path_mapping_ptr->path, -1);

        mutex_for_cached_keyset.lock();
        if (switchos_popserver_cached_keyset.find(tmp_key_with_path) == switchos_popserver_cached_keyset.end()) {
            // we need to insert here to avoid duplicated requests
            switchos_popserver_cached_keyset.insert(tmp_key_with_path);
        } else {
            is_cached = true;
        }
        mutex_for_cached_keyset.unlock();

        if (tmp_optype == packet_type_t::NETCACHE_WARMUPREQ_INSWITCH_POP || tmp_optype == packet_type_t::NETCACHE_GETREQ_POP) {
            if (!is_cached) {  // not cached
                // notify switchos.popworker for cache population/eviction
                bool res2 = switchos_netcache_getreq_pop_path_ptr_queue.write(tmp_key_path_mapping_ptr);
                if (!res2) {
                    printf("[switch os] message queue overflow of switchos.switchos_netcache_getreq_pop_path_ptr_queue!\n");
                } else {
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPSERVER] [line %d] write to switchos.switchos_netcache_getreq_pop_path_ptr_queue\n", __LINE__);
#endif
                }
            } else {  // cached or static workload pattern
                delete tmp_key_path_mapping_ptr;
                tmp_key_path_mapping_ptr = NULL;
            }
        } else if (tmp_optype == packet_type_t::DELETE_SWITCHOS) {
            if (is_cached) {
                bool res2 = switchos_netcache_getreq_pop_path_ptr_queue.write(tmp_key_path_mapping_ptr);
                if (!res2) {
                    printf("[switch os] message queue overflow of switchos.switchos_netcache_getreq_pop_path_ptr_queue!\n");
                } else {
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPSERVER] [line %d] write to switchos.switchos_netcache_getreq_pop_path_ptr_queue\n", __LINE__);
#endif
                }
            } else {
                delete tmp_key_path_mapping_ptr;
                tmp_key_path_mapping_ptr = NULL;
            }
        } else {
            printf("[NETFETCH_POPSERVER_ERROR] [line %d] invalid packet type!\n", __LINE__);
        }
    }

    switchos_popserver_finish = true;
    close(switchos_popserver_udpsock);
    pthread_exit(nullptr);
}

void* run_switchos_popworker(void* param) {
    // store path for NetFetch
    char store_path_for_netfetch[MAX_BUFSIZE];

    // used to fetch value from server by controller
    struct sockaddr_in controller_popserver_addr;
    set_sockaddr(controller_popserver_addr, inet_addr(controller_ip_for_switchos), controller_popserver_port_start);
    socklen_t controller_popserver_addrlen = sizeof(struct sockaddr_in);

    // used by udp socket for cache population
    sockaddr_in reflector_cp2dpserver_addr;
    set_sockaddr(reflector_cp2dpserver_addr, inet_addr(reflector_ip_for_switchos), reflector_cp2dpserver_port);
    int reflector_cp2dpserver_addr_len = sizeof(struct sockaddr);

    // used by udpsocket to communicate with ptf.popserver
    sockaddr_in ptf_popserver_addr;
    set_sockaddr(ptf_popserver_addr, inet_addr("127.0.0.1"), switchos_ptf_popserver_port);
    int ptf_popserver_addr_len = sizeof(struct sockaddr);

    // used by popworker.evictclient
    sockaddr_in controller_evictserver_addr;
    set_sockaddr(controller_evictserver_addr, inet_addr(controller_ip_for_switchos), controller_evictserver_port);
    socklen_t controller_evictserver_addrlen = sizeof(struct sockaddr_in);

    // get valid server logical idxes (TMPDEBUG)
    for (int i = 0; i < server_physical_num; i++) {
        for (int j = 0; j < server_logical_idxes_list[i].size(); j++) {
            uint16_t tmp_global_server_logical_idx = server_logical_idxes_list[i][j];
            valid_global_server_logical_idxes.push_back(tmp_global_server_logical_idx);
        }
    }

    printf("[switchos.popworker] ready\n");
    switchos_ready_threads++;

    while (!switchos_running) {
    }

    while (switchos_running) {
        // read from message queue

        key_path_mapping* tmp_key_path_mapping_ptr = switchos_netcache_getreq_pop_path_ptr_queue.read();
        // now we use tmp_key_path_mapping_ptr instead of tmp_netcache_getreq_pop_ptr
        if (tmp_key_path_mapping_ptr != NULL) {
            if (tmp_key_path_mapping_ptr->space_consumption > 0) {  // cache pop
                bool is_debug_without_free = false;
                // calculate global server logical and physical index
                std::pair<uint32_t, int> tmp_server_idx = find_logical_physical_server_idx_for_file(tmp_key_path_mapping_ptr->path);
                uint32_t tmp_global_server_logical_idx = tmp_server_idx.first;
                int tmp_server_physical_idx = tmp_server_idx.second;
                // get the pipeidx for the server
                uint16_t tmp_pipeidx = server_pipeidxes[tmp_server_physical_idx];
#ifdef NETFETCH_POPSERVER_DEBUG
                // print the key and path for test
                printf("[POPWORKER] [line %d] tmp_key_path_mapping_ptr->path: %s\n", __LINE__, tmp_key_path_mapping_ptr->path);
                printf("[POPWORKER] [line %d] tmp_global_server_logical_idx: %d\n", __LINE__, tmp_global_server_logical_idx);
#endif
                auto check_key = tmp_key_path_mapping_ptr->getreq_pop_ptr->key();
                auto check_path = std::string(tmp_key_path_mapping_ptr->path);
                auto check_pair = std::make_pair(check_key, check_path);
                if (switchos_cached_keyidx_map.find(check_pair) != switchos_cached_keyidx_map.end()) {
                    // enter if block when the request is processed and a duplicate packet arrives
                    newly_admitted_paths.clear();
                    delete tmp_key_path_mapping_ptr;
                    tmp_key_path_mapping_ptr = NULL;
                    continue;
                }

                uint32_t tmp_frequency = 10000;  // default frequency for warmup path

                // obtain the real space consumption
                if (tmp_key_path_mapping_ptr->space_consumption > 1) {
                    tmp_frequency = 10000;                                                       // default frequency for hot reported path
                    std::vector<std::string> internal_paths = extractPathLevels(check_path, 0);  // internal paths consist of itself
                    // check if the internal paths are in the cache
                    uint16_t tmp_space_consumption = 1;                     // since the last path is not in the cache
                    for (int i = internal_paths.size() - 2; i >= 0; --i) {  // only check the internal paths except the last one
                        auto check_internal_key = generate_key_t_for_cachepop(internal_paths[i]);
                        auto check_internal_path = std::string(internal_paths[i]);
                        auto check_internal_pair = std::make_pair(check_internal_key, check_internal_path);
                        if (switchos_cached_keyidx_map.find(check_internal_pair) != switchos_cached_keyidx_map.end()) {
                            break;
                        } else {
                            tmp_space_consumption++;
                        }
                    }
                    tmp_key_path_mapping_ptr->space_consumption = tmp_space_consumption;
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] internal_paths.size(): %d\n", __LINE__, internal_paths.size());
                    for (int i = 0; i < internal_paths.size(); ++i) {
                        printf("[POPWORKER] [line %d] internal_paths[%d]: %s\n", __LINE__, i, internal_paths[i].c_str());
                    }
                    printf("[POPWORKER] [line %d] tmp_key_path_mapping_ptr->space_consumption: %d\n", __LINE__, tmp_key_path_mapping_ptr->space_consumption);
#endif
                }

                // compute scaled freq. for newly hot reported path
                double tmp_scaled_frequency = static_cast<double>(tmp_frequency) / tmp_key_path_mapping_ptr->space_consumption;
#ifdef NETFETCH_POPSERVER_DEBUG
                printf("[POPWORKER] [line %d] tmp_scaled_frequency: %f\n", __LINE__, tmp_scaled_frequency);
#endif

                // compute internal paths to cached for newly admitted paths
                if (tmp_key_path_mapping_ptr->space_consumption > 1) {
                    std::string newly_requested_path(tmp_key_path_mapping_ptr->path, tmp_key_path_mapping_ptr->path_length);
                    std::vector<std::string> internal_paths = get_internal_paths(newly_requested_path);
                    // push the last number_of_evicted_paths
                    int start_index = internal_paths.size() - 1;
                    int end_index = internal_paths.size() + 1 - tmp_key_path_mapping_ptr->space_consumption;
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] start_index: %d, end_index: %d\n", start_index, end_index);
#endif
                    for (int i = start_index; i >= end_index; --i) {
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] current idx: %d, admitting path: %s\n", __LINE__, i, internal_paths[i].c_str());
#endif
                        newly_admitted_paths.push_back(internal_paths[i]);
                    }
                }
                // store path into newly_admitted_paths
                newly_admitted_paths.push_back(tmp_key_path_mapping_ptr->path);
#ifdef NETFETCH_POPSERVER_DEBUG

#endif
#if 0
                if (is_cache_empty == false) {
                    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
                        printf("[POPWORKER] [line %d] newly_admitted_paths[%d]: %s\n", __LINE__, i, newly_admitted_paths[i].c_str());
                    }
                }
#endif
                // find corresponding pipeline idx
                tmp_server_physical_idx = -1;
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
                tmp_pipeidx = server_pipeidxes[tmp_server_physical_idx];

                // 0. init some variables for admission and eviction
                std::vector<netreach_key_t> admitted_keys(newly_admitted_paths.size());
                for (int i = 0; i < newly_admitted_paths.size(); ++i) {
                    admitted_keys[i] = generate_key_t_for_cachepop(newly_admitted_paths[i]);
                }
                uint32_t tmp_seq = 0;
                bool tmp_stat = false;
                bool is_nlatest_for_largevalue = false;

                // 1. first fetch metadata from server and use val length to calculate space consumption
                std::vector<val_t> tmp_val = fetch_metadata_from_controller(admitted_keys, &tmp_seq, tmp_global_server_logical_idx, &tmp_stat, &is_nlatest_for_largevalue, controller_popserver_addr);

                // 1.1 compute pipeline index for tmp_val
                std::vector<std::pair<uint16_t, uint32_t>> tmp_pipeidxes_logicalidxes = get_pipeline_logical_idx_for_admitted_keys(admitted_keys, tmp_val);
                std::vector<uint16_t> tmp_pipeidxes(tmp_pipeidxes_logicalidxes.size());
                std::vector<uint32_t> tmp_logicalidxes(tmp_pipeidxes_logicalidxes.size());
                for (int i = 0; i < tmp_pipeidxes_logicalidxes.size(); ++i) {
                    tmp_pipeidxes[i] = tmp_pipeidxes_logicalidxes[i].first;
                    tmp_logicalidxes[i] = tmp_pipeidxes_logicalidxes[i].second;
                }

                // 2. determine we need how much space to cache based on val length
                is_file_admission = set_is_file_admission(tmp_val);
#if 0
                if (is_cache_empty == false) {
                    std::cout << "[" << __LINE__ << "]" << "start admission and eviction" << std::endl;
                }
#endif
                // 3. admission and eviction
                std::vector<idx_t> free_idxes = get_free_idx(tmp_key_path_mapping_ptr->space_consumption, is_file_admission, tmp_pipeidxes);  // first for dir
                if (free_idxes.size() == tmp_key_path_mapping_ptr->space_consumption) {                                                       // with free idx
                                                                                                                                              // update switchos_freeidxes using free_idxes
#if 0
                    if (is_cache_empty == false) {
                        std::cout << "[" << __LINE__ << "]" << "with free idx" << std::endl;
                    }
#endif
                    for (int i = 0; i < free_idxes.size(); ++i) {
                        switchos_freeidxes[i] = free_idxes[i];
                        switchos_freeidxes[i].idx_for_latest_deleted_vallen = switchos_perpipeline_cached_empty_index[tmp_pipeidxes[i]];
                        switchos_perpipeline_cached_empty_index[tmp_pipeidxes[i]] += 1;
                    }
                    // we use number_of_evicted_paths to maintain consistency with else block, but we will not evict keys in current case
                    number_of_evicted_paths = tmp_key_path_mapping_ptr->space_consumption;
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] number_of_evicted_paths: %d\n", __LINE__, number_of_evicted_paths);
#endif
                } else {  // without enough free space
                    is_debug_without_free = true;
#if 0
                    if (is_cache_empty == false) {
                        std::cout << "[" << __LINE__ << "]" << "without enough free space" << std::endl;
                    }
#endif
                    tmp_key_path_mapping_ptr->space_consumption = tmp_key_path_mapping_ptr->space_consumption - free_idxes.size();
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] No free space in cache, need to evict\n", __LINE__);
#endif
                    while (!is_fs_tree_ready) {
                    };

                    // read frequency for all paths
                    uint32_t loaded_frequency_counters[FREQUENCY_COUNTER_SIZE];
                    consume(loaded_frequency_counters);
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] loaded_frequency_counters: \n", __LINE__);
                    for (int i = 0; i < FREQUENCY_COUNTER_SIZE; ++i) {
                        printf("%d ", loaded_frequency_counters[i]);
                    }
                    printf("\n");
#endif

                    // update scaled frequency for all leaf nodes
                    new_update_scaled_frequency_for_leaf_nodes(root, sorted_path_scaled_frequency_space_tuples, loaded_frequency_counters);
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] sorted_path_scaled_frequency_space_tuples.size(): %d\n", __LINE__, sorted_path_scaled_frequency_space_tuples.size());
                    for (auto it = sorted_path_scaled_frequency_space_tuples.begin(); it != sorted_path_scaled_frequency_space_tuples.end(); ++it) {
                        printf("path: %s, scaled_frequency: %f, space_consumption: %d\n", std::get<0>(*it)->name.c_str(), std::get<1>(*it), std::get<2>(*it));
                    }
#endif

                    // prepare more victims to mitigate false positive (cold item becomes hot during one period)
                    int number_of_victims = 2 * tmp_key_path_mapping_ptr->space_consumption + 2;
#if 0
                    printf("[POPWORKER] [line %d] number_of_victims: %d\n", __LINE__, number_of_victims);
#endif

                    // need to check if the newly admitted path will affect some scaled frequency
                    std::vector<std::tuple<std::string, double, int>> potential_victims = findKVictims(  // int for space consumption
                        sorted_path_scaled_frequency_space_tuples, number_of_victims,
                        std::string(tmp_key_path_mapping_ptr->path, tmp_key_path_mapping_ptr->path_length));
#if 0
                    printf("[POPWORKER] [line %d] potential_victims.size(): %zu\n", __LINE__, potential_victims.size());
                    for (size_t i = 0; i < potential_victims.size(); ++i) {
                        printf("[POPWORKER] [line %d] potential_victims[%zu]: name: %s, scaled_freq: %.2f, space_consumption: %d\n",
                               __LINE__, i, std::get<0>(potential_victims[i]).c_str(),
                               std::get<1>(potential_victims[i]),
                               std::get<2>(potential_victims[i]));
                    }
#endif
                    // skip admission if the newly admitted path is not hot enough
                    is_skip_admission = checkAndSetSkipAdmission(potential_victims, tmp_scaled_frequency);
                    if (is_skip_admission) {
#if 0
                        std::cout << "[" << __LINE__ << "]" << "skip admission freq too low" << std::endl;
#endif
                        // abort admission if scaled frequency is too low
                        // delete from popserver_cached_keyset
                        mutex_for_cached_keyset.lock();
                        if (switchos_popserver_cached_keyset.find(KeyWithPath(tmp_key_path_mapping_ptr->getreq_pop_ptr->key(), tmp_key_path_mapping_ptr->path_length, tmp_key_path_mapping_ptr->path)) != switchos_popserver_cached_keyset.end()) {
                            switchos_popserver_cached_keyset.erase(KeyWithPath(tmp_key_path_mapping_ptr->getreq_pop_ptr->key(), tmp_key_path_mapping_ptr->path_length, tmp_key_path_mapping_ptr->path));
                        }
                        mutex_for_cached_keyset.unlock();
                        sorted_path_scaled_frequency_space_tuples.clear();
                        newly_admitted_paths.clear();
                        delete tmp_key_path_mapping_ptr;
                        tmp_key_path_mapping_ptr = NULL;
                        continue;
                    }
#if 0
                    std::cout << "[" << __LINE__ << "]" << "admission freq enough" << std::endl;
#endif
                    // find pipeidx for victims in O(victims.size()) time
                    std::vector<std::string> victims;
                    for (size_t i = 0; i < potential_victims.size(); ++i) {
                        victims.push_back(std::get<0>(potential_victims[i]));
                    }
                    std::vector<struct idx_t> victim_idxes(potential_victims.size());
                    for (size_t i = 0; i < victims.size(); i++) {
                        KeyWithPath tmp_key_with_path(generate_key_t_for_cachepop(victims[i]), victims[i].length(), victims[i].c_str());
                        auto it = switchos_popworker_cached_keyset.find(tmp_key_with_path);
                        if (it != switchos_popworker_cached_keyset.end()) {
                            victim_idxes[i].idx_for_latest_deleted_vallen = it->idx_for_latest_deleted_vallen;
                            victim_idxes[i].idx_for_regs = it->idx_for_regs;
                            victim_idxes[i].bitmap = it->bitmap;
                        } else {
                            printf("[POPWORKER_ERROR] [line %d] sampled path not found in switchos_popworker_cached_keyset\n", __LINE__);
                            exit(-1);
                        }
                    }

                    // load freq. of potential victims
                    uint32_t frequency_counters[victims.size()];
                    load_freq_for_potential_victims(victims, frequency_counters, victim_idxes, reflector_cp2dpserver_addr);
#if 0
                    std::cout << "[" << __LINE__ << "]" << "load freq for potential victims" << std::endl;
#endif
                    // get cache indexes of true victims
                    number_of_evicted_paths = tmp_key_path_mapping_ptr->space_consumption;
                    std::vector<std::pair<std::string, struct idx_t>> newly_evicted_idx = find_true_victims(potential_victims, frequency_counters, number_of_evicted_paths, is_file_admission);
#ifdef NETFETCH_POPSERVER_DEBUG
                    if (!is_skip_admission) {
                        printf("[POPWORKER] [line %d] number_of_evicted_paths: %d\n", __LINE__, number_of_evicted_paths);
                        for (int i = 0; i < newly_evicted_idx.size(); ++i) {
                            printf("[POPWORKER] [line %d] newly_evicted_idx[%d]: %s\n", __LINE__, i, newly_evicted_idx[i].first.c_str());
                        }
                    }
#endif
                    // if (!is_skip_admission) {
                    //     printf("[POPWORKER] [line %d] number_of_evicted_paths: %d\n", __LINE__, number_of_evicted_paths);
                    //     for (int i = 0; i < newly_evicted_idx.size(); ++i) {
                    //         printf("[POPWORKER] [line %d] newly_evicted_idx[%d]: %s\n", __LINE__, i, newly_evicted_idx[i].first.c_str());
                    //     }
                    // } else {
                    //     std::cout << "[" << __LINE__ << "]" << "skip admission for find_true_victims" << std::endl;
                    //     printf("[POPWORKER] [line %d] number_of_evicted_paths: %d\n", __LINE__, number_of_evicted_paths);
                    //     for (int i = 0; i < newly_evicted_idx.size(); ++i) {
                    //         printf("[POPWORKER] [line %d] newly_evicted_idx[%d]: %s\n", __LINE__, i, newly_evicted_idx[i].first.c_str());
                    //     }
                    // }

                    // TODO replace the file
                    // skip admission if cannot find enough victims, only occur in file admission
                    if (is_skip_admission && is_file_admission) {
                    } else {
                    }
                    if (false) {
#ifdef NETFETCH_POPSERVER_DEBUG
                        printf("[POPWORKER] [line %d] skip admission this time\n", __LINE__);
#endif
                        // remove hot reported key from swithcos_popworker_cached_keyset and switchos_popserver_cached_keyset
                        mutex_for_cached_keyset.lock();
                        if (switchos_popserver_cached_keyset.find(KeyWithPath(tmp_key_path_mapping_ptr->getreq_pop_ptr->key(), tmp_key_path_mapping_ptr->path_length, tmp_key_path_mapping_ptr->path)) != switchos_popserver_cached_keyset.end()) {
                            switchos_popserver_cached_keyset.erase(KeyWithPath(tmp_key_path_mapping_ptr->getreq_pop_ptr->key(), tmp_key_path_mapping_ptr->path_length, tmp_key_path_mapping_ptr->path));
                        }
                        mutex_for_cached_keyset.unlock();
                        newly_admitted_paths.clear();
                        sorted_path_scaled_frequency_space_tuples.clear();
                        delete tmp_key_path_mapping_ptr;
                        tmp_key_path_mapping_ptr = NULL;
                        continue;
                    }

                    std::vector<struct idx_t> switchos_evictidx(number_of_evicted_paths);
#if 0
                    std::cout << "[" << __LINE__ << "]" << "number_of_evicted_paths: " << number_of_evicted_paths << std::endl;
                    std::cout << "[" << __LINE__ << "]" << "newly_evicted_idx.size(): " << newly_evicted_idx.size() << std::endl;
#endif
                    for (int i = 0; i < number_of_evicted_paths; ++i) {
                        switchos_evictidx[i] = newly_evicted_idx[i].second;
                        newly_evicted_paths.push_back(newly_evicted_idx[i].first);
                    }
#ifdef NETFETCH_POPSERVER_DEBUG
                    printf("[POPWORKER] [line %d] number_of_evicted_paths: %d\n", __LINE__, number_of_evicted_paths);
                    for (int i = 0; i < number_of_evicted_paths; ++i) {
                        printf("[POPWORKER] [line %d] newly_evicted_paths[%d]: %s\n", __LINE__, i, newly_evicted_paths[i].c_str());
                        printf("[POPWORKER] [line %d] switchos_evictidx[%d]: %d, %d, %s\n", __LINE__, i, switchos_evictidx[i].idx_for_latest_deleted_vallen, switchos_evictidx[i].idx_for_regs, (print_bits(switchos_evictidx[i].bitmap)).c_str());
                    }
#endif
#if 0
                    std::cout << "[" << __LINE__ << "]" << "start eviction" << std::endl;
#endif
                    // evict and reset some cached structures
                    std::vector<netreach_key_t> cur_evictkey = eviction_with_popserver_controller(number_of_evicted_paths, switchos_evictidx, ptf_popserver_addr, controller_evictserver_addr);
#if 0
                    std::cout << "[" << __LINE__ << "]" << "end eviction" << std::endl;
#endif
                    // set freeidx as evictidx for cache popluation later; first use free space
                    for (int i = 0; i < free_idxes.size(); ++i) {
                        switchos_freeidxes[i] = free_idxes[i];
                        switchos_freeidxes[i].idx_for_latest_deleted_vallen = switchos_perpipeline_cached_empty_index[tmp_pipeidxes[i]];
                        switchos_perpipeline_cached_empty_index[tmp_pipeidxes[i]] += 1;
                    }
                    // TODO
                    if (is_skip_admission && is_file_admission) {
                        is_file_admission = false;
                        is_corner_case = true;
#if 0
                        std::cout << "[" << __LINE__ << "]" << "corner case" << std::endl;
#endif
                    }
                    is_corner_case = true;
#if 0
                    std::cout << "[" << __LINE__ << "]" << "adjust bitmap for eviction and admission" << std::endl;
#endif
                    // adjust bitmap for eviction and admission
                    adjust_bitmap_update_rowstates(switchos_evictidx, tmp_pipeidxes, is_file_admission, free_idxes.size());
#ifdef NETFETCH_POPSERVER_DEBUG
                    // print row state array
                    print_row_state_array(tmp_pipeidxes);
                    // print switchos_freeidxes for test
                    for (int i = 0; i < newly_admitted_paths.size(); ++i) {
                        printf("[POPWORKER] [line %d] switchos_freeidx = %d, %d, %s\n", __LINE__,
                               switchos_freeidxes[i].idx_for_latest_deleted_vallen, switchos_freeidxes[i].idx_for_regs, (print_bits(switchos_freeidxes[i].bitmap)).c_str());
                    }
#endif
#if 0
                    std::cout << "[" << __LINE__ << "]" << "update file system tree for eviction" << std::endl;
#endif
                    // update file system tree for eviction
                    auto copy_newly_evicted_paths = newly_evicted_paths;
                    std::sort(copy_newly_evicted_paths.begin(), copy_newly_evicted_paths.end(),
                              [](const std::string& a, const std::string& b) {
                                  return a.size() > b.size();  // 按长度升序
                              });
                    for (int i = 0; i < copy_newly_evicted_paths.size(); ++i) {
                        remove_node(copy_newly_evicted_paths[i], root);
                    }
#ifdef NETFETCH_POPSERVER_DEBUG
                    // print file system tree for test
                    print_tree(root);
#endif

                    mutex_for_cached_keyset.lock();
                    for (int i = 0; i < number_of_evicted_paths; ++i) {
                        inout_paths.insert(newly_evicted_paths[i]);
                        KeyWithPath tmp_cur_evictkey_with_path(cur_evictkey[i], newly_evicted_paths[i].length(), newly_evicted_paths[i].c_str());
                        // std::cout <<"["<<__LINE__<<"]"<< "[line" << __LINE__ << "] try to evict" << newly_evicted_paths[i].c_str() << std::endl;
                        if (switchos_popserver_cached_keyset.find(tmp_cur_evictkey_with_path) == switchos_popserver_cached_keyset.end()) {
                            std::cout << "[" << __LINE__ << "]" << "evict key not found in popserver_cached_keyset" << std::endl;
                        }
                        INVARIANT(switchos_popserver_cached_keyset.find(tmp_cur_evictkey_with_path) != switchos_popserver_cached_keyset.end());

                        switchos_popserver_cached_keyset.erase(tmp_cur_evictkey_with_path);
                    }
                    mutex_for_cached_keyset.unlock();
                    for (int i = 0; i < number_of_evicted_paths; ++i) {
                        KeyWithPath tmp_cur_evictkey_with_path(cur_evictkey[i], newly_evicted_paths[i].length(), newly_evicted_paths[i].c_str());
                        INVARIANT(switchos_popworker_cached_keyset.find(tmp_cur_evictkey_with_path) != switchos_popworker_cached_keyset.end());
                        switchos_popworker_cached_keyset.erase(tmp_cur_evictkey_with_path);
                    }
#if 0
                    std::cout << "[" << __LINE__ << "]" << "prepared for without free idx" << std::endl;
#endif
                }  // end else

                // 4. cache population for new record
                size_t number_of_paths_for_admission = newly_admitted_paths.size();
#ifdef NETFETCH_POPSERVER_DEBUG
                printf("[POPWORKER] [line %d] number_of_paths_for_admission: %d\n", __LINE__, number_of_paths_for_admission);
                // dump path need admission
                for (auto& path : newly_admitted_paths) {
                    printf("[POPWORKER] [line %d] admitted paths: %s\n", __LINE__, path.c_str());
                }
#endif
                for (int i = 0; i < number_of_paths_for_admission; ++i) {
                    INVARIANT(switchos_freeidxes[i].idx_for_latest_deleted_vallen >= 0 && switchos_freeidxes[i].idx_for_latest_deleted_vallen < switch_kv_bucket_num * 5);
                }
                // generate tokens for newly admitted paths
                generate_tokens_for_newly_admitted_paths(number_of_paths_for_admission, tmp_pipeidxes, admitted_keys);

                // fetch metadata again to update cache set in server
                std::vector<val_t> real_val = real_fetch_metadata_from_controller(admitted_keys, &tmp_seq, tmp_logicalidxes, &tmp_stat, &is_nlatest_for_largevalue, controller_popserver_addr, number_of_paths_for_admission);
                if (is_corner_case) {
#if 0
                    std::cout << "[" << __LINE__ << "]" << "corner case real_val" << std::endl;
#endif
                    // modify the last one in real_val to 24B dir metadata
                    real_val[number_of_paths_for_admission - 1].val_length = dir_metadata_length;
                    tmp_val[number_of_paths_for_admission - 1].val_length = dir_metadata_length;
                }
                std::vector<val_t> adjusted_val = adjust_val_for_inswitch_admission(real_val, number_of_paths_for_admission);
                if (is_corner_case) {
#if 0
                    std::cout << "[" << __LINE__ << "]" << "corner case adjusted_val" << std::endl;
#endif
                    adjusted_val[number_of_paths_for_admission - 1].val_length = dir_metadata_length;
                }
                // print admit path and key and token
                // for (int i = 0; i < number_of_paths_for_admission; ++i) {
                //     printf("[POPWORKER] [line %d] admitted path: %s, key: %u, token: %u\n", __LINE__, newly_admitted_paths[i].c_str(), admitted_keys[i], switchos_freeidxes[i].idx_for_latest_deleted_vallen);
                // }
                send_cache_pop_inswitch(is_nlatest_for_largevalue, admitted_keys, adjusted_val, &tmp_seq, &tmp_stat, reflector_cp2dpserver_addr, number_of_paths_for_admission, tmp_val);
                send_cache_pop_popserver(ptf_popserver_addr, controller_popserver_addr, admitted_keys, tmp_pipeidxes, tmp_logicalidxes, number_of_paths_for_admission, newly_admitted_paths);

                // update inswitch cache metadata
                for (int i = 0; i < number_of_paths_for_admission; ++i) {
                    switchos_cached_keyidx_map[std::make_pair(admitted_keys[i], newly_admitted_paths[i])] = switchos_freeidxes[i].idx_for_latest_deleted_vallen;
                    switchos_perpipeline_cached_keyarray[tmp_pipeidxes[i]][switchos_freeidxes[i].idx_for_latest_deleted_vallen] = admitted_keys[i];
                    switchos_perpipeline_cached_serveridxarray[tmp_pipeidxes[i]][switchos_freeidxes[i].idx_for_latest_deleted_vallen] = tmp_logicalidxes[i];
                }

                // update popserver cache set
                if (newly_admitted_paths.size() > 1) {
                    mutex_for_cached_keyset.lock();
                    for (int i = 0; i < newly_admitted_paths.size() - 1; ++i) {
                        KeyWithPath tmp_key_with_path(generate_key_t_for_cachepop(newly_admitted_paths[i]),
                                                      newly_admitted_paths[i].length(), newly_admitted_paths[i].c_str(), -1);
                        if (switchos_popserver_cached_keyset.find(tmp_key_with_path) == switchos_popserver_cached_keyset.end()) {
                            switchos_popserver_cached_keyset.insert(tmp_key_with_path);
                        }
                    }
                    mutex_for_cached_keyset.unlock();
                }

                // update popworker cache set
                for (int i = 0; i < newly_admitted_paths.size(); ++i) {
                    KeyWithPath tmp_key_with_path(generate_key_t_for_cachepop(newly_admitted_paths[i]),
                                                  newly_admitted_paths[i].length(), newly_admitted_paths[i].c_str(),
                                                  switchos_freeidxes[i].idx_for_latest_deleted_vallen, switchos_freeidxes[i].idx_for_regs, switchos_freeidxes[i].bitmap);
                    if (switchos_popworker_cached_keyset.find(tmp_key_with_path) == switchos_popworker_cached_keyset.end()) {
                        switchos_popworker_cached_keyset.insert(tmp_key_with_path);
                    }
                }

                // update fs tree for admission
                if (is_fs_tree_ready) {
                    auto copy_newly_admitted_paths = newly_admitted_paths;
                    std::sort(copy_newly_admitted_paths.begin(), copy_newly_admitted_paths.end(),
                              [](const std::string& a, const std::string& b) {
                                  return a.size() < b.size();
                              });
                    for (int i = 0; i < copy_newly_admitted_paths.size(); ++i) {
                        add_node(copy_newly_admitted_paths[i], root);
                        // if (inout_paths.find(copy_newly_admitted_paths[i]) != inout_paths.end()) {
                        //     // inout_paths.erase(copy_newly_admitted_paths[i]);
                        //     std::cout << "[" << __LINE__ << "]" << "admit evicted path" << copy_newly_admitted_paths[i] << std::endl;
                        // } else if (is_debug_without_free) {
                        //     std::cout << "[" << __LINE__ << "]" << "admit new path" << copy_newly_admitted_paths[i] << std::endl;
                        // }
                    }
#ifdef NETFETCH_POPSERVER_DEBUG
                    // print file system tree for test
                    print_tree(root);
#endif
                }

                // 5. free memory
                delete tmp_key_path_mapping_ptr;
                tmp_key_path_mapping_ptr = NULL;
                newly_admitted_paths.clear();
                newly_evicted_paths.clear();
                sorted_path_scaled_frequency_space_tuples.clear();
                ptf_sendsize = 0;
                ptf_recvsize = 0;
                memset(switchos_freeidxes, 0, sizeof(struct idx_t) * MAX_DEPTH);
                is_skip_admission = false;
                is_file_admission = false;
                is_corner_case = false;

            } else {  // space_consumption = 0, for deletion request; will not use in current version
                if (workload_mode == 0 || workload_mode == 1) {
                    delete tmp_key_path_mapping_ptr;
                    tmp_key_path_mapping_ptr = NULL;
                    continue;
                }
            }  // end else for handling delete requests

        }  // end if for tmp_key_path_mapping_ptr != NULL
        if (tmp_key_path_mapping_ptr != NULL) {
            delete tmp_key_path_mapping_ptr;
            tmp_key_path_mapping_ptr = NULL;
        }

    }  // end while

    // finish popworker

    // send SWITCHOS_PTF_POPSERVER_END to ptf.popserver
    memcpy(ptfbuf, &SWITCHOS_PTF_POPSERVER_END, sizeof(int));
    udpsendto(switchos_popworker_popclient_for_ptf_udpsock, ptfbuf, sizeof(int), 0, &ptf_popserver_addr, ptf_popserver_addr_len, "switchos.popworker.popclient_for_ptf");

    close(switchos_popworker_popclient_for_controller_udpsock);
    close(switchos_popworker_popclient_for_reflector_udpsock);
    close(switchos_popworker_popclient_for_reflector_ecivtion);
    close(switchos_popworker_evictclient_for_controller_udpsock);
    close(switchos_popworker_popclient_for_ptf_udpsock);
    close(switchos_evictserver_popclient_for_ptf_udpsock);
    close(switchos_evictserver_evictclient_for_controller_udpsock);
    pthread_exit(nullptr);
}

int main(int argc, char** argv) {
    parse_ini("config.ini");
    parse_control_ini("control_type.ini");

    // prepare for hash ring
    std::vector<std::string> namespaces;
    Patch::patch_parse_ini();
    if (Patch::patch_mode != 0) {
        for (int i = 0; i < Patch::patch_server_logical_num; ++i) {
            namespaces.push_back("ns" + std::to_string(i + 1));
        }
    } else {
        for (int i = 0; i < server_total_logical_num; ++i) {
            namespaces.push_back("ns" + std::to_string(i + 1));
        }
    }
    constructHashRing(namespaces);

    prepare_switchos();

    pthread_t popserver_thread;
    int ret = pthread_create(&popserver_thread, nullptr, run_switchos_popserver, nullptr);
    if (ret) {
        COUT_N_EXIT("Error: " << ret);
    }

    pthread_t popworker_thread;
    ret = pthread_create(&popworker_thread, nullptr, run_switchos_popworker, nullptr);
    if (ret) {
        COUT_N_EXIT("Error: " << ret);
    }

    pthread_t timerThread;
    ret = pthread_create(&timerThread, nullptr, periodicFunction, nullptr);
    if (ret) {
        COUT_N_EXIT("Error: " << ret);
    }

    while (switchos_ready_threads < switchos_expected_ready_threads)
        sleep(1);
    printf("[switchos] all threads ready\n");

    switchos_running = true;
    is_timer_running = true;

    // connection from controller
    while (!switchos_popserver_finish) {
    }

    switchos_running = false;
    is_timer_running = false;

    void* status;
    int rc = pthread_join(popserver_thread, &status);
    if (rc) {
        COUT_N_EXIT("Error:unable to join," << rc);
    }
    rc = pthread_join(popworker_thread, &status);
    if (rc) {
        COUT_N_EXIT("Error:unable to join," << rc);
    }
    // close timer thread (load freq.)
    rc = pthread_join(timerThread, &status);
    if (rc) {
        COUT_N_EXIT("Error:unable to join," << rc);
    }

    free_common();
    close_switchos();
    printf("[switchos] all threads end\n");
}