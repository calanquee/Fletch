
#include <tbb/concurrent_unordered_map.h>

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
// Qingxiu: comment the following line
RocksdbWrapper* db_wrappers = NULL;
RocksdbWrapper* index_db_wrappers = NULL;

tbb::concurrent_unordered_map<std::string, uint64_t> dir_id_map;

// invalidation list
tbb::concurrent_unordered_map<std::string, uint16_t> invalidation_list;  // path and version number
std::mutex mutex_for_invalidation_list;

// path resolution, check invalidation list, and permission check
std::pair<bool, std::vector<uint16_t>> kvs_path_resolution(std::vector<std::string>& levels, bool is_cached, std::vector<uint16_t> internal_versions, std::vector<uint64_t> parent_ids, uint16_t local_server_logical_idx, uint16_t skip_last_level = 0) {
    // check if any internal path is in the invalidation list
    bool is_invalidated = false;

    // write down the latest version number for each internal path
    std::vector<uint16_t> latest_versions;
    // if (is_cached) {
    for (int i = 0; i < levels.size() - 1; i++) {
        mutex_for_invalidation_list.lock();
        if (invalidation_list.find(levels[i]) != invalidation_list.end()) {
            // check version number
            if (internal_versions[i] < invalidation_list[levels[i]]) {
                is_invalidated = true;
            }
            latest_versions.push_back(invalidation_list[levels[i]]);
        } else {
            latest_versions.push_back(0);
        }
        mutex_for_invalidation_list.unlock();
    }
#ifdef DEBUG_NETFETCH
    printf("[CSCACHE_DEBUG] [line %d] is_invalidated: %s\n", __LINE__, is_invalidated ? "true" : "false");
#endif
    // }

    bool is_permission_check_success = true;
    // std::cout << "is_invalidated: " << is_invalidated <<"is_cached: " << is_cached << std::endl;
    if (!is_invalidated && is_cached) {
        // skip permission check and return latest version number
        return {is_permission_check_success, latest_versions};
    } else {
        // path resolution for the path
        // std::cout << "path resolution for the path parent_ids size: " << parent_ids.size() << std::endl;
        // std::cout << "is_invalidated: " << is_invalidated <<" is_cached: " << is_cached << std::endl;
        // // dump internal_versions
        // for (int i = 0; i < internal_versions.size(); i++) {
        //     std::cout << "internal_versions[" << i << "]: " << internal_versions[i] << std::endl;
        // }
        val_t tmp_internal_val;

        for (int i = 0; i < levels.size() - skip_last_level; ++i) {
            bool result = db_wrappers[local_server_logical_idx].get_path_with_id(uint64_to_hex(parent_ids[i]), levels[i], tmp_internal_val);
            // if (result == false) {
            //     printf("get_path_with_id failed, %s:%s not exist\n", uint64_to_hex(parent_ids[i]).c_str(), levels[i].c_str());
            // }
            if (tmp_internal_val.val_length > 0) {
                is_permission_check_success = true;
            } else {
                is_permission_check_success = false;
                break;
            }
        }
#ifdef DEBUG_NETFETCH
        printf("[CSCACHE_DEBUG] [line %d] is_permission_check_success: %s\n", __LINE__, is_permission_check_success ? "true" : "false");
#endif
        return {is_permission_check_success, latest_versions};
    }
}

// 将 std::unordered_map 转换为 tbb::concurrent_unordered_map
void convert_to_concurrent_map(
    const std::unordered_map<std::string, uint64_t>& src_map,
    tbb::concurrent_unordered_map<std::string, uint64_t>& dest_map) {
    // 遍历 std::unordered_map 并插入到 tbb::concurrent_unordered_map
    for (const auto& entry : src_map) {
        dest_map.insert(entry);
    }
}
void init_dir_id_mapping(uint16_t local_server_logical_idx) {
    std::unordered_map<std::string, uint64_t> temp_map;
    index_db_wrappers[local_server_logical_idx].load_entire_db(temp_map);
    // convert it to dir_id_map
    convert_to_concurrent_map(temp_map, dir_id_map);
}

std::vector<uint64_t> get_parent_ids_with_all_levels(std::vector<std::string>& levels) {
    // the last level is the path itself skip
    // the first level's parent id is 0 push it into vector and skip
    std::vector<uint64_t> parent_ids;
    parent_ids.push_back(0);  // The first level's parent id is 0

    for (size_t i = 0; i < levels.size() - 1; ++i) {
        auto it = dir_id_map.find(levels[i]);
        if (it != dir_id_map.end()) {
            parent_ids.push_back(it->second);
        } else {
            // Handle the case where the directory is not found
            // For now, we can just return an empty vector
            return std::vector<uint64_t>();
        }
    }

    return parent_ids;
}
// open/ stat
std::pair<bool, std::vector<uint16_t>> kvs_get_path(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // print levels size
    // printf("levels size: %d\n", levels.size());
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx, 1);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    // print last version size
    // printf("[%d]latest_versions size: %d\n",__LINE__, latest_versions.size());
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        bool result = db_wrappers[local_server_logical_idx].get_path_with_id(uint64_to_hex(parent_ids.back()), queried_path, val);
        // if (result == false) {
        // printf("get_path_with_id failed, %s:%s not exist\n", uint64_to_hex(parent_ids.back()), queried_path);
        // }
        return {true, latest_versions};
    }
    return {false, {}};
}
// chmod
std::pair<bool, std::vector<uint16_t>> kvs_update_path(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    // print latest_versions size
    // printf("latest_versions size: %d\n", latest_versions.size());
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        db_wrappers[local_server_logical_idx].put_path_with_id(uint64_to_hex(parent_ids.back()), queried_path, val);
        return {true, latest_versions};
    }
    return {false, {}};
}
// mv file
std::pair<bool, std::vector<uint16_t>> kvs_mv_file(std::string& queried_path, std::string& new_queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    // print latest_versions size
    // printf("latest_versions size: %d\n", latest_versions.size());
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        db_wrappers[local_server_logical_idx].rename_file_with_id(uint64_to_hex(parent_ids.back()), queried_path, new_queried_path, val);
        return {true, latest_versions};
    }
    return {false, {}};
}
// touch
std::pair<bool, std::vector<uint16_t>> kvs_create_path(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx, 1);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    // print latest_versions size
    // printf("latest_versions size: %d\n", latest_versions.size());
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        db_wrappers[local_server_logical_idx].put_path_with_id(uint64_to_hex(parent_ids.back()), queried_path, val);
        return {true, latest_versions};
    }
    return {false, {}};
}
// rm
std::pair<bool, std::vector<uint16_t>> kvs_delete_path(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        db_wrappers[local_server_logical_idx].remove_path_with_id(uint64_to_hex(parent_ids.back()), queried_path);
        return {true, latest_versions};
    }
    return {false, {}};
}
// readdir
std::pair<size_t, std::vector<uint16_t>> kvs_readdir(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {10, std::vector<uint16_t>(levels.size() - 1)};

    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {0, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {10, latest_versions};
    if (!is_permission_check_success) {
        return {0, {}};
    } else {
        auto it = dir_id_map.find(queried_path);
        if (it == dir_id_map.end()) {
            return {0, {}};
        }

        std::vector<std::pair<std::string, val_t>> results;
        size_t ls_res = db_wrappers[local_server_logical_idx].scan_path_with_id(uint64_to_hex(it->second), results);
        if (ls_res == (size_t)-1) {
            return {(size_t)-1, {}};  // fail
        }
        // copy the first 10 results to val
        size_t return_entry_num = std::min((size_t)10, (size_t)results.size());
        if (val.val_data != NULL) {
            delete [] val.val_data;  // free the old val_data
            val.val_length = 0;
            val.val_data = NULL;  // reset val_data to NULL
    
        }

        val.val_data = new char[FILE_META_SIZE * (return_entry_num)];
        for (int i = 0; i < return_entry_num; i++) {
            auto& cp_val = results[i].second;
            memcpy(val.val_data + val.val_length, cp_val.val_data, cp_val.val_length);
            val.val_length += cp_val.val_length;
        }

        INVARIANT(val.val_data != NULL);

        return {return_entry_num, latest_versions};
    }
    return {0, {}};
}
// rmdir
std::pair<bool, std::vector<uint16_t>> kvs_rmdir(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        auto it = dir_id_map.find(queried_path);
        if (it == dir_id_map.end()) {
            return {false, {}};
        }
        uint64_t dir_id = it->second;
        // check whether has son
        if (db_wrappers[local_server_logical_idx].dir_has_son(uint64_to_hex(dir_id))) {
            // delete fail
            return {false, {}};
        }
        // delete it from mapping dir_id_map
        dir_id_map.unsafe_erase(it);
        // delete it from index db
        index_db_wrappers[local_server_logical_idx].remove_directory_index(queried_path);
        // delete it from data db
        db_wrappers[local_server_logical_idx].remove_path_with_id(uint64_to_hex(parent_ids.back()), queried_path);
        return {true, latest_versions};
    }
    return {false, {}};
}
// mkdir
std::pair<bool, std::vector<uint16_t>> kvs_mkdir(std::string& queried_path, val_t& val, std::vector<std::string>& levels, bool is_client_cached, std::vector<uint16_t> internal_versions, uint16_t local_server_logical_idx) {
    // return {true, std::vector<uint16_t>(levels.size() - 1)};
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    // if parent_ids is empty, no parent dir, fail,return empty value
    if (parent_ids.empty()) {
        return {false, {}};
    }

    auto permission_check_results = kvs_path_resolution(levels, is_client_cached, internal_versions, parent_ids, local_server_logical_idx, 1);
    bool is_permission_check_success = permission_check_results.first;
    std::vector<uint16_t> latest_versions = permission_check_results.second;
    // return {true, latest_versions};
    if (!is_permission_check_success) {
        return {false, {}};
    } else {
        auto it = dir_id_map.find(queried_path);
        if (it != dir_id_map.end()) {  // exist
            return {false, {}};
        }

        // delete it from index db
        uint64_t dir_id = index_db_wrappers[local_server_logical_idx].put_directory_index(queried_path);
        // delete it from mapping dir_id_map
        dir_id_map[queried_path] = dir_id;  // insert it
        // delete it from data db
        db_wrappers[local_server_logical_idx].put_path_with_id(uint64_to_hex(parent_ids.back()), queried_path, val);
        return {true, latest_versions};
    }
    return {false, {}};
}

void kvs_admission(val_t& tmp_val, std::vector<std::string>& levels, std::string queried_path_for_admission, uint16_t local_server_logical_idx) {
    auto parent_ids = get_parent_ids_with_all_levels(levels);
    if (parent_ids.empty()) {
        return;
    }
    db_wrappers[local_server_logical_idx].get_path_with_id(uint64_to_hex(parent_ids.back()), queried_path_for_admission, tmp_val);
}

bool kvs_flush_and_compact(uint16_t local_server_logical_idx) {
    bool db_res = db_wrappers[local_server_logical_idx].flush_and_compact();
    bool index_db_res = index_db_wrappers[local_server_logical_idx].flush_and_compact();
    return db_res && index_db_res;
}