
#include <openssl/md5.h>
#include <pthread.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>

#include "../common/dynamic_array.h"
#include "../common/helper.h"
#include "../common/key.h"
#include "../common/metadata.h"
#include "../common/pkt_ring_buffer.h"
#include "../common/rocksdb_wrapper.h"
#include "../common/snapshot_record.h"
#include "../common/val.h"
#include "common_impl.h"

std::unordered_set<std::string> internal_dirs_set;

std::string get_parent_directory(const std::string& file_path) {
    // 找到最后一个斜杠的位置
    std::size_t pos = file_path.find_last_of("/\\");

    // 如果没有斜杠，说明没有父目录，抛出异常或返回空字符串
    if (pos == std::string::npos) {
        return "";  // 没有父目录
    }

    // 返回父目录部分（不包括最后的斜杠）
    return file_path.substr(0, pos);
}

int main(int argc, char** argv) {
    RocksdbWrapper::prepare_rocksdb();
    parse_ini("config.ini");
    parse_control_ini("control_type.ini");
    if (argc != 3) {
        printf("Usage: ./load_local server_physical_idx path_file.out\n");
        exit(-1);
    }
    int server_physical_idx = atoi(argv[1]);
    INVARIANT(server_physical_idx >= 0);
    INVARIANT(server_physical_idx < server_physical_num);

    std::string file_out_path = std::string(argv[2]);

    RocksdbWrapper db_wrapper(NOCACHE_ID);
    RocksdbWrapper index_db_wrapper(NOCACHE_ID);
    printf("init db %d\n", NOCACHE_ID);
    db_wrapper.open(server_physical_idx);
    index_db_wrapper.open_index(server_physical_idx);
    // put 100 paths into db and read them back
    std::ifstream file_out(file_out_path);
    std::string line;

    // create a 24 bytes val, each byte is 0
    val_t dir_val;
    dir_val.val_length = 24;
    dir_val.val_data = new char[24];
    memset(dir_val.val_data, 0, 24);
    for (int i = 0; i < 24; i++) {
        dir_val.val_data[i] = i;
    }
    val_t file_val;
    file_val.val_length = 40;
    file_val.val_data = new char[40];
    memset(file_val.val_data, 0, 40);
    for (int i = 0; i < 40; i++) {
        file_val.val_data[i] = i;
    }

    std::vector<std::string> paths;
    int count = 0;
    while (std::getline(file_out, line)) {
        if (count++ % 1000000 == 0) {
            // break;
            // print progress
            printf("count: %dK\n", count / 1000);
        }
        paths.push_back(line);
        std::vector<std::string> levels = extractPathLevels(line, 2);
        // only need for dirs
        levels.pop_back();
        for (auto& level : levels) {
            if (internal_dirs_set.find(level) != internal_dirs_set.end()) {
                continue;
            }
            internal_dirs_set.insert(level);
        }
    }
    count = 0;
    // get start time
    // convert set_to_vector
    std::vector<std::string> internal_dirs(internal_dirs_set.begin(), internal_dirs_set.end());
    auto start = std::chrono::high_resolution_clock::now();
    sort(internal_dirs.begin(), internal_dirs.end());
    index_db_wrapper.put_dirs_index(internal_dirs);
    // get end time
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    printf("index Elapsed time: %f seconds\n", elapsed.count());
    std::vector<uint64_t> dir_ids;
    index_db_wrapper.get_dirs_index(internal_dirs, dir_ids);
    // generate a mapping
    std::unordered_map<std::string, uint64_t> dir_id_map;
    for (int i = 0; i < internal_dirs.size(); i++) {
        dir_id_map[internal_dirs[i]] = dir_ids[i];
    }

    // for (int i = 0; i < 10; i++) {
    // printf("dir: %s, id: %lu\n", internal_dirs[i].c_str(), dir_ids[i]);
    // }
    start = std::chrono::high_resolution_clock::now();
    for (auto& dir : internal_dirs) {
        auto parent_dir = get_parent_directory(dir);
        auto it = dir_id_map.find(parent_dir);
        if (it == dir_id_map.end()) {
            // it is the root dir
            db_wrapper.put_path_with_id(uint64_to_hex((uint64_t)0), dir, dir_val);
        }else{
            db_wrapper.put_path_with_id(uint64_to_hex(it->second), dir, dir_val);
        }
        // db_wrapper.put_path_with_id(uint64_to_hex(dir_id_map[get_parent_directory(dir)]), dir, dir_val);
    }
    for (auto& path : paths) {
        if (count++ % 1000000 == 0) {
            // break;
            // print progress
            printf("count: %dK\n", count / 1000);
        }
        db_wrapper.put_path_with_id(uint64_to_hex(dir_id_map[get_parent_directory(path)]), path, file_val);
        // printf("put key: %s val %d\n", path.c_str(), file_val.val_length);
    }
    // end = std::chrono::high_resolution_clock::now();
    // elapsed = end - start;
    // printf("db Elapsed time: %f seconds\n", elapsed.count());
}

// #include <openssl/md5.h>
// #include <pthread.h>

// #include <fstream>
// #include <iostream>
// #include <sstream>
// #include <thread>
// #include <vector>
// #include <unordered_set>
// #include <mutex>
// #include <queue>
// #include <condition_variable>
// #include "../common/dynamic_array.h"
// #include "../common/helper.h"
// #include "../common/key.h"
// #include "../common/metadata.h"
// #include "../common/pkt_ring_buffer.h"
// #include "../common/rocksdb_wrapper.h"
// #include "../common/snapshot_record.h"
// #include "../common/val.h"
// #include "common_impl.h"

// std::unordered_set<std::string> internal_dirs;
// std::mutex dir_mutex; // 用于保护 internal_dirs 的互斥锁
// std::mutex db_mutex;  // 用于保护 RocksdbWrapper 写操作的互斥锁

// // 提取路径层级的函数
// std::vector<std::string> extractPathLevels(const std::string& path, int n) {
//     std::vector<std::string> levels;
//     std::string currentPath = "/";
//     levels.push_back(currentPath);
//     int count = 1;

//     for (size_t start = 1, end; count <= n && (end = path.find('/', start)) != std::string::npos; start = end + 1) {
//         currentPath += path.substr(start, end - start);
//         levels.push_back(currentPath);
//         count++;
//     }
//     return levels;
// }

// void processPaths(const std::vector<std::string>& paths, RocksdbWrapper& db_wrapper, val_t& dir_val, val_t& file_val) {
//     for (const auto& line : paths) {
//         std::vector<std::string> levels = extractPathLevels(line, 2);
//         levels.pop_back(); // 只处理目录

//         for (const auto& level : levels) {
//             std::lock_guard<std::mutex> lock(dir_mutex);
//             auto result = internal_dirs.insert(level);
//             if (!result.second) continue; // 已存在

//             // 线程安全地写入数据库
//             {
//                 std::lock_guard<std::mutex> db_lock(db_mutex);
//                 db_wrapper.put_path(level, dir_val);
//             }
//         }

//         // 写入文件路径
//         {
//             std::lock_guard<std::mutex> db_lock(db_mutex);
//             db_wrapper.put_path(line, file_val);
//         }
//     }
// }

// int main(int argc, char** argv) {
//     RocksdbWrapper::prepare_rocksdb();
//     parse_ini("config.ini");
//     parse_control_ini("control_type.ini");

//     if (argc != 3) {
//         printf("Usage: ./load_local server_physical_idx path_file.out\n");
//         exit(-1);
//     }

//     int server_physical_idx = atoi(argv[1]);
//     INVARIANT(server_physical_idx >= 0 && server_physical_idx < server_physical_num);

//     std::string file_out_path(argv[2]);

//     RocksdbWrapper db_wrapper(NOCACHE_ID);
//     printf("init db %d\n", NOCACHE_ID);
//     db_wrapper.open(server_physical_idx);

//     // 初始化 val_t 对象
//     val_t dir_val, file_val;
//     dir_val.val_length = 24;
//     dir_val.val_data = new char[24];
//     std::memset(dir_val.val_data, 0, 24);

//     file_val.val_length = 40;
//     file_val.val_data = new char[40];
//     std::memset(file_val.val_data, 0, 40);

//     // 多线程处理路径
//     std::ifstream file_out(file_out_path);
//     std::string line;
//     std::vector<std::string> paths;
//     paths.reserve(100000); // 提前分配空间

//     const size_t num_threads = std::thread::hardware_concurrency(); // 根据硬件并发数选择线程数
//     const size_t chunk_size = 10000; // 每个任务的路径数量

//     std::vector<std::thread> threads;
//     std::vector<std::vector<std::string>> chunks;

//     // 分块读取文件
//     std::vector<std::string> chunk;
//     int count = 0;
//     while (std::getline(file_out, line)) {
//         chunk.push_back(line);
//         if (chunk.size() >= chunk_size) {
//             chunks.push_back(std::move(chunk));
//             chunk.clear();
//         }
//         if (++count % 100000 == 0) {
//             printf("Processed %dK paths\n", count / 1000);
//         }
//     }
//     if (!chunk.empty()) {
//         chunks.push_back(std::move(chunk));
//     }

//     // 创建线程处理每个块
//     for (auto& chunk : chunks) {
//         threads.emplace_back([&db_wrapper, &dir_val, &file_val, chunk]() {
//             processPaths(chunk, db_wrapper, dir_val, file_val);
//         });
//     }

//     // 等待所有线程完成
//     for (auto& thread : threads) {
//         if (thread.joinable()) {
//             thread.join();
//         }
//     }

//     // 清理内存
//     delete[] dir_val.val_data;
//     delete[] file_val.val_data;

//     printf("Completed processing %d paths\n", count);
//     return 0;
// }
