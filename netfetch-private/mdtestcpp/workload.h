#ifndef WORKLOAD_H
#define WORKLOAD_H

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common_impl.h"
#include "hashring.h"
#include "macro.h"
#include "threadpool.h"
extern std::vector<uint32_t> rand_array;
extern std::vector<Operation> op_array;

extern std::vector<std::pair<std::string, uint32_t>> reqs;
extern std::vector<std::tuple<Operation, std::string, uint32_t>> reqs_mixed;
extern unsigned long long folders_per_depth[10];
extern unsigned long long file_strides[10];
extern unsigned long long folder_strides[10];

// extern std::unordered_set<std::string> cached_set;
extern std::vector<std::unordered_map<std::string, std::string>> dynamic_maps;  // 32M / 40 KOPS = 800s prepare 128 maps
extern std::atomic<int> dynamic_iteration;
extern std::vector<int> sorted_cached_set;
// extern int random_seed;
void init_rand_array(int stop);
int read_workload_from_request_file(int is_uniform, int items, double request_pruning_factor, std::string &request_filename_base);
void generate_reqs_by_rand_array(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir, bool is_file_test = true);
void generate_reqs_mixed_by_rand_array(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir);
void generate_reqs_mixed_by_rand_array_rmdir(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir);

void generate_op_array(int total_ops);
void prepare_dynmaic_map(const std::string &path);
int parse_and_count_weight(const std::string &path, int &total_weight, int kv_count, std::unordered_set<std::string> &processed_paths, std::vector<int> &weights, int &currentrow, int &lastrow);
int parse_and_count_weight_rmdir(const std::string &path, int &total_weight, int kv_count, std::unordered_set<std::string> &processed_paths, std::vector<int> &weights, int &currentrow, int &lastrow);
void updateWeights(std::vector<int> &weights, int &lastrow, int &currentrow, int weightToAdd, int kv_count, int &total_weight);
void gen_cached_set(const std::string &path);
int fast_extract_last_integer(const std::string &line);
std::string gen_file_name(const std::string &path, int idx);
unsigned long long getParentDir(unsigned long long item_num, int items_per_dir);
#endif