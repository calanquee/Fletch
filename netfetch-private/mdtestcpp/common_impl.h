#ifndef COMMON_IMPL_H
#define COMMON_IMPL_H

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <map>
#include <sstream>
#include <vector>
#include <queue>
#include <random>
#include <chrono>
#include "dictionary.h"
#include "iniparser.h"

enum Operation {
    MKDIR = 0,
    RMDIR = 1,
    TOUCH = 2,
    LOAD = 3,
    RM = 4,
    STAT = 5,
    OPEN = 6,
    CLOSE = 7,
    FINISH = 8,
    WARMUP = 9,
    CHMOD = 10,
    MV = 11,
    READDIR = 12,
    STATDIR = 13,
    OBSOLETE = 14,
    OP_COUNT  // total number of operations
};

enum SubOperation {
    BOTTLENECK = 0,
    ROTATION = 1,
    BOTTLENECKHIT = 2,
    ROTATIONHIT = 3,
    SUB_OP_COUNT  // total number of sub operations
};

#define MAX_SECTIONS 100
#define MAX_IDXES 200
typedef unsigned char token_t;

extern std::atomic<int> rtries;

extern dictionary* ini;
extern std::map<Operation, double> operationRatios;

extern unsigned long long items;
extern int branch_factor;
extern std::string stat_name;
extern std::string testdir;
extern std::string base_tree_name;
extern unsigned long long items_per_dir;

// mdtest input
extern int current_method;
extern int hot_path_size;
extern int path_resolution_offset;
extern short dst_port;
extern const char* dst_ip;
extern const char* bottleneck_ip;
extern const char* rotation_ip;
extern const char* hot_paths_filename;
extern int dir_metadata_length;
extern int file_metadata_length;
extern int mode;
extern int server_logical_num;
extern int bottleneck_id;
extern int rotation_id;
extern int uid;
extern int gid;
extern int client_logical_num;
extern double request_pruning_factor;
extern std::string dynamic_rule;
extern int dynamic_period;
extern int dynamic_scale;
// mdtest output
extern const char* file_out;
extern int flag_for_file_out_generation;
extern int create_only;
extern int stat_only;
extern int read_only;
extern int remove_only;
extern int open_close_only;
extern int chmod_only;
extern int mv_only;
extern int leaf_only;
extern int is_mixed;
extern int is_single_level_lock;
extern __useconds_t rate_delay;
// Function declarations
void init_ini(const char* config_file);
void get_current_method();
void get_hot_path_size();
void get_path_resolution_offset();
void get_dst_port();
void get_dst_ip();
void get_hot_paths_filename();
void get_file_out();
void get_dir_metadata_length();
void get_file_metadata_length();
void get_mode();
void get_server_logical_num();
void get_bottleneck_id();
void get_rotation_id();
void get_uid();
void get_gid();
void get_flag_for_file_out_generation();
void free_ini();
void parser_main(const char* config_file);
void parser_mixed();
void print_mixed_ratio();
void parser_dynamic();
void print_dynamic_ratio();
void set_create();
void set_mkdir();
void set_rmdir();

std::string getParentDirectory(const std::string& path);
std::string OperationToString(Operation op);
std::string SubOperationToString(SubOperation sub_op);
std::vector<std::string> extractPathLevels(const std::string& path, int n);
std::vector<std::string> gen_dir_tree(int depth, int branch, const std::string& base_dir);
extern int rd_seed;
#endif  // COMMON_IMPL_H
