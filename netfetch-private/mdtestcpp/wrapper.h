#ifndef WRAPPER_H
#define WRAPPER_H
#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <tbb/concurrent_unordered_map.h>

#include <array>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common_impl.h"
#include "fssocket.h"
#include "macro.h"
#include "threadpool.h"

// #define NETFETCH_DEBUG
// need double check 26/15 ?

extern int MAX_RETRIES;
extern size_t cache_capacity;

extern std::mutex path_token_map_mutex;
extern tbb::concurrent_unordered_map<std::string, token_t> path_token_map;
extern std::vector<std::unordered_map<std::string, token_t>> path_token_maps;
// extern path_token_maps;
extern std::unordered_map<std::string, int> path_cnt_map;

extern std::unordered_map<std::string, uint16_t> path_permission_map;
extern std::vector<std::unordered_map<std::string, uint16_t>> path_permission_maps;  // for client-side caching
extern std::unordered_map<std::string, int> path_frequency_map;
extern std::vector<std::unordered_map<std::string, int>> path_frequency_maps;  // Frequency tracking maps for client-side caching

void removeExtraSlashes(std::string& input);
int calculate_path_depth(const std::string& path);

bool netfetch_chmod(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_mv(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_mkdir(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_rmdir(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_touch(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_rm(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_stat(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_open(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_close(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_finish_signal(std::string& path, int sockfd,int thread_id, uint32_t nodeidx);
bool netfetch_write_cache(std::string& path, int sockfd);
bool netfetch_load(std::string& path, int sockfd, int thread_id);
bool netfetch_loaddir(std::string& path, int sockfd, int thread_id);
bool netfetch_readdir(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
bool netfetch_statdir(std::string& path, int sockfd, int thread_id, uint32_t nodeidx);
std::vector<uint8_t> searchTokensforPaths(const std::vector<std::string>& paths);
void setTokensforPaths(const std::vector<std::string>& paths, const std::vector<token_t>& tokens);
std::vector<uint8_t> searchTokensforPaths(const std::vector<std::string>& paths, int thread_id);
void setTokensforPaths(const std::vector<std::string>& paths, const std::vector<token_t>& tokens, int thread_id);
std::pair<bool, std::vector<uint16_t>> searchCacheforPaths(const std::vector<std::string>& paths, int thread_id);
void addInternalDirIntoCache(const std::vector<std::string>& paths, std::vector<uint16_t> versions, int thread_id);
void addallDirIntoCache(const std::vector<std::string>& paths, std::vector<uint16_t> versions, int thread_id);
std::vector<uint16_t> get_versions(char* recvbuf, int recvsize, int levels);

bool compare_key_match(std::string &path, const std::vector<unsigned char>& last_key, const char* recvbuf, int offset);
// std::vector<std::string> extractPathLevels(const std::string& path, int n);
#endif