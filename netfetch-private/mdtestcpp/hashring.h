#ifndef HASHRING_H
#define HASHRING_H

#include <openssl/md5.h>
#include <pthread.h>

#include <cstring>
#include <map>
#include <queue>
#include <string>
#include <thread>
#include <vector>

// Declare global hash ring map
// extern std::map<std::vector<unsigned char>, std::pair<std::string, uint16_t>> hashRing;

// Function declarations
std::vector<unsigned char> computeMD5(const std::string& input);
void constructHashRing(const std::vector<std::string>& namespaces);
void initHashRing(int server_logical_num);
std::string mapToNameNode(const std::vector<unsigned char>& hashValue);
uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue) ;
// std::vector<std::string> splitPath(const char* queried_path, int file_parser_offset);
// std::vector<std::string> splitPath(const std::string& queried_path, int file_parser_offset);

#endif  // HASHRING_H
