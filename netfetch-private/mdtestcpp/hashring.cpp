#include "hashring.h"
#include <vector>
#include <string>
#include <map>
#include <openssl/md5.h>
#include <algorithm>

std::vector<std::pair<std::vector<unsigned char>, std::pair<std::string, uint16_t>>> hashRing;

std::vector<unsigned char> computeMD5(const std::string& input) {
    std::vector<unsigned char> digest(MD5_DIGEST_LENGTH);
    MD5(reinterpret_cast<const unsigned char*>(input.c_str()), input.length(), digest.data());
    return digest;
}

void constructHashRing(const std::vector<std::string>& namespaces) {
    uint16_t logical_idx = 0;
    for (const auto& ns : namespaces) {
        for (int i = 0; i < 100; ++i) {
            std::string key = ns + "/" + std::to_string(i);
            std::vector<unsigned char> value = computeMD5(key);
            hashRing.emplace_back(value, std::make_pair(ns, logical_idx));
        }
        logical_idx++;
    }
    std::sort(hashRing.begin(), hashRing.end(), 
        [](const auto& a, const auto& b) { return a.first < b.first; });
}

std::string mapToNameNode(const std::vector<unsigned char>& hashValue) {
    auto it = std::lower_bound(hashRing.begin(), hashRing.end(), hashValue, 
        [](const auto& pair, const auto& value) { return pair.first < value; });

    if (it != hashRing.end()) {
        return it->second.first;
    } else {
        return hashRing.front().second.first; // Wrap around to the first node
    }
}

uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue) {
    auto it = std::lower_bound(hashRing.begin(), hashRing.end(), hashValue, 
        [](const auto& pair, const auto& value) { return pair.first < value; });

    if (it != hashRing.end()) {
        return it->second.second;
    } else {
        return hashRing.front().second.second; // Wrap around to the first node
    }
}

void initHashRing(int server_logical_num) {
    std::vector<std::string> namespaces;
    for (int i = 0; i < server_logical_num; ++i) {
        namespaces.push_back("ns" + std::to_string(i + 1));
    }
    constructHashRing(namespaces);
}
