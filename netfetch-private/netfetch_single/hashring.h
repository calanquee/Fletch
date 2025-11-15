#ifndef HASHRING_H
#define HASHRING_H

#include <pthread.h>
#include <queue>
#include <thread>
#include <vector>
#include <openssl/md5.h>
#include <cstring>
#include <map>
#include <algorithm> // for lexicographical_compare


// std::string computeMD5(const std::string& input);
// void constructHashRing(const std::vector<std::string>& namespaces);
// std::string mapToNameNode(const std::string& hashValue);
// uint32_t mapToNameNodeidx(const std::string& hashValue);

// Global variable definition
std::map<std::vector<unsigned char>, std::pair<std::string, uint16_t>> hashRing;

// Function to compute MD5 hash of a string
std::vector<unsigned char> computeMD5(const std::string& input) {
    std::vector<unsigned char> digest(MD5_DIGEST_LENGTH);
    MD5(reinterpret_cast<const unsigned char*>(input.c_str()), input.length(), digest.data());
    return digest;
}

// Function to construct the hash ring from namespaces
void constructHashRing(const std::vector<std::string>& namespaces) {
    uint16_t logical_idx = 0;
    for (const auto& ns : namespaces) {
        for (int i = 0; i < 100; ++i) {
            std::string key = ns + "/" + std::to_string(i);
            std::vector<unsigned char> value = computeMD5(key);
            hashRing[value] = std::make_pair(ns, logical_idx);
        }
        logical_idx++;
    }
}

// Function to compare MD5 hash vectors lexicographically
bool compareMD5(const std::vector<unsigned char>& a, const std::vector<unsigned char>& b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

// Function to map a hash value to a name node
std::string mapToNameNode(const std::vector<unsigned char>& hashValue) {
    auto it = std::find_if(hashRing.begin(), hashRing.end(), 
        [&](const auto& pair) { return !compareMD5(pair.first, hashValue); });

    if (it != hashRing.end()) {
        return it->second.first;
    } else {
        return hashRing.begin()->second.first; // Wrap around to the first node
    }
}

// Function to map a hash value to a name node index
uint32_t mapToNameNodeidx(const std::vector<unsigned char>& hashValue) {
    auto it = std::find_if(hashRing.begin(), hashRing.end(), 
        [&](const auto& pair) { return !compareMD5(pair.first, hashValue); });

    if (it != hashRing.end()) {
        return it->second.second;
    } else {
        return hashRing.begin()->second.second; // Wrap around to the first node
    }
}

std::vector<std::string> splitPath(const char* queried_path, int file_parser_offset) {
    std::vector<std::string> result;
    // add root directory
    if (file_parser_offset == 0) {
        result.push_back("/");
    }
    // convert the input C-string to a C++ string for easier manipulation
    std::string full_path(queried_path);
    // find each '/' in the path and add the corresponding substrings to the result vector
    size_t pos = 0;
    std::string current_path = "/";
    while ((pos = full_path.find('/', pos + 1)) != std::string::npos) {
        current_path = full_path.substr(0, pos);
        file_parser_offset -= 1;
        if (file_parser_offset <= 0) {
            result.push_back(current_path);
        }
    }
    // add the full path
    result.push_back(full_path);
    return result;
}

#endif
