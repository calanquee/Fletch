#ifndef NETFETCH_HELPER_H
#define NETFETCH_HELPER_H

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <fstream>
#include <set>
#include <signal.h> // for signal and raise
#include <arpa/inet.h> // inetaddr conversion
#include <sys/time.h> // struct timeval
#include <string.h>
#include <map>
#include <mutex>
#include <iostream>

uint16_t get_path_at_depth(char *path, int path_length, int keydepth, char *result) {
	int path_depth=1;
	for(int i=0;i<path_length;++i){
		if(path[i]=='/'){
			path_depth++;
		}
	}
    keydepth = path_depth - keydepth + 1;
	if(keydepth == 1){
        memcpy(result, "/", 1);
        return 1;
    }
    else{
        for(int i=0;i<path_length;++i){
            if(path[i]=='/'){
                keydepth--;
            }
            if(keydepth==0){
                memcpy(result, path, i);
                return i;
            }
            if(i==path_length-1){
                memcpy(result, path, path_length);
                return path_length;
            }
        }
    }
}

std::vector<std::string> get_internal_paths(const std::string& path) {
    std::vector<std::string> internal_paths;
    std::string current_path;
	std::string root_path = "/";
    std::istringstream iss(path);
    std::string part;

	// Qingxiu: start from "/"
	internal_paths.push_back(root_path);

    while (std::getline(iss, part, '/')) {
        if (!part.empty()) {
            current_path += "/" + part;
            internal_paths.push_back(current_path);
        }
    }

	// Remove the last path as it's the full path, not an internal path
    if (!internal_paths.empty()) {
        internal_paths.pop_back();
    }

    return internal_paths;
}

void get_internal_path_to_evict(std::string path, int free_space, std::vector<std::string>& victims) {
    // Qingxiu: compute all internal paths stored in a stack
    std::vector<std::string> internal_paths = get_internal_paths(path);
    int start_idx = internal_paths.size() - 1;
    while(free_space){
        victims.push_back(internal_paths[start_idx]);
        free_space -= 1;
        start_idx -= 1;
    }
}

void printLocalPort(int sockfd) {
    struct sockaddr_in local_addr;
    socklen_t addr_len = sizeof(local_addr);
    if (getsockname(sockfd, (struct sockaddr*)&local_addr, &addr_len) == -1) {
        perror("getsockname");
    } else {
        printf("Local port: %d\n", ntohs(local_addr.sin_port));
    }
}

uint32_t swap_bytes(uint32_t value) {
    uint32_t byte1 = (value & 0x000000FF) << 8;
    uint32_t byte2 = (value & 0x0000FF00) >> 8;
    uint32_t byte3 = (value & 0x00FF0000) << 8;
    uint32_t byte4 = (value & 0xFF000000) >> 8;
    return byte1 | byte2 | byte3 | byte4;
}

#endif // NETFETCH_HELPER_H