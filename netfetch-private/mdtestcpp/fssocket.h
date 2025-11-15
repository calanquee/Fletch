#ifndef SOCKET_H
#define SOCKET_H

#include <arpa/inet.h>
#include <execinfo.h>  // backtrace
#include <netinet/in.h>
#include <openssl/md5.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <vector>


#include "common_impl.h"
#include "hashring.h"
#include "macro.h"

#define UNUSED(var) ((void)var)
#define INVARIANT(cond)                \
    if (!(cond)) {                     \
        COUT_THIS(#cond << " failed"); \
        COUT_POS();                    \
        print_stacktrace();            \
        abort();                       \
    }
static inline void print_stacktrace() {
    int size = 16;
    void* array[size];
    int stack_num = backtrace(array, size);
    char** stacktrace = backtrace_symbols(array, stack_num);
    for (int i = 0; i < stack_num; ++i) {
        printf("%s\n", stacktrace[i]);
    }
    free(stacktrace);
}
#define COUT_THIS(this) std::cout << this << std::endl;
#define COUT_VAR(this) std::cout << #this << ": " << this << std::endl;
#define COUT_POS() COUT_THIS("at " << __FILE__ << ":" << __LINE__)
#define COUT_N_EXIT(msg) \
    COUT_THIS(msg);      \
    COUT_POS();          \
    abort();



// Metadata structure
struct metadata {
    uint64_t mLastMod;
    uint64_t mSize;
    uint64_t mBlockSize;
    uint64_t mLastAccess;
    uint16_t mPermissions;
    uint16_t mReplication;
    uint8_t mKind;
    uint16_t owner_length;
    char mOwner[256];
    uint16_t group_length;
    char mGroup[256];
};

extern FILE* file_fd;
extern int flag_fprintf;

void create_udpsock(int& sockfd, bool need_timeout, const char* role, int timeout_sec, int timeout_usec, int udp_rcvbufsize);
void udpsendto(int sockfd, const void* buf, size_t len, int flags, const struct sockaddr_in* dest_addr, socklen_t addrlen);
bool udprecvfrom(int sockfd, void* buf, size_t len, int flags, struct sockaddr_in* src_addr, socklen_t* addrlen, int& recvsize);
void set_recvtimeout(int sockfd, int timeout_sec, int timeout_usec);
void set_sockaddr(sockaddr_in& addr, uint32_t bigendian_saddr, short littleendian_port);

std::vector<std::string> extractPathLevels(const std::string& path, int n);

int prepare_packet_for_WARMUP(std::string& path, char* buf);
int prepare_packet_for_PUT(std::string& path, int keydepth, std::string& operation, std::vector<token_t>& tokens, char* buf, bool is_bak = false);
int prepare_packet_for_GET(std::string& path, std::vector<token_t>& tokens, char* buf, bool is_client_cached = false);

void initialize_file_out();

uint64_t ntohll(uint64_t val);
void parse_metadata(char* metadata_buffer, struct metadata* queried_path_metadata);

#endif  // SOCKET_H
