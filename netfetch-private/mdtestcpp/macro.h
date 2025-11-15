#ifndef MACRO_H
#define MACRO_H

#define RELEASE_VERS "1.9.3"
#define TEST_DIR "#test-dir"
#define ITEM_COUNT 25000
#define WARMUP_LIMIT 200
#define CAPACITY_PER_LINE 16

#define PACKET_LEN_SWITCH_HIT_WITHOUT_VALUE 2 + size_of_key
// Qingxiu: 2B optype + 2B keydepth + hash + 2B path_len + path
#define WARMUP_ACK_LEN 4 + size_of_key

#define WARMUP_LIMIT 200
#define CAPACITY_PER_LINE 16

#define MAX_LEN 1024
#define MAX_COMPONENTS 20              // max depth
#define MAX_LENGTH 256                 // max length of each component
#define TIMEOUT_SEC 5                  // timeout 5 s
#define TIMEOUT_USEC 0                 // timeout 0 us
#define UDP_DEFAULT_RCVBUFSIZE 212992  // 208KB used in linux by default
#define UDP_LARGE_RCVBUFSIZE 8388608   // 8MB (used by socket to receive large data; client_num + 1 for controller + server_num)
#define MAX_BUFSIZE 4096

#define size_of_key 8

#define NETCACHE_ID 0
#define NETFETCH_ID 1
#define NOCACHE_ID 2
#define NETFETCH_ID_1 3
#define NETFETCH_ID_2 4
#define NETFETCH_ID_3 5
#define CSCACHE_ID 6
#define CSFLETCH_ID 7

#endif  // MACRO_H