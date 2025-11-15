/*
 * Copyright (C) 2003, The Regents of the University of California.
 *  Produced at the Lawrence Livermore National Laboratory.
 *  Written by Christopher J. Morrone <morrone@llnl.gov>,
 *  Bill Loewe <loewe@loewe.net>, Tyce McLarty <mclarty@llnl.gov>,
 *  and Ryan Kroiss <rrkroiss@lanl.gov>.
 *  All rights reserved.
 *  UCRL-CODE-155800
 *
 *  Please read the COPYRIGHT file.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License (as published by
 *  the Free Software Foundation) version 2, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  terms and conditions of the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * CVS info:
 *   $RCSfile: mdtest.c,v $
 *   $Revision: 1.4 $
 *   $Date: 2013/11/27 17:05:31 $
 *   $Author: brettkettering $
 */
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <sys/time.h>
#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>  // For smart pointers
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
// #include "mpi.h"
#include "wrapper.h"
// jz modified
// add ini_parser
#include "common_impl.h"
#include "fssocket.h"
#include "hashring.h"
#include "macro.h"
#include "threadpool.h"
#include "ticker.h"
#include "workload.h"

#define FILEMODE S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH
#define DIRMODE S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IXOTH
/*
 * Try using the system's PATH_MAX, which is what realpath and such use.
 */
// #define MAX_LEN PATH_MAX
/*
#define MAX_LEN 1024
*/

// #define MAX_CLIENT_SPLIT  128

typedef struct
{
    double entry[10];
} table_t;

int rank;
int size = 1;
// std::string testdir;
std::string testdirpath;
std::string top_dir;
// std::string base_tree_name;
std::vector<std::string> filenames;  // Replacing char** with a vector of strings
std::string hostname;
std::string unique_dir;
std::string mk_name;
// std::string stat_name;
std::string read_name;
std::string rm_name;
std::string unique_mk_dir;
std::string unique_chdir_dir;
std::string unique_stat_dir;
std::string unique_read_dir;
std::string unique_rm_dir;
std::string unique_rm_uni_dir;
char *write_buffer = NULL;
char *read_buffer = NULL;
int barriers = 1;

// int branch_factor = 1;
int depth = 0;
// int is_load = 0;

// int thread_count = 64;
int first = 1;
int last = 0;
int stride = 1;
int iterations = 1;
int is_uniform = 0;
std::string request_filename_base;
bool need_warm = false;

std::vector<long long> OperationTime(OP_COUNT);

/*
 * This is likely a small value, but it's sometimes computed by
 * branch_factor^(depth+1), so we'll make it a larger variable,
 * just in case.
 */
unsigned long long num_dirs_in_tree = 0;
/*
 * As we start moving towards Exascale, we could have billions
 * of files in a directory. Make room for that possibility with
 * a larger variable.
 */
// unsigned long long items_per_dir = 0;
int random_seed = 0;
int shared_file = 0;
int files_only = 0;
int dirs_only = 0;
int pre_delay = 0;
int unique_dir_per_task = 0;
int time_unique_dir_overhead = 0;
int verbose = 0;
int throttle = 1;
int send_finish = 0;

int collective_creates = 0;
size_t write_bytes = 0;
size_t read_bytes = 0;
int sync_file = 0;
int path_count = 0;
int nstride = 0; /* neighbor stride */

/* for making/removing unique directory && stating/deleting subdirectory */
enum { MK_UNI_DIR,
    STAT_SUB_DIR,
    READ_SUB_DIR,
    RM_SUB_DIR,
       RM_UNI_DIR };

void print_help();
void parse_dirpath(char *dirpath_arg);

void init_mdtest(int argc, char **argv) {
    parser_main("config.ini");
    // construct hash ring
    initHashRing(server_logical_num);

    /* Check for -h parameter before MPI_Init so the mdtest binary can be
       called directly, without, for instance, mpirun. */
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            print_help();
        }
    }
    int c = 0;
    while (1) {
        c = getopt(argc, argv, "a:Ab:BcCd:De:Ef:FGghI:l:Lmn:N:p:rR::s:StTuvw:Wyz:Y:P:QqR");
        if (c == -1) {
            break;
        }
        switch (c) {
        case 'a':
            thread_count = std::atoi(optarg);
            break;
        case 'A':
            is_load = 1;
            break;
        case 'b':
            branch_factor = std::atoi(optarg);
            break;
        case 'B':
            barriers = 0;
            break;
        case 'c':
            collective_creates = 1;
            break;
        case 'C':
            create_only = 1;
            break;
        case 'd':
            parse_dirpath(optarg);
            break;
        case 'D':
            dirs_only = 1;
            break;
        case 'e':
            read_bytes = (size_t)std::strtoul(optarg, (char **)NULL, 10);
            break;
        case 'E':
            read_only = 1;
            break;
        case 'G':
            mv_only = 1;
            break;
        case 'g':
            chmod_only = 1;
            break;
        case 'f':
            first = std::atoi(optarg);
            break;
        case 'F':
            files_only = 1;
            break;
        case 'h':
            print_help();
            break;
        case 'I':
            items_per_dir = (unsigned long long)std::strtoul(optarg, (char **)NULL, 10);
            break;
            // items_per_dir = atoi(optarg); break;
        case 'l':
            last = atoi(optarg);
            break;
        case 'L':
            leaf_only = 1;
            break;
        case 'n':
            items = (unsigned long long)std::strtoul(optarg, (char **)NULL, 10);
            // print items
            // std::cout << "Items: " << items << std::endl;
            break;
        case 'N':
            nstride = std::atoi(optarg);
            break;
        case 'p':
            pre_delay = std::atoi(optarg);
            rate_delay = pre_delay;
            printf("[INFO] rate limiter %d us\n", rate_delay);
            break;
        case 'r':
            remove_only = 1;
            break;
        case 'R':
            if (optarg == NULL) {
                random_seed = std::time(NULL);
                random_seed += rank;
            } else {
                // Qingxiu: random seed here
                random_seed = std::atoi(optarg) + rank;
                printf("[METFETCH_MDTEST] random_seed = %d\n", random_seed);
            }
            break;
        case 's':
            stride = std::atoi(optarg);
            break;
        case 'S':
            shared_file = 1;
            break;
        case 't':
            time_unique_dir_overhead = 1;
            break;
        case 'T':
            stat_only = 1;

            break;
        case 'v':
            open_close_only = 1;
            break;
        case 'u':
            unique_dir_per_task = 1;
            break;
        case 'w':
            write_bytes = (size_t)std::strtoul(optarg, (char **)NULL, 10);
            break;
            // write_bytes = atoi(optarg);   break;
        case 'W':
            need_warm = true;
            break;
        case 'y':
            sync_file = 1;
            break;
        case 'z':
            depth = std::atoi(optarg);
            break;
        case 'Y':
            request_filename_base = std::string(optarg);
            if (request_filename_base == "uniform") {
                is_uniform = 1;
            }
            break;
        case 'P':
            // double
            request_pruning_factor = std::atof(optarg);
            break;
        case 'Q':
            send_finish = 1;
            break;
        // case 'q': // mkdir

        //     break;
        // case 'R': // rmdir

        //     break;
        case 'm':
            is_mixed = 1;
            break;
        }
    }
    if (mode == 1 && server_logical_num != 128 && request_pruning_factor <= 1.0) {
        request_pruning_factor = 1.0;
    }
    flag_fprintf = ((flag_for_file_out_generation == 1) && ((shared_file == 0) || (shared_file == 1)));
    if (flag_fprintf && thread_count > 1) {
        perror("if you want generate file.out, thread_count should be 1!");
        exit(0);
    }
    if (flag_fprintf)
        initialize_file_out();

    thread_pool = new ThreadPool(thread_count, client_logical_num);
    if (current_method == NOCACHE_ID || current_method == CSCACHE_ID) {
        sleep(20);
    }
    init_histograms();
    create_ticker();
}

char *timestamp() {
    static char datestring[80];
    time_t timestamp;

    fflush(stdout);
    timestamp = time(NULL);
    strftime(datestring, 80, "%m/%d/%Y %T", localtime(&timestamp));

    return datestring;
}

bool is_id_in_rank(int id) {
    return true;
    // printf("%d %d %d\n",items,size,rank);
    int q = items / size;
    int r = items % size;

    int start = (rank < r) ? rank * (q + 1) : r * (q + 1) + (rank - r) * q;
    int end = start + ((rank < r) ? q : q - 1);
    return (id >= start && id <= end);
}

void delay_secs(int delay) {
    if (rank == 0 && delay > 0) {
        sleep(delay);
    }
    // MPI_Barrier(testcomm);
}

void offset_timers(double *t, int tcount) {
    double toffset;
    int i;
    time_t now = time(nullptr);
    double now_in_seconds = static_cast<double>(now);
    toffset = now_in_seconds - t[tcount];
    for (i = 0; i < tcount + 1; i++) {
        t[i] += toffset;
    }
}

void offset_timers(std::vector<std::chrono::high_resolution_clock::time_point> &time_points, int tcount) {
    //
    if (tcount < 0 || tcount >= time_points.size()) {
        std::cerr << "Invalid tcount value." << std::endl;
        return;
    }

    auto current_time = std::chrono::high_resolution_clock::now();
    auto toffset = current_time - time_points[tcount];

    for (int i = 0; i < time_points.size(); i++) {
        time_points[i] += toffset;
    }
}

void parse_dirpath(char *dirpath_arg) {
    char *tmp = dirpath_arg;
    char delimiter_string[3] = {'@', '\n', '\0'};

    path_count = 0;
    filenames.clear();

    if (*tmp != '\0')
        path_count++;
    while (*tmp != '\0') {
        if (*tmp == '@') {
            path_count++;
        }
        tmp++;
    }

    char *token = strtok(dirpath_arg, delimiter_string);
    while (token != NULL) {
        filenames.emplace_back(token);
        token = strtok(NULL, delimiter_string);
    }

    if (filenames.size() != path_count) {
        std::cerr << "Expected " << path_count << " paths, but found " << filenames.size() << std::endl;
    }
}

/*
 * This function copies the unique directory name for a given option to
 * the "to" parameter. Some memory must be allocated to the "to" parameter.
 */

void unique_dir_access(int opt, std::string &to) {
    switch (opt) {
    case MK_UNI_DIR:
        to = unique_chdir_dir;
        break;
    case STAT_SUB_DIR:
        to = unique_stat_dir;
        break;
    case READ_SUB_DIR:
        to = unique_read_dir;
        break;
    case RM_SUB_DIR:
        to = unique_rm_dir;
        break;
    case RM_UNI_DIR:
        to = unique_rm_uni_dir;
        break;
    default:
        // Optionally handle unexpected `opt` values
        break;
    }
}

/* helper for creating/removing items */

void create_remove_items_helper(int dirs, int create, const std::string &path, unsigned long long itemNum) {
    // printf("create_remove_items_helper %d\n",items_per_dir);
    for (unsigned long long i = 0; i < items_per_dir; i++) {
        std::string curr_item;
        if (dirs) {
            if (create) {
                curr_item = path + "/dir." + mk_name + std::to_string(itemNum + i);
                // printf("[NETFETCH_DEBUG] create dir: %s\n", curr_item.c_str());
#ifdef NETFETCH_DEBUG
                std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] create dir: " << curr_item << std::endl;
#endif
                // printf("[NETFETCH_DEBUG] create dir: %s\n", curr_item.c_str());
                Task *task = new Task(curr_item, Operation::MKDIR);
                thread_pool->add_task(task);
            } else {
                curr_item = path + "/dir." + rm_name + std::to_string(itemNum + i);
#ifdef NETFETCH_DEBUG
                std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "]delete dir: " << curr_item << std::endl;
#endif
                // printf("[NETFETCH_DEBUG] create dir: %s\n", curr_item.c_str());
                Task *task = new Task(curr_item, Operation::RMDIR);
                thread_pool->add_task(task);
            }
        }

        /*
         * !dirs
         */

        else {
            int fd;
            if (create) {
                // create files
                // Qingxiu: curr_item is the file path
                // add range
                // for each client skip other range
                curr_item = path + "/file." + mk_name + std::to_string(itemNum + i);
                // printf("[NETFETCH_DEBUG] create dir: %s\n", curr_item.c_str());
                if (collective_creates) {
                    Task *task = new Task(curr_item, Operation::TOUCH);
                    thread_pool->add_task(task);
                } else {
                    if (shared_file) {
                        // if (is_id_in_rank(itemNum + i)) {
                        Task *task = new Task(curr_item, Operation::TOUCH);
                        thread_pool->add_task(task);
                        // } else {
                        //     // TODO count file_create_skip
                        //     // file_create_skip++;
                        // }
                    } else {
                        // if (is_id_in_rank(itemNum + i)) {
                        Task *task = new Task(curr_item, Operation::TOUCH);
                        thread_pool->add_task(task);
                        // } else {
                        //     // TODO count file_create_skip
                        //     // file_create_skip++;
                        // }

#ifdef NETFETCH_DEBUG
                        printf("[NETFETCH_DEBUG] create file: %s\n", curr_item.c_str());
                        fflush(stdout);
#endif
                    }
                }

                if (write_bytes > 0) {
                    // Qingxiu: replace it with print first
                    // TODO: no need for this method since we don't need to write file
#ifdef NETFETCH_DEBUG
                    printf("[NETFETCH_DEBUG] write file: %s\n", curr_item.c_str());
#endif
                }

                // Qingxiu: close file here, replace by print frist
                // TODO: replace it with construct packet and send packet
#ifdef NETFETCH_DEBUG
                std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "]create file: " << curr_item << std::endl;
#endif
                // Qingxiu: comment the next line since it will influence load balance ratio
                // Task *task = new Task(curr_item, Operation::CLOSE);
                // thread_pool->add_task(task);
            } else {
                // remove files
                curr_item = path + "/file." + rm_name + std::to_string(itemNum + i);

                if (!(shared_file)) {
#ifdef NETFETCH_DEBUG
                    std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] delete file: " << curr_item << std::endl;
#endif
                    // Qingxiu: delete file here, replace by print frist
                    Task *task = new Task(curr_item, Operation::RM);
                    thread_pool->add_task(task);
                }
            }
        }
    }
}

/* helper function to do collective operations */
void collective_helper(int dirs, int create, const std::string &path, unsigned long long itemNum) {
    for (unsigned long long i = 0; i < items_per_dir; i++) {
        std::string curr_item;
        if (dirs) {
            if (create) {
                // Create directories
                curr_item = path + "/dir." + mk_name + std::to_string(itemNum + i);
                Task *task = new Task(curr_item, Operation::MKDIR);
                thread_pool->add_task(task);

            } else {
                // Remove directories
                curr_item = path + "/dir." + rm_name + std::to_string(itemNum + i);
                Task *task = new Task(curr_item, Operation::RMDIR);
                thread_pool->add_task(task);
            }

        } else {
            // int fd;
            if (create) {
                // create files
                // will not be used in current test
                perror("create files in collective mode is not supported");
                exit(1);
                // sprintf(curr_item, "%s/file.%s%llu", path, mk_name, itemNum + i);

                // if ((fd = creat(curr_item, FILEMODE)) == -1) {
                //     perror("unable to create file");
                // }

                // if (close(fd) == -1) {
                //     perror("unable to close file");
                // }

            } else {
                // remove files
                // will not be used in current test
                perror("remove files in collective mode is not supported");
                exit(1);
                // sprintf(curr_item, "%s/file.%s%llu", path, rm_name, itemNum + i);

                // if (!(shared_file)) {
                //     Task *task = new Task(curr_item, Operation::RM);
                //     thread_pool->add_task(task);
                // }
            }
        }
    }
}

/* recusive function to create and remove files/directories from the
   directory tree */
// Qingxiu: from file_test(): create_remove_items(0, 0, 1, 0, temp_path, 0);
// Qingxiu: from directory_test(): create_remove_items(0, 1, 1, 0, temp_path, 0);
void create_remove_items(int currDepth, int dirs, int create, int collective,
                         std::string path, unsigned long long dirNum) {
    unsigned long long currDir = dirNum;
    // printf("[METFETCH_MDTEST] create_remove_items: currDepth = %d, dirs = %d, create = %d, collective = %d, path = %s, dirNum = %llu\n",
    //        currDepth, dirs, create, collective, path.c_str(), dirNum);
    if (currDepth == 0) {
        /* create items at this depth */
        if (!leaf_only || (depth == 0 && leaf_only)) {
            if (collective) {
                collective_helper(dirs, create, path, 0);
            } else {
                create_remove_items_helper(dirs, create, path, 0);
            }
        }

        if (depth > 0) {
            create_remove_items(++currDepth, dirs, create, collective, path, ++dirNum);
        }

    } else if (currDepth <= depth) {
        /* iterate through the branches */
        for (int i = 0; i < branch_factor; i++) {
            /* determine the current branch and append it to the path */
            std::string new_path = path + "/" + base_tree_name + "." + std::to_string(currDir) + "/";

            /* create the items in this branch */
            if (!leaf_only || (leaf_only && currDepth == depth)) {
                if (collective) {
                    collective_helper(dirs, create, new_path, currDir * items_per_dir);
                } else {
                    create_remove_items_helper(dirs, create, new_path, currDir * items_per_dir);
                }
            }

            /* make the recursive call for the next level below this branch */
            create_remove_items(
                ++currDepth,
                dirs,
                create,
                collective,
                new_path,
                (currDir * (unsigned long long)branch_factor) + 1);
            currDepth--;

            /* reset the path */
            currDir++;
        }
    }
}

/* stats all of the items created as specified by the input parameters */
// Qingxiu: from test_file(): mdtest_stat(0, 0, temp_path);
void mdtest_stat(int random, int dirs, const std::string &path, int count) {
    /* determine the number of items to stat*/
    unsigned long long stop = 0, item_num;
    if (leaf_only) {
        stop = items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth));
    } else {
        // Qingxiu: here, items is the total number of items (-n)
        // Qingxiu: modify this parameter to use my own request distribution
        stop = count;
    }
#ifdef OLD_V
    /* iterate over all of the item IDs */
    for (unsigned long long i = 0; i < stop; i++) {
        /*
         * It doesn't make sense to pass the address of the array because that would
         * be like passing char **. Tested it on a Cray and it seems to work either
         * way, but it seems that it is correct without the "&".
         *
            memset(&item, 0, MAX_LEN);
         */
        std::string item;
        /* determine the item number to stat */
        // Qingxiu: rewrite it to use my own request distribution
        // TODO
        if (random) {
            item_num = rand_array[i];
        } else {
            item_num = rand_array[i];
        }

        /* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir *
                        (num_dirs_in_tree - (unsigned long long)pow(branch_factor, depth));
        }

        /* create name of file/dir to stat */
        // Qingxiu: not use for current test: ./mdtest -n 10 -F -z 9 -V 3 -d /

        item = dirs ? "dir." : "file.";
        item += stat_name + std::to_string(item_num);

        /* determine the path to the file/dir to be stated */
        // Qingxiu: give a file NO. and get the full path
        // Qingxiu: item_num range: (0, n-1)
        unsigned long long parent_dir = item_num / items_per_dir;
        if (parent_dir > 0) {  // item is not in the tree's root directory
            /* prepend parent directory to item's path */
            item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;

            // still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;
            }
        }

        /* Now get the item to have the full path */
        item = path + "/" + item;

        /* Replace with actual operation */
        // Assuming Task is a class that handles file operations, and thread_pool is an appropriate thread pool instance
        Task *task = new Task(item, dirs ? Operation::STAT : Operation::STAT);
        thread_pool->add_task(task);
    }
#endif

    for (std::pair<std::string, uint32_t> &req : reqs) {
        Task *task = new Task(req.first, Operation::STAT, req.second);
        thread_pool->add_task(task);
    }
}
/* stats all of the items created as specified by the input parameters */
// Qingxiu: from test_file(): mdtest_stat(0, 0, temp_path);
void mdtest_open(int count) {
    /* determine the number of items to stat*/
    unsigned long long stop = 0, item_num;
    if (leaf_only) {
        stop = items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth));
    } else {
        // Qingxiu: here, items is the total number of items (-n)
        // Qingxiu: modify this parameter to use my own request distribution
        stop = count;
    }

    for (std::pair<std::string, uint32_t> &req : reqs) {
        Task *task = new Task(req.first, Operation::OPEN, req.second);
        thread_pool->add_task(task);
    }
}

void mdtest_stat_in_open_close(int count) {
    /* determine the number of items to stat*/
    unsigned long long stop = 0, item_num;
    if (leaf_only) {
        stop = items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth));
    } else {
        // Qingxiu: here, items is the total number of items (-n)
        // Qingxiu: modify this parameter to use my own request distribution
        stop = count;
    }

    for (std::pair<std::string, uint32_t> &req : reqs) {
        Task *task = new Task(req.first, Operation::STAT, req.second);
        thread_pool->add_task(task);
    }
}

/* stats all of the items created as specified by the input parameters */
// Qingxiu: from test_file(): mdtest_stat(0, 0, temp_path);
void mdtest_close(int count) {
    /* determine the number of items to stat*/
    unsigned long long stop = 0, item_num;
    if (leaf_only) {
        stop = items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth));
    } else {
        // Qingxiu: here, items is the total number of items (-n)
        // Qingxiu: modify this parameter to use my own request distribution
        stop = count;
    }

    for (std::pair<std::string, uint32_t> &req : reqs) {
        Task *task = new Task(req.first, Operation::CLOSE, req.second);
        thread_pool->add_task(task);
    }
}
/* reads all of the items created as specified by the input parameters */
void mdtest_read(int random, int dirs, const std::string &path) {
    std::unique_ptr<char[]> read_buffer;

    /* allocate read buffer */
    if (read_bytes > 0) {
        read_buffer = std::make_unique<char[]>(read_bytes);
        if (!read_buffer) {
            perror("out of memory");
            return;
        }
    }

    /* determine the number of items to read */
    unsigned long long stop = leaf_only ? items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth)) : items;

    /* iterate over all of the item IDs */
    for (unsigned long long i = 0; i < stop; i++) {
        /*
         * It doesn't make sense to pass the address of the array because that would
         * be like passing char **. Tested it on a Cray and it seems to work either
         * way, but it seems that it is correct without the "&".
         *
          memset(&item, 0, MAX_LEN);
         */
        unsigned long long item_num = random ? rand_array[i] : i;

        /* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir * (num_dirs_in_tree - static_cast<unsigned long long>(pow(branch_factor, depth)));
        }
        std::string item;
        /* create name of file to read */
        if (dirs) {
            ; /* N/A */
        } else {
            item = "file." + read_name + std::to_string(item_num);
        }

        /* determine the path to the file/dir to be read'ed */
        unsigned long long parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {  // item is not in the tree's root directory
            /* prepend parent directory to item's path */
            item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;

            // still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;
            }
        }

        /* Now get item to have the full path */
        /* open file for reading */
        // Qingxiu: replace it with print first
        // TODO: hdfs open file
        std::string full_path = path + "/" + item;
#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] open file: " << full_path << std::endl;
#endif

        auto task_open = new Task(full_path, Operation::OPEN);
        thread_pool->add_task(task_open);
        /* read file */
        // Qingxiu: replace it with print first
        // TODO: no need to use since we target 0B file
        if (read_bytes > 0) {
#ifdef NETFETCH_DEBUG
            std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] read file: " << full_path << std::endl;
#endif
        }

        /* close file */
        // Qingxiu: replace it with print first
        // TODO: hdfs close file
        auto task_close = new Task(full_path, Operation::CLOSE);
        thread_pool->add_task(task_close);
#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] close file: " << full_path << std::endl;
#endif
    }
}

/* mv all of the items created as specified by the input parameters */
void mdtest_mv(int random, int dirs, const std::string &path) {
    std::unique_ptr<char[]> read_buffer;

    /* allocate read buffer */
    if (read_bytes > 0) {
        read_buffer = std::make_unique<char[]>(read_bytes);
        if (!read_buffer) {
            perror("out of memory");
            return;
        }
    }

    /* determine the number of items to read */
    unsigned long long stop = leaf_only ? items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth)) : items;

    /* iterate over all of the item IDs */
    for (unsigned long long i = 0; i < stop; i++) {
        /*
         * It doesn't make sense to pass the address of the array because that would
         * be like passing char **. Tested it on a Cray and it seems to work either
         * way, but it seems that it is correct without the "&".
         *
          memset(&item, 0, MAX_LEN);
         */
        unsigned long long item_num = random ? rand_array[i] : i;

        /* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir * (num_dirs_in_tree - static_cast<unsigned long long>(pow(branch_factor, depth)));
        }
        std::string item;
        /* create name of file to read */
        if (dirs) {
            ; /* N/A */
        } else {
            item = "file." + read_name + std::to_string(item_num);
        }

        /* determine the path to the file/dir to be read'ed */
        unsigned long long parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {  // item is not in the tree's root directory
            /* prepend parent directory to item's path */
            item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;

            // still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;
            }
        }

        std::string full_path = path + "/" + item;

        auto task_open = new Task(full_path, Operation::MV);
        thread_pool->add_task(task_open);

#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] MV file: " << full_path << std::endl;
#endif
    }
}

/* chmod all of the items created as specified by the input parameters */
void mdtest_chmod(int random, int dirs, const std::string &path) {
    std::unique_ptr<char[]> read_buffer;

    /* allocate read buffer */
    if (read_bytes > 0) {
        read_buffer = std::make_unique<char[]>(read_bytes);
        if (!read_buffer) {
            perror("out of memory");
            return;
        }
    }

    /* determine the number of items to read */
    unsigned long long stop = leaf_only ? items_per_dir * static_cast<unsigned long long>(pow(branch_factor, depth)) : items;

    /* iterate over all of the item IDs */
    for (unsigned long long i = 0; i < stop; i++) {
        /*
         * It doesn't make sense to pass the address of the array because that would
         * be like passing char **. Tested it on a Cray and it seems to work either
         * way, but it seems that it is correct without the "&".
         *
          memset(&item, 0, MAX_LEN);
         */
        unsigned long long item_num = random ? rand_array[i] : i;

        /* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir * (num_dirs_in_tree - static_cast<unsigned long long>(pow(branch_factor, depth)));
        }
        std::string item;
        /* create name of file to read */
        if (dirs) {
            ; /* N/A */
        } else {
            item = "file." + read_name + std::to_string(item_num);
        }

        /* determine the path to the file/dir to be read'ed */
        unsigned long long parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {  // item is not in the tree's root directory
            /* prepend parent directory to item's path */
            item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;

            // still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = base_tree_name + "." + std::to_string(parent_dir) + "/" + item;
            }
        }

        /* Now get item to have the full path */
        /* open file for reading */
        // Qingxiu: replace it with print first
        // TODO: hdfs open file
        std::string full_path = path + "/" + item;

        auto task_open = new Task(full_path, Operation::CHMOD);
        thread_pool->add_task(task_open);

#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] CHMOD file: " << full_path << std::endl;
#endif
    }
}

/* This method should be called by rank 0.  It subsequently does all of
   the creates and removes for the other ranks */
void collective_create_remove(int create, int dirs, int ntasks, const std::string &path) {
    std::string temp;
    printf("collective_create_remove %d %d %d %s\n", create, dirs, ntasks, path.c_str());

    /* rank 0 does all of the creates and removes for all of the ranks */
    for (int i = 0; i < ntasks; i++) {
        temp = testdir + "/";

        /* set the base tree name appropriately */
        if (unique_dir_per_task) {
            base_tree_name = "mdtest_tree." + std::to_string(i);
        } else {
            base_tree_name = "mdtest_tree";
        }

        /* Setup to do I/O to the appropriate test dir */
        temp += base_tree_name + ".0";
        /* set all item names appropriately */
        if (!shared_file) {
            mk_name = "mdtest." + std::to_string((0 + (0 * nstride)) % ntasks) + ".";
            stat_name = "mdtest." + std::to_string((0 + (1 * nstride)) % ntasks) + ".";
            read_name = "mdtest." + std::to_string((0 + (2 * nstride)) % ntasks) + ".";
            rm_name = "mdtest." + std::to_string((0 + (3 * nstride)) % ntasks) + ".";
        }
        if (unique_dir_per_task) {
            unique_mk_dir = testdir + "/mdtest_tree." + std::to_string((i + (0 * nstride)) % ntasks) + ".0";
            unique_chdir_dir = testdir + "/mdtest_tree." + std::to_string((i + (1 * nstride)) % ntasks) + ".0";
            unique_stat_dir = testdir + "/mdtest_tree." + std::to_string((i + (2 * nstride)) % ntasks) + ".0";
            unique_read_dir = testdir + "/mdtest_tree." + std::to_string((i + (3 * nstride)) % ntasks) + ".0";
            unique_rm_dir = testdir + "/mdtest_tree." + std::to_string((i + (4 * nstride)) % ntasks) + ".0";
            unique_rm_uni_dir = testdir;
        }
        create_remove_items(0, dirs, create, 1, temp, 0);
    }

    /* reset all of the item names */
    base_tree_name = unique_dir_per_task ? "mdtest_tree.0" : "mdtest_tree";

    if (!shared_file) {
        mk_name = "mdtest." + std::to_string((0 + (0 * nstride)) % ntasks) + ".";
        stat_name = "mdtest." + std::to_string((0 + (1 * nstride)) % ntasks) + ".";
        read_name = "mdtest." + std::to_string((0 + (2 * nstride)) % ntasks) + ".";
        rm_name = "mdtest." + std::to_string((0 + (3 * nstride)) % ntasks) + ".";
    }
    if (unique_dir_per_task) {
        unique_mk_dir = testdir + "/mdtest_tree." + std::to_string((0 + (0 * nstride)) % ntasks) + ".0";
        unique_chdir_dir = testdir + "/mdtest_tree." + std::to_string((0 + (1 * nstride)) % ntasks) + ".0";
        unique_stat_dir = testdir + "/mdtest_tree." + std::to_string((0 + (2 * nstride)) % ntasks) + ".0";
        unique_read_dir = testdir + "/mdtest_tree." + std::to_string((0 + (3 * nstride)) % ntasks) + ".0";
        unique_rm_dir = testdir + "/mdtest_tree." + std::to_string((0 + (4 * nstride)) % ntasks) + ".0";
        unique_rm_uni_dir = testdir;
    }
}

void directory_test(int iteration, int ntasks, const std::string &path) {
    // int size;
    std::vector<std::chrono::high_resolution_clock::time_point> time_points;
    std::string temp_path;

    int req_count = 0;
    // Qingxiu: this is file request file
    if (!request_filename_base.empty()) {
        req_count = read_workload_from_request_file(is_uniform, items, request_pruning_factor, request_filename_base);
        if (!is_load && (server_logical_num > 2))
            generate_reqs_by_rand_array(path, base_tree_name, stat_name, branch_factor, items_per_dir, false);
    } else if (flag_for_file_out_generation || is_load) {
        // do nothing
    } else {
        // error and exit
        std::cerr << "Error: request_filename_base is empty" << std::endl;
        exit(1);
    }

    printf("[LINE %d] req_count = %d\n", __LINE__, req_count);

    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 1st time

    /* create phase */
    if (create_only) {
        if (unique_dir_per_task) {
            unique_dir_access(MK_UNI_DIR, temp_path);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 0);
            }
        } else {
            temp_path = path;
        }

        /* "touch" the files */
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(1, 1, ntasks, temp_path);
            }
        } else {
            /* create directories */
            create_remove_items(0, 1, 1, 0, temp_path, 0);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 2nd time

    /* stat phase */
    if (stat_only) {
        if (unique_dir_per_task) {
            unique_dir_access(STAT_SUB_DIR, temp_path);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 1);
            }
        } else {
            temp_path = path;
        }

        /* stat directories */
        if (random_seed > 0) {
            mdtest_stat(1, 1, temp_path, req_count);
        } else {
            mdtest_stat(0, 1, temp_path, req_count);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 3rd time

    /* read phase */
    if (read_only) {
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR, temp_path);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 2);
            }
        } else {
            temp_path = path;
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 4th time

    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_SUB_DIR, temp_path);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 3);
            }
        } else {
            temp_path = path;
        }

        /* remove directories */
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(0, 1, ntasks, temp_path);
            }
        } else {
            create_remove_items(0, 1, 0, 0, temp_path, 0);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 5th time

    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_UNI_DIR, temp_path);
        } else {
            temp_path = path;
        }
    }

    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(time_points, 4);
    }

    OperationTime[Operation::MKDIR] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[1] - time_points[0]).count();
    OperationTime[Operation::STAT] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[2] - time_points[1]).count();
    OperationTime[Operation::RMDIR] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[4] - time_points[3]).count();
}

int get_hot_paths_item_number(std::vector<int> &hot_paths_item_number) {
    if (is_uniform) {
        // random generate hot_path_size items in hot_paths_item_number
        // no repeared
        std::random_device rd;
        std::mt19937 gen(rd_seed);
        std::uniform_int_distribution<int> dis(0, items - 1);
        for (int i = 0; i < hot_path_size; i++) {
            int item_number = dis(gen);
            while (std::find(hot_paths_item_number.begin(), hot_paths_item_number.end(), item_number) != hot_paths_item_number.end()) {
                item_number = dis(gen);
            }
            hot_paths_item_number.push_back(item_number);
        }
        return hot_path_size;
    }
    std::ifstream hot_paths(hot_paths_filename);
    if (!hot_paths) {
        std::cerr << "Error: cannot open file " << hot_paths_filename << std::endl;
        exit(1);
    }

    int item_number, freq;
    std::string tmppath;
    int count = 0;  // Count of valid items
    while (hot_paths >> item_number >> tmppath >> freq) {
        removeExtraSlashes(tmppath);
        if (calculate_path_depth(tmppath) <= path_resolution_offset + 4) {
            hot_paths_item_number.push_back(item_number);
            count++;
        } else if (is_mixed && remove_only) {  // rmdir tests
            if (calculate_path_depth(tmppath) <= path_resolution_offset + 5) {
                hot_paths_item_number.push_back(item_number);
                count++;
            }
        }
        if (count == hot_path_size)  // Stop when full cache
            break;
    }
    hot_paths.close();
    return count;
}

void mdtest_write_hot_paths_to_switch_for_netcache(std::string &path) {
    std::vector<int> hot_paths_item_numbers;
    int hot_paths_count = get_hot_paths_item_number(hot_paths_item_numbers);

    std::cout << "[NETFETCH_FS][line " << __LINE__ << "] we warmed: " << hot_paths_count << std::endl;

    int parent_dir = 0, item_num = 0;
    std::string item;

    for (int i = 0; i < hot_paths_count; i++) {
        if (i % WARMUP_LIMIT == WARMUP_LIMIT - 1) {
            // std::cout << "We have warmed " << i << " items. have a break\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        item_num = hot_paths_item_numbers[i];
        // item: file.mdtest.0.0
        item = "file." + std::string(stat_name) + std::to_string(item_num);
        parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {
            // Construct directory tree based on parent_dir
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            }
        }
        std::string fullPath = path + "/" + item;
        // auto levels = extractPathLevels(fullPath, path_resolution_offset);
#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_FS][line " << __LINE__ << "] write into switch: " << fullPath << std::endl;
#endif

        Task *task = new Task(fullPath, Operation::WARMUP);
        thread_pool->add_task(task);
    }
}

void mdtest_write_hot_paths_to_switch_for_netfetch_uniform(std::string &path) {
    std::vector<int> hot_paths_item_numbers;
    int hot_paths_count = get_hot_paths_item_number(hot_paths_item_numbers);

    std::cout << "[NETFETCH_FS][line " << __LINE__ << "] we warmed: " << hot_paths_count << std::endl;

    int parent_dir = 0, item_num = 0;
    std::string item;
    std::unordered_set<std::string> processed_paths;
    for (int i = 0; i < hot_paths_count; i++) {
        if (i % WARMUP_LIMIT == WARMUP_LIMIT - 1) {
            // std::cout << "We have warmed " << i << " items. have a break\n";
            std::this_thread::sleep_for(std::chrono::seconds(4));
        }

        item_num = hot_paths_item_numbers[i];
        // item: file.mdtest.0.0
        item = "file." + std::string(stat_name) + std::to_string(item_num);
        parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {
            // Construct directory tree based on parent_dir
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            }
        }
        std::string fullPath = path + "/" + item;
        auto levels = extractPathLevels(fullPath, path_resolution_offset);
#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_FS][line " << __LINE__ << "] write into switch: " << fullPath << std::endl;
#endif
        for (auto &level : levels) {
            if (processed_paths.find(level) == processed_paths.end()) {
                processed_paths.insert(level);
                Task *task = new Task(level, Operation::WARMUP);
                thread_pool->add_task(task);
            }
        }
        // Task *task = new Task(fullPath, Operation::WARMUP);
        // thread_pool->add_task(task);
    }
}

void mdtest_write_hot_paths_to_switch_for_netfetch(const std::string &path) {
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "[mdtest_write_hot_paths_to_switch_for_netfetch]we plan to warm " << kv_count << std::endl;

    std::ifstream file(hot_paths_filename);
    if (!file) {
        std::cerr << "Error opening " << hot_paths_filename << std::endl;
        return;
    }
    std::string line;
    std::vector<int> weights(kv_count + 1, 0);
    std::unordered_set<std::string> processed_paths;
    int total_weight = 0, lastrow = 0, currentrow = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        int number1, number2;
        std::string tmp_path;
        if (iss >> number1 >> tmp_path >> number2) {
            if (parse_and_count_weight(tmp_path, total_weight, kv_count, processed_paths, weights, currentrow, lastrow)) {
                break;
            }
        }
    }
    file.close();
#ifdef NETFETCH_DEBUG
    // dump processed_paths and its size
    // std::cout << "processed_paths size: " << processed_paths.size() << std::endl;
    // for (auto &path : processed_paths) {
    //     std::cout << path << std::endl;
    // }
#endif
}

void mdtest_write_hot_paths_to_switch(std::string &path, bool is_file_test = true) {
    // printf("[mdtest_write_hot_paths_to_switch] path: %s\n", path.c_str());
    if (hot_path_size == 0)
        return;
    int sleep_sec = 0;
    if (current_method == NETCACHE_ID) {
        // return; // debug thpt
        mdtest_write_hot_paths_to_switch_for_netcache(path);
        printf("sleep for tofino ready\n");
        sleep_sec = hot_path_size / 500 > 0 ? hot_path_size / 500 : 1;
        sleep(sleep_sec);
    }
    if (current_method == NETFETCH_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == NETFETCH_ID_3 || current_method == CSFLETCH_ID) {
        if (is_uniform) {
            mdtest_write_hot_paths_to_switch_for_netfetch_uniform(path);
        } else {
            mdtest_write_hot_paths_to_switch_for_netfetch(path);
        }
        printf("sleep for tofino ready\n");
        sleep_sec = hot_path_size / 200 > 0 ? hot_path_size / 200 : 1;
        if (current_method == CSFLETCH_ID) {
            sleep_sec = hot_path_size / 500 > 0 ? hot_path_size / 500 : 1;
        }
        sleep(sleep_sec);
        // sleep(10);
    }
}

void mdtest_write_hot_paths_to_switch_for_netcache_rmdir(std::string &path) {
    std::vector<int> hot_paths_item_numbers;
    int hot_paths_count = get_hot_paths_item_number(hot_paths_item_numbers);

    std::cout << "[NETFETCH_FS][line " << __LINE__ << "] we warmed: " << hot_paths_count << std::endl;

    int parent_dir = 0, item_num = 0;
    std::string item;

    for (int i = 0; i < hot_paths_count; i++) {
        if (i % WARMUP_LIMIT == WARMUP_LIMIT - 1) {
            // std::cout << "We have warmed " << i << " items. have a break\n";
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        item_num = hot_paths_item_numbers[i];
        // item: file.mdtest.0.0
        item = "file." + std::string(stat_name) + std::to_string(item_num);
        parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {
            // Construct directory tree based on parent_dir
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            }
        }
        std::string fullPath = path + "/" + item + ".rmdir";
        // auto levels = extractPathLevels(fullPath, path_resolution_offset);
#ifdef NETFETCH_DEBUG
        std::cout << "[NETFETCH_FS][line " << __LINE__ << "] write into switch: " << fullPath << std::endl;
#endif

        Task *task = new Task(fullPath, Operation::WARMUP);
        thread_pool->add_task(task);
    }
}

void mdtest_write_hot_paths_to_switch_for_netfetch_rmdir(const std::string &path) {
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "[mdtest_write_hot_paths_to_switch_for_netfetch]we plan to warm " << kv_count << std::endl;

    std::ifstream file(hot_paths_filename);
    if (!file) {
        std::cerr << "Error opening " << hot_paths_filename << std::endl;
        return;
    }
    std::string line;
    std::vector<int> weights(kv_count + 1, 0);
    std::unordered_set<std::string> processed_paths;
    int total_weight = 0, lastrow = 0, currentrow = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        int number1, number2;
        std::string tmp_path;
        if (iss >> number1 >> tmp_path >> number2) {
            if (parse_and_count_weight_rmdir(tmp_path, total_weight, kv_count, processed_paths, weights, currentrow, lastrow)) {
                break;
            }
        }
    }
    file.close();
}

void mdtest_write_hot_paths_to_switch_rmdir(std::string &path) {
    // printf("[mdtest_write_hot_paths_to_switch] path: %s\n", path.c_str());
    if (hot_path_size == 0)
        return;
    int sleep_sec = 0;
    if (current_method == NETCACHE_ID) {
        // return; // debug thpt
        mdtest_write_hot_paths_to_switch_for_netcache_rmdir(path);
        printf("sleep for tofino ready\n");
        sleep_sec = hot_path_size / 500 > 0 ? hot_path_size / 500 : 1;
        sleep(sleep_sec);
    }
    if (current_method == NETFETCH_ID || current_method == NETFETCH_ID_1 || current_method == NETFETCH_ID_2 || current_method == NETFETCH_ID_3 || current_method == CSFLETCH_ID) {
        mdtest_write_hot_paths_to_switch_for_netfetch_rmdir(path);
        printf("sleep for tofino ready\n");
        sleep_sec = hot_path_size / 200 > 0 ? hot_path_size / 200 : 1;
        sleep(sleep_sec);
        // sleep(10);
    }
}

void prepare_client_cache(std::string &path) {
    if (!(current_method == CSCACHE_ID || current_method == CSFLETCH_ID)) {
        return;
    }
    std::unordered_set<std::string> in_cache_dirs;
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "prepare_client_cache " << kv_count << std::endl;

    // std::ifstream file(hot_paths_filename);
    // if (!file) {
    //     std::cerr << "Error opening " << hot_paths_filename << std::endl;
    //     return;
    // }
    std::string line;
    std::unordered_set<std::string> processed_paths;

    // while (std::getline(file, line)) {
    //     std::istringstream iss(line);
    //     int number1, number2;
    //     std::string tmp_path;
    //     if (iss >> number1 >> tmp_path >> number2) {
    //         auto levels = extractPathLevels(tmp_path, path_resolution_offset);
    //         levels.pop_back();

    //         // in_cache_dirs.insert(level);
    //         // generate version vector, fill it with 0
    //         std::vector<uint16_t> version_vector(levels.size(), 0);
    //         for (int i = 0; i < client_logical_num; i++) {
    //             addInternalDirIntoCache(levels, version_vector, i);
    //         }
    //     }
    // }
    auto warmup_dirs = gen_dir_tree(depth + 2 - 1, branch_factor, path);
    for (auto &dir : warmup_dirs) {
        // std::cout << "dir:" << dir << std::endl;
        auto levels = extractPathLevels(dir, path_resolution_offset);

        // generate version vector, fill it with 0
        std::vector<uint16_t> version_vector(levels.size(), 0);
        for (int i = 0; i < client_logical_num; i++) {
            addallDirIntoCache(levels, version_vector, i);
        }
    }
    sleep(5);
    // file.close();
    std::cout << "we warm client cache " << path_permission_maps[0].size() << std::endl;
}

void prepare_hot_path_token(std::string &path) {
    if (!(current_method == NETFETCH_ID || current_method == NETFETCH_ID_3 || current_method == CSFLETCH_ID)) {
        return;
    }
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "prepare_hot_path_token " << kv_count << std::endl;

    std::ifstream file(hot_paths_filename);
    if (!file) {
        std::cerr << "Error opening " << hot_paths_filename << std::endl;
        return;
    }
    std::string line;
    std::vector<int> weights(kv_count + 1, 0);
    std::unordered_set<std::string> processed_paths;
    int total_weight = 0, lastrow = 0, currentrow = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        int number1, number2;
        std::string tmp_path;
        if (iss >> number1 >> tmp_path >> number2) {
            if (currentrow++ >= hot_path_size * 5) {
                break;
            }
            auto levels = extractPathLevels(tmp_path, path_resolution_offset);
            for (auto &level : levels) {
                for (int i = 0; i < client_logical_num; i++) {
                    path_token_maps[i][level] = (token_t)1;
                }
            }
        }
    }
    file.close();
}

void prepare_hot_path_token_rmdir(std::string &path) {
    if (!(current_method == NETFETCH_ID || current_method == NETFETCH_ID_3 || current_method == CSFLETCH_ID)) {
        return;
    }
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "prepare_hot_path_token " << kv_count << std::endl;

    std::ifstream file(hot_paths_filename);
    if (!file) {
        std::cerr << "Error opening " << hot_paths_filename << std::endl;
        return;
    }
    std::string line;
    std::vector<int> weights(kv_count + 1, 0);
    std::unordered_set<std::string> processed_paths;
    int total_weight = 0, lastrow = 0, currentrow = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        int number1, number2;
        std::string tmp_path;
        if (iss >> number1 >> tmp_path >> number2) {
            if (currentrow++ >= hot_path_size * 5) {
                break;
            }
            auto levels = extractPathLevels(tmp_path, path_resolution_offset);
            for (auto &level : levels) {
                for (int i = 0; i < client_logical_num; i++) {
                    // if its the last one in levels
                    if (level == levels.back()) {
                        path_token_maps[i][level + ".rmdir"] = (token_t)1;
                    } else {
                        path_token_maps[i][level] = (token_t)1;
                    }
                    // path_token_maps[i][level] = (token_t)1;
                }
            }
        }
    }
    file.close();
}

void file_test(int iteration, int ntasks, const std::string &path) {
    // int size;
    // printf("file_test %d %d %s\n", iteration, ntasks, path.c_str());
    std::vector<std::chrono::high_resolution_clock::time_point> time_points;

    // char temp_path[MAX_LEN];
    std::string tmp_path_str = path;

    int req_count = 0;
    if (!request_filename_base.empty()) {
        req_count = read_workload_from_request_file(is_uniform, items, request_pruning_factor, request_filename_base);
        if (!is_load && (server_logical_num > 2))
            generate_reqs_by_rand_array(path, base_tree_name, stat_name, branch_factor, items_per_dir, true);
    } else if (flag_for_file_out_generation || is_load) {
        // do nothing
    } else {
        // error and exit
        std::cerr << "Error: request_filename_base is empty" << std::endl;
        exit(1);
    }

    printf("[LINE %d] req_count = %d\n", __LINE__, req_count);

    if (create_only == 0 && rank == 0) {
        // Qingxiu: write cache to switch

#ifdef NETFETCH_DEBUG
        printf("[line %d] temp_path: %s\n", __LINE__, tmp_path_str.c_str());
#endif
        if (!(current_method == NOCACHE_ID || current_method == CSCACHE_ID) && ((mode == 1 && rotation_id == bottleneck_id) || mode == 0 || need_warm == true)) {
            // for mode ==1 static mode
            // only first rotation need warmup
            printf("start warmup\n");
            mdtest_write_hot_paths_to_switch(tmp_path_str);
            // sleep for tofino ready
        } else if (current_method != NOCACHE_ID) {
            prepare_hot_path_token(tmp_path_str);
        }
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            prepare_client_cache(testdir);
        }

#ifdef NETFETCH_DEBUG
        printf("[line %d] ready for mdtest_write_hot_paths_to_switch\n", __LINE__);
#endif
    }
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    printf("ready for test\n");
    start_ticker();
    // Qingxiu: t[1] - t[0] is creation time
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 1st time

    // printf("[line %d] path == %s\n", __LINE__, path.c_str());
    // fflush(stdout);
    /* create phase */
    if (create_only) {
        if (unique_dir_per_task) {
            unique_dir_access(MK_UNI_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 0);
            }
        } else {
            tmp_path_str = path;
        }

        /* "touch" the files */
        if (collective_creates) {
            collective_create_remove(1, 0, ntasks, tmp_path_str);
            // MPI_Barrier(testcomm);
            while (!thread_pool->is_phase_end()) {
                if (rtries >= 128 && mode == 1) {
                    exit(-1);
                }
            }
        }

        create_remove_items(0, 0, 1, 0, tmp_path_str, 0);
    }
    // printf("ready for test\n");
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 2nd time

    /* stat phase */

    if (stat_only && !open_close_only) {
        // printf("[line %d] temp_path == %s\n", __LINE__, temp_path);
        if (unique_dir_per_task) {
            // printf("[line %d] temp_path == %s\n", __LINE__, temp_path);
            // printf("[line %d] path == %s", __LINE__, path);
            fflush(stdout);
            unique_dir_access(STAT_SUB_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 1);
            }
        } else {
            tmp_path_str = path;
        }

        /* stat files */
        if (random_seed > 0) {
            mdtest_stat(1, 0, tmp_path_str, req_count);
        } else {
            mdtest_stat(0, 0, tmp_path_str, req_count);
        }
    } else if (stat_only && open_close_only) {
        mdtest_stat_in_open_close(req_count);
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 3rd time

    /* read phase */
    if (read_only) {
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 2);
            }
        } else {
            tmp_path_str = path;
        }

        /* read files */
        if (random_seed > 0) {
            mdtest_read(1, 0, tmp_path_str);
        } else {
            mdtest_read(0, 0, tmp_path_str);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 4th time

    /* chmod phase */
    if (chmod_only) {
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 3);
            }
        } else {
            tmp_path_str = path;
        }

        /* read files */
        if (random_seed > 0) {
            mdtest_chmod(1, 0, tmp_path_str);
        } else {
            mdtest_chmod(0, 0, tmp_path_str);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 5th time

    /* mv phase */
    if (mv_only) {
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 4);
            }
        } else {
            tmp_path_str = path;
        }

        /* read files */
        if (random_seed > 0) {
            mdtest_mv(1, 0, tmp_path_str);
        } else {
            mdtest_mv(0, 0, tmp_path_str);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 6th time

    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_SUB_DIR, tmp_path_str);
            if (!time_unique_dir_overhead) {
                offset_timers(time_points, 5);
            }
        } else {
            tmp_path_str = path;
        }

        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(0, 0, ntasks, tmp_path_str);
            }
        } else {
            create_remove_items(0, 0, 0, 0, tmp_path_str, 0);
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 7th time

    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_UNI_DIR, tmp_path_str);
        } else {
            tmp_path_str = path;
        }
    }

    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(time_points, 6);
    }
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 8 time

    if (open_close_only) {
        mdtest_open(req_count);
    }
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 9 time

    if (open_close_only) {
        mdtest_close(req_count);
    }
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    time_points.push_back(std::chrono::high_resolution_clock::now());  // the 10 time

    OperationTime[Operation::TOUCH] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[1] - time_points[0]).count();
    OperationTime[Operation::STAT] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[2] - time_points[1]).count();
    OperationTime[Operation::OPEN] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[3] - time_points[2]).count();
    OperationTime[Operation::CHMOD] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[4] - time_points[3]).count();
    OperationTime[Operation::MV] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[5] - time_points[4]).count();
    OperationTime[Operation::RM] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[6] - time_points[5]).count();
    OperationTime[Operation::OPEN] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[8] - time_points[7]).count();
    OperationTime[Operation::CLOSE] = std::chrono::duration_cast<std::chrono::milliseconds>(time_points[9] - time_points[8]).count();
}

void print_help() {
    std::vector<std::string> opts = {
        "Usage: mdtest [-b branching_factor] [-B] [-c] [-C] [-d testdir] [-D] [-e number_of_bytes_to_read]",
        "              [-E] [-f first] [-F] [-h] [-i iterations] [-I items_per_dir] [-l last] [-L]",
        "              [-n number_of_items] [-N stride_length] [-p seconds] [-r]",
        "              [-R[seed]] [-s stride] [-S] [-t] [-T] [-u] [-v]",
        "              [-V verbosity_value] [-w number_of_bytes_to_write] [-y] [-z depth]",
        "\t-b: branching factor of hierarchical directory structure",
        "\t-B: no barriers between phases",
        "\t-c: collective creates: task 0 does all creates",
        "\t-C: only create files/dirs",
        "\t-d: the directory in which the tests will run",
        "\t-D: perform test on directories only (no files)",
        "\t-e: bytes to read from each file",
        "\t-E: only read files/dir",
        "\t-f: first number of tasks on which the test will run",
        "\t-F: perform test on files only (no directories)",
        "\t-h: prints this help message",
        "\t-i: number of iterations the test will run",
        "\t-I: number of items per directory in tree",
        "\t-l: last number of tasks on which the test will run",
        "\t-L: files only at leaf level of tree",
        "\t-n: every process will creat/stat/read/remove # directories and files",
        "\t-N: stride # between neighbor tasks for file/dir operation (local=0)",
        "\t-p: pre-iteration delay (in seconds)",
        "\t-r: only remove files or directories left behind by previous runs",
        "\t-R: randomly stat files (optional argument for random seed)",
        "\t-s: stride between the number of tasks for each test",
        "\t-S: shared file access (file only, no directories)",
        "\t-t: time unique working directory overhead",
        "\t-T: only stat files/dirs",
        "\t-u: unique working directory for each task",
        "\t-v: verbosity (each instance of option increments by one)",
        "\t-V: verbosity value",
        "\t-w: bytes to write to each file after it is created",
        "\t-y: sync file after writing",
        "\t-z: depth of hierarchical directory structure",
        "\t-Y: request distribution filename",
        "\t-P: request pruning factor",
        ""};

    for (const auto &opt : opts) {
        std::cout << opt << std::endl;
    }

    std::exit(0);
}

void summarize_results(int iterations) {
    // TODO: collect results
    std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT> total_counts{};
    for (int i = 0; i < local_counts.size(); i++) {
        for (int j = 0; j < OP_COUNT; j++) {
            for (int k = 0; k < SUB_OP_COUNT; k++) {
                total_counts[j][k] += local_counts[i][j][k];
            }
        }
    }
    // Formatting output
    std::cout << std::fixed << std::setprecision(3);  // set precision to 3 decimal places

    // start output results with a line

    std::cout << std::endl
              << "---------------------------OUTPUT---------------------------------"
              << std::endl;
    for (int op = 0; op < OP_COUNT; ++op) {
        std::cout << std::left << std::setw(18) << OperationToString((Operation)op) << " :" << std::setw(18) << OperationTime[op] << " ms, ";
        for (int sub_op = SubOperation::BOTTLENECK; sub_op <= SubOperation::ROTATION; ++sub_op) {
            std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                      << std::right << std::setw(10) << total_counts[op][sub_op] / (OperationTime[op] / 1000.0) << "ops/sec, ";
        }
        for (int sub_op = SubOperation::BOTTLENECKHIT; sub_op <= SubOperation::ROTATIONHIT; ++sub_op) {
            std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                      << std::right << std::setw(10) << total_counts[op][sub_op] << ", ";
        }
        std::cout << std::endl;
    }
    if (mode == 0 || rate_delay > 0){
        summarize_histograms();
    }
    // sort path_cnt_map by value
#ifdef NETFETCH_DEBUG
    std::vector<std::pair<std::string, int>> path_cnt_vec(path_cnt_map.begin(), path_cnt_map.end());
    std::sort(path_cnt_vec.begin(), path_cnt_vec.end(), [](const std::pair<std::string, int> &a, const std::pair<std::string, int> &b) {
        return a.second > b.second;
    });
    // dump path_cnt_map for debug
    for (const auto &pair : path_cnt_vec) {
        std::cout << pair.first << " : " << pair.second << std::endl;
    }
#endif
}
/* Checks to see if the test setup is valid.  If it isn't, perror. */

// paras: time interval
void summarize_mixed_results(std::chrono::high_resolution_clock::time_point startmixed, std::chrono::high_resolution_clock::time_point endmixed) {
    std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT> total_counts{};
    for (int i = 0; i < local_counts.size(); i++) {
        for (int j = 0; j < OP_COUNT; j++) {
            for (int k = 0; k < SUB_OP_COUNT; k++) {
                total_counts[j][k] += local_counts[i][j][k];
            }
        }
    }

    // Formatting output
    std::cout << std::fixed << std::setprecision(3);  // set precision to 3 decimal places

    // Calculate the total duration of the mixed workload
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endmixed - startmixed).count();

    // start output results with a line
    std::cout << std::endl
              << "---------------------------MIXED WORKLOAD OUTPUT---------------------------------"
              << std::endl;
    std::cout << "Total Duration: " << duration << " ms" << std::endl;
    // summarize total thpt when output single thpt
    // init a vector to store total thpt
    std::vector<double> total_thpt(SUB_OP_COUNT, 0.0);
    for (int op = 0; op < OP_COUNT; ++op) {
        std::cout << std::left << std::setw(18) << OperationToString((Operation)op) << " :" << std::setw(18) << duration << " ms, ";
        for (int sub_op = SubOperation::BOTTLENECK; sub_op <= SubOperation::ROTATION; ++sub_op) {
            std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                      << std::right << std::setw(10) << total_counts[op][sub_op] / (duration / 1000.0) << "ops/sec, ";
        }
        for (int sub_op = SubOperation::BOTTLENECKHIT; sub_op <= SubOperation::ROTATIONHIT; ++sub_op) {
            std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                      << std::right << std::setw(10) << total_counts[op][sub_op] << ", ";
        }
        std::cout << std::endl;
        // add total thpt
        for (int sub_op = SubOperation::BOTTLENECK; sub_op <= SubOperation::ROTATIONHIT; ++sub_op) {
            total_thpt[sub_op] += total_counts[op][sub_op];
        }
    }
    // output total thpt
    std::cout << std::left << std::setw(18) << "Total" << " :" << std::setw(18) << duration << " ms, ";
    for (int sub_op = SubOperation::BOTTLENECK; sub_op <= SubOperation::ROTATION; ++sub_op) {
        std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                  << std::right << std::setw(10) << total_thpt[sub_op] / (duration / 1000.0) << "ops/sec, ";
    }
    for (int sub_op = SubOperation::BOTTLENECKHIT; sub_op <= SubOperation::ROTATIONHIT; ++sub_op) {
        std::cout << std::left << std::setw(12) << SubOperationToString((SubOperation)sub_op) << " :"
                  << std::right << std::setw(10) << int(total_thpt[sub_op]) << ", ";
    }
    if (mode == 0 || rate_delay > 0){
        summarize_histograms();
    }
}

void valid_tests() {
    /* if dirs_only and files_only were both left unset, set is mixed*/
    if (!dirs_only && !files_only && !is_mixed) {
        dirs_only = files_only = 1;
        // is_mixed = 1;
    }

    /* if shared file 'S' access, no directory tests */
    if (shared_file) {
        dirs_only = 0;
    }

    /* check for no barriers with shifting processes for different phases.
       that is, one may not specify both -B and -N as it will introduce
       race conditions that may cause errors stat'ing or deleting after
       creates.
     */
    if ((barriers == 0) && (nstride != 0) && (rank == 0)) {
        perror("Possible race conditions will occur: -B not compatible with -N");
    }

    /* check for collective_creates incompatibilities */
    if (shared_file && collective_creates && rank == 0) {
        perror("-c not compatible with -S");
    }
    if (path_count > 1 && collective_creates && rank == 0) {
        perror("-c not compatible with multiple test directories");
    }
    if (collective_creates && !barriers) {
        perror("-c not compatible with -B");
    }

    /* check for shared file incompatibilities */
    if (unique_dir_per_task && shared_file && rank == 0) {
        perror("-u not compatible with -S");
    }

    /* check multiple directory paths and strided option */
    if (path_count > 1 && nstride > 0) {
        perror("cannot have multiple directory paths with -N strides between neighbor tasks");
    }

    /* check for shared directory and multiple directories incompatibility */
    if (path_count > 1 && unique_dir_per_task != 1) {
        perror("shared directory mode is not compatible with multiple directory paths");
    }

    /* check if more directory paths than ranks */
    if (path_count > size) {
        printf("path_count: %d, size: %d\n", path_count, size);
        perror("cannot have more directory paths than MPI tasks");
    }

    /* check depth */
    if (depth < 0) {
        perror("depth must be greater than or equal to zero");
    }
    /* check branch_factor */
    if (branch_factor < 1 && depth > 0) {
        perror("branch factor must be greater than or equal to zero");
    }
    /* check for valid number of items */
    if ((items > 0) && (items_per_dir > 0)) {
        perror("only specify the number of items or the number of items per directory");
    }
}

// Qingxiu: from main(): create_remove_directory_tree(1, 0, testdir, 0);
void create_remove_directory_tree(int create, int currDepth, const std::string &path, int dirNum) {
    if (is_mixed)
        return;  // skip when mixed workload
    if (currDepth == 0) {
        std::string dir = path + "/" + base_tree_name + "." + std::to_string(dirNum) + "/";

        if (create) {
            // Qingxiu: replace it with print first
            // TODO: create directory in hdfs
            std::string root_dir = path + "/" + base_tree_name + "." + std::to_string(dirNum);
            Task *task = new Task(root_dir, Operation::MKDIR);
            thread_pool->add_task(task);
#ifdef NETFETCH_DEBUG
            std::cout << "[NETFETCH_DEBUG][line " << __LINE__ << "] create directory: " << __LINE__ << dir << std::endl;
#endif
        }

        create_remove_directory_tree(create, ++currDepth, dir, ++dirNum);

        if (!create) {
            // Qingxiu: replace it with print first
            // TODO: hdfs delete directory

            Task *task = new Task(dir, Operation::RMDIR);
            thread_pool->add_task(task);
#ifdef NETFETCH_DEBUG
            printf("[NETFETCH_DEBUG] remove directory: %s\n", dir.c_str());
#endif
        }
    } else if (currDepth <= depth) {
        std::string temp_path = path;
        int currDir = dirNum;

        for (int i = 0; i < branch_factor; i++) {
            std::string next_dir = base_tree_name + "." + std::to_string(currDir) + "/";
            std::string full_path = temp_path + next_dir;

            if (create) {
                // Qingxiu: replace it with print first
                // TODO: hdfs mkdir

                Task *task = new Task(temp_path, Operation::MKDIR);
                thread_pool->add_task(task);
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_DEBUG] mkdir: %s\n", temp_path.c_str());
#endif
            }

            create_remove_directory_tree(create, ++currDepth,
                                         temp_path, (branch_factor * currDir) + 1);
            currDepth--;

            if (!create) {
                // Qingxiu: replace it with print first
                // TODO: hdfs delete directory
                Task *task = new Task(temp_path, Operation::RMDIR);
                thread_pool->add_task(task);
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_DEBUG] remove directory: %s\n", temp_path.c_str());
#endif
            }

            currDir++;
        }
    }
}


int main(int argc, char **argv) {
    // jz modified
    // add ini parse;
    init_mdtest(argc, argv);

    // socket

    // mdtest parameters
    int i, j, k, c;
    int nodeCount;

    struct {
        int first;
        int last;
        int stride;
    } range = {0, 0, 1};

    // initialize_socket(rank);

    // nodeCount = size / count_tasks_per_node();
    nodeCount = size;

    printf("-- started at %s --\n\n", timestamp());
    printf("mdtest-%s was launched with %d total task(s) on %d node(s)\n", RELEASE_VERS, size, nodeCount);
    fflush(stdout);
    fprintf(stdout, "Command line used:");
    for (i = 0; i < argc; i++) {
        fprintf(stdout, " %s", argv[i]);
    }
    fprintf(stdout, "\n");
    fflush(stdout);

    /* Parse command line options */

    if (!create_only && !stat_only && !read_only && !remove_only && !is_mixed) {
        create_only = stat_only = read_only = remove_only = 1;
    }

    valid_tests();
    // print items
    // printf("items: %d\n", items);
    /* setup total number of items and number of items per dir */
    if (depth <= 0) {
        num_dirs_in_tree = 1;
    } else {
        if (branch_factor < 1) {
            num_dirs_in_tree = 1;
        } else if (branch_factor == 1) {
            num_dirs_in_tree = depth + 1;
        } else {
            num_dirs_in_tree =
                (1 - pow(branch_factor, depth + 1)) / (1 - branch_factor);
        }
    }
    if (items_per_dir > 0) {
        items = items_per_dir * num_dirs_in_tree;
    } else {
        if (leaf_only) {
            if (branch_factor <= 1) {
                items_per_dir = items;
            } else {
                items_per_dir = items / pow(branch_factor, depth);
                items = items_per_dir * pow(branch_factor, depth);
                // printf("items: %d, items_per_dir: %d\n", items, items_per_dir);
            }
        } else {
            items_per_dir = items / num_dirs_in_tree;
            items = items_per_dir * num_dirs_in_tree;
        }
    }

    /* initialize rand_array */
    if (random_seed > 0) {
        srand(random_seed);

        int stop = 0;
        int s;

        if (leaf_only) {
            stop = items_per_dir * (int)pow(branch_factor, depth);
        } else {
            stop = items;
        }
        init_rand_array(stop);
    }

    /* allocate and initialize write buffer with # */
    if (write_bytes > 0) {
        write_buffer = (char *)malloc(write_bytes);
        if (write_buffer == NULL) {
            perror("out of memory");
            exit(1);
        }
        memset(write_buffer, 0x23, write_bytes);
    }

    /* setup directory path to work in */
    if (path_count == 0) { /* special case where no directory path provided with '-d' option */
        // getcwd(testdirpath, MAX_LEN);
        path_count = 1;
    } else {
        // strcpy(testdirpath, filenames[rank % path_count]);
        testdirpath = filenames[rank % path_count];
    }

    /*   if directory does not exist, create it */
    if ((rank < path_count)) {
#ifdef NETFETCH_DEBUG
        printf("[NETFETCH_ERROR] Unable to access test directory path, \"%s\"\n", testdirpath.c_str());
#endif
    }

    char temp_buffer[MAX_LEN];  // Temporary buffer

    if (gethostname(temp_buffer, MAX_LEN) == -1) {
        perror("gethostname");
        // MPI_Abort(MPI_COMM_WORLD, 2); // Handle error as appropriate
    } else {
        hostname = std::string(temp_buffer);  // Convert C-style string to std::string
    }

    if (last == 0) {
        first = size;
        last = size;
    }

    if (unique_dir_per_task) {
        base_tree_name = "mdtest_tree." + std::to_string(rank);
    } else {
        base_tree_name = "mdtest_tree";
    }

    /* start and end times of directory tree create/remove */
    std::chrono::high_resolution_clock::time_point startCreate, endCreate;

    /* default use shared directory */
    mk_name = "mdtest.shared.";
    stat_name = "mdtest.shared.";
    read_name = "mdtest.shared.";
    rm_name = "mdtest.shared.";

    /* Run the tests */
    for (int i = first; i <= last && i <= size; i += stride) {
        range.last = i - 1;
        // MPI_Group_range_incl(worldgroup, 1, (void *)&range, &testgroup);
        // MPI_Comm_create(MPI_COMM_WORLD, testgroup, &testcomm);
        if (rank == 0) {
            if (files_only && dirs_only) {
                printf("\n%d tasks, %llu files/directories\n", i, i * items);
            } else if (files_only) {
                if (!shared_file) {
                    printf("\n%d tasks, %llu files\n", i, i * items);
                } else {
                    printf("\n%d tasks, %llu files\n", i, items);
                }
            } else if (dirs_only) {
                printf("\n%d tasks, %llu directories\n", i, i * items);
            }
        }

        for (int j = 0; j < iterations; j++) {
            // strcpy(testdir, testdirpath);
            // if (testdir[strlen(testdir) - 1] != '/') {
            //     strcat(testdir, "/");
            // }
            // strcat(testdir, TEST_DIR);
            // sprintf(testdir, "%s.%d.d.%d.n.%d.b.%d", testdir, j, depth, items, branch_factor);

            std::string testdir = testdirpath;  // Assuming testdirpath is already a std::string
            // Ensure the directory path ends with a slash
            if (testdir.back() != '/') {
                testdir += '/';
            }
            // Append TEST_DIR
            testdir += TEST_DIR;
            // Append formatted string components
            // debug print all number
            // printf("j: %d, depth: %d, items: %d, branch_factor: %d\n", j, depth, items, branch_factor);
            testdir += "." + std::to_string(j) + ".d." + std::to_string(depth) + ".n." + std::to_string(items) + ".b." + std::to_string(branch_factor);

            // create testdir in hdfs
            if ((rank < path_count) && (current_method == NOCACHE_ID || current_method == CSCACHE_ID) && is_mixed == 0 && is_load == 1) {
#ifdef NETFETCH_DEBUG
                printf("[NETFETCH_DEBUG][line%d] create test dir: %s\n", __LINE__, testdir.c_str());
#endif
                // Task *task = new Task(testdir, Operation::MKDIR);
                // thread_pool->add_task(task);
            }

            // MPI_Barrier(MPI_COMM_WORLD);
            // Qingxiu: mdtest starts here!
            /* create hierarchical directory structure */
            // MPI_Barrier(MPI_COMM_WORLD);
            if (create_only) {
                startCreate = std::chrono::high_resolution_clock::now();  // Qingxiu: record start time
                if (unique_dir_per_task) {
                    if (collective_creates && (rank == 0)) {
                        /*
                         * This is inside two loops, one of which already uses "i" and the other uses "j".
                         * I don't know how this ever worked. I'm changing this loop to use "k".
                         */
                        for (int k = 0; k < size; k++) {
                            base_tree_name = "mdtest_tree." + std::to_string(k);

                            /*
                             * Let's pass in the path to the directory we most recently made so that we can use
                             * full paths in the other calls.
                             */
                            create_remove_directory_tree(1, 0, testdir, 0);
                        }
                    } else if (!collective_creates) {
                        /*
                         * Let's pass in the path to the directory we most recently made so that we can use
                         * full paths in the other calls.
                         */
                        create_remove_directory_tree(1, 0, testdir, 0);
                    }
                } else {
                    if (rank == 0) {
                        /*
                         * Let's pass in the path to the directory we most recently made so that we can use
                         * full paths in the other calls.
                         */
                        create_remove_directory_tree(1, 0, testdir, 0);
                    }
                }
                // MPI_Barrier(MPI_COMM_WORLD);
                while (!thread_pool->is_phase_end()) {
                    if (rtries >= 128 && mode == 1) {
                        exit(-1);
                    }
                }
                endCreate = std::chrono::high_resolution_clock::now();
                // summary_table[j].entry[8] =
                //     num_dirs_in_tree / (endCreate - startCreate);

            } else {
                //
                // summary_table[j].entry[8] = 0;
            }

            std::string unique_mk_dir = testdir + "/" + base_tree_name + ".0";
            std::string unique_chdir_dir = testdir + "/" + base_tree_name + ".0";
            std::string unique_stat_dir = testdir + "/" + base_tree_name + ".0";
            std::string unique_read_dir = testdir + "/" + base_tree_name + ".0";
            std::string unique_rm_dir = testdir + "/" + base_tree_name + ".0";
            std::string unique_rm_uni_dir = testdir;
            if (rank < i) {
                if (!shared_file) {
                    mk_name = "mdtest." + std::to_string((rank + (0 * nstride)) % i) + ".";
                    stat_name = "mdtest." + std::to_string((rank + (1 * nstride)) % i) + ".";
                    read_name = "mdtest." + std::to_string((rank + (2 * nstride)) % i) + ".";
                    rm_name = "mdtest." + std::to_string((rank + (3 * nstride)) % i) + ".";
                }

                if (unique_dir_per_task) {
                    unique_mk_dir = testdir + "/mdtest_tree." + std::to_string((rank + (0 * nstride)) % i) + ".0";
                    unique_chdir_dir = testdir + "/mdtest_tree." + std::to_string((rank + (1 * nstride)) % i) + ".0";
                    unique_stat_dir = testdir + "/mdtest_tree." + std::to_string((rank + (2 * nstride)) % i) + ".0";
                    unique_read_dir = testdir + "/mdtest_tree." + std::to_string((rank + (3 * nstride)) % i) + ".0";
                    unique_rm_dir = testdir + "/mdtest_tree." + std::to_string((rank + (4 * nstride)) % i) + ".0";
                    unique_rm_uni_dir = testdir;  // Assuming this is just the test directory
                }

                top_dir = unique_mk_dir;

                if (dirs_only && !shared_file) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    directory_test(j, i, unique_mk_dir);
                }
                if (files_only) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    file_test(j, i, unique_mk_dir);
                }
            }

            /* remove directory structure */

            // MPI_Barrier(MPI_COMM_WORLD);
            if (remove_only) {
                startCreate = std::chrono::high_resolution_clock::now();
                if (unique_dir_per_task) {
                    if (collective_creates && (rank == 0)) {
                        /*
                         * This is inside two loops, one of which already uses "i" and the other uses "j".
                         * I don't know how this ever worked. I'm changing this loop to use "k".
                         */
                        for (int k = 0; k < size; k++) {
                            base_tree_name = "mdtest_tree." + std::to_string(k);

                            /*
                             * Let's pass in the path to the directory we most recently made so that we can use
                             * full paths in the other calls.
                             */
                            create_remove_directory_tree(0, 0, testdir, 0);
                        }
                    } else if (!collective_creates) {
                        /*
                         * Let's pass in the path to the directory we most recently made so that we can use
                         * full paths in the other calls.
                         */
                        create_remove_directory_tree(0, 0, testdir, 0);
                    }
                } else {
                    if (rank == 0) {
                        create_remove_directory_tree(0, 0, testdir, 0);
                    }
                }
                while (!thread_pool->is_phase_end()) {
                    if (rtries >= 128 && mode == 1) {
                        exit(-1);
                    }
                }
                // MPI_Barrier(MPI_COMM_WORLD);
                endCreate = std::chrono::high_resolution_clock::now();
                // summary_table[j].entry[9] = num_dirs_in_tree / (endCreate - startCreate);

                if ((rank < path_count) && is_mixed == 0) {
                    Task *task = new Task(testdir, Operation::RMDIR);
                    thread_pool->add_task(task);
                }

            } else {
                // summary_table[j].entry[9] = 0;
            }
        }
        if (is_mixed == 0)
            summarize_results(iterations);

        if (i == 1 && stride > 1) {
            i = 0;
        }
    }

    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }

    // Task *task_x = new Task("/#test-dir.0.d.2.n.32760.b.4/mdtest_tree.0/mdtest_tree.2/mdtest_tree.10/file.mdtest.shared.16595", Operation::STAT, bottleneck_id);  // Assuming MKDIR recreates the directory
    // thread_pool->add_task(task_x);
    // while (!thread_pool->is_phase_end()) {if(rtries >= 128 && mode == 1){exit(-1);}
    // }
    // exit(0);
    // do mixed workload exp2 here
    // is_mixed create_oly remove_only
    printf("[LINE %d] is_mixed: %d, create_only: %d, remove_only: %d\n", __LINE__, is_mixed, create_only, remove_only);
    if (is_mixed == 1 && create_only == 1) {  // create + mkdir
        //  if cscache and csfletch only test mkdir here
        //  print start
        bool mkdir_only = false;
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            std::cout << "only mkdir for cscaching and csfletch" << std::endl;
            // goto mkdir_only;
            mkdir_only = true;
        }

        // do create first
        set_create();
        print_mixed_ratio();
        int req_count = 0;
        if (!request_filename_base.empty()) {
            req_count = read_workload_from_request_file(is_uniform, items, request_pruning_factor, request_filename_base);
        } else if (flag_for_file_out_generation || is_load) {
            // do nothing
        } else {
            // error and exit
            std::cerr << "Error: request_filename_base is empty" << std::endl;
            exit(1);
        }
        std::cout << "split workload: create + mkdir " << req_count << std::endl;
        generate_op_array(req_count);
        // set mixed ratio create = 100.0

        printf("[LINE %d] req_count: %d\n", __LINE__, req_count);

        delete_operations.reserve(req_count / server_logical_num);
        // rmdir_operations.reserve(req_count / server_logical_num);
        rename_operations.reserve(req_count / server_logical_num);
        // generate operations
        // generate_op_array(req_count);
        testdir = "/";
        testdir += TEST_DIR;
        // Append formatted string components
        // debug print all number
        // printf("j: %d, depth: %d, items: %d, branch_factor: %d\n", j, depth, items, branch_factor);
        testdir += "." + std::to_string(0) + ".d." + std::to_string(depth) + ".n." + std::to_string(items) + ".b." + std::to_string(branch_factor);

        base_tree_name = "mdtest_tree";
        stat_name = "mdtest.shared.";
        auto temp_path = testdir + "/" + base_tree_name + ".0";
        // generate reqs
        generate_reqs_mixed_by_rand_array(temp_path, base_tree_name, stat_name, branch_factor, items_per_dir);

        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        // print reqs_mixed for debug
        // for (auto &req : reqs_mixed) {
        //     std::cout << OperationToString(std::get<0>(req)) << " " << std::get<1>(req) << " " << std::get<2>(req) << std::endl;
        // }
        // record start time
        if (mkdir_only == true) {
            if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
                prepare_client_cache(testdir);
            }
        }
        auto start_time = std::chrono::high_resolution_clock::now();
        start_ticker();
        if (mkdir_only == false) {
            // send reqs
            for (auto &req : reqs_mixed) {
                Operation op = std::get<0>(req);
                std::string path = std::get<1>(req);
                uint32_t nodeidx = std::get<2>(req);
                auto new_path = path + ".create";
                // add to delete set
                delete_operations.insert(req);
                Task *task = new Task(new_path, Operation::TOUCH, nodeidx);
                thread_pool->add_task(task);
            }
            while (!thread_pool->is_phase_end()) {
                if (rtries >= 128 && mode == 1) {
                    exit(-1);
                }
            }
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        if (mkdir_only == false) {
            OperationTime[Operation::TOUCH] = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

            // roll back
            for (const auto &delete_op : delete_operations) {
                std::string path = std::get<1>(delete_op) + ".create";
#ifdef NETFETCH_DEBUG
                std::cout << "[Rollback] create deleted path: " << path << std::endl;
#endif
                Task *task = new Task(path, Operation::RM, std::get<2>(delete_op));  // Assuming TOUCH recreates the file
                thread_pool->add_task(task);
            }
            while (!thread_pool->is_phase_end()) {
                if (rtries >= 128 && mode == 1) {
                    exit(-1);
                }
            }
        }
        // then do mkdir
        set_mkdir();

        start_time = std::chrono::high_resolution_clock::now();
        for (auto &req : reqs_mixed) {
            Operation op = std::get<0>(req);
            std::string path = std::get<1>(req);
            uint32_t nodeidx = std::get<2>(req);
            auto new_path = path + ".mkdir";
            // add to delete set
            rename_operations.insert(req);
            Task *task = new Task(new_path, Operation::MKDIR, nodeidx);
            thread_pool->add_task(task);
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        end_time = std::chrono::high_resolution_clock::now();

        OperationTime[Operation::MKDIR] = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        // roll back
        for (const auto &rmdir_op : rename_operations) {
            std::string path = std::get<1>(rmdir_op) + ".mkdir";
#ifdef NETFETCH_DEBUG
            std::cout << "[Rollback] create deleted path: " << path << std::endl;
#endif
            Task *task = new Task(path, Operation::RMDIR, std::get<2>(rmdir_op));  // Assuming TOUCH recreates the file
            thread_pool->add_task(task);
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        summarize_results(0);
    } else if (is_mixed == 1 && remove_only == 1) {  // rmdir
        // set mixed ratio rmdir = 100.0
        std::cout << "set mixed ratio rmdir = 100.0" << std::endl;
        set_rmdir();
        print_mixed_ratio();
        int req_count = 0;
        if (!request_filename_base.empty()) {
            req_count = read_workload_from_request_file(is_uniform, items, request_pruning_factor, request_filename_base);
        } else if (flag_for_file_out_generation || is_load) {
            // do nothing
        } else {
            // error and exit
            std::cerr << "Error: request_filename_base is empty" << std::endl;
            exit(1);
        }
        std::cout << "split workload: rmdir " << req_count << std::endl;
        generate_op_array(req_count);
        // set mixed ratio create = 100.0

        delete_operations.reserve(req_count / server_logical_num);
        // rmdir_operations.reserve(req_count / server_logical_num);
        rename_operations.reserve(req_count / server_logical_num);
        // generate operations
        // generate_op_array(req_count);
        testdir = "/";
        testdir += TEST_DIR;
        // Append formatted string components
        // debug print all number
        // printf("j: %d, depth: %d, items: %d, branch_factor: %d\n", j, depth, items, branch_factor);
        testdir += "." + std::to_string(0) + ".d." + std::to_string(depth) + ".n." + std::to_string(items) + ".b." + std::to_string(branch_factor);

        base_tree_name = "mdtest_tree";
        stat_name = "mdtest.shared.";
        auto temp_path = testdir + "/" + base_tree_name + ".0";
        // generate reqs
        generate_reqs_mixed_by_rand_array_rmdir(temp_path, base_tree_name, stat_name, branch_factor, items_per_dir);

        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        for (auto &req : reqs_mixed) {
            Operation op = std::get<0>(req);
            std::string path = std::get<1>(req);
            uint32_t nodeidx = std::get<2>(req);
            // auto new_path = path ;
            // add to delete set
            if (delete_operations.find(req) == delete_operations.end()) {
                delete_operations.insert(req);
                Task *task = new Task(path, Operation::MKDIR, nodeidx);
                thread_pool->add_task(task);
            }
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }

        // warmup for netcache and netfetch
        if (!(current_method == NOCACHE_ID || current_method == CSCACHE_ID) && ((mode == 1 && rotation_id == bottleneck_id) || mode == 0 || need_warm == true)) {
            // for mode ==1 static mode
            // only first rotation need warmup
            printf("start warmup\n");
            mdtest_write_hot_paths_to_switch_rmdir(temp_path);
            // sleep for tofino ready
        } else if (current_method != 2) {
            prepare_hot_path_token_rmdir(temp_path);
        }
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            prepare_client_cache(testdir);
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }

        start_ticker();
        auto start_time = std::chrono::high_resolution_clock::now();
        for (auto &req : reqs_mixed) {
            Operation op = std::get<0>(req);
            std::string path = std::get<1>(req);
            uint32_t nodeidx = std::get<2>(req);
            // auto new_path = path + ".rmdir";
            // add to delete set

            Task *task = new Task(path, Operation::RMDIR, nodeidx);
            thread_pool->add_task(task);
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        auto end_time = std::chrono::high_resolution_clock::now();

        OperationTime[Operation::RMDIR] = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        summarize_results(0);

        // rollback rmdir maybe failed
        // print
        std::cout << "Rollback rmdir operations1: " << delete_operations.size() << std::endl;
        // Rollback delete operations
        for (const auto &delete_op : delete_operations) {
            std::string path = std::get<1>(delete_op);
            Task *task = new Task(path, Operation::RMDIR, std::get<2>(delete_op));  // Assuming TOUCH recreates the file
            thread_pool->add_task(task);
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }

    } else if (is_mixed) {
        // init variable
        // Create a container to record delete and rename operations.
        // There may be multiple delete operations in one mixed workload, but you only need to record the first one.

        parser_mixed();
        // print mixed ratio
        print_mixed_ratio();
        testdir = "/";
        testdir += TEST_DIR;
        // Append formatted string components
        // debug print all number
        // printf("j: %d, depth: %d, items: %d, branch_factor: %d\n", j, depth, items, branch_factor);
        testdir += "." + std::to_string(0) + ".d." + std::to_string(depth) + ".n." + std::to_string(items) + ".b." + std::to_string(branch_factor);

        base_tree_name = "mdtest_tree";
        stat_name = "mdtest.shared.";
        auto temp_path = testdir + "/" + base_tree_name + ".0";

        if (mode == 0) {
            // dynamic mode
            parser_dynamic();
            print_dynamic_ratio();
            gen_cached_set(temp_path);
            prepare_dynmaic_map(temp_path);
            MAX_RETRIES = 3;
            // print dynamic map for debug
#ifdef NETFETCH_DEBUG
            int i = 0;
            std::cout << "dynamic map size: " << dynamic_maps.size() << std::endl;
            for (auto &dynamic_map : dynamic_maps) {
                i++;
                if (i == 10)
                    break;
                int j = 0;
                for (auto &c : sorted_cached_set) {
                    std::cout << "[" << j++ << "]" << "(" << c << ", " << fast_extract_last_integer(dynamic_map[gen_file_name(temp_path, c)]) << "), ";
                }
                std::cout << std::endl
                          << "--------------------------------------------"
                          << std::endl;
            }
#endif
        }
        // load rand array
        int req_count = 0;
        if (!request_filename_base.empty()) {
            req_count = read_workload_from_request_file(is_uniform, items, request_pruning_factor, request_filename_base);
        } else if (flag_for_file_out_generation || is_load) {
            // do nothing
        } else {
            // error and exit
            std::cerr << "Error: request_filename_base is empty" << std::endl;
            exit(1);
        }

        printf("[LINE %d] req_count: %d\n", __LINE__, req_count);

        delete_operations.reserve(req_count / server_logical_num);
        // rmdir_operations.reserve(req_count / server_logical_num);
        rename_operations.reserve(req_count / server_logical_num);
        // generate operations
        generate_op_array(req_count);

        // generate reqs
        generate_reqs_mixed_by_rand_array(temp_path, base_tree_name, stat_name, branch_factor, items_per_dir);
        if (!(current_method == NOCACHE_ID || current_method == CSCACHE_ID) && ((mode == 1 && rotation_id == bottleneck_id) || mode == 0 || need_warm == true)) {
            // for mode ==1 static mode
            // only first rotation need warmup
            printf("[LINE %d] start warmup\n", __LINE__);
            mdtest_write_hot_paths_to_switch(temp_path);
            // sleep for tofino ready
            if ((current_method == NETFETCH_ID || current_method == CSFLETCH_ID) && mode == 0) {
                Task *task = new Task("/afterwarmup/afterwarmup", Operation::WARMUP, bottleneck_id);  // end warmup
                thread_pool->add_task(task);
                sleep(10);
            }
        } else if (current_method != 2) {
            prepare_hot_path_token(temp_path);
        }
        if (current_method == CSCACHE_ID || current_method == CSFLETCH_ID) {
            prepare_client_cache(testdir);
        }

        // printf("[LINE %d] reqs_mixed size: %zu\n", __LINE__, reqs_mixed.size());

        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                printf("[LINE %d] thread pool is not end, but rtries >= 128, exit\n", __LINE__);
                exit(-1);
            }
        }
        // printf("[LINE %d] thread pool is end, continue\n", __LINE__);
        // print reqs_mixed for debug
        // for (auto &req : reqs_mixed) {
        //     std::cout << OperationToString(std::get<0>(req)) << " " << std::get<1>(req) << " " << std::get<2>(req) << std::endl;
        // }
        std::chrono::high_resolution_clock::time_point startmixed, endmixed;
        // record start time
        start_ticker();
        startmixed = std::chrono::high_resolution_clock::now();
        // send reqs
        int dir_counter = 0, slow_dir_counter = 0;
        for (auto &req : reqs_mixed) {
            Operation op = std::get<0>(req);
            std::string path = std::get<1>(req);
            uint32_t nodeidx = std::get<2>(req);
#if 0
            std::cout << "[MIXED][line " << __LINE__ << "] send req: " << OperationToString(op) << " " << path << " " << nodeidx << std::endl;
#endif

            // Check if the operation is a delete or rename and if it has already been recorded
            if (op == Operation::RM && delete_operations.find(req) == delete_operations.end()) {
                delete_operations.insert(req);
            } else if (op == Operation::MV && rename_operations.find(req) == rename_operations.end()) {
                rename_operations.insert(req);
            }
            if (op == Operation::RMDIR) {
                std::string test_dir = temp_path + "/test" + std::to_string(slow_dir_counter);
                slow_dir_counter++;
                Task *task = new Task(test_dir, Operation::RMDIR, nodeidx);
                thread_pool->add_task(task);
                rmdir_operations.insert(test_dir);
            } else if (op == Operation::MKDIR) {
                std::string test_dir = temp_path + "/test" + std::to_string(dir_counter);
                dir_counter++;
                Task *task = new Task(test_dir, Operation::MKDIR, nodeidx);
                thread_pool->add_task(task);

                rmdir_operations.insert(test_dir);
            } else {
                Task *task = new Task(path, op, nodeidx);
                thread_pool->add_task(task);
            }
        }
        // end get results
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                printf("[LINE %d] thread pool is not end, but rtries >= 128, exit\n", __LINE__);
                exit(-1);
            }
        }
        // record end time

        endmixed = std::chrono::high_resolution_clock::now();
        summarize_mixed_results(startmixed, endmixed);
        dump_per_sec_total_counts();
        stop_ticker();
        if ((current_method == CSCACHE_ID || current_method == CSFLETCH_ID) && mode == 0) {
            goto skip_rollback;
        }
        if (mode == 0) {
            mode = 1;  // switch to static mode to rollback deleted files
        }
        // roll back rename and delete by delete_operations and rename_operations
        // Rollback rename operations

        // print rollback size
        // we should rollback rmdir first and wait for it finish
        std::cout << "Rollback rmdir operations2: " << rmdir_operations.size() << std::endl;
        for (const auto &rmdir_path : rmdir_operations) {
#ifdef NETFETCH_DEBUG
            std::cout << "[Rollback] create rmdir path: " << rmdir_path << std::endl;
#endif
            // printf("bottleneck_id %d == rotation_id %d \n", bottleneck_id, rotation_id);

            if(bottleneck_id != rotation_id) {
                Task *task_2 = new Task(rmdir_path, Operation::RMDIR, rotation_id);  // Assuming MKDIR recreates the directory
                thread_pool->add_task(task_2);
            }
            Task *task_1 = new Task(rmdir_path, Operation::RMDIR, bottleneck_id);  // Assuming MKDIR recreates the directory
            thread_pool->add_task(task_1);
            
        }
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
        std::cout << "Rollback rename operations: " << rename_operations.size() << std::endl;
        for (const auto &rename_op : rename_operations) {
            std::string path = std::get<1>(rename_op);
            std::string renamed_path = path + ".bak";  // Assuming original path is stored with "_original" suffix
// print rollback path for debug
#ifdef NETFETCH_DEBUG
            std::cout << "[Rollback] create rename path: " << renamed_path << " to " << path << std::endl;
#endif
            // std::cout << "[Rollback] create rename path: " << std::get<2>(rename_op) << std::endl;
            // recover it by delete and create
            Task *task_1 = new Task(path, Operation::TOUCH, std::get<2>(rename_op));  // Assuming TOUCH recreates the file
            thread_pool->add_task(task_1);
            // delete the renamed file
            Task *task_2 = new Task(renamed_path, Operation::RM, std::get<2>(rename_op));
            thread_pool->add_task(task_2);
        }
        std::cout << "Rollback delete operations: " << delete_operations.size() << std::endl;
        // Rollback delete operations
        for (const auto &delete_op : delete_operations) {
            std::string path = std::get<1>(delete_op);
#ifdef NETFETCH_DEBUG
            std::cout << "[Rollback] create deleted path: " << path << std::endl;
#endif
            // std::cout << "[Rollback] create rename path: " << std::get<2>(delete_op) << std::endl;
            Task *task = new Task(path, Operation::TOUCH, std::get<2>(delete_op));  // Assuming TOUCH recreates the file
            thread_pool->add_task(task);
        }
        // wait rollback done
        while (!thread_pool->is_phase_end()) {
            if (rtries >= 128 && mode == 1) {
                exit(-1);
            }
        }
    }

skip_rollback:
    if (is_mixed) {
    } else {
        dump_per_sec_total_counts();
    }
    stop_ticker();
    // send finish signal
    if (send_finish && mode == 1 && (current_method == CSCACHE_ID || current_method == CSFLETCH_ID)) {
        if (bottleneck_id == rotation_id) {
            Task *task = new Task("/you/dont/care/the/path", Operation::FINISH, bottleneck_id);
            thread_pool->add_task(task);
        } else {
            Task *task = new Task("/you/dont/care/the/path", Operation::FINISH, bottleneck_id);
            thread_pool->add_task(task);
            task = new Task("/you/dont/care/the/path", Operation::FINISH, rotation_id);
            thread_pool->add_task(task);
        }
    }
    while (!thread_pool->is_phase_end()) {
        if (rtries >= 128 && mode == 1) {
            exit(-1);
        }
    }
    printf("\n-- finished at %s --\n", timestamp());
    fflush(stdout);
    // free_ini();
    close_histograms();
    delete thread_pool;
    exit(0);
}
