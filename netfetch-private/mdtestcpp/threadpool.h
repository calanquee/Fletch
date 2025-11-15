#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>
#include <netinet/in.h>
#include <pthread.h>
#include <boost/lockfree/queue.hpp>
#include <atomic>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "common_impl.h"
#include "fssocket.h"
#include "workload.h"
#include "wrapper.h"
// Enum for operations
extern std::atomic<bool> running;
// delete_operations
extern std::unordered_set<std::tuple<Operation, std::string, uint32_t>,
                          std::function<std::size_t(const std::tuple<Operation, std::string, uint32_t>&)>>
    delete_operations;

extern std::unordered_set<std::string> rmdir_operations;

extern std::unordered_set<std::tuple<Operation, std::string, uint32_t>,
                          std::function<std::size_t(const std::tuple<Operation, std::string, uint32_t>&)>>
    rename_operations;

extern std::vector<std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT>> local_counts;
extern std::array<hdr_histogram*, OP_COUNT> histograms;
// Task structure
struct Task {
    Task(const char* _path, Operation _operation);
    Task(const std::string& _path, Operation _operation);
    Task(const std::string& _path, Operation _operation, uint32_t _nodeidx);

    ~Task();
    Operation op;
    std::string path;
    uint32_t nodeidx = 0;
};

// TaskQueue class
class TaskQueue {
public:
    void push(Task* task);
    bool pop(Task*& task);
    void shutdown();
    int size();
    bool is_queue_empty();

private:
    std::queue<Task*> queue;
    pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
    bool stop = false;
};
class nolockTaskQueue {
public:
    void push(Task* task);
    bool pop(Task*& task);
    void shutdown();
    int size();
    bool is_queue_empty();
    explicit nolockTaskQueue(size_t capacity = 128000);
    
private:
    size_t max_capacity;
    boost::lockfree::queue<Task*> queue;
    std::atomic<size_t> current_size{0};
    std::atomic<bool> stop{false};
    mutable std::mutex mtx;
    std::condition_variable cond_full;
    std::condition_variable cond_empty;
};
// ThreadPool class
class ThreadPool {
public:
    ThreadPool(int size);
    ThreadPool(int size, int logical_clients_number);
    ~ThreadPool();
    void add_task(Task* task);
    void add_task_with_sending_rate(Task* task, __useconds_t usec); 
    bool is_queue_empty();
    bool is_phase_end();
    void set_phase_start(int counter);

private:
    void handle_task(Task* task, int thread_client_fd, int thread_id);
    void handle_task_dynmaic(Task* task, int thread_client_fd, int thread_id);
    void throttle_ops(const std::chrono::high_resolution_clock::time_point& startTimeNanos, int opsdone, int rate_delay);
    void bind_thread_to_core(int thread_idx);
    std::vector<std::thread> threads;
    TaskQueue* task_queue;
    // multiple task queues
    // std::vector<TaskQueue*> task_queues;
    bool stop;
    std::atomic<int> phase_counter{0};
    std::atomic<int> rate_counter{0};
    int logical_clients_number = 1;
};

void init_histograms();
void close_histograms();
void summarize_histograms();

extern int thread_count;
extern int is_load;

extern ThreadPool* thread_pool;
#endif  // THREADPOOL_H
