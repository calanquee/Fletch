#include "threadpool.h"

#include <unistd.h>

#include <cstring>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
std::atomic<bool> running(true);
std::mutex warmup_mutex;
std::vector<std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT>> local_counts;
std::array<hdr_histogram*, OP_COUNT> global_histograms[2];
std::vector<std::array<hdr_histogram*, OP_COUNT>> local_histograms[2];  // local histograms
// 1 is bottleneck
// 0 is rotation
std::unordered_set<std::tuple<Operation, std::string, uint32_t>,
                   std::function<std::size_t(const std::tuple<Operation, std::string, uint32_t>&)>>
    delete_operations(0, [](const auto& t) {
        return std::hash<Operation>{}(std::get<0>(t)) ^ (std::hash<std::string>{}(std::get<1>(t)) << 1) ^ (std::hash<uint32_t>{}(std::get<2>(t)) << 2);
    });
std::unordered_set<std::string> rmdir_operations;
std::unordered_set<std::tuple<Operation, std::string, uint32_t>,
                   std::function<std::size_t(const std::tuple<Operation, std::string, uint32_t>&)>>
    rename_operations(0, [](const auto& t) {
        return std::hash<Operation>{}(std::get<0>(t)) ^ (std::hash<std::string>{}(std::get<1>(t)) << 1) ^ (std::hash<uint32_t>{}(std::get<2>(t)) << 2);
    });

int is_load = 0;
int thread_count = 64;

ThreadPool* thread_pool = nullptr;

// Task constructor
Task::Task(const char* _path, Operation _operation)
    : op(_operation) {
    path = std::string(_path);
}
Task::Task(const std::string& _path, Operation _operation)
    : path(_path), op(_operation) {}

Task::Task(const std::string& _path, Operation _operation, uint32_t _nodeidx)
    : path(_path), op(_operation), nodeidx(_nodeidx) {}

// Task destructor
Task::~Task() {
}
// generate 128 random number in a vector

// // TaskQueue methods
void TaskQueue::push(Task* task) {
    pthread_mutex_lock(&mutex);
    queue.push(task);
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
}

bool TaskQueue::pop(Task*& task) {
    pthread_mutex_lock(&mutex);
    while (queue.empty() && !stop) {
        pthread_cond_wait(&cond, &mutex);
    }
    // if (running == false && rate_delay > 0) {
    //     while (!queue.empty()) {
    //         task = queue.front();
    //         queue.pop();
    //         delete task;
    //         task = NULL;
    //     }
    //     pthread_mutex_unlock(&mutex);
    //     return false;
    // }
    if (!queue.empty()) {
        task = queue.front();
        queue.pop();
        pthread_mutex_unlock(&mutex);
        return true;
    } else {
        pthread_mutex_unlock(&mutex);
        return false;
    }
}

void TaskQueue::shutdown() {
    pthread_mutex_lock(&mutex);
    stop = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&mutex);
}

bool TaskQueue::is_queue_empty() {
    pthread_mutex_lock(&mutex);
    bool done = queue.empty();
    pthread_mutex_unlock(&mutex);
    return done;
}
int TaskQueue::size() {
    pthread_mutex_lock(&mutex);
    int size = queue.size();
    pthread_mutex_unlock(&mutex);
    return size;
}
nolockTaskQueue::nolockTaskQueue(size_t capacity)
    : queue(capacity), current_size(0), max_capacity(capacity), stop(false) {}

void nolockTaskQueue::push(Task* task) {
    std::unique_lock<std::mutex> lock(mtx);
    cond_full.wait(lock, [this]() { return current_size < max_capacity || stop; });

    if (stop)
        return;

    if (queue.push(task)) {
        ++current_size;
        cond_empty.notify_one();  // 通知等待任务的线程
    }
}

bool nolockTaskQueue::pop(Task*& task) {
    std::unique_lock<std::mutex> lock(mtx);
    cond_empty.wait(lock, [this]() { return current_size > 0 || stop; });

    if (stop && current_size == 0) {
        return false;
    }

    if (queue.pop(task)) {
        --current_size;
        cond_full.notify_one();
        return true;
    }

    return false;
}

void nolockTaskQueue::shutdown() {
    {
        std::lock_guard<std::mutex> lock(mtx);
        stop = true;
    }
    cond_full.notify_all();
    cond_empty.notify_all();
}

bool nolockTaskQueue::is_queue_empty() {
    return current_size.load(std::memory_order_relaxed) == 0;
}

int nolockTaskQueue::size() {
    return static_cast<int>(current_size.load(std::memory_order_relaxed));
}

bool ThreadPool::is_queue_empty() {
    if (running == false && mode == 0) {
    return true;
    }
    return task_queue->is_queue_empty();
}
void ThreadPool::add_task(Task* task) {
    phase_counter++;
    // std::cout << "phase_counter " << phase_counter << std::endl;
    // if (!flag_fprintf && task_queue->size() > 100 * thread_count) {
    //     // sleep 0.1s
    //     usleep(100000);
    // }
    task_queue->push(task);
}
void ThreadPool::add_task_with_sending_rate(Task* task, __useconds_t usec) {
    if (mode == 1 && rate_delay > 0 && running == false) {
        delete task;
        task = NULL;
        return;
    }
    phase_counter++;
    rate_counter++;
    // std::cout << "phase_counter " << phase_counter << std::endl;
    usleep(usec);
    task_queue->push(task);
}
// ThreadPool methods
ThreadPool::ThreadPool(int size)
    : task_queue(new TaskQueue()), stop(false) {
    local_counts.resize(size);
    for (int i = 0; i < size; ++i) {
        threads.emplace_back([this, i]() {
            std::string thread_name = "mdtest.thread" + std::to_string(i);
            int thread_client_fd;
            create_udpsock(thread_client_fd, true, thread_name.c_str(), TIMEOUT_SEC, TIMEOUT_USEC, UDP_DEFAULT_RCVBUFSIZE);
            // printf("thread %d start with socketfd %d\n", i, thread_client_fd);
            while (!stop) {
                Task* task;
                if (task_queue->pop(task)) {
                    if (task == NULL) {
                        std::cerr << "pop NULL task:error\n";
                        continue;
                    }
                    if (mode == 1) {
                        handle_task(task, thread_client_fd, i);
                    } else {
                        handle_task_dynmaic(task, thread_client_fd, i);
                    }
                    delete task;
                    task = NULL;
                }
            }
            close(thread_client_fd);
        });
    }
}

ThreadPool::ThreadPool(int size, int logical_clients_number)
    : task_queue(new TaskQueue()), stop(false) {
    //  init task_queues 8 thread 1 queue
    // // task_queues.resize(logical_clients_number);
    // for (int i = 0; i < logical_clients_number; ++i) {
    //     task_queues[i] = new TaskQueue();
    // }

    local_counts.resize(size);
    // init  std::vector<tbb::concurrent_unordered_map<std::string, token_t>> path_token_maps;
    // init each path_token_maps
    path_token_maps.resize(logical_clients_number);
    path_permission_maps.resize(logical_clients_number);
    path_frequency_maps.resize(logical_clients_number);
    for (int i = 0; i < logical_clients_number; ++i) {
        path_token_maps[i] = std::unordered_map<std::string, token_t>();
        path_permission_maps[i] = std::unordered_map<std::string, uint16_t>();
        path_frequency_maps[i] = std::unordered_map<std::string, int>();
    }

    size_t num_cores = std::thread::hardware_concurrency();  // Get the number of CPU cores
    // print
    printf("[CPU] num_cores: %ld\n", num_cores);
    for (int i = 0; i < size; ++i) {
        threads.emplace_back([this, i, num_cores]() {
            bind_thread_to_core(num_cores - i % num_cores - 1);
            // if (rate_delay > 0 && mode == 1) {
            //     // sleep for a random time between 0 and rate_delay us
            //     useconds_t randomMinorDelay = rand() % rate_delay;
            //     usleep(randomMinorDelay);
            // }
            std::string thread_name = "mdtest.thread" + std::to_string(i);
            int thread_client_fd;
            create_udpsock(thread_client_fd, true, thread_name.c_str(), TIMEOUT_SEC, TIMEOUT_USEC, UDP_DEFAULT_RCVBUFSIZE);
            // printf("thread %d start with socketfd %d\n", i, thread_client_fd);
            static thread_local int opsdone = 0;
            static thread_local auto startTimeNanos = std::chrono::high_resolution_clock::now();
            static thread_local bool is_start = false;
            // auto start = std::chrono::high_resolution_clock::now();
            // int counter = 0;
            while (!stop) {
                Task* task;
                if (task_queue->pop(task)) {
                    if (task == NULL) {
                        std::cerr << "pop NULL task:error\n";
                        continue;
                    }
                    if (task->op != Operation::WARMUP && !is_start) {
                        is_start = true;

                        if (rate_delay > 0 && mode == 1) {
                            // sleep for a random time between 0 and rate_delay us
                            // real random delay
                            //
                            auto radnom_seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
                            std::mt19937 gen(radnom_seed);
                            std::uniform_int_distribution<> dis(0, rate_delay);
                            useconds_t randomMinorDelay = dis(gen);
                            // useconds_t randomMinorDelay = rate_delay - rand() % rate_delay;
                            usleep(randomMinorDelay);
                        } else {
                            usleep(i * 1000 / thread_count);
                        }
                        startTimeNanos = std::chrono::high_resolution_clock::now();
                        opsdone = 0;
                    }
                    if (rate_delay > 0 && mode == 1 && task->op != Operation::WARMUP && running != false) {
                        handle_task(task, thread_client_fd, i);
                        opsdone++;
                        throttle_ops(startTimeNanos, opsdone, rate_delay);
                    } else if (mode == 1) {
                        // counter++;
                        // if(counter == 10){
                        //     break;
                        // }
                        // printf("handle task %s %s\n", task->path.c_str(), OperationToString(task->op).c_str());
                        handle_task(task, thread_client_fd, i);
                        // auto end = std::chrono::high_resolution_clock::now();
                        // int64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
                        // printf("Thread %d handled task %s %s in %ld microseconds\n", i, task->path.c_str(), OperationToString(task->op).c_str(), duration);
                        // start = end;
                        
                    } else {
                        handle_task_dynmaic(task, thread_client_fd, i);
                    }
                    delete task;
                    task = NULL;
                }
            }
            close(thread_client_fd);
        });
    }
}

ThreadPool::~ThreadPool() {
    stop = true;
    task_queue->shutdown();
    for (auto& thread : threads) {
        thread.join();
    }
    // Clean up any remaining tasks
    Task* remainingTask;
    while (task_queue->pop(remainingTask)) {
        delete remainingTask;
        remainingTask = NULL;
    }

    // Safely delete the task queue
    delete task_queue;
    task_queue = nullptr;
}

void ThreadPool::handle_task(Task* task, int thread_client_fd, int thread_id) {
    // dump task
    // __useconds_t delay_first, delay_second;
    // delay_first = rate_delay * thread_id / thread_count;
    // delay_second = rate_delay - delay_first;
    // if (rate_delay > 0) {
    //     if (mode == 1 && running == false) {
    //         phase_counter--;
    //         return;
    //     }
    //     // if (task->op != Operation::WARMUP) {
    //     //     usleep(delay_first);
    //     // }
    // }
#ifdef NETFETCH_DEBUG
    printf("TASK %s %s\n", task->path.c_str(), OperationToString(task->op).c_str());
#endif
    // removeExtraSlashes(task->path);
    // if (is_load == 1) {
    //     printf("TASK %s %s\n", task->path.c_str(), OperationToString(task->op).c_str());
    // }

    bool is_not_skip = false;
    auto start = std::chrono::high_resolution_clock::now();
    switch (task->op) {
    case MKDIR:
        if (is_load == 1) {
            // do nothing
            // is_not_skip = netfetch_loaddir(task->path, thread_client_fd, thread_id);
        } else {
            is_not_skip = netfetch_mkdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        break;
    case RMDIR:
        is_not_skip = netfetch_rmdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case TOUCH:
        if (is_load == 1) {
            is_not_skip = netfetch_load(task->path, thread_client_fd, thread_id);
        } else {
            is_not_skip = netfetch_touch(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        break;
    case LOAD:
        // netfetch_load(task->path, thread_client_fd, thread_id);
        perror("we will not use mdtest to load now!");
        exit(1);
        break;
    case RM:
        if (rename_operations.find(std::make_tuple(task->op, task->path, task->nodeidx)) != rename_operations.end()) {
            auto new_path = task->path + ".bak";
            is_not_skip = netfetch_rm(new_path, thread_client_fd, thread_id, task->nodeidx);
        } else {
            is_not_skip = netfetch_rm(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        // is_not_skip = netfetch_rm(task->path, thread_client_fd, thread_id, task->nodeidx);
        // phase_counter--;
        break;
    case STAT:
        is_not_skip = netfetch_stat(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case OPEN:
        is_not_skip = netfetch_open(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case CLOSE:
        is_not_skip = netfetch_close(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case FINISH:
        netfetch_finish_signal(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case WARMUP:
        warmup_mutex.lock();
        netfetch_write_cache(task->path, thread_client_fd);
        is_not_skip = true;
        phase_counter--;
        warmup_mutex.unlock();
        break;
    case CHMOD:
        is_not_skip = netfetch_chmod(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case MV:
        is_not_skip = netfetch_mv(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case STATDIR:
        is_not_skip = netfetch_statdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case READDIR:
        is_not_skip = netfetch_readdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    default:
        break;
    }
    // phase_counter--;
    auto end = std::chrono::high_resolution_clock::now();
    if (is_not_skip) {
        int64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        hdr_record_value(local_histograms[task->nodeidx != bottleneck_id][thread_id][task->op], duration);
    }

    // if (rate_delay > 0) {
    //     if (task->op != Operation::WARMUP) {
    //         usleep(delay_second);
    //     }
    // }
}
void ThreadPool::throttle_ops(const std::chrono::high_resolution_clock::time_point& startTimeNanos, int opsdone, int rate_delay) {
    auto targetOpsTickNs = std::chrono::microseconds(rate_delay);
    auto deadline = startTimeNanos + opsdone * targetOpsTickNs;
    auto now = std::chrono::high_resolution_clock::now();

    if (now < deadline) {
        // std::cout << "now < deadline" << std::endl;
        std::this_thread::sleep_for(deadline - now);
    }
}
//
void ThreadPool::handle_task_dynmaic(Task* task, int thread_client_fd, int thread_id) {
    if (mode == 0 && running == false) {
        phase_counter--;
        return;
    }
    // dump task
#ifdef NETFETCH_DEBUG
    printf("TASK %s %s\n", task->path.c_str(), OperationToString(task->op).c_str());
#endif
    int iteration = dynamic_iteration.load();
    if (dynamic_maps[iteration].find(task->path) != dynamic_maps[iteration].end()) {
        task->path = dynamic_maps[iteration][task->path];
    }
    // std::tuple<Operation, std::string, uint32_t>
    auto req = std::make_tuple(task->op, task->path, task->nodeidx);
    if (task->op == Operation::RM && delete_operations.find(req) == delete_operations.end()) {
        delete_operations.insert(req);
    } else if (task->op == Operation::MV && rename_operations.find(req) == rename_operations.end()) {
        rename_operations.insert(req);
    } else if (task->op == Operation::RMDIR && rmdir_operations.find(task->path) == rmdir_operations.end()) {
        rmdir_operations.insert(task->path);
    }

    bool is_not_skip = false;
    auto start = std::chrono::high_resolution_clock::now();
    switch (task->op) {
    case MKDIR:
        if (is_load == 1) {
            is_not_skip = netfetch_loaddir(task->path, thread_client_fd, thread_id);
        } else {
            is_not_skip = netfetch_mkdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        break;
    case RMDIR:
        is_not_skip = netfetch_rmdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case TOUCH:
        if (is_load == 1) {
            is_not_skip = netfetch_load(task->path, thread_client_fd, thread_id);
        } else {
            is_not_skip = netfetch_touch(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        break;
    case LOAD:
        // netfetch_load(task->path, thread_client_fd, thread_id);
        perror("we will not use mdtest to load now!");
        exit(1);
        break;
    case RM:
        if (rename_operations.find(std::make_tuple(task->op, task->path, task->nodeidx)) != rename_operations.end()) {
            auto new_path = task->path + ".bak";
            is_not_skip = netfetch_rm(new_path, thread_client_fd, thread_id, task->nodeidx);
        } else {
            is_not_skip = netfetch_rm(task->path, thread_client_fd, thread_id, task->nodeidx);
        }
        phase_counter--;
        // is_not_skip = netfetch_rm(task->path, thread_client_fd, thread_id, task->nodeidx);
        // phase_counter--;
        break;
    case STAT:
        is_not_skip = netfetch_stat(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case OPEN:
        is_not_skip = netfetch_open(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case CLOSE:
        is_not_skip = netfetch_close(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case FINISH:
        netfetch_finish_signal(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case WARMUP:
        warmup_mutex.lock();
        netfetch_write_cache(task->path, thread_client_fd);
        is_not_skip = true;
        phase_counter--;
        warmup_mutex.unlock();
        break;
    case CHMOD:
        is_not_skip = netfetch_chmod(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case MV:
        is_not_skip = netfetch_mv(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case STATDIR:
        is_not_skip = netfetch_statdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    case READDIR:
        is_not_skip = netfetch_readdir(task->path, thread_client_fd, thread_id, task->nodeidx);
        phase_counter--;
        break;
    default:
        break;
    }
    // phase_counter--;
    auto end = std::chrono::high_resolution_clock::now();
    if (is_not_skip) {
        int64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        hdr_record_value(local_histograms[task->nodeidx != bottleneck_id][thread_id][task->op], duration);
    }
}

bool ThreadPool::is_phase_end() {
    // std::cout << "phase_counter " << phase_counter << " is_queue_empty " << this->is_queue_empty() << std::endl;
    // if (running == false && rate_delay > 0) {
    //     phase_counter = 0;
    // }
    return phase_counter == 0 && this->is_queue_empty();
}

void ThreadPool::set_phase_start(int counter) {
    phase_counter = counter;
}

void init_histograms() {
    for (int _ = 0; _ <= 1; _++) {
        for (int i = 0; i < OP_COUNT; ++i) {
            hdr_init(1, 200000, 1, &global_histograms[_][i]);
        }
    }

    for (int _ = 0; _ <= 1; _++) {
        local_histograms[_].resize(thread_count);
        for (int i = 0; i < thread_count; ++i) {
            // local_histograms[i].resize(OP_COUNT);
            for (int j = 0; j < OP_COUNT; ++j) {
                hdr_init(1, 200000, 1, &local_histograms[_][i][j]);
            }
        }
    }
}
void close_histograms() {
    for (int _ = 0; _ <= 1; _++) {
        for (int i = 0; i < OP_COUNT; ++i) {
            hdr_close(global_histograms[_][i]);
        }
    }
    for (int _ = 0; _ <= 1; _++) {
        for (int i = 0; i < thread_count; ++i) {
            for (int j = 0; j < OP_COUNT; ++j) {
                hdr_close(local_histograms[_][i][j]);
            }
        }
    }
}

void export_histogram_to_csv(hdr_histogram* histogram, const std::string& filename) {
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << filename << std::endl;
        return;
    }
    file << "Value,Count,TotalCount\n";

    struct hdr_iter iter;
    hdr_iter_init(&iter, histogram);
    while (hdr_iter_next(&iter)) {
        int64_t value = iter.value_iterated_to;
        int64_t count = iter.count;
        int64_t total_count = iter.cumulative_count;
        file << value << "," << count << "," << total_count << "\n";
    }

    file.close();
    // std::cout << "Histogram data exported to " << filename << std::endl;
}

void summarize_histograms() {
    for (int _ = 0; _ <= 1; _++) {
        for (int i = 0; i < thread_count; ++i) {
            for (int j = 0; j < OP_COUNT; ++j) {
                hdr_add(global_histograms[_][j], local_histograms[_][i][j]);
            }
        }
    }

    std::cout << std::endl
              << std::endl
              << std::left << std::setw(15) << "Operation"
              << std::right << std::setw(20) << "Average Latency (μs)"
              << std::setw(20) << "p90 Latency (μs)"
              << std::setw(20) << "p95 Latency (μs)"
              << std::setw(20) << "p99 Latency (μs)" << std::endl;

    for (int _ = 0; _ <= 1; _++) {
        if (_ == 0) {
            std::cout << std::string(30, '-') << "bottleneck" << std::string(30, '-') << std::endl;

        } else {
            std::cout << std::string(31, '-') << "rotation" << std::string(31, '-') << std::endl;
        }
        for (int i = 0; i < OP_COUNT; ++i) {
            int64_t mean = int64_t(hdr_mean(global_histograms[_][i]));

            int64_t p90 = hdr_value_at_percentile(global_histograms[_][i], 90.0);
            int64_t p95 = hdr_value_at_percentile(global_histograms[_][i], 95.0);
            int64_t p99 = hdr_value_at_percentile(global_histograms[_][i], 99.0);
            std::cout << std::left << std::setw(15) << OperationToString(static_cast<Operation>(i))
                      << std::right << std::setw(20) << (mean > 0 ? mean : 0)
                      << std::setw(20) << (p90 > 0 ? p90 : 0)
                      << std::setw(20) << (p95 > 0 ? p95 : 0)
                      << std::setw(20) << (p99 > 0 ? p99 : 0) << std::endl;
            if (mean > 0) {
                if (_ == 0) {
                    std::string latency_file = "latency_" + std::to_string(bottleneck_id) + "_" + std::to_string(rotation_id) + "_" + std::to_string(bottleneck_id) + "_" + OperationToString(static_cast<Operation>(i)) + ".csv";
                    export_histogram_to_csv(global_histograms[_][i], latency_file);
                } else {
                    std::string latency_file = "latency_" + std::to_string(bottleneck_id) + "_" + std::to_string(rotation_id) + "_" + std::to_string(rotation_id) + "_" + OperationToString(static_cast<Operation>(i)) + ".csv";
                    export_histogram_to_csv(global_histograms[_][i], latency_file);
                }
            }
        }
    }
}

void ThreadPool::bind_thread_to_core(int thread_idx) {
    // Set CPU Affinity for the current thread
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);             // Clear the CPU set
    CPU_SET(thread_idx, &cpuset);  // Bind to the specified core (thread_idx)

    // Get the current thread's handle
    pthread_t current_thread = pthread_self();

    // Apply the CPU affinity settings
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to set CPU affinity for thread " << thread_idx << std::endl;
    } else {
        // std::cout << "Thread " << std::this_thread::get_id() << " bound to core " << thread_idx << std::endl;
    }
}
