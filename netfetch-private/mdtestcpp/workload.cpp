#include "workload.h"

std::atomic<int> dynamic_iteration(0);

std::vector<uint32_t> rand_array;
std::vector<Operation> op_array;
// req:
// path, nodeidx
std::vector<std::pair<std::string, uint32_t>> reqs;
std::vector<std::tuple<Operation, std::string, uint32_t>> reqs_mixed;
std::unordered_map<uint32_t, std::pair<std::string, uint32_t>> reqs_set;
std::unordered_set<uint32_t> not_reqs_set;
std::vector<std::unordered_map<std::string, std::string>> dynamic_maps;  // 32M / 40 KOPS = 800s prepare 128 maps
std::vector<int> sorted_cached_set;
std::vector<int> new_sorted_cached_set;
std::unordered_set<int> cached_set;
std::unordered_set<int> new_cached_set;
// int random_seed = 0;
void init_rand_array(int stop) {
    rand_array.resize(stop);

    for (int s = 0; s < stop; s++) {
        rand_array[s] = s;
    }

    /* shuffle list randomly */
    int n = stop;
    while (n > 1) {
        n--;

        /*
         * Generate a random number in the range 0 .. n
         *
         * rand() returns a number from 0 .. RAND_MAX. Divide that
         * by RAND_MAX and you get a floating point number in the
         * range 0 .. 1. Multiply that by n and you get a number in
         * the range 0 .. n.
         */

        int k =
            (int)(((double)rand() / (double)RAND_MAX) * (double)n);

        /*
         * Now move the nth element to the kth (randomly chosen)
         * element, and the kth element to the nth element.
         */

        int tmp = rand_array[k];
        rand_array[k] = rand_array[n];
        rand_array[n] = tmp;
    }
}

int read_workload_from_request_file(int is_uniform, int items, double request_pruning_factor, std::string &request_filename_base) {
    // Random generator for pruning
    std::random_device rd;
    std::mt19937 gen(rd_seed);
    std::uniform_real_distribution<> dis(0.0, 1.0);
    rand_array.clear();
    unsigned long long count = 0;
    if (is_uniform) {
        std::cout << "uniform test" << std::endl;
        for (unsigned long long i = 0; i < items; i++) {
            double possibilty = (1.0 / request_pruning_factor);
            if (request_pruning_factor < 1.0)
                possibilty = 1.0;
            if (dis(gen) <= possibilty) {
                rand_array.push_back(i);
                ++count;
            }
        }
        std::mt19937 gen(rd_seed);
        std::shuffle(rand_array.begin(), rand_array.end(), gen);
        return count;
    }

    std::ifstream file(request_filename_base);
    if (!file.is_open()) {
        std::cerr << "Error: read_workload_from_request_file cannot open file " << request_filename_base << std::endl;
        std::exit(1);
    }

    file.seekg(0, std::ios::end);
    std::streampos file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    rand_array.reserve(file_size / 10);  // reserve space for the vector AUSSUME 10 bytes per line

    std::string line;

    std::string content;
    content.resize(file_size);
    file.read(&content[0], file_size);
    file.close();
    std::istringstream stream(content);
    double possibilty = (1.0 / request_pruning_factor);
    if (request_pruning_factor < 1.0)
        possibilty = 1.0;
    while (std::getline(stream, line)) {
        if (dis(gen) <= possibilty) {
            uint32_t value = std::stoul(line);
            rand_array.push_back(value);
            ++count;
        }
    }
    // if request_pruning_factor < 1.0 and server_logical_numeber = 128, we need to double the rand_array
    if (request_pruning_factor < 1.0) {
        rand_array.insert(rand_array.end(), rand_array.begin(), rand_array.end());
        // shuffle rand_array
        std::mt19937 gen(rd_seed);
        std::shuffle(rand_array.begin(), rand_array.end(), gen);
        count *= 2;
    }else{
        std::mt19937 gen(rd_seed);
        std::shuffle(rand_array.begin(), rand_array.end(), gen);
    }

    // file.close();
    // Resize rand_array and copy data
    return count;
}

unsigned long long getParentDir(unsigned long long item_num, int items_per_dir){
    return 0;
}

void generate_reqs_by_rand_array(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir, bool is_file_test) {
    int parent_dir = 0, item_num = 0;
    std::string item;
    reqs.clear();
    reqs_set.clear();

    reqs.reserve(rand_array.size() / 4);
    reqs_set.reserve(rand_array.size() / 4);
    for (auto &item_num : rand_array) {
        // item: file.mdtest.0.0
        if (not_reqs_set.find(item_num) != not_reqs_set.end()) {
            continue;
        }
        if (reqs_set.find(item_num) != reqs_set.end()) {
            // printf("%s Mapped to: %d need\n", reqs_set[item_num].first.c_str(), reqs_set[item_num].second);
            reqs.push_back(reqs_set[item_num]);
            // printf("%s Mapped to: %d needed\n", reqs_set[item_num].first.c_str(), reqs_set[item_num].second);
            continue;
        }
        std::string type_str = "file.";
        item = type_str + std::string(stat_name) + std::to_string(item_num);
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
        // check partition
        auto hashValue = computeMD5(fullPath);
        uint32_t nodeidx = mapToNameNodeidx(hashValue);
        if (nodeidx != bottleneck_id && nodeidx != rotation_id && mode == 1) {
#ifdef NETFETCH_DEBUG
            // printf("%s Mapped to: %d skip\n", fullPath.c_str(), nodeidx);
#endif
            not_reqs_set.insert(item_num);
            continue;
        }
        // printf("%s Mapped to: %d send\n", fullPath.c_str(), nodeidx);
        reqs.push_back(std::make_pair(fullPath, nodeidx));
        reqs_set[item_num] = std::make_pair(fullPath, nodeidx);
        // printf("%s Mapped to: %d sended\n", fullPath.c_str(), nodeidx);
    }
    // Clear reqs_set
    // std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>().swap(reqs_set);
    std::cout << "generate done reqs size: " << reqs.size() << std::endl;
}

bool generate50PercentChance() {
    std::mt19937 gen(rd_seed);
    std::bernoulli_distribution dist(0.5);

    return dist(gen);
}

void generate_op_array(int total_ops) {
    // if (request_pruning_factor < 1.0) {
        // total_ops = total_ops * 2;
    // }

    op_array.clear();
    // std::cout << "total_ops: " << total_ops << std::endl;
    op_array.reserve(total_ops);

    // number of operations
    std::map<Operation, int> operationCounts;
    int generated_count = 0;
    for (const auto &[op, ratio] : operationRatios) {
        int count = static_cast<int>(ratio * total_ops);
        operationCounts[op] = count;
        generated_count += count;
    }

    // not enough operations, add more
    int difference = total_ops - generated_count;
    // std::cout << "difference: " << difference << std::endl;
    if (difference > 0) {
        //
        std::vector<std::pair<Operation, double>> sortedRatios(operationRatios.begin(), operationRatios.end());
        std::sort(sortedRatios.begin(), sortedRatios.end(), [](const auto &a, const auto &b) {
            return a.second > b.second;
        });

        for (int i = 0; i < difference; ++i) {
            operationCounts[sortedRatios[i % sortedRatios.size()].first]++;
        }
    }

    // generate op_array
    for (const auto &[op, count] : operationCounts) {
        if (op == Operation::TOUCH) {
            op_array.insert(op_array.end(), count, op);
        }
    }

    for (const auto &[op, count] : operationCounts) {
        // op_array.insert(op_array.end(), count, op);
        if (op == Operation::MV || op == Operation::RM || op == Operation::RMDIR || op == Operation::TOUCH) {
            // if it is delete or rmdir or mv, move it to the end (contention)
        } else {
            op_array.insert(op_array.end(), count, op);
        }
    }

    // shuffle op_array with a fixed seed
    std::mt19937 gen(rd_seed);
    if (mode == 1) {
        // std::shuffle(op_array.begin() + operationCounts.at(Operation::TOUCH), op_array.end(), gen);
        std::shuffle(op_array.begin(), op_array.end(), gen);
    } else if (mode == 0) {
        std::shuffle(op_array.begin(), op_array.end(), gen);
    }
    
    // generate op_array
    for (const auto &[op, count] : operationCounts) {
        if (op == Operation::MV) {
            op_array.insert(op_array.end(), count, op);
        }
    }
    for (const auto &[op, count] : operationCounts) {
        if (op == Operation::RM) {
            op_array.insert(op_array.end(), count, op);
        }
    }
    for (const auto &[op, count] : operationCounts) {
        if (op == Operation::RMDIR) {
            op_array.insert(op_array.end(), count, op);
        }
    }
}

void generate_reqs_mixed_by_rand_array(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir) {
    int parent_dir = 0, item_num = 0;
    int rand_array_size = rand_array.size();
    if (rand_array_size != op_array.size()) {
        std::cerr << "Error: rand_array size" << rand_array_size << " is not equal to op_array size " << op_array.size() << std::endl;
        std::exit(1);
    }
    if (reqs.size() != 0) {
        std::cerr << "Error: We are doing mixed workload, reqs should be empty" << std::endl;
        std::exit(1);
    }
    std::string item;
    reqs_mixed.clear();
    reqs_set.clear();
    reqs_mixed.reserve(int(rand_array_size / request_pruning_factor));
    reqs_set.reserve(int(rand_array_size / request_pruning_factor));
    // iterate over rand_array and op_array

    for (int i = 0; i < rand_array_size; i++) {
        // item: file.mdtest.0.0
        if (not_reqs_set.find(rand_array[i]) != not_reqs_set.end()) {
            continue;
        }
        if (reqs_set.find(rand_array[i]) != reqs_set.end()) {
            std::string dir_path = reqs_set[rand_array[i]].first;
            if (op_array[i] == Operation::MKDIR || op_array[i] == Operation::RMDIR || op_array[i] == Operation::STATDIR) {
                dir_path = getParentDirectory(reqs_set[rand_array[i]].first);
            } else if (op_array[i] == Operation::READDIR) {
                dir_path = getParentDirectory(reqs_set[rand_array[i]].first);
                // std::cout << "gen " << dir_path << std::endl;
            }
            reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, reqs_set[rand_array[i]].second));
            continue;
        }

        item = "file." + std::string(stat_name) + std::to_string(rand_array[i]);
        parent_dir = rand_array[i] / items_per_dir;

        if (parent_dir > 0) {
            // Construct directory tree based on parent_dir
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            }
        }
        std::string fullPath = path + "/" + item;
        // if(i < 10){
        //     printf("rand array[%d]: %d, item: %s\n", i, rand_array[i], fullPath.c_str());
        // }

        // check partition
        auto hashValue = computeMD5(fullPath);
        uint32_t nodeidx = mapToNameNodeidx(hashValue);
        if (nodeidx != bottleneck_id && nodeidx != rotation_id && mode == 1) {
#ifdef NETFETCH_DEBUG
            printf("%s Mapped to: %d skip\n", fullPath.c_str(), nodeidx);
#endif
            not_reqs_set.insert(rand_array[i]);
            continue;
        }
        std::string dir_path = fullPath;
        // if it is a dir operation, get the parent dir
        if (op_array[i] == Operation::MKDIR || op_array[i] == Operation::RMDIR || op_array[i] == Operation::STATDIR) {
            dir_path = getParentDirectory(fullPath);
        } else if (op_array[i] == Operation::READDIR) {
            dir_path = getParentDirectory(fullPath);
            // std::cout << "gen " << dir_path << std::endl;
        }
        // if (op_array[i] == Operation::MKDIR || op_array[i] == Operation::RMDIR) {
        //     // if (generate50PercentChance()) {
        //     reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, nodeidx));
        //     // reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, rotation_id));
        //     // }
        // } else {
        reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, nodeidx));
        // }
        reqs_set[rand_array[i]] = std::make_pair(fullPath, nodeidx);
    }
    // Clear reqs_set
    std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>().swap(reqs_set);
}

void generate_reqs_mixed_by_rand_array_rmdir(const std::string &path, std::string &base_tree_name, std::string &stat_name, int branch_factor, int items_per_dir) {
    int parent_dir = 0, item_num = 0;
    int rand_array_size = rand_array.size();
    if (rand_array_size != op_array.size()) {
        std::cerr << "Error: rand_array size" << rand_array_size << " is not equal to op_array size " << op_array.size() << std::endl;
        std::exit(1);
    }
    if (reqs.size() != 0) {
        std::cerr << "Error: We are doing mixed workload, reqs should be empty" << std::endl;
        std::exit(1);
    }
    std::string item;
    reqs_mixed.clear();
    reqs_set.clear();
    reqs_mixed.reserve(int(rand_array_size / request_pruning_factor));
    reqs_set.reserve(int(rand_array_size / request_pruning_factor));
    // iterate over rand_array and op_array

    for (int i = 0; i < rand_array_size; i++) {
        // item: file.mdtest.0.0
        if (not_reqs_set.find(rand_array[i]) != not_reqs_set.end()) {
            continue;
        }
        if (reqs_set.find(rand_array[i]) != reqs_set.end()) {
            std::string dir_path = reqs_set[rand_array[i]].first;

            reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, reqs_set[rand_array[i]].second));
            continue;
        }

        item = "file." + std::string(stat_name) + std::to_string(rand_array[i]);
        parent_dir = rand_array[i] / items_per_dir;

        if (parent_dir > 0) {
            // Construct directory tree based on parent_dir
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            while (parent_dir > branch_factor) {
                parent_dir = (parent_dir - 1) / branch_factor;
                item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
            }
        }
        std::string fullPath = path + "/" + item + ".rmdir";
        // check partition
        auto hashValue = computeMD5(fullPath);
        uint32_t nodeidx = mapToNameNodeidx(hashValue);
        if (nodeidx != bottleneck_id && nodeidx != rotation_id && mode == 1) {
#ifdef NETFETCH_DEBUG
            printf("%s Mapped to: %d skip\n", fullPath.c_str(), nodeidx);
#endif
            not_reqs_set.insert(rand_array[i]);
            continue;
        }
        std::string dir_path = fullPath;
        // if it is a dir operation, get the parent dir

        reqs_mixed.push_back(std::make_tuple(op_array[i], dir_path, nodeidx));
        reqs_set[rand_array[i]] = std::make_pair(fullPath, nodeidx);
    }
    // Clear reqs_set
    std::unordered_map<uint32_t, std::pair<std::string, uint32_t>>().swap(reqs_set);
}

void updateWeights_set(std::vector<int> &weights, int &lastrow, int &currentrow, int weightToAdd, int kv_count, int &total_weight) {
    while (weights[lastrow] > CAPACITY_PER_LINE - weightToAdd && lastrow < currentrow) {
        lastrow++;
    }
    if (weights[lastrow] <= CAPACITY_PER_LINE - weightToAdd) {
        weights[lastrow] += weightToAdd;
    } else if (weights[currentrow] <= CAPACITY_PER_LINE - weightToAdd) {
        weights[currentrow] += weightToAdd;
    } else {
        currentrow++;
        if (currentrow % WARMUP_LIMIT == WARMUP_LIMIT - 1) {
        }
        weights[currentrow] += weightToAdd;
    }
}

// Function to split the path, print according to the file_parse_offset, and calculate the weight
int parse_and_count_weight_set(const std::string &path, int &total_weight, int kv_count, std::unordered_set<std::string> &processed_paths, std::vector<int> &weights, int &currentrow, int &lastrow) {
    int count = 0;
    // Split the path by "/"

    auto levels = extractPathLevels(path, path_resolution_offset);
    for (size_t i = 0; i < levels.size(); ++i) {
        // Check if this path has been processed before
        if (processed_paths.find(levels[i]) == processed_paths.end()) {
            // Add path to hash table to mark it as processed
            processed_paths.insert(levels[i]);
            total_weight += 3;
            // If this is the last token, adjust weight as we will handle the full path separately
            if (i == levels.size() - 1) {
                total_weight += 2;

                updateWeights_set(weights, lastrow, currentrow, 5, kv_count, total_weight);
                // only insert the last level into the cached_set

                break;
            }
            updateWeights_set(weights, lastrow, currentrow, 3, kv_count, total_weight);
            // cached_set.insert(levels[i]);
        }
    }

    return (currentrow >= kv_count || total_weight >= kv_count * CAPACITY_PER_LINE) ? 1 : 0;
}

void updateWeights(std::vector<int> &weights, int &lastrow, int &currentrow, int weightToAdd, int kv_count, int &total_weight) {
    while (weights[lastrow] > CAPACITY_PER_LINE - weightToAdd && lastrow < currentrow) {
        lastrow++;
    }
    if (weights[lastrow] <= CAPACITY_PER_LINE - weightToAdd) {
        weights[lastrow] += weightToAdd;
    } else if (weights[currentrow] <= CAPACITY_PER_LINE - weightToAdd) {
        weights[currentrow] += weightToAdd;
    } else {
        currentrow++;
        if (currentrow % WARMUP_LIMIT == WARMUP_LIMIT - 1) {
            std::this_thread::sleep_for(std::chrono::seconds(4));
        }
        weights[currentrow] += weightToAdd;
    }
}

// Function to split the path, print according to the file_parse_offset, and calculate the weight
int parse_and_count_weight(const std::string &path, int &total_weight, int kv_count, std::unordered_set<std::string> &processed_paths, std::vector<int> &weights, int &currentrow, int &lastrow) {
    int count = 0;
    // Split the path by "/"

    auto levels = extractPathLevels(path, path_resolution_offset);
    for (size_t i = 0; i < levels.size(); ++i) {
        // Check if this path has been processed before
        if (processed_paths.find(levels[i]) == processed_paths.end()) {
            // Add path to hash table to mark it as processed
            processed_paths.insert(levels[i]);
            total_weight += 3;
            // If this is the last token, adjust weight as we will handle the full path separately
            if (i == levels.size() - 1) {
                total_weight += 2;
#ifdef NETFETCH_DEBUG
                // std::cout << "Path: " << levels[i] << ", Weight: 5\n *lastrow " << lastrow << " " << weights[lastrow] << " *currentrow " << currentrow << " " << weights[currentrow] << std::endl;
#endif
                updateWeights(weights, lastrow, currentrow, 5, kv_count, total_weight);

                Task *task = new Task(levels[i], Operation::WARMUP);
                thread_pool->add_task(task);
                break;
            }
#ifdef NETFETCH_DEBUG
            // std::cout << "Path: " << levels[i] << ", Weight: 3\n *lastrow " << lastrow << " " << weights[lastrow] << " *currentrow " << currentrow << " " << weights[currentrow] << std::endl;
#endif
            updateWeights(weights, lastrow, currentrow, 3, kv_count, total_weight);

            Task *task = new Task(levels[i], Operation::WARMUP);
            thread_pool->add_task(task);
        }
    }

    return (currentrow >= kv_count || total_weight >= kv_count * CAPACITY_PER_LINE) ? 1 : 0;
}

int parse_and_count_weight_rmdir(const std::string &path, int &total_weight, int kv_count, std::unordered_set<std::string> &processed_paths, std::vector<int> &weights, int &currentrow, int &lastrow) {
    int count = 0;
    // Split the path by "/"

    auto levels = extractPathLevels(path, path_resolution_offset);
    for (size_t i = 0; i < levels.size(); ++i) {
        // Check if this path has been processed before
        if (processed_paths.find(levels[i]) == processed_paths.end()) {
            // Add path to hash table to mark it as processed
            processed_paths.insert(levels[i]);
            total_weight += 3;
            // If this is the last token, adjust weight as we will handle the full path separately
            if (i == levels.size() - 1) {
                total_weight += 0;
#ifdef NETFETCH_DEBUG
                // std::cout << "Path: " << levels[i] << ", Weight: 5\n *lastrow " << lastrow << " " << weights[lastrow] << " *currentrow " << currentrow << " " << weights[currentrow] << std::endl;
#endif
                updateWeights(weights, lastrow, currentrow, 3, kv_count, total_weight);

                Task *task = new Task(levels[i] + ".rmdir", Operation::WARMUP);
                thread_pool->add_task(task);
                break;
            }
#ifdef NETFETCH_DEBUG
            // std::cout << "Path: " << levels[i] << ", Weight: 3\n *lastrow " << lastrow << " " << weights[lastrow] << " *currentrow " << currentrow << " " << weights[currentrow] << std::endl;
#endif
            updateWeights(weights, lastrow, currentrow, 3, kv_count, total_weight);

            Task *task = new Task(levels[i], Operation::WARMUP);
            thread_pool->add_task(task);
        }
    }

    return (currentrow >= kv_count || total_weight >= kv_count * CAPACITY_PER_LINE) ? 1 : 0;
}

std::string gen_file_name(const std::string &path, int idx) {
    auto item = "file." + std::string(stat_name) + std::to_string(idx);
    int parent_dir = idx / items_per_dir;

    if (parent_dir > 0) {
        // Construct directory tree based on parent_dir
        item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
        while (parent_dir > branch_factor) {
            parent_dir = (parent_dir - 1) / branch_factor;
            item = std::string(base_tree_name) + "." + std::to_string(parent_dir) + "/" + item;
        }
    }
    std::string fullPath = path + "/" + item;
    return fullPath;
}

void gen_cached_set(const std::string &path) {
    int kv_count = hot_path_size;  // Assuming a defined kv_count value

    std::cout << "we plan to prepare cached_set" << kv_count << std::endl;

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
            sorted_cached_set.push_back(number1);
            cached_set.insert(number1);
            if (parse_and_count_weight_set(tmp_path, total_weight, kv_count, processed_paths, weights, currentrow, lastrow)) {
                break;
            }
        }
    }
    file.close();
}

void addRandomNumbersToBack(
    std::vector<int> &sorted_cached_set,
    std::unordered_set<int> &cached_set,
    int count,
    int items) {
    std::srand(rd_seed);  // Seed the random number generator

    while (count > 0) {
        int random_num = std::rand() % items;  // Generate a random number in [0, items)

        // Check if the number is not in the cached_set
        if (cached_set.find(random_num) == cached_set.end()) {
            sorted_cached_set.push_back(random_num);  // Add to the vector
            cached_set.insert(random_num);            // Add to the unordered_set
            --count;                                  // Decrement the counter
        }
    }
}

void addRandomNumbersToFront(
    std::vector<int> &sorted_cached_set,
    std::unordered_set<int> &cached_set,
    int count,
    int items) {
    std::srand(rd_seed);  // Seed the random number generator
    std::vector<int> rand_set;
    while (count > 0) {
        int random_num = std::rand() % items;  // Generate a random number in [0, items)

        // Check if the number is not in the cached_set
        if (cached_set.find(random_num) == cached_set.end()) {
            // sorted_cached_set.insert(sorted_cached_set.begin(), random_num); // Add to the front of the vector
            rand_set.push_back(random_num);
            cached_set.insert(random_num);  // Add to the unordered_set
            --count;                        // Decrement the counter
        }
    }
    // set sorted_cached_set = rand_set + sorted_cached_set
    sorted_cached_set.insert(sorted_cached_set.begin(), rand_set.begin(), rand_set.end());
}

void replaceRandomly(
    std::unordered_set<int> &cached_set,
    std::vector<int> &sorted_cached_set,
    int window_size,
    int iterations,
    int num_to_replace,
    std::vector<std::vector<int>> &replacement_history) {
    std::srand(rd_seed);             // Seed the random number generator
    replacement_history.push_back(sorted_cached_set);  // Save the initial state
    for (int i = 0; i < iterations - 1; ++i) {
        // Step 2: Randomly sample 200 elements from sorted_cached_set
        std::vector<int> selected_elements;
        std::sample(sorted_cached_set.begin(), sorted_cached_set.end(), std::back_inserter(selected_elements),
                    num_to_replace, std::mt19937{std::random_device{}()});  // Randomly select 200 elements

        // Step 3: Generate 200 new unique elements and replace the selected elements
        std::unordered_set<int> new_elements;
        while (new_elements.size() < num_to_replace) {
            int random_num = std::rand() % items;  // Generate a random number in [0, items)
            // Ensure the random number is not already in the cached_set
            if (cached_set.find(random_num) == cached_set.end() && new_elements.find(random_num) == new_elements.end()) {
                new_elements.insert(random_num);
            }
        }

        // Step 4: Replace the selected elements with the new elements in both cached_set and sorted_cached_set
        auto new_elem_iter = new_elements.begin();
        for (int j = 0; j < num_to_replace; ++j) {
            // Replace in cached_set
            cached_set.erase(selected_elements[j]);  // Remove from cached_set
            cached_set.insert(*new_elem_iter);       // Add new element to cached_set

            // Replace in sorted_cached_set (directly modify at position j)
            sorted_cached_set[std::find(sorted_cached_set.begin(), sorted_cached_set.end(), selected_elements[j]) - sorted_cached_set.begin()] = *new_elem_iter;

            ++new_elem_iter;  // Move to the next new element
        }

        // Step 5: Save the result after this replacement (record sorted_cached_set)
        replacement_history.push_back(sorted_cached_set);
    }
}

void prepare_dynmaic_map(const std::string &path) {
    int iterations = 128;
    dynamic_maps.reserve(iterations);
    // assume I have a cache set
    if (dynamic_rule == "static") {
        // std::cout << "preapre for static" << std::endl;
        for (int i = 0; i < iterations; i++) {
            std::unordered_map<std::string, std::string> dynamic_map;
            new_sorted_cached_set = sorted_cached_set;
            for (int i = 0; i < sorted_cached_set.size(); i++) {
                int idx = sorted_cached_set[i];
                int new_idx = new_sorted_cached_set[i];
                dynamic_map[gen_file_name(path, idx)] = gen_file_name(path, new_idx);
            }
            // std::cout << dynamic_map.size() << std::endl;
            dynamic_maps.push_back(dynamic_map);
        }
    } else if (dynamic_rule == "hot-out") {
        new_sorted_cached_set = sorted_cached_set;
        new_cached_set = cached_set;
        addRandomNumbersToBack(new_sorted_cached_set, new_cached_set, iterations * dynamic_scale, items);
        size_t window_size = sorted_cached_set.size();
        size_t total_size = new_sorted_cached_set.size();
        for (size_t step = 0; step < iterations; ++step) {
            std::unordered_map<std::string, std::string> dynamic_map;
            size_t start = step * dynamic_scale;

            size_t end = start + window_size;
            if (end >= total_size)
                break;

            for (size_t i = start, j = 0; i < end && j < window_size; ++i, ++j) {
                int idx = sorted_cached_set[j];
                int new_idx = new_sorted_cached_set[i];
                dynamic_map[gen_file_name(path, idx)] = gen_file_name(path, new_idx);
            }
            dynamic_maps.push_back(dynamic_map);
        }
    } else if (dynamic_rule == "hot-in") {
        // do something
        new_sorted_cached_set = sorted_cached_set;
        new_cached_set = cached_set;
        addRandomNumbersToFront(new_sorted_cached_set, new_cached_set, iterations * dynamic_scale, items);
        size_t window_size = sorted_cached_set.size();
        size_t total_size = new_sorted_cached_set.size();
        int cnt = 0;
        for (size_t step = 0; step < iterations; ++step) {
            std::unordered_map<std::string, std::string> dynamic_map;

            size_t start = total_size - step * dynamic_scale;

            size_t end = start - window_size;
            // [end, start)
            if (end < 0)
                break;

            for (size_t i = end, j = 0; i < start && j < window_size; ++i, ++j) {
                int idx = sorted_cached_set[j];
                int new_idx = new_sorted_cached_set[i];
                dynamic_map[gen_file_name(path, idx)] = gen_file_name(path, new_idx);
            }
#if 0
            if (cnt++ < 5) {
                std::cout << "cnt: " << cnt << std::endl;
                // print new added items
                for (size_t i = end; i < end + dynamic_scale; ++i) {
                    int new_idx = new_sorted_cached_set[i];
                    std::cout << i << ": " << gen_file_name(path, new_idx) << std::endl;
                }
            }
#endif
            dynamic_maps.push_back(dynamic_map);
        }
    } else if (dynamic_rule == "random") {
        // do something
        new_sorted_cached_set = sorted_cached_set;
        new_cached_set = cached_set;
        std::vector<std::vector<int>> replacement_history;
        size_t window_size = sorted_cached_set.size();

        replaceRandomly(new_cached_set, new_sorted_cached_set, window_size, iterations, dynamic_scale, replacement_history);
        for (auto &new_sorted_cached_set : replacement_history) {
            std::unordered_map<std::string, std::string> dynamic_map;
            for (size_t i = 0; i < window_size; ++i) {
                int idx = sorted_cached_set[i];
                int new_idx = new_sorted_cached_set[i];
                dynamic_map[gen_file_name(path, idx)] = gen_file_name(path, new_idx);
            }
            dynamic_maps.push_back(dynamic_map);
        }

    } else {
        std::cerr << "Invalid dynamic rule" << std::endl;
        return;
    }
}

int fast_extract_last_integer(const std::string &line) {
    int i = line.size() - 1;
    while (i >= 0 && !std::isdigit(line[i])) {
        --i;
    }
    int end = i;
    while (i >= 0 && std::isdigit(line[i])) {
        --i;
    }
    if (end >= 0) {
        return std::stoi(line.substr(i + 1, end - i));
    }
    return -1;
}