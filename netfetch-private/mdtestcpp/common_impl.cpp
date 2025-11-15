#include "common_impl.h"

// Define global variables
dictionary* ini;

std::atomic<int> rtries(0);

unsigned long long items;
int branch_factor = 1;
std::string stat_name;
std::string testdir;
std::string base_tree_name;
unsigned long long items_per_dir = 0;

int current_method;
int hot_path_size;
int path_resolution_offset;
short dst_port;
short warmup_notify_port;
const char* dst_ip;
const char* controller_ip;
const char* bottleneck_ip;
const char* rotation_ip;
const char* hot_paths_filename;
int dir_metadata_length;
int file_metadata_length;
int mode;
int server_logical_num;
int bottleneck_id;
int rotation_id;
int uid;
int gid;
int client_logical_num = 1;
const char* file_out;
const char* switchos_ip;
int flag_for_file_out_generation;
double request_pruning_factor = 1;
int create_only = 0;
int stat_only = 0;
int read_only = 0;
int remove_only = 0;
int open_close_only = 0;
int chmod_only = 0;
int mv_only = 0;
int leaf_only = 0;
int is_mixed = 0;
int rd_seed = 36;
int is_single_level_lock = 0;
__useconds_t rate_delay = 0;
std::map<Operation, double> operationRatios;

const std::map<std::string, Operation> operationMap = {
    {"open_close", Operation::OPEN},
    {"stat", Operation::STAT},
    {"create", Operation::TOUCH},
    {"delete", Operation::RM},
    {"rename", Operation::MV},
    {"chmod", Operation::CHMOD},
    {"readdir", Operation::READDIR},
    {"statdir", Operation::STATDIR},
    {"mkdir", Operation::MKDIR},
    {"rmdir", Operation::RMDIR}};

void init_ini(const char* config_file) {
    ini = iniparser_load(config_file);
}

void get_current_method() {
    current_method = iniparser_getint(ini, "global:current_method", -1);
    is_single_level_lock = iniparser_getint(ini, "global:single_lock", 0);
}

void get_hot_path_size() {
    hot_path_size = iniparser_getint(ini, "global:hot_path_size", -1);
}

void get_path_resolution_offset() {
    path_resolution_offset = iniparser_getint(ini, "global:path_resolution_offset", -1);
}

void get_dst_port() {
    int tmp_dst_port = iniparser_getint(ini, "global:dst_port", -1);
    dst_port = (short)tmp_dst_port;
}

void get_dst_ip() {
    dst_ip = iniparser_getstring(ini, "global:dst_ip", NULL);
    controller_ip = iniparser_getstring(ini, "global:controller_ip", NULL);
    bottleneck_ip = iniparser_getstring(ini, "global:bottleneck_ip", NULL);
    rotation_ip = iniparser_getstring(ini, "global:rotation_ip", NULL);
}

void get_hot_paths_filename() {
    hot_paths_filename = iniparser_getstring(ini, "global:hot_paths_filename", NULL);
}

void get_file_out() {
    file_out = iniparser_getstring(ini, "global:file_out", NULL);
}

void get_dir_metadata_length() {
    dir_metadata_length = iniparser_getint(ini, "global:dir_metadata_length", -1);
}

void get_file_metadata_length() {
    file_metadata_length = iniparser_getint(ini, "global:file_metadata_length", -1);
}

void get_mode() {
    mode = iniparser_getint(ini, "global:mode", 1);
    if (mode == 0) {
        std::random_device rd;
        std::mt19937 seed_gen(std::chrono::system_clock::now().time_since_epoch().count());

        std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());
        rd_seed = dist(seed_gen);
    } else {
        std::random_device rd;
        std::mt19937 seed_gen(std::chrono::system_clock::now().time_since_epoch().count());

        std::uniform_int_distribution<int> dist(0, std::numeric_limits<int>::max());
        rd_seed = dist(seed_gen);
    }
}

void get_server_logical_num() {
    server_logical_num = iniparser_getint(ini, "global:server_logical_num", -1);
}

void get_bottleneck_id() {
    bottleneck_id = iniparser_getint(ini, "global:bottleneck_id", -1);
}

void get_rotation_id() {
    rotation_id = iniparser_getint(ini, "global:rotation_id", -1);
}

void get_uid() {
    uid = iniparser_getint(ini, "global:uid", -1);
}

void get_gid() {
    gid = iniparser_getint(ini, "global:gid", -1);
}

void get_flag_for_file_out_generation() {
    flag_for_file_out_generation = iniparser_getint(ini, "global:flag_for_file_out_generation", -1);
}

void get_client_logical_num() {
    client_logical_num = iniparser_getint(ini, "global:client_logical_num", -1);
}
// void get_warmup_notify_port() {
//     warmup_notify_port = iniparser_getint(ini, "global:warmup_notify_port", -1);
// }

// void get_switchos_ip() {
//     switchos_ip = iniparser_getstring(ini, "global:switchos_ip", NULL);
// }

void free_ini() {
    iniparser_freedict(ini);
}

void parser_main(const char* config_file) {
    init_ini(config_file);
    get_current_method();
    get_hot_path_size();
    get_path_resolution_offset();
    get_dst_port();
    get_dst_ip();
    get_hot_paths_filename();
    get_file_out();
    get_dir_metadata_length();
    get_file_metadata_length();
    get_mode();
    get_server_logical_num();
    get_bottleneck_id();
    get_rotation_id();
    get_uid();
    get_gid();
    get_flag_for_file_out_generation();
    get_client_logical_num();
    // get_warmup_notify_port()
}

void parser_mixed() {
    double total_ratio = 0.0;
    for (const auto& [key, op] : operationMap) {
        std::string full_key = "mixed:" + key;
        double ratio = iniparser_getdouble(ini, full_key.c_str(), 0.0);

        if (key == "open_close") {
            operationRatios[Operation::OPEN] = ratio / 2.0;
            operationRatios[Operation::CLOSE] = ratio / 2.0;
            total_ratio += ratio;
        } else {
            operationRatios[op] = ratio;
            total_ratio += ratio;
        }
    }
    // if there is no mixed ratio in the config file, use default ratio 100.0 to stat only
    if (total_ratio == 0.0) {
        // print warning
        std::cerr << "No mixed ratio found in the config file, use default ratio 100.0 to stat only" << std::endl;
        operationRatios[Operation::STAT] = 100.0;
        total_ratio = 100.0;
    }
    // normalize ratios
    for (auto& [op, ratio] : operationRatios) {
        ratio /= total_ratio;
    }
}

void set_create() {
    for (const auto& [key, op] : operationMap) {
        if (key == "create") {
            operationRatios[op] = 1.0;
        }
    }
}

void set_mkdir() {
    for (const auto& [key, op] : operationMap) {
        if (key == "mkdir") {
            operationRatios[op] = 1.0;
        }
    }
}

void set_rmdir() {
    for (const auto& [key, op] : operationMap) {
        if (key == "rmdir") {
            operationRatios[op] = 1.0;
        }
    }
    // if there is no mixed ratio in the config file, use default ratio 100.0 to stat only
}

void print_mixed_ratio() {
    for (const auto& [key, value] : operationRatios) {
        std::cout << OperationToString(key) << ":" << value << " ";
    }
    std::cout << std::endl;
}

std::string dynamic_rule;
int dynamic_period;
int dynamic_scale;
void parser_dynamic() {
    dynamic_rule = std::string(iniparser_getstring(ini, "dynamic:rule", "static"));
    dynamic_period = iniparser_getint(ini, "dynamic:period", 10);
    dynamic_scale = iniparser_getint(ini, "dynamic:scale", 100);
}
void print_dynamic_ratio() {
    std::cout << "Dynamic rule: " << dynamic_rule << std::endl;
    std::cout << "Dynamic period: " << dynamic_period << std::endl;
    std::cout << "Dynamic scale: " << dynamic_scale << std::endl;
}
std::string OperationToString(Operation op) {
    switch (op) {
    case MKDIR:
        return "MKDIR";
    case RMDIR:
        return "RMDIR";
    case TOUCH:
        return "CREATE";
    case LOAD:
        return "LOAD";
    case RM:
        return "DELETE";
    case STAT:
        return "STAT";
    case OPEN:
        return "OPEN";
    case CLOSE:
        return "CLOSE";
    case FINISH:
        return "FINISH";
    case WARMUP:
        return "WARMUP";
    case CHMOD:
        return "CHMOD";
    case MV:
        return "MV";
    case READDIR:
        return "READDIR";
    case STATDIR:
        return "STATDIR";
    default:
        return "UNKNOWN";
    }
}

std::string SubOperationToString(SubOperation sub_op) {
    switch (sub_op) {
    case BOTTLENECK:
        return "BOTTLENECK";
    case ROTATION:
        return "ROTATION";
    case BOTTLENECKHIT:
        return "BOTTLENECK HIT";
    case ROTATIONHIT:
        return "ROTATION HIT";
    default:
        return "UNKNOWN";
    }
}

std::string getParentDirectory(const std::string& path) {
    size_t pos = path.find_last_of("/\\");
    if (pos == std::string::npos) {
        return "";
    }
    return path.substr(0, pos);
}

std::vector<std::string> extractPathLevels(const std::string& path, int n) {
    std::vector<std::string> levels;
    std::string currentPath;
    std::stringstream ss(path);
    std::string item;
    int count = 0;
    currentPath = "/";
    count++;
    if (count > n) {
        levels.push_back(currentPath);
    }
    while (std::getline(ss, item, '/')) {
        if (!item.empty()) {
            if (currentPath == "/")
                currentPath += item;
            else
                currentPath += "/" + item;
            count++;
            if (count > n) {
                levels.push_back(currentPath);
            }
        }
    }
    return levels;
}

std::vector<std::string> gen_dir_tree(int depth, int branch, const std::string& base_dir) {
    if (depth <= 0) {
        throw std::invalid_argument("Depth must be greater than 0");
    }
    if (branch <= 0) {
        throw std::invalid_argument("Branch factor must be greater than 0");
    }

    struct Node {
        int id;
        std::vector<int> path_to_node;
    };

    int node_id = 0;
    std::queue<Node> queue;
    queue.push({node_id, {}});

    std::vector<std::string> dir_tree;

    while (!queue.empty()) {
        Node current = queue.front();
        queue.pop();

        // Generate the path string
        std::ostringstream path_str;
        path_str << base_dir;
        for (int num : current.path_to_node) {
            path_str << "/mdtest_tree." << num;
        }
        path_str << "/mdtest_tree." << current.id;

        dir_tree.push_back(path_str.str());

        // Generate child nodes if depth not yet reached
        if (current.path_to_node.size() < static_cast<size_t>(depth - 1)) {
            for (int i = 0; i < branch; ++i) {
                ++node_id;
                std::vector<int> new_path = current.path_to_node;
                new_path.push_back(current.id);
                queue.push({node_id, new_path});
            }
        }
    }

    return dir_tree;
}