#include <omp.h>
#include <openssl/md5.h>

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common_impl.h"
// std::map<std::vector<unsigned char>, std::pair<std::string, uint16_t>> hashRing;
std::vector<std::pair<std::vector<unsigned char>, std::pair<std::string, uint16_t>>> hashRing;

std::unordered_map<int, std::string> idx_to_name;
std::unordered_map<int, int> fast_count_numbers_in_file(const std::string& file_path) {
    std::unordered_map<int, int> number_count;
    std::ifstream file(file_path, std::ios::in | std::ios::ate); 

    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << file_path << std::endl;
        return number_count;
    }

    std::streamsize file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::string file_content(static_cast<size_t>(file_size), '\0');
    file.read(&file_content[0], file_size);  

    std::istringstream iss(file_content);  
    std::string word;

    while (iss >> word) {
        try {
            int num = std::stoi(word);
            number_count[num]++;
        } catch (const std::invalid_argument&) {
            continue;  // 忽略非数字字符
        }
    }

    return number_count;
}

// Function to compute MD5 hash of a string
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


// Function to compare MD5 hash vectors lexicographically
bool compareMD5(const std::vector<unsigned char>& a, const std::vector<unsigned char>& b) {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
}

// Function to map a hash value to a name node
// std::string mapToNameNode(const std::vector<unsigned char>& hashValue) {
//     auto it = std::find_if(hashRing.begin(), hashRing.end(),
//                            [&](const auto& pair) { return !compareMD5(pair.first, hashValue); });

//     if (it != hashRing.end()) {
//         return it->second.first;
//     } else {
//         return hashRing.begin()->second.first;  // Wrap around to the first node
//     }
// }
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



int fast_extract_last_integer(const std::string& line) {
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

void initHashRing(int server_logical_num) {
    std::vector<std::string> namespaces;
    for (int i = 0; i < server_logical_num; ++i) {
        namespaces.push_back("ns" + std::to_string(i + 1));
    }
    constructHashRing(namespaces);
}


std::unordered_map<int, int> fast_counter_nodes_load(
    const std::string& access_pattern,
    const std::unordered_map<int, int>& countmap
    // const std::unordered_map<std::string, std::string> &hashRing,
    // const std::vector<std::string> &sorted_keys
) {
    std::unordered_map<int, int> nodes_load;

    std::ifstream file(access_pattern, std::ios::in | std::ios::ate);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << access_pattern << std::endl;
        return nodes_load;
    }

    std::streamsize file_size = file.tellg();
    file.seekg(0, std::ios::beg);
    std::string file_content(static_cast<size_t>(file_size), '\0');
    file.read(&file_content[0], file_size);
    file.close();

    std::vector<std::string> lines;
    std::istringstream iss(file_content);
    std::string line;

    while (std::getline(iss, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }

#pragma omp parallel
    {
        std::unordered_map<int, int> local_load;
        std::unordered_map<int, std::string> id_name;
#pragma omp for nowait
        for (size_t i = 0; i < lines.size(); ++i) {
            std::string line = lines[i];
            line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());

            if (!line.empty()) {
                int ext = fast_extract_last_integer(line);
                std::string rmdir_str = line + ".rmdir";
                if (ext != -1 && countmap.find(ext) != countmap.end()) {
                    id_name[ext] = rmdir_str;
                    // std::cout << ext << " " << line << std::endl;
                    auto md5_value = computeMD5(rmdir_str);
                    auto nsidx = mapToNameNodeidx(md5_value);
                    int freq = countmap.at(ext);
                    local_load[nsidx] += freq;
                }
            }
        }

#pragma omp critical
        {
            for (const auto& pair : local_load) {
                nodes_load[pair.first] += pair.second;
            }
        }
#pragma omp critical
        {
            for (const auto& pair : id_name) {
                idx_to_name[pair.first] = pair.second;
            }
        }
    }
    // printf("idx_to_name.size() %d\n", idx_to_name.size());
    return nodes_load;
}

void print_sorted_fast_countmap_to_file(const std::unordered_map<int, int>& countmap, const std::string& output_file) {
    std::ofstream out_file(output_file);
    if (!out_file.is_open()) {
        std::cerr << "Failed to open output file: " << output_file << std::endl;
        return;
    }
    // printf("idx_to_name.size() %d\n", idx_to_name.size());
    std::vector<std::pair<int, int>> sorted_countmap(countmap.begin(), countmap.end());

    std::sort(sorted_countmap.begin(), sorted_countmap.end(),
              [](const std::pair<int, int>& a, const std::pair<int, int>& b) {
                  return a.second > b.second;
              });
    int count = 0;
    for (const auto& pair : sorted_countmap) {
        if (count++ > 100000) {
            break;
        }
        out_file << pair.first << " " << idx_to_name[pair.first] << " " << pair.second << std::endl;
    }

    out_file.close();
}

int main(int argc, char* argv[]) {
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <file_out> <access_file> <hot_freq_file>" << std::endl;
        return 1;
    }
    idx_to_name.reserve(100000000);
    // int server_logical_num = 16;
    std::string file_path = std::string(argv[1]);
    std::string access_pattern = std::string(argv[2]);  // file.out
    std::string hot_freq_file = std::string(argv[3]);

    std::cout << file_path << " " << access_pattern << std::endl;
    parser_main("config.ini");
    int ns_num = server_logical_num;
    std::vector<std::string> namespaces;
    for (int i = 1; i <= ns_num; ++i) {
        namespaces.push_back("ns" + std::to_string(i));
    }

    initHashRing(server_logical_num);

    auto start = std::chrono::high_resolution_clock::now();
    auto fast_countmap = fast_count_numbers_in_file(access_pattern);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "fastcountmap optime: " << duration.count() << " s" << " fast_countmap size " << fast_countmap.size() << std::endl;

    start = std::chrono::high_resolution_clock::now();
    auto nodes_load = fast_counter_nodes_load(file_path, fast_countmap);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "nodes_load optime: " << duration.count() << " s" << " nodes_load size " << nodes_load.size() << std::endl;

    print_sorted_fast_countmap_to_file(fast_countmap, hot_freq_file);

    for (const auto& pair : nodes_load) {
        std::cout << pair.first << ": " << pair.second << std::endl;
    }



    auto max_load_node = std::max_element(nodes_load.begin(), nodes_load.end(),
                                          [](const std::pair<int, int>& a, const std::pair<int, int>& b) {
                                              return a.second < b.second;
                                          });

    // caculate normalized nodes laod and print all nodes load by the highest load node
    if (max_load_node != nodes_load.end()) {
        double max_load = max_load_node->second;
        std::cout << "Normalized nodes load:" << std::endl;
        std::cout << "[ ";
        for (const auto& pair : nodes_load) {
            double normalized_load = pair.second / max_load;
            std::cout << "ns" << pair.first + 1 << ": " << normalized_load << ", ";
        }
        std::cout << " ]"<< std::endl;
    }

    // caculate fairness index
    double sum_loads = 0.0;
    double sum_squares = 0.0;
    for (const auto& pair : nodes_load) {
        double load = static_cast<double>(pair.second);
        sum_loads += load;
        sum_squares += load * load;
    }

    double fairness_index = (sum_loads * sum_loads) / (nodes_load.size() * sum_squares);
    std::cout << "Fairness index: " << fairness_index << std::endl;

    if (max_load_node != nodes_load.end()) {
        std::cout << "The namenode with the most load is: ns" << max_load_node->first + 1 << std::endl;
    }

    return 0;
}