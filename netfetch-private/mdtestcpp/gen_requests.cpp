#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <thread>
#include <mutex>

namespace fs = std::filesystem;

void split_files(const std::vector<std::string>& filenames, const std::string& split_dir, int start, int count, int part_index) {
    std::ofstream part_file(split_dir + "/requests." + std::to_string(part_index));
    if (!part_file.is_open()) {
        std::cerr << "Error opening part file: " << split_dir + "/requests." + std::to_string(part_index) << std::endl;
        return;
    }
    for (int j = 0; j < count; ++j) {
        part_file << filenames[start + j] << "\n";
    }
}

int main(int argc, char *argv[]) {
    // Disable synchronization with C stdio for faster I/O.
    std::ios::sync_with_stdio(false);
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0] << " <file_out> <access_file> <combined_file> <split_count>" << std::endl;
        return 1;
    }

    std::string file_out_name = argv[1];
    std::string access_file_name = argv[2];
    std::string combined_file_name = argv[3];
    int split_count = std::stoi(argv[4]);

    std::ifstream file_out(file_out_name);
    std::ifstream access_file(access_file_name);
    std::ofstream combined_file(combined_file_name);

    if (!file_out.is_open() || !access_file.is_open() || !combined_file.is_open()) {
        std::cerr << "Error opening one or more files." << std::endl;
        return 1;
    }

    // Step 1: Read file_out and create a map of x to filename.
    std::unordered_map<int, std::string> file_map;
    // 100000000
    file_map.reserve(100000000);
    std::string line;
    while (std::getline(file_out, line)) {
        size_t pos = line.rfind('.');
        if (pos != std::string::npos) {
            int x = std::stoi(line.substr(pos + 1));
            file_map[x] = line;
        }
    }

    // Step 2: Read access_file and write the corresponding filenames to combined_file.
    int file_index;
    std::vector<std::string> filenames;
    filenames.reserve(100000000);
    while (access_file >> file_index) {
        if (file_map.find(file_index) != file_map.end()) {
            combined_file << file_map[file_index] << std::endl;
            filenames.push_back(file_map[file_index]);
        }
    }

    file_out.close();
    access_file.close();
    combined_file.close();

    // Step 3: Create the split directory.
    std::string split_dir = combined_file_name + "_split";
    fs::create_directory(split_dir);

    // Step 4: Split the combined_file into the specified number of parts.
    int total_files = filenames.size();
    int files_per_part = total_files / split_count;
    int remainder = total_files % split_count;
    int start = 0;

    std::vector<std::thread> threads;
    for (int i = 0; i < split_count; ++i) {
        int count = files_per_part + (i < remainder ? 1 : 0);
        threads.emplace_back(split_files, std::ref(filenames), std::ref(split_dir), start, count, i);
        start += count;
    }

    // Join all threads.
    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "Files have been split into " << split_count << " parts successfully in directory: " << split_dir << std::endl;
    return 0;
}
