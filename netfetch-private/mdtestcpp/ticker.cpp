#include "ticker.h"

#include "threadpool.h"
#define MAX_DYNAMIC_ITERATION 26
// std::atomic<bool> running(true);
std::atomic<bool> is_ticker_start(false);

std::vector<std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT>> per_sec_total_counts;

void ticker() {
    int counter = 0;
    // wait to start
    while (!is_ticker_start) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT> last_sec_total_counts;
    // init it with 0
    for (int i = 0; i < OP_COUNT; i++) {
        for (int j = 0; j < SUB_OP_COUNT; j++) {
            last_sec_total_counts[i][j] = 0;
        }
    }
    std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT> cur_sec_total_counts;
    std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT> diff_sec_total_counts;

    int second_counter = 0;

    while (running) {
        auto start_time = std::chrono::steady_clock::now();

        // calculate the total counts in for cur second
        // std::vector<std::array<std::array<int, SUB_OP_COUNT>, OP_COUNT>> local_counts;
        // summary the local counts for each thread
        for (int i = 0; i < OP_COUNT; i++) {
            for (int j = 0; j < SUB_OP_COUNT; j++) {
                cur_sec_total_counts[i][j] = 0;
                for (int k = 0; k < thread_count; k++) {
                    cur_sec_total_counts[i][j] += local_counts[k][i][j];
                }
            }
        }

        // calculate the diff and push it into per_sec_total_counts
        for (int i = 0; i < OP_COUNT; i++) {
            for (int j = 0; j < SUB_OP_COUNT; j++) {
                diff_sec_total_counts[i][j] = cur_sec_total_counts[i][j] - last_sec_total_counts[i][j];
            }
        }
        per_sec_total_counts.push_back(diff_sec_total_counts);

        // update the last_sec_total_counts
        for (int i = 0; i < OP_COUNT; i++) {
            for (int j = 0; j < SUB_OP_COUNT; j++) {
                last_sec_total_counts[i][j] = cur_sec_total_counts[i][j];
            }
        }

        auto end_time = std::chrono::steady_clock::now();

        std::chrono::duration<double> execution_time = end_time - start_time;

        auto sleep_duration = std::chrono::seconds(1) - execution_time;

        if (sleep_duration > std::chrono::milliseconds(0)) {
            std::this_thread::sleep_for(sleep_duration);
        }

        second_counter++;
        if (second_counter >= dynamic_period && mode == 0) {
            std::cout << "change dynamic_iteration" << std::endl;
            dynamic_iteration.fetch_add(1);
            second_counter = 0;

            if (dynamic_iteration.load() >= MAX_DYNAMIC_ITERATION && mode == 0 && (current_method == 6 or current_method == 7)) {
                running = false;
            }
        }
        // if (mode == 1 && rate_delay > 0) {
            // if (second_counter > 60) {
                // running = false;
            // }
        // }
    }
}

void create_ticker() {
    std::thread ticker_thread(ticker);
    ticker_thread.detach();
}

void start_ticker() {
    is_ticker_start = true;
}

void stop_ticker() {
    running = false;
}

void dump_per_sec_total_counts() {
    // dump the per_sec_total_counts
    // print it in json style
    // print it sec by sec
    // print it with operation and sub operation name
    // print per_sec_total_counts size
    std::cout << "per_sec_total_counts size: " << per_sec_total_counts.size() << std::endl;
    for (int i = 0; i < per_sec_total_counts.size(); i++) {
        std::vector<double> total_thpt(SUB_OP_COUNT, 0.0);

        bool all_operations_zero = true;

        std::cout << "{";
        bool first_op = true;

        for (int j = 0; j < OP_COUNT; j++) {
            bool all_subops_zero = true;

            for (int k = 0; k < SUB_OP_COUNT; k++) {
                if (per_sec_total_counts[i][j][k] != 0) {
                    all_subops_zero = false;
                    all_operations_zero = false;
                    break;
                }
            }

            if (all_subops_zero) {
                continue;
            }

            if (!first_op) {
                std::cout << ",";
            }
            first_op = false;

            std::cout << "\"" << OperationToString((Operation)j) << "\":{";

            bool first_subop = true;
            for (int k = 0; k < SUB_OP_COUNT; k++) {
                if (!first_subop) {
                    std::cout << ",";
                }
                std::cout << "\"" << SubOperationToString((SubOperation)k) << "\":" << per_sec_total_counts[i][j][k];
                first_subop = false;
            }
            std::cout << "}";
        }

        if (all_operations_zero) {
            continue;
        }
        std::cout << "\"" << "Total" << "\":{";
        for (int j = 0; j < OP_COUNT; j++) {
            for (int k = 0; k < SUB_OP_COUNT; k++) {
                total_thpt[k] += per_sec_total_counts[i][j][k];
            }
        }
        for (int k = 0; k < SUB_OP_COUNT; k++) {
            std::cout << "\"" << SubOperationToString((SubOperation)k) << "\":" << total_thpt[k] << ",";
        }
        std::cout << "}";
        std::cout << "}" << std::endl;
    }
}

// void dump_per_sec_mixed() {
//     // dump the per_sec_total_counts
//     // print it in json style
//     // print it sec by sec
//     // print it with operation and sub operation name

//     // summ all op in one sec and dump them per second
//     for (int i = 0; i < per_sec_total_counts.size(); i++) {
//         bool all_operations_zero = true;

//         std::cout << "{";
//         bool first_op = true;

//         std::array<int, OP_COUNT> mixed_counts = {0};

//         for (int j = 0; j < OP_COUNT; j++) {
//             for (int k = 0; k < SUB_OP_COUNT; k++) {
//                 mixed_counts[j] += per_sec_total_counts[i][j][k];
//             }
//         }

//         for (int j = 0; j < OP_COUNT; j++) {
//             if (mixed_counts[j] != 0) {
//                 all_operations_zero = false;
//                 if (!first_op) {
//                     std::cout << ",";
//                 }
//                 first_op = false;
//                 std::cout << "\"" << OperationToString((Operation)j) << "\":" << mixed_counts[j];
//             }
//         }

//         if (all_operations_zero) {
//             continue;
//         }

//         std::cout << "}" << std::endl;
//     }
// }
