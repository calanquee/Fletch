#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/compaction_filter.h>
#include <rocksdb/status.h>

void PerformCompaction(rocksdb::DB* db) {
    rocksdb::CompactRangeOptions options;
    // options.exclusive_manual_compaction = true; // 独占模式，防止其他线程并发触发 Compaction
    // options.change_level = false;              // 不改变文件的层级结构
    // options.target_level = -1;                 // 目标层级为当前默认层级

    // 调用 Compaction
    rocksdb::Status status = db->CompactRange(options, nullptr, nullptr); // nullptr 表示整个键范围
    if (status.ok()) {
        std::cout << "Compaction completed successfully." << std::endl;
    } else {
        std::cerr << "Compaction failed: " << status.ToString() << std::endl;
    }
}

int main(int argc, char* argv[]) {
    // 数据库路径
    // 从参数中获取数据库路径
    const std::string db_path = argc > 1 ? argv[1] : "/home/jz/db_bak/index/worker0.db";
    // const std::string db_path = "/home/jz/db_bak/nocache/worker0.db";

    // 配置选项
    rocksdb::Options options;
    // options.create_if_missing = true; // 如果数据库不存在，则创建
    options.compaction_style = rocksdb::kCompactionStyleUniversal;
    options.target_file_size_base = 1024 * 1024 * 1024; // 1GB
    // 打开数据库
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    if (!status.ok()) {
        std::cerr << "Failed to open RocksDB: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "RocksDB opened successfully." << std::endl;

    // 主动触发 Compaction
    PerformCompaction(db);

    // 清理并关闭数据库
    delete db;
    std::cout << "RocksDB closed." << std::endl;

    return 0;
}
// g++ -MT compact_db.o -MMD -MP -MF compact_db.Td -std=c++14 -O3 -g -Wall -march=native -fno-omit-frame-pointer  -I../boost_1_81_0/install/include -I/usr/include -I//include -I../rocksdb-6.22.1/include -I/opt/intel/mkl/include  -c -o compact_db.o compact_db.c
// g++ -no-pie -pthread -g -L../boost_1_81_0/install/lib -L/usr/lib/x86_64-linux-gnu -L../common -L//lib/ -L../rocksdb-6.22.1 -L/opt/intel/mkl/lib/intel64 -Wl,-rpath=/opt/intel/mkl/lib/intel64  compact_db.o -lpthread -ldl -lstdc++fs -lboost_system -lcommon  -lssl -lcrypto -ltbb  -lrocksdb -lzstd -lbz2 -llz4 -lsnappy -lz -ldl -o compact_db