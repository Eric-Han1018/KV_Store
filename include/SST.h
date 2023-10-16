#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
using namespace std;
namespace fs = std::filesystem;

// Global constant variables
namespace BPlusTreeConstants {
    const int entries_per_node = 3;
}

class BPlusTree {
    public:
        int32_t num_levels;
        int32_t root_offset;
        string  SST_file_name;

        // BPlusTree(string SST_file_name): SST_file_name(SST_file_name) {

        // }
};