#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
using namespace std;
namespace fs = std::filesystem;

// B-Tree non-leaf Node (members stored contiguously)
typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeNode {
    int64_t keys[constants::KEYS_PER_NODE] = {0}; // Keys in each node
    int32_t ptrs[constants::KEYS_PER_NODE + 1] = {0}; // File offsets to children
    int32_t size = 0;
} BTreeNode;

class SST {
    public:
        vector<fs::path> sorted_dir; // The sorted list of all SST files (ascending order, need to reverse when iterate)

        SST() {
            // Get a sorted list of existing SST files
            for (auto& file_path : fs::directory_iterator(constants::DATA_FOLDER)) {
                sorted_dir.push_back(file_path);
            }
            sort(sorted_dir.begin(), sorted_dir.end());
        }

        const int64_t* get(const int64_t& key, const bool& use_btree);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree);

    private:
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset, const bool& use_btree);
        const int64_t* search_SST_BTree(int& fd, const int64_t& key, const int32_t& leaf_offset);
        const int64_t* search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset);
        const int32_t search_BTree_non_leaf_nodes(const int& fd, const int64_t& key, const int32_t& leaf_offset);
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, int32_t& leaf_offset);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const int32_t& leaf_offset, const bool& use_btree);
        const int32_t scan_helper_BTree(const int& fd, const int64_t& key1, const int32_t& leaf_offset);
        const int32_t scan_helper_Binary(const int& fd, const int64_t& key1, const int32_t& num_elements, const int32_t& leaf_offset);
};