#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "bloomFilter.h"
#include "bufferpool.h"
using namespace std;
namespace fs = std::filesystem;

// B-Tree non-leaf Node (members stored contiguously)
typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeNode {
    int64_t keys[constants::KEYS_PER_NODE] = {0}; // Keys in each node
    int32_t ptrs[constants::KEYS_PER_NODE + 1] = {0}; // File offsets to children
    int32_t size = 0;
} BTreeNode;

typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeLeafNode {
    pair<int64_t, int64_t> data[constants::KEYS_PER_NODE];
} BTreeLeafNode;

class Level {
    public:
        size_t cur_size;
        vector<fs::path> sorted_dir;
        vector<fs::path> sorted_filters;

        Level() {
            cur_size = 0;
        };

        ~Level() {};

        void clear_level();
};

class LSMTree {
    public:
        string db_name;
        Bufferpool* buffer;
        vector<Level> levels;
        size_t num_levels;

        LSMTree(string db_name, size_t depth, Bufferpool* buffer = nullptr) : db_name(db_name), buffer(buffer), num_levels(1) {
            // Get a sorted list of existing SST files
            // for (auto& file_path : fs::directory_iterator(constants::DATA_FOLDER + db_name + '/')) {
            //     sorted_dir.push_back(file_path);
            // }
            // sort(sorted_dir.begin(), sorted_dir.end());
            while ((depth--) > 0) {
                levels.emplace_back();
            }
        }

        const int64_t* get(const int64_t& key, const bool& use_btree);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree);
        
        // LSMTree functions
        void add_SST(const string& file_name);

    private:
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset, const bool& use_btree);
        const int64_t* search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset);
        const int64_t* search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset);
        const int32_t search_BTree_non_leaf_nodes(const int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset);
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, int32_t& leaf_offset);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const int32_t& leaf_offset, const bool& use_btree);
        const int32_t scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& leaf_offset);
        const int32_t scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& num_elements, const int32_t& leaf_offset);
        const string parse_pid(const string& file_name, const int32_t&);
        void read(const string& file_path, int fd, char*& data, off_t offset, bool isLeaf);

        // LSMTree functions
        void merge_down(vector<Level>::iterator current);
        void merge_down_helper(vector<Level>::iterator cur_level, vector<Level>::iterator next_level);

        // BloomFilter functions
        bool check_bloomFilter(fs::path& filter_path, const int64_t& key);
};