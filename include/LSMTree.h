#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include <queue>
#include "constants.h"
#include "bufferpool.h"
#include "aligned_KV_vector.h"
#include "bloomFilter.h"
#include "util.h"
using namespace std;
namespace fs = std::filesystem;

struct HeapNode {
    pair<int64_t, int64_t> data;
    int arrayIndex; // this index corresponds the index of sorted_dir, the greater the index, newer the file
};

// Custom comparator for creating a min-heap with priority for newer file in duplicates
static auto comp = [](const HeapNode &a, const HeapNode &b) {
    if (a.data.first == b.data.first)
        return a.arrayIndex < b.arrayIndex; // Prioritize newer file
    return a.data.first > b.data.first;
};

class Level {
    public:
        size_t cur_size;
        size_t level;
        bool last_level;
        vector<fs::path> sorted_dir;

        Level(size_t level) : cur_size(0), level(level) {
            last_level = false;
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
        fs::path sst_path;
        fs::path filter_path;

        LSMTree(string db_name, size_t depth, Bufferpool* buffer = nullptr) : db_name(db_name), buffer(buffer), num_levels(1) {
            while (depth > 0) {
                int cur_depth = constants::LSMT_DEPTH - depth;
                // FIXME: what if the depth is not enough?
                levels.emplace_back(Level(cur_depth));
                --depth;
            }
            levels[0].last_level = true;
            sst_path = constants::DATA_FOLDER + db_name + "/sst/";
            filter_path = constants::DATA_FOLDER + db_name + "/filter/";
        }

        const int64_t* get(const int64_t& key, const bool& use_btree);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree);
        
        // LSMTree functions
        void add_SST(const string& file_name);
        // Check if the FIRST level of LSMTree is full
        inline bool check_LSMTree_compaction() {
            return levels[0].cur_size + 1 < constants::LSMT_SIZE_RATIO;
        }

        string generate_filename(const size_t& level, const int64_t& min_key, const int64_t& max_key, const int32_t& leaf_ends);
        void print_lsmt();
        void parse_SST_level(const string& file_name, size_t& level);
        size_t calculate_sst_size(Level& cur_level);

    private:
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start, const bool& use_btree);
        const int64_t* search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        const int64_t* search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        const int32_t search_BTree_non_leaf_nodes(const int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, size_t& leaf_end);
        void parse_SST_offset(const string& file_name, size_t& leaf_end);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const size_t& file_end, const size_t& non_leaf_start, size_t& scanPageCount, const bool& use_btree);
        void merge_scan_results(vector<pair<int64_t, int64_t>>*& sorted_KV, const size_t& first_end);
        const int32_t scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const size_t& file_end, const size_t& non_leaf_start);
        const int32_t scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& num_elements, const size_t& file_end, const size_t& non_leaf_start);
        const string parse_pid(const string& file_name, const int32_t&);
        bool read(const string& file_path, const int& fd, char*& data, const off_t& offset, const size_t& scanPageCount, const bool& isLeaf);

        // LSMTree functions
        void merge_down(const vector<Level>::iterator& current);
        void merge_down_helper(const vector<Level>::iterator& cur_level, const vector<Level>::iterator& next_level, const int& num_sst, const bool& last_compaction, const bool& largest_level);
        void largest_level_move_down(const vector<Level>::iterator& cur_level);
        friend void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level);

        // BloomFilter functions
        bool check_bloomFilter(const fs::path& filter_path, const int64_t& key, Level& cur_level);
};