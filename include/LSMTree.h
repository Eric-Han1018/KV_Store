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
using namespace std;
namespace fs = std::filesystem;

struct HeapNode {
    pair<int64_t, int64_t> data;
    int arrayIndex; // this index corresponds the index of sorted_dir, the greater the index, newer the file
};

// Custom comparator for creating a min-heap with priority for newer file in duplicates
auto comp = [](const HeapNode &a, const HeapNode &b) {
    if (a.data.first == b.data.first)
        return a.arrayIndex < b.arrayIndex; // Prioritize newer file
    return a.data.first > b.data.first;
};

class Level {
    public:
        size_t cur_size;
        size_t max_size;    // TODO: will be useful if implement Dostoevsky
        size_t level;
        vector<fs::path> sorted_dir;

        Level(size_t max_size, size_t level) : cur_size(0), max_size(max_size), level(level) {};

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
            while (depth > 0) {
                int cur_depth = constants::LSMT_DEPTH - depth;
                levels.emplace_back(Level(pow(constants::LSMT_SIZE_RATIO, cur_depth+1), cur_depth));
                --depth;
            }
        }

        const int64_t* get(const int64_t& key, const bool& use_btree);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree);
        
        // LSMTree functions
        void add_SST(const string& file_name);
        // Check if the FIRST level of LSMTree is full
        inline bool check_LSMTree_compaction() {
            return levels[0].cur_size + 1 < constants::LSMT_SIZE_RATIO;
        }
        void print_lsmt() {
            for (int i = 0; i < num_levels; ++i) {
                cout << "level " << to_string(i) << " cur_size: " << levels[i].cur_size << " max_size: " << levels[i].max_size << endl;
                if (levels[i].cur_size > 0) {
                    for (int j = 0; j < levels[i].cur_size; ++j) {
                        cout << " sorted_dir: " << levels[i].sorted_dir[j].c_str() << endl;
                    }
                }
            }
        }

    private:
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start, const bool& use_btree);
        const int64_t* search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        const int64_t* search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        const int32_t search_BTree_non_leaf_nodes(const int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, size_t& file_end);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const size_t& file_end, const size_t& non_leaf_start, const bool& use_btree);
        const int32_t scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const size_t& file_end, const size_t& non_leaf_start);
        const int32_t scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& num_elements, const size_t& file_end, const size_t& non_leaf_start);
        const string parse_pid(const string& file_name, const int32_t&);
        void read(const string& file_path, int fd, char*& data, off_t offset, bool isLeaf);

        // LSMTree functions
        void merge_down(const vector<Level>::iterator& current);
        void merge_down_helper(const vector<Level>::iterator& cur_level, const vector<Level>::iterator& next_level, const int& num_sst, const bool& last_level);
        void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level);
};