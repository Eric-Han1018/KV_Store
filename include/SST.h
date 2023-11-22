#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "bufferpool.h"
using namespace std;
namespace fs = std::filesystem;

class SST {
    public:
        string db_name;
        fs::path sst_path;
        fs::path filter_path;
        vector<fs::path> sorted_dir; // The sorted list of all SST files (ascending order, need to reverse when iterate)
        Bufferpool* buffer;

        SST(string db_name, Bufferpool* buffer = nullptr) : db_name(db_name), buffer(buffer) {
            sst_path = constants::DATA_FOLDER + db_name + "/sst/";
            filter_path = constants::DATA_FOLDER + db_name + "/filter/";
            // Get a sorted list of existing SST & Bloom Filter files
            for (auto& file_path : fs::directory_iterator(sst_path)) {
                sorted_dir.push_back(file_path.path().filename());
            }
            sort(sorted_dir.begin(), sorted_dir.end());
        }

        const int64_t* get(const int64_t& key, const bool& use_btree);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree);

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
};