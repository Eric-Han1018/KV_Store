#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
using namespace std;
namespace fs = std::filesystem;


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

        const int64_t* get(const int64_t& key);
        void scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2);

    private:
        const int64_t* search_SST(const fs::path& file_path, const int64_t& key);
        void parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key);
        void scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2);
        void merge_scan_results(vector<pair<int64_t, int64_t>>*& sorted_KV, const size_t& first_end);
};