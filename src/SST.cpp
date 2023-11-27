#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "SST.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

// Search matching key in SSTs in the order of youngest to oldest
const int64_t* SST::get(const int64_t& key) {
    // Iterate to read each file in descending order (new->old)
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        #ifdef DEBUG
            cout << "Searching in file: " << *file_path_itr << "..." << endl;
        #endif

        // Skip if the key is not between min_key and max_key
        int64_t min_key, max_key;
        parse_SST_name(*file_path_itr, min_key, max_key);
        if (key < min_key || key > max_key) {
            #ifdef DEBUG
                cout << "key is not in range of: " << *file_path_itr << endl;
            #endif
            continue;
        }

        const int64_t* value = search_SST(*file_path_itr, key);
        if (value != nullptr) return value;
        #ifdef DEBUG
            cout << "Not found key: " << key << " in file: " << *file_path_itr << endl;
        #endif
    }
    
    return nullptr;
}

// Get min_key and max_key from a SST file's name
void SST::parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key) {
    vector<string> parsed_name(4);
    string segment;
    stringstream name_stream(file_name);
    // Split by "_" and "."
    int first = file_name.find("_");
    int last = file_name.find_last_of("_");
    int dot = file_name.find_last_of(".");

    min_key = strtoll(file_name.substr(first+1, last - first - 1).c_str(), nullptr, 10);
    max_key = strtoll(file_name.substr(last+1, dot - last - 1).c_str(), nullptr, 10);
}

// Helper function to search the key in a SST file
const int64_t* SST::search_SST(const fs::path& file_path, const int64_t& key) {
    auto file_size = fs::file_size(file_path);
    auto num_elements = file_size / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    int low = 0;
    int high = num_elements - 1;
    int mid;

    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY);
    #ifdef DEBUG
        assert(fd != -1);
    #endif


    // Binary search
    while (low <= high) {
        mid = (low + high) / 2;
        // Do one I/O at each hop
        // FIXME: need to confirm if this is what required
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, mid*constants::PAIR_SIZE);
        #ifdef DEBUG
            assert(ret == constants::PAIR_SIZE);
        #endif

        if (cur.first == key) {
            close(fd);
            return new int64_t(cur.second);
        } else if (cur.first > key) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    close(fd);

    return nullptr;
}

// Merge the scanned arrays from two SSTs (stored as two consecutive sorted ranges, separated by first_end in sorted_KV)
// The final sorted array is stored back in sorted_KV
void SST::merge_scan_results(vector<pair<int64_t, int64_t>>*& sorted_KV, const size_t& first_end) {
    std::vector<std::pair<int64_t, int64_t>> tmp;
    tmp.reserve(sorted_KV->size());
    size_t i = 0, j = first_end;
    while (i < first_end && j < sorted_KV->size()) {
        if ((*sorted_KV)[i].first < (*sorted_KV)[j].first) {
            tmp.emplace_back((*sorted_KV)[i]);
            ++i;
        }
        else if ((*sorted_KV)[i].first > (*sorted_KV)[j].first) {
            tmp.emplace_back((*sorted_KV)[j]);
            ++j;
        }
        else {
            tmp.emplace_back((*sorted_KV)[i]);
            ++i;
            ++j;
        }
    }
    while (i < first_end) {
        tmp.emplace_back((*sorted_KV)[i]);
        ++i;
    }
    while (j < sorted_KV->size()) {
        tmp.emplace_back((*sorted_KV)[j]);
        ++j;
    }
    *sorted_KV = std::move(tmp);
}

void SST::scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2) {
    size_t len;

    // Scan each SST
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        #ifdef DEBUG
            cout << "Scanning file: " << *file_path_itr << "..." << endl;
        #endif
        // Skip if the keys is not between min_key and max_key
        int64_t min_key, max_key;
        parse_SST_name(*file_path_itr, min_key, max_key);
        if (key2 < min_key || key1 > max_key) {
            #ifdef DEBUG
                cout << "key is not in range of: " << *file_path_itr << endl;
            #endif
            continue;
        }

        // Store current size of array for merging
        len = sorted_KV->size();

        // Scan the SST
        scan_SST(*sorted_KV, *file_path_itr, key1, key2);

        // Merge into one sorted array
        merge_scan_results(sorted_KV, len);
    }
}

// Scan SST to get keys within range
// The implementation is similar with search_SST()
void SST::scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2) {
    auto file_size = fs::file_size(file_path);
    int num_elements = file_size / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    int low = 0;
    int high = num_elements - 1;
    int mid;

    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY);
    #ifdef DEBUG
        assert(fd != -1);
    #endif


    // Binary search to find the first element >= key1
    while (low != high) {
        mid = (low + high) / 2;
        // Do one I/O at each hop
        // FIXME: need to confirm if this is what required
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, mid*constants::PAIR_SIZE);
        #ifdef DEBUG
            assert(ret == constants::PAIR_SIZE);
        #endif

        if (cur.first < key1) {
            low = mid + 1; // target can only in right half
        } else {
            high = mid; // target can at mid or in left half
        }
    }

    // Low and high both points to what we are looking for
    for (auto i=low; i < num_elements ; ++i) {
        // Iterate each element and push to vector
        int ret = pread(fd, (char*)&cur, constants::PAIR_SIZE, i*constants::PAIR_SIZE);
        #ifdef DEBUG
            assert(ret == constants::PAIR_SIZE);
        #endif

        if (cur.first <= key2) {
            sorted_KV.emplace_back(cur);
        } else {
            break; // until meeting the first value out of range
        }
    }

    close(fd);
}