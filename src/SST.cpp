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
#include <cerrno>
#include <cstring>
using namespace std;

// Search matching key in SSTs in the order of youngest to oldest
const int64_t* SST::get(const int64_t& key, const bool& use_btree) {
    // Iterate to read each file in descending order (new->old)
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        #ifdef DEBUG
            cout << "Searching in file: " << *file_path_itr << "..." << endl;
        #endif
        // Skip if the key is not between min_key and max_key
        int64_t min_key, max_key;
        int32_t leaf_offset;
        parse_SST_name(*file_path_itr, min_key, max_key, leaf_offset);
        if (key < min_key || key > max_key) {
            #ifdef DEBUG
                cout << "key is not in range of: " << *file_path_itr << endl;
            #endif
            continue;
        }

        const int64_t* value = search_SST(*file_path_itr, key, leaf_offset, use_btree);
        if (value != nullptr) return value;
        #ifdef DEBUG
            cout << "Not found key: " << key << " in file: " << *file_path_itr << endl;
        #endif
    }
    
    return nullptr;
}

// Get min_key and max_key from a SST file's name
void SST::parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, int32_t& leaf_offset) {
    vector<string> parsed_name(4);
    string segment;
    stringstream name_stream(file_name);
    // Split by "_" and "."
    int first = file_name.find("_");
    int second = file_name.substr(first+1).find("_") + first + 1;
    int last = file_name.find_last_of("_");
    int dot = file_name.find_last_of(".");

    min_key = strtoll(file_name.substr(first+1, second - first - 1).c_str(), nullptr, 10);
    max_key = strtoll(file_name.substr(second+1, last - second - 1).c_str(), nullptr, 10);
    leaf_offset = stoi(file_name.substr(last+1, dot - last - 1));
}

// Search in BTree non-leaf nodes to find the offset of the leaf
const int32_t SST::search_BTree_non_leaf_nodes(const int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset) {
    int32_t offset = 0;
    BTreeNode* curNode;
    char* tmp;

    // Traverse B-Tree non-leaf nodes
    while (offset < leaf_offset) {
        // Read corresponding node
        read(file_path.c_str(), fd, tmp, offset, false);
        curNode = (BTreeNode*)tmp;

        // Binary search
        int low = 0;
        int high = curNode->size - 1;
        int mid;
        while (low < high) {
            mid = (low + high) / 2;
            if (curNode->keys[mid] < key) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        if (curNode->keys[low] < key) {
            offset = curNode->ptrs[curNode->size];
        } else {
            offset = curNode->ptrs[low];
        }

    }

    return offset;
}

// Perform BTree search in SST
const int64_t* SST::search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset) {
    // Search BTree non-leaf nodes to find the offset of leaf
    const int32_t offset = search_BTree_non_leaf_nodes(fd, file_path, key, leaf_offset);
    // Binary search in the leaf node
    BTreeLeafNode* leafNode;
    char* tmp;
    read(file_path.c_str(), fd, tmp, offset, true);
    leafNode = (BTreeLeafNode*)tmp;

    int low = 0;
    int high = constants::KEYS_PER_NODE - 1;
    int mid;

    while (low <= high) {
        mid = (low + high) / 2;
        pair<int64_t, int64_t> cur = leafNode->data[mid];
        if (cur.first == key) {
            return new int64_t(cur.second);
        } else if (cur.first > key) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    return nullptr;
}

// Perform original binary search in SST
const int64_t* SST::search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset) {
    auto file_size = fs::file_size(file_path);
    auto num_elements = (file_size - leaf_offset) / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    BTreeLeafNode* leafNode;
    int prevPage = -1;
    int low = 0;
    int high = num_elements - 1;
    int mid;

    // Binary search
    while (low <= high) {
        mid = (low + high) / 2;
        // Do one I/O per page if not in bufferpool
        int curPage = floor((mid*constants::PAIR_SIZE) / constants::PAGE_SIZE);
        if (curPage != prevPage) {
            char* tmp;
            read(file_path.c_str(), fd, tmp, leaf_offset + (curPage * constants::PAGE_SIZE), true);
            leafNode = (BTreeLeafNode*)tmp;
            prevPage = curPage;
        }
        cur = leafNode->data[mid - curPage * constants::KEYS_PER_NODE];

        if (cur.first == key) {
            return new int64_t(cur.second);
        } else if (cur.first > key) {
            high = mid - 1;
        } else {
            low = mid + 1;
        }
    }

    return nullptr;
}

// Helper function to search the key in a SST file
const int64_t* SST::search_SST(const fs::path& file_path, const int64_t& key, const int32_t& leaf_offset, const bool& use_btree) {
    const int64_t* result = nullptr;
    // Open the SST file
    // int fd = open(file_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
    int fd = open(file_path.c_str(), O_RDONLY | O_SYNC, 0777);
    #ifdef ASSERT
        assert(fd != -1);
    #endif

    if (use_btree && leaf_offset != 0) {
        result = search_SST_BTree(fd, file_path, key, leaf_offset);
    } else {
        result = search_SST_Binary(fd, file_path, key, leaf_offset);
    }
    
    close(fd);

    return result;
}

void SST::scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree) {
    size_t len;

    // Scan each SST
    for (auto file_path_itr = sorted_dir.rbegin(); file_path_itr != sorted_dir.rend(); ++file_path_itr) {
        #ifdef DEBUG
            cout << "Scanning file: " << *file_path_itr << "..." << endl;
        #endif
        // Skip if the keys is not between min_key and max_key
        int64_t min_key, max_key;
        int32_t leaf_offset;
        parse_SST_name(*file_path_itr, min_key, max_key, leaf_offset);
        if (key2 < min_key || key1 > max_key) {
            #ifdef DEBUG
                cout << "key is not in range of: " << *file_path_itr << endl;
            #endif
            continue;
        }

        // Store current size of array for merging
        len = sorted_KV->size();

        // Scan the SST
        scan_SST(*sorted_KV, *file_path_itr, key1, key2, leaf_offset, use_btree);

        // Merge into one sorted array
        // FIXME: ask Prof if merge() is allowed
        // FIXME: is using merge efficient?
        inplace_merge(sorted_KV->begin(), sorted_KV->begin()+len, sorted_KV->end());
    }
}

const int32_t SST::scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& leaf_offset) {
    int32_t offset = search_BTree_non_leaf_nodes(fd, file_path, key1, leaf_offset);
    BTreeLeafNode* leafNode;
    char* tmp;
    read(file_path.c_str(), fd, tmp, offset, true);
    leafNode = (BTreeLeafNode*)tmp;

    int low = 0;
    int high = constants::KEYS_PER_NODE - 1;
    int mid;

    while (low != high) {
        mid = (low + high) / 2;
        pair<int64_t, int64_t>& cur = leafNode->data[mid];
        if (cur.first == key1) {
            low = mid;
            break;
        } else if (cur.first < key1) {
            low = mid + 1; // target can only in right half
        } else {
            high = mid; // target can at mid or in left half
        }
    }
    return (int)((offset - leaf_offset) / constants::PAIR_SIZE) + low;
}

const int32_t SST::scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& num_elements, const int32_t& leaf_offset) {
    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    BTreeLeafNode* leafNode;
    int low = 0;
    int high = num_elements - 1;
    int mid;
    int prevPage = -1;

    // Binary search to find the first element >= key1
    while (low != high) {
        mid = (low + high) / 2;

        // Do one I/O per page if not in bufferpool
        int curPage = floor((mid*constants::PAIR_SIZE) / constants::PAGE_SIZE);
        if (curPage != prevPage) {
            char* tmp;
            read(file_path.c_str(), fd, tmp, leaf_offset + (curPage * constants::PAGE_SIZE), true);
            leafNode = (BTreeLeafNode*)tmp;
            prevPage = curPage;
        }
        cur = leafNode->data[mid - curPage * constants::KEYS_PER_NODE];

        if (cur.first == key1) {
            low = mid;
            break;
        } else if (cur.first < key1) {
            low = mid + 1; // target can only in right half
        } else {
            high = mid; // target can at mid or in left half
        }
    }
    return low;
}

// Scan SST to get keys within range
// The implementation is similar with search_SST()
void SST::scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const int32_t& leaf_offset, const bool& use_btree) {
    // Open the SST file
    // int fd = open(file_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
    int fd = open(file_path.c_str(), O_RDONLY | O_SYNC, 0777);
    #ifdef ASSERT
        assert(fd != -1);
    #endif

    auto file_size = fs::file_size(file_path);
    int num_elements = (int)((file_size - leaf_offset) / constants::PAIR_SIZE);

    int32_t start;
    if (use_btree && leaf_offset != 0) {
        start = scan_helper_BTree(fd, file_path, key1, leaf_offset);
    } else {
        start = scan_helper_Binary(fd, file_path, key1, num_elements, leaf_offset);
    }

    // Low and high both points to what we are looking for
    pair<int64_t, int64_t> cur;
    int64_t prev;
    BTreeLeafNode* leafNode;
    int prevPage = -1;

    for (auto i=start; i < num_elements ; ++i) {
        // Record previous key to prevent reading redundant padded values
        if (i > start) {
            prev = cur.first;
        }
        // Iterate each element and push to vector
        int curPage = floor((i*constants::PAIR_SIZE) / constants::PAGE_SIZE);
        if (curPage != prevPage) {
            char* tmp;
            read(file_path.c_str(), fd, tmp, leaf_offset + (curPage * constants::PAGE_SIZE), true);
            leafNode = (BTreeLeafNode*)tmp;
            prevPage = curPage;
        }
        cur = leafNode->data[i - curPage * constants::KEYS_PER_NODE];

        if (cur.first <= key2) { 
            if (i > start && cur.first == prev) { // Our keys are unique
                break;
            }
            sorted_KV.emplace_back(cur);
        } else {
            break; // until meeting the first value out of range
        }
    }

    close(fd);
}

// Combine SST file's name with offset to get the page ID
const string SST::parse_pid(const string& file_path, const int32_t& offset) {
    size_t lastSlash = file_path.find_last_of('/');
    size_t lastDot = file_path.find_last_of('.');
    string file_name = file_path.substr(lastSlash + 1, lastDot - lastSlash - 1);
    
    // Combine the extracted part with offset into a string
    stringstream combinedString;
    combinedString << file_name << "_" << offset;
    return combinedString.str();
}

// Read either from bufferpool or SST
void SST::read(const string& file_path, int fd, char*& data, off_t offset, bool isLeaf) {
    const string p_id = parse_pid(file_path, offset);
    char* tmp = (char*)new BTreeNode();
    
    if (constants::USE_BUFFER_POOL && buffer->get_from_buffer(p_id, tmp)) {}
    else {
        int ret = pread(fd, tmp, constants::KEYS_PER_NODE * constants::PAIR_SIZE, offset);
        #ifdef ASSERT
            assert(ret == constants::KEYS_PER_NODE * constants::PAIR_SIZE);
        #endif
        
        buffer->insert_to_buffer(p_id, isLeaf, tmp);
    }
    data = tmp;
}