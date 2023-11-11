#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "database.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

// TODO: Insert a key-value pair into the memtable
void Database::put(const int64_t& key, const int64_t& value) {
    if (memtable->put(key, value) == memtableFull) {
        string file_path = writeToSST();
        #ifdef DEBUG
            cout << "Memtable capacity reaches maximum. Data has been " <<
                    "saved to: " << file_path << endl;
        #endif

        memtable->put(key, value);
    }
}

const int64_t* Database::get(const int64_t& key){
    int64_t* result;
    if(memtable->get(result, key) == notInMemtable) {
        return sst->get(key);
    }
    return result;
}

const vector<pair<int64_t, int64_t>>* Database::scan(const int64_t& key1, const int64_t& key2) {
    #ifdef DEBUG
        // Check if key1 < key2
        assert(key1 < key2);
    #endif

    vector<pair<int64_t, int64_t>>* sorted_KV = new vector<pair<int64_t, int64_t>>;

    // Scan the memtable
    memtable->scan(*sorted_KV, memtable->root, key1, key2);

    // Scan each SST
    sst->scan(sorted_KV, key1, key2);

    return sorted_KV;
}

// When memtable reaches its capacity, write it into an SST
string Database::writeToSST() {
    // Content in std::vector is stored contiguously
    vector<pair<int64_t, int64_t>> sorted_KV;
    scan_memtable(sorted_KV, memtable->root);

    // Create file name based on current time
    // TODO: modify file name to a smarter way
    string file_name = constants::DATA_FOLDER;
    time_t current_time = time(0);
    clock_t current_clock = clock(); // In case there is a tie in time()
    file_name.append(to_string(current_time)).append(to_string(current_clock)).append("_").append(to_string(memtable->min_key)).append("_").append(to_string(memtable->max_key)).append(".bytes");

    // Write data structure to binary file
    // FIXME: do we need O_DIRECT for now?
    int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_SYNC, 0777);
    #ifdef DEBUG
        assert(fd!=-1);
    #endif
    int test = pwrite(fd, (char*)&sorted_KV[0], sorted_KV.size()*constants::PAIR_SIZE, 0);
    #ifdef DEBUG
        assert(test!=-1);
    #endif
    close(fd);

    // Add to the maintained directory list
    sst->sorted_dir.emplace_back(file_name);

    // Clear the memtable
    clear_tree();

    return file_name;
}


// Helper function to recursively perform inorder scan
void Database::scan_memtable(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root) {
    if (root != nullptr) {
        scan_memtable(sorted_KV, root->left);
        sorted_KV.emplace_back(root->key, root->value);
        scan_memtable(sorted_KV, root->right);
    }
}

// Clear all the nodes in the tree
void Database::clear_tree() {
    delete memtable->root;
    memtable->root = nullptr;
    memtable->curr_size = 0;
    memtable->min_key = numeric_limits<int64_t>::max();
    memtable->max_key = numeric_limits<int64_t>::min();
}