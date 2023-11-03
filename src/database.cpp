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
        cout << "Memtable capacity reaches maximum. Data has been " <<
                "saved to: " << file_path << endl;

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
    // Check if key1 < key2
    assert(key1 < key2);

    vector<pair<int64_t, int64_t>>* sorted_KV = new vector<pair<int64_t, int64_t>>;

    // Scan the memtable
    memtable->scan(*sorted_KV, memtable->root, key1, key2);

    // Scan each SST
    sst->scan(sorted_KV, key1, key2);

    return sorted_KV;
}

void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, int64_t& key, vector<pair<int64_t, int64_t>>& sorted_KV, int32_t current_level) {
    // If first time access the level, create it
    if (current_level >= (int32_t)non_leaf_nodes.size()) {
        non_leaf_nodes.emplace_back(vector<BTreeNode>());
        counters.emplace_back(0);
    }
    int& counter = counters[current_level];
    vector<BTreeNode>& level = non_leaf_nodes[current_level];

    // Get the offset in the node
    int offset = counter % KEYS_PER_NODE;
    // If first time access the node
    if (offset == 0) {
        // Case 1: the node needs to be sent to higher level
        if (counter != 0 // Exclude the first element
        && ((current_level + 1) >= (int32_t)non_leaf_nodes.size()  // It is the first time access the next level
        || (counter / KEYS_PER_NODE) > counters[current_level + 1])) { // The node has not been sent up to the next level
            insertHelper(non_leaf_nodes, counters, key, sorted_KV, current_level + 1);
            return;
        }
        // Case 2: The previous node has been sent to the higher level. Now insert the next node
        level.emplace_back(BTreeNode());
    }
    BTreeNode& node = level[counter / KEYS_PER_NODE];

    // Insert into the node
    cout << "Insert into level: " << -1 * (current_level + 1) <<", Offset: " << offset << ", count: " << counter << endl;
    node.keys[offset] = key;
    node.ptrs[offset] = (counter / KEYS_PER_NODE) * (KEYS_PER_NODE + 1) + offset;
    node.ptrs[offset + 1] = node.ptrs[offset] + 1;
    // # ptrs = # keys + 1, so add one more ptr in the end of each node
    // if (offset == KEYS_PER_NODE - 1) {
    //     if (current_level == 0 && ((counter / KEYS_PER_NODE) * (KEYS_PER_NODE + 1) + offset) <= (int32_t)sorted_KV.size() / KEYS_PER_NODE)
    //         node.ptrs[KEYS_PER_NODE] = (counter / KEYS_PER_NODE) * (KEYS_PER_NODE + 1) + offset;
    //     else if (current_level > 0 && ((counter / KEYS_PER_NODE) * (KEYS_PER_NODE + 1) + offset) <= (int32_t)non_leaf_nodes[current_level - 1].size())
    //         node.ptrs[KEYS_PER_NODE] = (counter / KEYS_PER_NODE) * (KEYS_PER_NODE + 1) + offset;
    // }
    ++node.size;
    // The counter counts the next element to insert
    ++counter;
}

void convertToSST(vector<pair<int64_t, int64_t>>& sorted_KV) {
    vector<vector<BTreeNode>> non_leaf_nodes;
    vector<int32_t> counters;
    int32_t current_level = 0;

    // FIXME: need to exclude padding when scanning!
    if ((int32_t)sorted_KV.size() % KEYS_PER_NODE != 0) {
        int32_t padding = KEYS_PER_NODE - ((int32_t)sorted_KV.size() % KEYS_PER_NODE);
        for (int32_t i = 0; i < padding; ++i) {
            sorted_KV.emplace_back(sorted_KV.back());
        }
    }


    int32_t bound;
    if ((int32_t)sorted_KV.size() / KEYS_PER_NODE % (KEYS_PER_NODE + 1) == 0)
        bound = (int32_t)sorted_KV.size() - 1;
    else
        bound = (int32_t)sorted_KV.size();
    for (int32_t count = 0; count < bound; ++count) {
        // These are all non-leaf nodes
        if (count % KEYS_PER_NODE == KEYS_PER_NODE - 1) {
            cout << sorted_KV[count].first << endl;
            insertHelper(non_leaf_nodes, counters, sorted_KV[count].first, sorted_KV, current_level);
        }
    }

    for (int i = (int)non_leaf_nodes.size() - 1; i >= 0; --i) {
        for (auto node : non_leaf_nodes[i]) {
            for (int j = 0; j < node.size; ++j) {
                cout << node.ptrs[j] << "(" << node.keys[j] << ")";
            }
            cout << node.ptrs[node.size] << "     ";
        }
        cout << endl;
    }
    int i = 1;
    for (auto kv : sorted_KV) {
        cout << "{" << kv.first << "} ";
        if (i % KEYS_PER_NODE == 0) cout << "     ";
        ++i;
    }
    cout << endl;
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
    assert(fd!=-1);
    int test = pwrite(fd, (char*)&sorted_KV[0], sorted_KV.size()*constants::PAIR_SIZE, 0);
    assert(test!=-1);
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


void scan_memtable(vector<pair<int64_t, int64_t>>& sorted_KV, Node* root) {
    if (root != nullptr) {
        scan_memtable(sorted_KV, root->left);
        sorted_KV.emplace_back(root->key, root->value);
        scan_memtable(sorted_KV, root->right);
    }
}

int main() {
    Database db(21);

    int keys[] = {1, 2, 7, 16, 21, 22, 24, 29, 31, 32, 33, 35, 40, 61, 73, 74, 82, 90, 94, 95, 97};
    for (int key : keys) {
        db.put(key, 6);
    }

    vector<pair<int64_t, int64_t>> sorted_KV;
    scan_memtable(sorted_KV, db.memtable->root);

    convertToSST(sorted_KV);

    return 0;
}