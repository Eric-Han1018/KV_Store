#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "util.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
using namespace std;

size_t murmur_hash(const string& key){
    uint32_t seed = 443;
    const int length = key.length();
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x86_32(key.c_str(), length, seed, &hash);
    return static_cast<size_t>(hash);
}

size_t murmur_hash(const int64_t& key, const uint32_t& seed) {
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x86_32(&key, sizeof(int64_t), seed, &hash);
    return static_cast<size_t>(hash);
}

/* Insert non-leaf elements into their node in the corresponding level
 * Non-leaf Node Structure (see SST.h): |...keys...|...offsets....|# of keys|
 */
void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, const int32_t current_level, const int32_t& max_size) {
    // If first time access the level, create it
    if (current_level >= (int32_t)non_leaf_nodes.size()) {
        non_leaf_nodes.emplace_back(vector<BTreeNode>());
        counters.emplace_back(0);
        non_leaf_nodes.back().emplace_back(BTreeNode());
    }

    // Get the offset in the node
    int offset = counters[current_level] % constants::KEYS_PER_NODE;
    // During a B-Tree split, we split the big node into half-and-half
    const int split_offset = (constants::KEYS_PER_NODE + 1) / 2;

    if (offset == split_offset && counters[current_level] < max_size - 1) {
        // Push the middle point to the next level
        insertHelper(non_leaf_nodes, counters, key, current_level + 1, (max_size - 1) / (split_offset + 1));
        // Create a new non-leaf node, and the second half goes there
        non_leaf_nodes[current_level].emplace_back(BTreeNode());
    } else {
        BTreeNode& node = non_leaf_nodes[current_level].back();
        // When a node does not meet the split criterion, just add the key to the node
        node.keys[node.size] = key;
        // A node might not be full, so we need to count the size
        // This counts the number of elements inside this node
        ++node.size;
    }
    // The counter counts the next element to insert
    // It counts the number of elements interted among all nodes on the current level
    ++counters[current_level];
}

// Print the B-Tree for debugging
void print_B_Tree(vector<vector<BTreeNode>>& non_leaf_nodes, aligned_KV_vector& sorted_KV) {
    // Testing SST
    for (int i = (int)non_leaf_nodes.size() - 1; i >= 0; --i) {
        for (auto node : non_leaf_nodes[i]) {
            for (int j = 0; j < node.size; ++j) {
                cout << node.ptrs[j] << "(" << node.keys[j] << ")";
            }
            cout << node.ptrs[node.size] << "     ";
        }
        cout << endl;
    }

    for (int i = 0; i < sorted_KV.size(); ++i) {
        auto kv = sorted_KV.data[i];
        cout << "{" << kv.first << "} ";
        if ((i+1) % constants::KEYS_PER_NODE == 0) cout << "     ";
    }
    cout << endl;
}

// Since we leave the ptrs empty in insertHelper(), we now need to fill up all the pointers(offsets) in the B-Tree
void insertFixUp(vector<vector<BTreeNode>>& non_leaf_nodes, const int32_t& sorted_KV_size) {
    // Change ptrs to independent file offsets
    int32_t off = sorted_KV_size * constants::PAIR_SIZE;
    for (int32_t level_index = (int32_t)non_leaf_nodes.size() - 1; level_index >= 0; --level_index) {
        vector<BTreeNode>& level = non_leaf_nodes[level_index];
        int32_t next_size;
        // Calculate # of nodes in next level
        if (level_index >= 1) {
            next_size = (int32_t)non_leaf_nodes[level_index - 1].size();
        } else {
            next_size = (int32_t)sorted_KV_size / constants::KEYS_PER_NODE;
        }

        off += (int32_t)level.size() * (int32_t)sizeof(BTreeNode);
        int32_t per_level_counter = 0;
        for (BTreeNode& node : level) {
            for (int32_t ptr_index = 0; ptr_index <= node.size; ++ptr_index) {
                int32_t& offset = node.ptrs[ptr_index];
                // If offset exceeds bound, set to -1
                if (per_level_counter >= next_size) {
                    offset = -1;
                } else {
                    // If it is the last non-leaf level, need to take offset as multiple of KV stores
                    if (level_index == 0) {
                        offset = per_level_counter * constants::KEYS_PER_NODE * constants::PAIR_SIZE;
                    } else {
                        // Otherwise, just use BTreeNode size
                        offset = per_level_counter * (int32_t)sizeof(BTreeNode) + off;
                    }
                }
                ++per_level_counter;
            }
        }
    }
}