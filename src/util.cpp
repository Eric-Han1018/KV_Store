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
void insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level) {
    // If first time access the level, create it
    if (current_level >= (int32_t)non_leaf_nodes.size()) {
        non_leaf_nodes.emplace_back(vector<BTreeNode>());
        counters.emplace_back(0);
    }
    int& counter = counters[current_level];
    vector<BTreeNode>& level = non_leaf_nodes[current_level];

    // Get the offset in the node
    int offset = counter % constants::KEYS_PER_NODE;
    // If first time access the node
    if (offset == 0) {
        // Case 1: the node needs to be sent to higher level
        if (counter != 0 // Exclude the first element
        && ((current_level + 1) >= (int32_t)non_leaf_nodes.size()  // It is the first time access the next level
        || (counter / constants::KEYS_PER_NODE) > counters[current_level + 1])) { // The node has not been sent up to the next level
            insertHelper(non_leaf_nodes, counters, key, current_level + 1);
            return;
        }
        // Case 2: The previous node has been sent to the higher level. Now insert the next node
        level.emplace_back(BTreeNode());
    }
    BTreeNode& node = level[counter / constants::KEYS_PER_NODE];

    // Insert into the node
    node.keys[offset] = key;
    // Assign offsets, started with 0 at each level
    node.ptrs[offset] = (counter / constants::KEYS_PER_NODE) * (constants::KEYS_PER_NODE + 1) + offset;
    node.ptrs[offset + 1] = node.ptrs[offset] + 1; // The next ptr is always one index larger than the first one

    // A node might not be full, so need to count the size
    ++node.size;
    // The counter counts the next element to insert
    ++counter;
}