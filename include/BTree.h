#pragma once
#include <iostream>
#include <vector>
#include <filesystem>
#include <set>
#include <algorithm>
#include "constants.h"
#include "aligned_KV_vector.h"
using namespace std;
namespace fs = std::filesystem;

// B-Tree non-leaf Node (members stored contiguously)
typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeNonLeafNode {
    int64_t keys[constants::KEYS_PER_NODE] = {0}; // Keys in each node
    int32_t ptrs[constants::KEYS_PER_NODE + 1] = {0}; // File offsets to children
    int32_t size = 0;
} BTreeNonLeafNode;

// B-Tree leaf Node
typedef struct alignas(constants::KEYS_PER_NODE * constants::PAIR_SIZE) BTreeLeafNode {
    pair<int64_t, int64_t> data[constants::KEYS_PER_NODE]; // KV pairs
} BTreeLeafNode;

class BloomFilter;
class LSMTree;

class BTree {
    private:
        /* Note: We did not create a member for the leaf nodes because their formats are different under different cases:
         * 1. When flushing a memtable to SST: they are presented as a sorted KV array
         * 2. During Compaction: they are stored in an output buffer and flushed to storage page-by-page
         */
        vector<vector<BTreeNonLeafNode>> non_leaf_nodes; // Stores the B-Tree non-leaf nodes
        vector<int32_t> counters; // Counts the cumulative number of total elements among all the nodes in each level

    public:
        static const int32_t search_BTree_non_leaf_nodes(LSMTree& lsmtree, const int& fd, const fs::path& file_path
                                                        , const int64_t& key, const size_t& file_end, const size_t& non_leaf_start);
        void write_non_leaf_nodes_to_storage(const int& fd, int64_t& offset);
        int32_t convertToBTree(aligned_KV_vector& sorted_KV, BloomFilter& bloom_filter);
        void convertToBtree(const vector<int64_t>& non_leaf_keys, const int32_t& total_count);

    private:
        void insertHelper(const int64_t& key, const int32_t current_level, const int32_t& max_size);
        void insertFixUp(const int32_t& leaf_end);
        void print_B_Tree(aligned_KV_vector& sorted_KV);
};