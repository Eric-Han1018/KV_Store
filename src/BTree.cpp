#include "BTree.h"
#include "bloomFilter.h"
#include "LSMTree.h"

/* Insert non-leaf elements into their node in the corresponding level
 * Non-leaf Node Structure: |...keys...|...offsets....|# of keys|
 */
void BTree::insertHelper(const int64_t& key, const int32_t current_level, const int32_t& max_size) {
    // If first time access the level, create it
    if (current_level >= (int32_t)non_leaf_nodes.size()) {
        non_leaf_nodes.emplace_back(vector<BTreeNonLeafNode>());
        counters.emplace_back(0);
        non_leaf_nodes.back().emplace_back(BTreeNonLeafNode());
    }

    // Get the offset in the node
    int offset = counters[current_level] % constants::KEYS_PER_NODE;
    // During a B-Tree split, we split the big node into half-and-half
    const int split_offset = (constants::KEYS_PER_NODE + 1) / 2;

    if (offset == split_offset && counters[current_level] < max_size - 1) {
        // Push the middle point to the next level
        insertHelper(key, current_level + 1, (max_size - 1) / (split_offset + 1));
        // Create a new non-leaf node, and the second half goes there
        non_leaf_nodes[current_level].emplace_back(BTreeNonLeafNode());
    } else {
        BTreeNonLeafNode& node = non_leaf_nodes[current_level].back();
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
void BTree::print_B_Tree(aligned_KV_vector& sorted_KV) {
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
void BTree::insertFixUp(const int32_t& sorted_KV_size) {
    // Change ptrs to independent file offsets
    int32_t off = sorted_KV_size * constants::PAIR_SIZE;
    for (int32_t level_index = (int32_t)non_leaf_nodes.size() - 1; level_index >= 0; --level_index) {
        vector<BTreeNonLeafNode>& level = non_leaf_nodes[level_index];
        int32_t next_size;
        // Calculate # of nodes in next level
        if (level_index >= 1) {
            next_size = (int32_t)non_leaf_nodes[level_index - 1].size();
        } else {
            next_size = (int32_t)sorted_KV_size / constants::KEYS_PER_NODE;
        }

        off += (int32_t)level.size() * (int32_t)sizeof(BTreeNonLeafNode);
        int32_t per_level_counter = 0;
        for (BTreeNonLeafNode& node : level) {
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
                        // Otherwise, just use BTreeNonLeafNode size
                        offset = per_level_counter * (int32_t)sizeof(BTreeNonLeafNode) + off;
                    }
                }
                ++per_level_counter;
            }
        }
    }
}

// Given a SST file and a key, search in B-Tree non-leaf nodes to find the offset of the leaf
// The search is performed page-by-page from Buffer Pool
const int32_t BTree::search_BTree_non_leaf_nodes(LSMTree& lsmtree, const int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end
                                                                                                                        , const size_t& non_leaf_start) {
    int64_t offset = non_leaf_start;
    BTreeNonLeafNode* curNode;
    char* tmp;

    // If by any chance the SST only has one page, then it will just be a sorted array without any root
    #ifdef ASSERT
        assert(file_end >= non_leaf_start);
    #endif
    if (file_end == non_leaf_start) {
        return 0;
    }

    // Traverse B-Tree non-leaf nodes
    while (offset >= (int64_t)non_leaf_start) {
        // Read corresponding node from storage/buffer pool
        lsmtree.read(file_path.c_str(), fd, tmp, offset, false, false);
        curNode = (BTreeNonLeafNode*)tmp;
        #ifdef ASSERT
            assert(curNode->size != 0);
        #endif

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
        #ifdef ASSERT
            assert(offset < (int64_t)file_end);
        #endif

    }

    return offset;
}

// Write the B-Tree non-leaf nodes to storage
// At this stage, the leaf node should be already written to the storage
// B-Tree structure: |....sorted_KV (as leaf)....|root|..next level..|...next level...|
void BTree::write_non_leaf_nodes_to_storage(const int& fd, int64_t& offset) {
    // We traverse the levels reversely because the root is stored at the last element
    for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
        vector<BTreeNonLeafNode>& level = non_leaf_nodes[i];
        int nbytes = pwrite(fd, (char*)&level[0], level.size()*sizeof(BTreeNonLeafNode), offset);
        #ifdef ASSERT
            assert(nbytes == (int)(level.size()*sizeof(BTreeNonLeafNode)));
        #endif
        offset += nbytes;
    }
}

/* Convert a sorted KV array into a B-Tree
 * The sorted KV array itself is served as the leaf level
 * While iterating the KV array, we also set the Bloom Filter at the same time
 * B-Tree structure: |....sorted_KV (as leaf)....|root|..next level..|...next level...|
 * Return: offset to the end of leaf level
 */
int32_t BTree::convertToBTree(aligned_KV_vector& sorted_KV, BloomFilter& bloom_filter) {
    int32_t padding = 0;
    int32_t current_level = 0;

    // We pad repeated last element to form a complete leaf node
    if ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE != 0) {
        padding = constants::KEYS_PER_NODE - ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE);
        for (int32_t i = 0; i < padding; ++i) {
            sorted_KV.emplace_back(sorted_KV.back());
        }
    }

    // We need to send the last element in each leaf node into higher levels, except the last one
    int32_t bound = (int32_t)sorted_KV.size() - 1;
    // This stores the total number of keys in all non-leaf nodes
    int32_t max_non_leaf = sorted_KV.size() / constants::KEYS_PER_NODE - 1;

    // Iterate each leaf element to find all non-leaf elements
    for (int32_t count = 0; count < bound - padding; ++count) {
        // Insert bits in Bloom Filter
        bloom_filter.set(sorted_KV.data[count].first);
        // These are all elements in non-leaf nodes
        if (count % constants::KEYS_PER_NODE == constants::KEYS_PER_NODE - 1) {
            insertHelper(sorted_KV.data[count].first, current_level, max_non_leaf);
        }
    }
    // Iterating padded elements
    // We iterate paddings separately because we do not need to set the Bloom Filter for them
    for (int32_t count = bound - padding; count < bound; ++count) {
        // These are all elements in non-leaf nodes
        if (count % constants::KEYS_PER_NODE == constants::KEYS_PER_NODE - 1) {
            insertHelper(sorted_KV.data[count].first, current_level, max_non_leaf);
        }
    }
    // We still have one last element that has not been added to the filter
    bloom_filter.set(sorted_KV.data[bound].first);

    #ifdef DEBUG
        print_B_Tree(non_leaf_nodes, sorted_KV);
    #endif
    // Set up the pointers in the B-Tree
    insertFixUp(sorted_KV.size());

    #ifdef DEBUG
        print_B_Tree(non_leaf_nodes, sorted_KV);
    #endif

    // file offset to the end of the leaf nodes
    return sorted_KV.size() * constants::PAIR_SIZE;
}

/* If we do not have a complete sorted KV array but we only know which keys are in the non-leaf nodes,
 * such as during LSM-Tree compaction, we can use these non-leaf keys to build up our BTree.
 * total_count: total number of entries on the leaves
 */
void BTree::convertToBtree(const vector<int64_t>& non_leaf_keys, const int32_t& total_count) {
    int32_t current_level = 0;
    for (size_t i = 0; i < non_leaf_keys.size() - 1; ++i) {
        // Fill the non_leaf_keys into one of the non-leaf levels
        insertHelper(non_leaf_keys[i], current_level, non_leaf_keys.size() - 1);
    }
    // Calculate the pointers in the B-Tree
    insertFixUp(total_count);
}