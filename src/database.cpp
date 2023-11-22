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
#include <time.h>
using namespace std;


void Database::openDB(const string db_name) {
    this->db_name = db_name;
    fs::path directoryPath = constants::DATA_FOLDER + db_name;

    if (fs::exists(directoryPath) && fs::is_directory(directoryPath)) {
        #ifdef DEBUG
            std::cout << "Directory exists." << db_name << std::endl;
        #endif
    } else {
        #ifdef DEBUG
            std::cout << "Directory does not exist." << db_name <<  std::endl;
        #endif
        fs::create_directory(directoryPath);
    }
    memtable = new RBTree(memtable_capacity, memtable_root);
    bufferpool = new Bufferpool(constants::BUFFER_POOL_CAPACITY);
    sst = new SST(db_name, bufferpool);
}

void Database::closeDB() {
    if (memtable->memtable_size > 0) {
        string file_path = writeToSST();
        delete memtable;
    }
    if (sst) {
        delete sst;
    }
    if (bufferpool) {
        delete bufferpool;
    }
}

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

const int64_t* Database::get(const int64_t& key, const bool use_btree){
    int64_t* result;
    if(memtable->get(result, key) == notInMemtable) {
        return sst->get(key, use_btree);
    }
    return result;
}

const vector<pair<int64_t, int64_t>>* Database::scan(const int64_t& key1, const int64_t& key2, const bool use_btree) {
    // Check if key1 < key2
    #ifdef ASSERT
        assert(key1 < key2);
    #endif

    vector<pair<int64_t, int64_t>>* sorted_KV = new vector<pair<int64_t, int64_t>>;

    // Scan the memtable
    memtable->scan(*sorted_KV, memtable->root, key1, key2);

    // Scan each SST
    sst->scan(sorted_KV, key1, key2, use_btree);

    return sorted_KV;
}

/* Insert non-leaf elements into their node in the corresponding level
 * Non-leaf Node Structure (see SST.h): |...keys...|...offsets....|# of keys|
 */
void Database::insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, int64_t& key, int32_t current_level) {
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

/* Write the levles in the B-Tree to a SST file
 * SST structure: |root|..next level..|...next level...|....sorted_KV (as leaf)....|
 * Return: offset to leaf level
 */
int32_t Database::convertToSST(vector<vector<BTreeNode>>& non_leaf_nodes, aligned_KV_vector& sorted_KV) {
    // This counts the number of elements in each level
    vector<int32_t> counters;

    int32_t padding;
    int32_t current_level = 0;

    // To make things easier, we pad repeated last element to form a complete leaf node
    if ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE != 0) {
        padding = constants::KEYS_PER_NODE - ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE);
        for (int32_t i = 0; i < padding; ++i) {
            sorted_KV.emplace_back(sorted_KV.back());
        }
    }

    // To further simplify, we also send the last element of the last leaf node to its parent
    int32_t bound;
    if ((int32_t)sorted_KV.size() / constants::KEYS_PER_NODE % (constants::KEYS_PER_NODE + 1) == 0)
        // Except this case, where we do not need to worry
        bound = (int32_t)sorted_KV.size() - 1;
    else
        bound = (int32_t)sorted_KV.size();

    // Iterate each leaf element to find all non-leaf elements
    for (int32_t count = 0; count < bound; ++count) {
        // These are all elements in non-leaf nodes
        if (count % constants::KEYS_PER_NODE == constants::KEYS_PER_NODE - 1) {
            insertHelper(non_leaf_nodes, counters, sorted_KV.data[count].first, current_level);
        }
    }

    // Change ptrs to independent file offsets
    int32_t off = 0;
    for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
        vector<BTreeNode>& level = non_leaf_nodes[i];
        int32_t next_size;
        // Calculate # of nodes in next level
        if (i >= 1) {
            next_size = (int32_t)non_leaf_nodes[i - 1].size();
        } else {
            next_size = (int32_t)sorted_KV.size() / constants::KEYS_PER_NODE;
        }

        off += (int32_t)level.size() * (int32_t)sizeof(BTreeNode);
        for (BTreeNode& node : level) {
            for (int32_t& offset : node.ptrs) {
                // If offset exceeds bound, set to -1
                if (offset >= next_size) {
                    offset = -1;
                } else {
                    // If it is the last non-leaf level, need to take offset as multiple of KV stores
                    if (i == 0) {
                        offset = offset * constants::KEYS_PER_NODE * constants::PAIR_SIZE + off;
                    } else {
                        // Otherwise, just use BTreeNode size
                        offset = offset * (int32_t)sizeof(BTreeNode) + off;
                    }
                }
            }
        }
    }
    #ifdef DEBUG
        print_B_Tree(non_leaf_nodes, sorted_KV);
    #endif

    // FIXME: the current implementation is to add a root no matter if the number of KVs exceed a node's capacity
    // return non_leaf_nodes[0][0].size != 0 ? non_leaf_nodes[0][0].ptrs[0] : 0;
    return non_leaf_nodes[0][0].ptrs[0];
}

/* When memtable reaches its capacity, write it into an SST
 * File name format: timeclock_min_max_leafOffset.bytes
 */
string Database::writeToSST() {
    // Content in std::vector is stored contiguously
    aligned_KV_vector sorted_KV; // Stores all non-leaf elements
    vector<vector<BTreeNode>> non_leaf_nodes; // Stores all leaf elements
    scan_memtable(sorted_KV, memtable->root);

    int32_t leaf_offset;
    
    leaf_offset = convertToSST(non_leaf_nodes, sorted_KV);

    // Create file name based on current time
    // TODO: modify file name to a smarter way
    string file_name = constants::DATA_FOLDER + db_name + '/';
    time_t current_time = time(0);
    clock_t current_clock = clock(); // In case there is a tie in time()
    file_name.append(to_string(current_time)).append(to_string(current_clock)).append("_").append(to_string(memtable->min_key)).append("_").append(to_string(memtable->max_key)).append("_").append(to_string(leaf_offset)).append(".bytes");

    // Write data structure to binary file
    // FIXME: do we need O_DIRECT for now?
    int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd!=-1);
    #endif

    int nbytes;
    int offset = 0;
    // Write non-leaf levels, starting from root
    for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
        vector<BTreeNode>& level = non_leaf_nodes[i];
        nbytes = pwrite(fd, (char*)&level[0], level.size()*sizeof(BTreeNode), offset);
        #ifdef ASSERT
            assert(nbytes == (int)(level.size()*sizeof(BTreeNode)));
        #endif
        offset += nbytes;
    }
    
    // Write clustered leaves
    nbytes = pwrite(fd, (char*)&sorted_KV.data, sorted_KV.size()*constants::PAIR_SIZE, offset);
    #ifdef ASSERT
        assert(nbytes == (int)(sorted_KV.size()*constants::PAIR_SIZE));
    #endif

    close(fd);

    // Add to the maintained directory list
    sst->sorted_dir.emplace_back(file_name);

    // Clear the memtable
    clear_tree();

    return file_name;
}

// Helper function to recursively perform inorder scan
void Database::scan_memtable(aligned_KV_vector& sorted_KV, Node* root) {
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

// int main() {
//     srand (time(NULL));
//     Database db(constants::MEMTABLE_SIZE);

//     int keys[] = {1, 2, 7, 16, 21, 22, 24, 29, 31, 32, 33, 35, 40, 61, 73, 74, 82, 90, 94, 95, 97, -1, -2};
//     for (int key : keys) {
//         db.put(key, 6);
//     }

//     for (int i = 0; i < constants::MEMTABLE_SIZE; ++i) {
//         db.put((int64_t)rand(), 6);
//     }
//     db.put(-1, 6);


//     // Find all existing keys
//     for (int key : keys) {
//         const int64_t* value = db.get(key, true);
//         if (value != nullptr)
//             cout << "Found: " << key << "->" << *value << endl;
//     }

//     // Find non-existing keys
//     int keys_non[] = {3, 4, 5, 6, 8, 9, 13, 34, 37, 55, 70, 85, 96};
//     for (int key : keys_non) {
//         const int64_t* value = db.get(key, true);
//         if (value != nullptr)
//             cout << "Found: " << key << "->" << *value << endl;
//     }

//     // Scan 1: lower & higher bounds both within range
//     const vector<pair<int64_t, int64_t>>* values = db.scan(20, 93, true);
//     for (const auto& pair : *values) {
//         cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
//     }
//     delete values;

//     // Scan 2: lower & higher bounds both not within range
//     values = db.scan(-20, 100, true);
//     for (const auto& pair : *values) {
//         cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
//     }
//     delete values;

//     // Scan 3: all elements smaller than range
//     values = db.scan(-100, -20, true);
//     for (const auto& pair : *values) {
//         cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
//     }
//     delete values;

//     // Scan 4: all elements larger than range
//     values = db.scan(100, 200, true);
//     for (const auto& pair : *values) {
//         cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
//     }
//     delete values;

//     return 0;
// }