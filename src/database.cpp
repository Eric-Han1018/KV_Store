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
    bool db_exist = false;

    if (fs::exists(directoryPath / "sst") && fs::exists(directoryPath / "filter")
        && fs::is_directory(directoryPath / "sst") && fs::is_directory(directoryPath / "filter")) {
        #ifdef DEBUG
            std::cout << "Directory exists." << db_name << std::endl;
        #endif
        db_exist = true;
    } else {
        #ifdef DEBUG
            std::cout << "Directory does not exist." << db_name <<  std::endl;
        #endif
        fs::create_directories(directoryPath / "sst");
        fs::create_directory(directoryPath / "filter");
    }
    memtable = new RBTree(memtable_capacity, memtable_root);
    bufferpool = new Bufferpool(constants::BUFFER_POOL_CAPACITY);
    lsmtree = new LSMTree(db_name, constants::LSMT_DEPTH, bufferpool);

    if (db_exist) {
        // Restoring the sorted list of existing SST files when reopen DB
        vector<fs::path> sorted_dir;
        for (auto& file_path : fs::directory_iterator(lsmtree->sst_path)) {
            sorted_dir.push_back(file_path.path().filename());
        }
        sort(sorted_dir.begin(), sorted_dir.end());
        size_t cur_level;
        for (auto file_path_itr = sorted_dir.begin(); file_path_itr != sorted_dir.end(); ++file_path_itr) {
            lsmtree->parse_SST_level(*file_path_itr, cur_level);
            lsmtree->levels[cur_level].sorted_dir.push_back(*file_path_itr);
            ++lsmtree->levels[cur_level].cur_size;
        }
        lsmtree->levels[0].last_level = false;
        lsmtree->levels[cur_level].last_level = true;
        lsmtree->num_levels = cur_level + 1;

        /* Since we change the name of the last_level's SST in closeDB(), we need
           to change it back.
        */
        Level& last_level = lsmtree->levels[lsmtree->num_levels - 1];
        if (last_level.sorted_dir.size() > 0) {
            #ifdef ASSERT
                assert(last_level.sorted_dir.size() == 1);
            #endif
            string old_name = string(last_level.sorted_dir[0]);
            // Parse the last level's size
            int first = old_name.rfind('_');
            int second = old_name.rfind('.');
            last_level.cur_size = stoi(old_name.substr(first + 1, second - first - 1));

            // Change back to the original filename format
            string new_name = old_name.substr(0, first).append(".bytes");
            rename((lsmtree->sst_path / old_name).c_str(), (lsmtree->sst_path / new_name).c_str());
            last_level.sorted_dir[0] = new_name;
        }
    }
}

void Database::closeDB() {
    if (memtable->curr_size > 0) {
        string file_path = writeToSST();

        /* For the purpose of reopen the DB in the future, we need to record the
           size of the largest level, because it only has one big contiguous SST
           in Dostoevsky (without this record, we can't tell if the last level
           reaches capacity or not).
        */
        Level& last_level = lsmtree->levels[lsmtree->num_levels - 1];
        string old_name = string(last_level.sorted_dir[0]);
        #ifdef ASSERT
            assert(last_level.sorted_dir.size() == 1);
        #endif
        // We add the current size of the last_level to its filename
        // New format: <level>_<time><clock>_<min>_<max>_<BTree leaf end>_<level size>.bytes
        string new_name = old_name.substr(0, old_name.find('.')).append("_")
                                                                .append(to_string(last_level.cur_size))
                                                                .append(".bytes");
        rename((lsmtree->sst_path / old_name).c_str(), (lsmtree->sst_path / new_name).c_str());
    }
    if (memtable) {
        delete memtable;
    }
    if (lsmtree) {
        delete lsmtree;
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
        return lsmtree->get(key, use_btree);
    }
    if(*result == constants::TOMBSTONE){
        #ifdef DEBUG
            std::cout << "Has been deleted" << std::endl;
        #endif
        delete result;
        return nullptr;
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
    lsmtree->scan(sorted_KV, key1, key2, use_btree);
    removeTombstones(sorted_KV, constants::TOMBSTONE);
    return sorted_KV;
}

void Database::removeTombstones(std::vector<std::pair<int64_t, int64_t>>*& sorted_KV, int64_t tombstone) {
    if (sorted_KV) { // Check if the pointer is not null
        auto newEnd = std::remove_if(sorted_KV->begin(), sorted_KV->end(),
                                    [tombstone](const std::pair<int64_t, int64_t>& kv) {
                                        return kv.second == tombstone;
                                    });
        sorted_KV->erase(newEnd, sorted_KV->end());
    }
}

void Database::del(const int64_t& key){
    put(key, constants::TOMBSTONE);
}

/* Write the levles in the B-Tree to a SST file
 * SST structure: |....sorted_KV (as leaf)....|root|..next level..|...next level...|
 * Return: offset to leaf level
 */
int32_t Database::convertToSST(vector<vector<BTreeNode>>& non_leaf_nodes, aligned_KV_vector& sorted_KV, BloomFilter& bloom_filter) {
    // This counts the number of elements in each level
    vector<int32_t> counters;

    int32_t padding = 0;
    int32_t current_level = 0;

    // To make things easier, we pad repeated last element to form a complete leaf node
    if ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE != 0) {
        padding = constants::KEYS_PER_NODE - ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE);
        for (int32_t i = 0; i < padding; ++i) {
            sorted_KV.emplace_back(sorted_KV.back());
        }
    }

    int32_t bound = (int32_t)sorted_KV.size() - 1;
    int32_t max_non_leaf = sorted_KV.size() / constants::KEYS_PER_NODE - 1;

    // Iterate each leaf element to find all non-leaf elements
    for (int32_t count = 0; count < bound - padding; ++count) {
        // Insert bits in Bloom Filter
        bloom_filter.set(sorted_KV.data[count].first);
        // These are all elements in non-leaf nodes
        if (count % constants::KEYS_PER_NODE == constants::KEYS_PER_NODE - 1) {
            insertHelper(non_leaf_nodes, counters, sorted_KV.data[count].first, current_level, max_non_leaf);
        }
    }
    // Iterating padded elements
    for (int32_t count = bound - padding; count < bound; ++count) {
        // These are all elements in non-leaf nodes
        if (count % constants::KEYS_PER_NODE == constants::KEYS_PER_NODE - 1) {
            insertHelper(non_leaf_nodes, counters, sorted_KV.data[count].first, current_level, max_non_leaf);
        }
    }
    // We still have one last element that has not been added to the filter
    bloom_filter.set(sorted_KV.data[bound].first);

    #ifdef DEBUG
        print_B_Tree(non_leaf_nodes, sorted_KV);
    #endif

    insertFixUp(non_leaf_nodes, sorted_KV.size());

    #ifdef DEBUG
        print_B_Tree(non_leaf_nodes, sorted_KV);
    #endif

    return sorted_KV.size() * constants::PAIR_SIZE;
}

/* When memtable reaches its capacity, write it into an SST
 * File name format: timeclock_min_max_leaf-end-offset.bytes
 */
string Database::writeToSST() {
    // Content in std::vector is stored contiguously
    aligned_KV_vector sorted_KV(constants::MEMTABLE_SIZE); // Stores all non-leaf elements
    vector<vector<BTreeNode>> non_leaf_nodes; // Stores all leaf elements
    int32_t leaf_ends; // Stores the file offset of the end of leaf nodes
    scan_memtable(sorted_KV, memtable->root);
    
    // Check if we need to perform compaction in LSMTree
    bool ifCompact = lsmtree->check_LSMTree_compaction();

    // Create a Bloom Filter for the SST
    ++lsmtree->levels[0].cur_size; // We need to increment cur_size here because the Bloom Filter needs it
    BloomFilter bloom_filter(lsmtree->calculate_sst_size(lsmtree->levels[0]));

    // We only build up the BTree if we do not need any compaction
    if (ifCompact) {
        leaf_ends = convertToSST(non_leaf_nodes, sorted_KV, bloom_filter);
    } else {
        leaf_ends = sorted_KV.size() * constants::PAIR_SIZE;
        // We pad repeated last element to make it 4kb aligned
        if ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE != 0) {
            int32_t padding = constants::KEYS_PER_NODE - ((int32_t)sorted_KV.size() % constants::KEYS_PER_NODE);
            for (int32_t i = 0; i < padding; ++i) {
                sorted_KV.emplace_back(sorted_KV.back());
            }
        }
    }

    // Create file name based on current time
    string SST_path = lsmtree->sst_path;
    string filter_path = lsmtree->filter_path;
    string file_name;
    if (ifCompact)
        file_name = lsmtree->generate_filename(0, memtable->min_key, memtable->max_key, leaf_ends);
    else
        // If the file will be compated further, we randomly assign it two equal min&max keys
        file_name = lsmtree->generate_filename(0, constants::TOMBSTONE, constants::TOMBSTONE, leaf_ends);
    SST_path.append(file_name);
    filter_path.append(file_name);

    // Write data structure to binary file
    int fd = open(SST_path.c_str(), O_WRONLY | O_CREAT | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd!=-1);
    #endif

    int nbytes;
    int offset = 0;
    // Write clustered leaves
    nbytes = pwrite(fd, (char*)sorted_KV.data, sorted_KV.size()*constants::PAIR_SIZE, offset);
    #ifdef ASSERT
        assert(nbytes == (int)(sorted_KV.size()*constants::PAIR_SIZE));
    #endif

    if (ifCompact) {
        offset += nbytes;
        // Write non-leaf levels, starting from root
        for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
            vector<BTreeNode>& level = non_leaf_nodes[i];
            nbytes = pwrite(fd, (char*)&level[0], level.size()*sizeof(BTreeNode), offset);
            #ifdef ASSERT
                assert(nbytes == (int)(level.size()*sizeof(BTreeNode)));
            #endif
            offset += nbytes;
        }
    }

    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif

    if (ifCompact) {
        // Write Bloom Filter to storage
        bloom_filter.writeToBloomFilter(filter_path);
    }

    // Add to the maintained directory list
    lsmtree->add_SST(file_name);

    // Clear the memtable
    clear_tree();

    return SST_path;
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