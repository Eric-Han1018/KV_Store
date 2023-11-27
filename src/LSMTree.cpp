#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "LSMTree.h"
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
const int64_t* LSMTree::get(const int64_t& key, const bool& use_btree) {
    // Iterate to read each file in descending order (new->old)
    for (int i = 0; i < (int)num_levels; ++i) {
        #ifdef ASSERT
            assert(levels[i].sorted_dir.size() < constants::LSMT_SIZE_RATIO); // FIXME: Temp check
        #endif
        for (auto file_path_itr = levels[i].sorted_dir.rbegin(); file_path_itr != levels[i].sorted_dir.rend(); ++file_path_itr) {
            #ifdef DEBUG
                cout << "Searching in file: " << *file_path_itr << "..." << endl;
            #endif

            // Skip if Bloom Filter returns negative
            if (!check_bloomFilter(filter_path / (*file_path_itr), key, levels[i])) {
                #ifdef DEBUG
                    cout << "Bloom Filter returned false from: " << *file_path_itr << endl;
                #endif
                continue;
            }

            #ifdef DEBUG
                cout << "Bloom Filter returned true from: " << *file_path_itr << endl;
            #endif

            // Get end of leaf offset
            size_t file_end = fs::file_size(sst_path / *file_path_itr);
            size_t non_leaf_start;
            parse_SST_offset(*file_path_itr, non_leaf_start);
            const int64_t* value = search_SST(sst_path / (*file_path_itr), key, file_end, non_leaf_start, use_btree);
            if (value != nullptr && *value == constants::TOMBSTONE){
                delete value;
                return nullptr;
            }
            if (value != nullptr) return value;
            #ifdef DEBUG
                cout << "Not found key: " << key << " in file: " << *file_path_itr << endl;
            #endif
        }
    }
    return nullptr;
}

void LSMTree::add_SST(const string& file_name) {
    #ifdef DEBUG
        cout << "add_SST" << endl;
    #endif
    levels[0].sorted_dir.emplace_back(file_name);

    // At this point, the largest level is the first level and we compact it into one-large SST when the size is less than or equal to SIZE_RATIO
    if (levels[0].last_level && levels[0].cur_size > 1 && levels[0].cur_size <= constants::LSMT_SIZE_RATIO) {
        merge_down_helper(levels.begin(), levels.begin(), levels[0].sorted_dir.size(), true, true);
    }

    if (levels[0].cur_size == constants::LSMT_SIZE_RATIO) {
        merge_down(levels.begin());

        #ifdef ASSERT
            // At this stage, the compaction is finished, so all intermediate SSTs have been deleted.
            // Thus, all SSTs now are BTrees and they all have Bloom Filters
            for (Level& level : levels) {
                for (fs::path& file_name : level.sorted_dir) {
                    assert(fs::exists(sst_path / file_name));
                    assert(fs::exists(filter_path / file_name));
                }
            }
        #endif
    }
    #ifdef DEBUG
        print_lsmt();
    #endif
}

// Compact the current level on LSMTree to next level
void LSMTree::merge_down(const vector<Level>::iterator& cur_level) {
    #ifdef DEBUG
        cout << "merge_down" << endl;
    #endif
    vector<Level>::iterator next_level;
    
    next_level = cur_level + 1;

    if (cur_level->last_level && cur_level->cur_size == constants::LSMT_SIZE_RATIO) {
        largest_level_move_down(cur_level);
        return;
    }
    
    // We build up the BTree only after the last compaction
    bool last_compaction = false;
    if (next_level->cur_size + 1 < constants::LSMT_SIZE_RATIO) {
        last_compaction = true;
    }

    merge_down_helper(cur_level, next_level, cur_level->cur_size, last_compaction, cur_level->last_level);

    // check if the next_level is largest level, if so compact into one large SST if the cur_size <= LSMT_SIZE_RATIO
    if (next_level->last_level && next_level->cur_size > 1 && next_level->cur_size <= constants::LSMT_SIZE_RATIO) {
        merge_down_helper(next_level, next_level, next_level->sorted_dir.size(), true, true);
    }

    if (next_level->cur_size == constants::LSMT_SIZE_RATIO) {
        merge_down(next_level);
    }
}

// At this point, cur_level is the last_level and cur_size is full, we need to move it to the next_level
void LSMTree::largest_level_move_down(const vector<Level>::iterator& cur_level) {
    string new_filename = cur_level->sorted_dir[0].c_str();
    vector<Level>::iterator next_level = cur_level + 1;
    new_filename[0] = next_level->level;
    next_level->sorted_dir.emplace_back(cur_level->sorted_dir[0]);
    ++next_level->cur_size;
    next_level->last_level = true;
    cur_level->clear_level();
    cur_level->last_level = false;
    ++num_levels;
}

// Helper functions to compact SSTs in current level and send to next level
void LSMTree::merge_down_helper(const vector<Level>::iterator& cur_level, const vector<Level>::iterator& next_level, const int& num_sst, 
                                const bool& last_compaction, const bool& largest_level) {
    #ifdef DEBUG
        cout << "merge_down_helper" << endl;
    #endif

    vector<size_t> leaf_ends(num_sst, -1);
    vector<BTreeLeafNode> leafNodes(num_sst);
    // FIXME: ret好像没啥用诶
    vector<pair<int, int>> fds(num_sst); // pair<fd, ret> from open and pread

    for (int i = 0; i < num_sst; ++i) {
        // FIXME: Seems that we no longer need min and max
        parse_SST_offset(cur_level->sorted_dir[i], leaf_ends[i]);
        fds[i].first = open((sst_path / cur_level->sorted_dir[i]).c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
        #ifdef ASSERT
            assert(fds[i].first != -1);
        #endif
        fds[i].second = pread(fds[i].first, (char*)&leafNodes[i], constants::PAGE_SIZE, 0);
        #ifdef ASSERT
            assert(fds[i].second == (int)(constants::PAGE_SIZE));
        #endif
    }

    // loop over kv pairs, if encounters two same key, only the more recent version is kept
    // Output buffer that stores the KV-pairs
    aligned_KV_vector output_buffer(constants::KEYS_PER_NODE);
    // Bloom Filter for the SST
    BloomFilter bloom_filter(calculate_sst_size(*next_level));
    size_t total_count = 0; // Couts the total num of elements inserted so far
    int64_t SST_offset = 0;
    // Whenever the buffer is full, write to this file
    #ifdef ASSERT
        assert(!fs::exists(sst_path / "temp.bytes"));
    #endif
    string temp_name = sst_path / "temp.bytes";
    int fd = open(temp_name.c_str(), O_WRONLY | O_CREAT | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd != -1);
    #endif

    priority_queue<HeapNode, vector<HeapNode>, decltype(comp)> minHeap(comp); //initializes a priority queue (min-heap) of size 0, that stores HeapNode
    vector<int> pages_read(num_sst, 1);
    vector<int> indices(num_sst, 0);

    // Btree variables
    vector<vector<BTreeNode>> non_leaf_nodes; // Stores all leaf elements
    vector<int32_t> counters; // This counts the number of elements in each level
    int32_t current_level = 0;
    pair<int64_t, int64_t> last_kv = {-1, -1}; // This records the last key in the leaf

    // Initialize the heap with the first element of each SSTs
    for (int i = 0; i < num_sst; ++i) {
        if (indices[i] < constants::KEYS_PER_NODE) {
            minHeap.push({leafNodes[i].data[indices[i]], i});
            ++indices[i];
        }
    }

    while (!minHeap.empty()) {
        HeapNode node = minHeap.top();
        minHeap.pop();

        // Add to result if it's the first element or a non-duplicate
        if ((total_count == 0) || // If it is the first element we ever inserted
            (output_buffer.size() == 0 && last_kv.first != node.data.first) || // Compare current node with the last element in the flushed buffer
            (output_buffer.size() != 0 && output_buffer.back().first != node.data.first)) { // Compare current node with the last element in the current buffer
            output_buffer.emplace_back(node.data);
            ++total_count;
            if (last_compaction)
                bloom_filter.set(node.data.first);
            // Build the BTree non-leaf node
            if (last_compaction && total_count % constants::KEYS_PER_NODE == 0) {
                insertHelper(non_leaf_nodes, counters, output_buffer.back().first, current_level);
            }
            if (output_buffer.isFull()) {
                last_kv = output_buffer.flush_to_file(fd, SST_offset);
            }
            #ifdef DEBUG
                cout << "insert: key {" << node.data.first << "," << node.data.second << "} to output buffer" << endl;
            #endif
        }
        int index = node.arrayIndex;
        if (indices[index] < constants::KEYS_PER_NODE) {
            minHeap.push({leafNodes[index].data[indices[index]], index});
            ++indices[index];
        }

        if ((indices[index] * constants::PAIR_SIZE) >= fds[index].second && (pages_read[index] * constants::PAGE_SIZE) < leaf_ends[index]) { // read next page
            fds[index].second = pread(fds[index].first, (char*)&leafNodes[index], constants::PAGE_SIZE, pages_read[index] * constants::PAGE_SIZE);
            #ifdef ASSERT
                assert(fds[index].second == (int)(constants::PAGE_SIZE));
            #endif
            ++pages_read[index];
            indices[index] = 0;
        } else {
            // End of leaves
            fds[index].second = 0;
        }
    }

    // We pad repeated last element to form a complete 4kb node
    if (total_count % constants::KEYS_PER_NODE != 0) {
        int padding = constants::KEYS_PER_NODE - (total_count % constants::KEYS_PER_NODE);
        for (int32_t i = 0; i < padding; ++i) {
            #ifdef ASSERT
            if (output_buffer.size() > 0) { // Pad with the last element in the buffer
            #endif
                output_buffer.emplace_back(output_buffer.back());
            #ifdef ASSERT
            } else {
                assert(false); // If output_buffer is empty, then no need to pad
            }
            #endif
        }
        total_count += padding;
        #ifdef ASSERT
            assert(output_buffer.isFull());
        #endif
        if (last_compaction && total_count / constants::KEYS_PER_NODE % (constants::KEYS_PER_NODE + 1) != 0) {
            insertHelper(non_leaf_nodes, counters, output_buffer.back().first, current_level);
        }
        output_buffer.flush_to_file(fd, SST_offset);
    }

    // Below is in similar logic as in WriteToSST()
    // If it is not the last level, we do not build up the non-leaf nodes
    int64_t leaf_end = total_count*constants::PAIR_SIZE;

    if (last_compaction) {
        // Change ptrs to independent file offsets
        int64_t off = leaf_end;
        for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
            vector<BTreeNode>& level = non_leaf_nodes[i];
            int32_t next_size;
            // Calculate # of nodes in next level
            if (i >= 1) {
                next_size = (int32_t)non_leaf_nodes[i - 1].size();
            } else {
                next_size = total_count / constants::KEYS_PER_NODE;
            }

            off += (int32_t)level.size() * (int32_t)sizeof(BTreeNode);
            for (BTreeNode& node : level) {
                for (int32_t& offset : node.ptrs) {
                    // If offset exceeds bound, set to -1
                    if (offset >= next_size) {
                        offset = -1;
                    } else {
                        // If it is the last non-leaf level, need to take offset as multiple of KV stores and points to the leaves
                        if (i == 0) {
                            offset = offset * constants::KEYS_PER_NODE * constants::PAIR_SIZE;
                        } else {
                            // Otherwise, just use BTreeNode size
                            offset = offset * (int32_t)sizeof(BTreeNode) + off;
                        }
                    }
                }
            }
        }


        int64_t offset = leaf_end;
        // Write non-leaf levels, starting from root
        for (int32_t i = (int32_t)non_leaf_nodes.size() - 1; i >= 0; --i) {
            vector<BTreeNode>& level = non_leaf_nodes[i];
            int nbytes = pwrite(fd, (char*)&level[0], level.size()*sizeof(BTreeNode), offset);
            #ifdef ASSERT
                assert(nbytes == (int)(level.size()*sizeof(BTreeNode)));
            #endif
            offset += nbytes;
        }
    }

    int result = close(fd);
    #ifdef ASSERT
        assert(result != -1);
    #endif

    string output_filename = generate_filename(next_level->level, -1, -1, leaf_end);
    result = rename(temp_name.c_str(), (sst_path / output_filename).c_str());
    #ifdef ASSERT
        assert(result == 0);
    #endif
    // Write bloom filter to storage
    if (last_compaction)
        bloom_filter.writeToBloomFilter(filter_path / output_filename);

    // Compaction finished. Close and Remove all files on current level
    for (int i = 0; i < num_sst; ++i) {
        int result = close(fds[i].first);
        #ifdef ASSERT
            assert(result != -1);
        #endif
        bool remove_result = remove(sst_path / cur_level->sorted_dir[i]);
        #ifdef ASSERT
            assert(remove_result);
        #endif
        // Note: This may return false, which is by purpose, because we only build up the bloom filter for the last compaction
        remove(filter_path / cur_level->sorted_dir[i]);
    }

    // Add to the maintained directory list
    if (largest_level) {
        cur_level->sorted_dir.clear();
        cur_level->sorted_dir.emplace_back(output_filename);
    } else {
        cur_level->clear_level();
        ++next_level->cur_size;
        next_level->sorted_dir.emplace_back(output_filename);
    }
}

// FIXME: Move this and the one in database.cpp to ultil.cpp??
/* Insert non-leaf elements into their node in the corresponding level
 * Non-leaf Node Structure (see SST.h): |...keys...|...offsets....|# of keys|
 */
void LSMTree::insertHelper(vector<vector<BTreeNode>>& non_leaf_nodes, vector<int32_t>& counters, const int64_t& key, int32_t current_level) {
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

void Level::clear_level() {
    cur_size = 0;
    sorted_dir.clear();
}

// Get min_key, max_key and leaf offset from a SST file's name
void LSMTree::parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, size_t& file_end) {
    size_t first = file_name.find('_');
    size_t second = file_name.find('_', first + 1);
    size_t third = file_name.find('_', second + 1);
    size_t last = file_name.find_last_of('_');
    size_t dot = file_name.find_last_of('.');

    min_key = strtoll(file_name.substr(second + 1, third - second - 1).c_str(), nullptr, 10);
    max_key = strtoll(file_name.substr(third + 1, last - third - 1).c_str(), nullptr, 10);
    file_end = stoi(file_name.substr(last + 1, dot - last - 1));
}

// Get only the leaf offset from a SST file's name
void LSMTree::parse_SST_offset(const string& file_name, size_t& file_end) {
    size_t start_pos = file_name.find_last_of("_");
    size_t end_pos = file_name.find('.', start_pos);
    file_end = stoi(file_name.substr(start_pos + 1, end_pos - start_pos - 1));
}

// Get only the level from a SST file's name
void LSMTree::parse_SST_level(const string& file_name, size_t& level) {
    size_t first = file_name.find('_');
    level = stoi(file_name.substr(0, first - 1));
}

// Search in BTree non-leaf nodes to find the offset of the leaf
const int32_t LSMTree::search_BTree_non_leaf_nodes(const int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start) {
    int64_t offset = non_leaf_start;
    BTreeNode* curNode;
    char* tmp;

    // Traverse B-Tree non-leaf nodes
    while (offset >= (int64_t)non_leaf_start) {
        // Read corresponding node
        read(file_path.c_str(), fd, tmp, offset, false, false);
        curNode = (BTreeNode*)tmp;
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
            // if (offset < file_end) {
            //     cout << "offset: " << offset << " file_end: " << file_end << endl;
            // }
            assert(offset < (int64_t)file_end);
        #endif

    }

    return offset;
}

// Perform BTree search in SST
const int64_t* LSMTree::search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start) {
    // Search BTree non-leaf nodes to find the offset of leaf
    const int32_t offset = search_BTree_non_leaf_nodes(fd, file_path, key, file_end, non_leaf_start);
    if (offset < 0) return nullptr;
    // Binary search in the leaf node
    BTreeLeafNode* leafNode;
    char* tmp;
    read(file_path.c_str(), fd, tmp, offset, false, true);
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
const int64_t* LSMTree::search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start) {
    auto num_elements = non_leaf_start / constants::PAIR_SIZE;

    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    BTreeLeafNode* leafNode = nullptr;
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
            read(file_path.c_str(), fd, tmp, (curPage * constants::PAGE_SIZE), false, true);
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
const int64_t* LSMTree::search_SST(const fs::path& file_path, const int64_t& key, const size_t& file_end, const size_t& non_leaf_start, const bool& use_btree) {
    const int64_t* result = nullptr;
    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);

    #ifdef ASSERT
        assert(fd != -1);
    #endif

    if (use_btree) {
        result = search_SST_BTree(fd, file_path, key, file_end, non_leaf_start);
    } else {
        result = search_SST_Binary(fd, file_path, key, file_end, non_leaf_start);
    }
    
    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif

    return result;
}

void LSMTree::scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree) {
    size_t len;
    bool isLongScan = false;

    // Scan each SST
    for (int i = 0; i < (int)num_levels; ++i) {
        for (auto file_path_itr = levels[i].sorted_dir.rbegin(); file_path_itr != levels[i].sorted_dir.rend(); ++file_path_itr) {
            #ifdef DEBUG
                cout << "Scanning file: " << *file_path_itr << "..." << endl;
            #endif
            // Skip if the keys is not between min_key and max_key
            int64_t min_key, max_key;
            size_t file_end = fs::file_size(sst_path / *file_path_itr);
            size_t non_leaf_start;
            parse_SST_name(*file_path_itr, min_key, max_key, non_leaf_start);
            // if (key2 < min_key || key1 > max_key) {
            //     #ifdef DEBUG
            //         cout << "key is not in range of: " << *file_path_itr << endl;
            //     #endif
            //     continue;
            // }

            // Store current size of array for merging
            len = sorted_KV->size();

            // Scan the SST
            scan_SST(*sorted_KV, sst_path / (*file_path_itr), key1, key2, file_end, non_leaf_start, isLongScan, use_btree);

            // Merge into one sorted array
            // inplace_merge(sorted_KV->begin(), sorted_KV->begin()+len, sorted_KV->end());
            std::vector<std::pair<int64_t, int64_t>> tmp;
            tmp.reserve(sorted_KV->size());
            size_t i = 0, j = len;
            while (i < len && j < sorted_KV->size()) {
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
            while (i < len) {
                tmp.emplace_back((*sorted_KV)[i]);
                ++i;
            }
            while (j < sorted_KV->size()) {
                tmp.emplace_back((*sorted_KV)[j]);
                ++j;
            }
            *sorted_KV = std::move(tmp);
        }
    }
}

const int32_t LSMTree::scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const size_t& file_end, const size_t& non_leaf_start) {
    int32_t offset = search_BTree_non_leaf_nodes(fd, file_path, key1, file_end, non_leaf_start);
    if (offset < 0) return -1;
    BTreeLeafNode* leafNode;
    char* tmp;
    read(file_path.c_str(), fd, tmp, offset, false, true);
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
    return (int)(offset / constants::PAIR_SIZE) + low;
}

const int32_t LSMTree::scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1, const int32_t& num_elements, const size_t& file_end, const size_t& non_leaf_start) {
    // Variables used in binary search
    pair<int64_t, int64_t> cur;
    BTreeLeafNode* leafNode = nullptr;
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
            read(file_path.c_str(), fd, tmp, (curPage * constants::PAGE_SIZE), false, true);
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
void LSMTree::scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1, const int64_t& key2, const size_t& file_end, const size_t& non_leaf_start, bool& isLongScan, const bool& use_btree) {
    // Open the SST file
    int fd = open(file_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
 
    #ifdef ASSERT
        assert(fd != -1);
    #endif

    int num_elements = (int)(non_leaf_start / constants::PAIR_SIZE);

    int32_t start = -1;
    if (use_btree) {
        start = scan_helper_BTree(fd, file_path, key1, file_end, non_leaf_start);
        if (start == -1) return;
    } else {
        start = scan_helper_Binary(fd, file_path, key1, num_elements, file_end, non_leaf_start);
    }

    // Low and high both points to what we are looking for
    pair<int64_t, int64_t> cur;
    int64_t prev = -1; // FIXME: init to tombstone?
    BTreeLeafNode* leafNode = nullptr;
    int prevPage = -1;

    uint32_t scanRange = 0; // counts the number of pages that the scan spans
    bool bufferHit = true;

    for (auto i=start; i < num_elements ; ++i) {
        // Record previous key to prevent reading redundant padded values
        if (i > start) {
            prev = cur.first;
        }
        // Iterate each element and push to vector
        int curPage = floor((i*constants::PAIR_SIZE) / constants::PAGE_SIZE);
        if (curPage != prevPage) {
            // Under long scan, since the page returned from read() is not in buffer pool, no eviction anymore and we need to delte it manually
            if (isLongScan && !bufferHit) {
                delete leafNode;
            }
            // If the scan is larger than SCAN_RANGE_LIMIT, then do not put ANY following pages into buffer pool (including ones from other SSTs)
            ++scanRange;
            if (!isLongScan && scanRange >= constants::SCAN_RANGE_LIMIT) {
                #ifdef DEBUG
                    cout << "Long-Range Scan detected. Disabling buffer pool..." << endl;
                #endif
                isLongScan = true;
            }

            char* tmp = nullptr;
            bufferHit = read(file_path.c_str(), fd, tmp, (curPage * constants::PAGE_SIZE), isLongScan, true);
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
    if (isLongScan && !bufferHit) {
        delete leafNode;
    }
    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif
}

// Combine SST file's name with offset to get the page ID
const string LSMTree::parse_pid(const string& file_path, const int32_t& offset) {
    size_t lastSlash = file_path.find_last_of('/');
    size_t lastDot = file_path.find_last_of('.');
    // Note: Here "-1" means to distinguish between sst/ and filter/ (i.e. "t/" vs. "r/")
    string file_name = file_path.substr(lastSlash - 1, lastDot - lastSlash - 1);
    
    // Combine the extracted part with offset into a string
    stringstream combinedString;
    combinedString << file_name << "_" << offset;
    return combinedString.str();
}

// Read either from bufferpool or SST
bool LSMTree::read(const string& file_path, const int& fd, char*& data, const off_t& offset, const bool& isLongScan, const bool& isLeaf) {
    #ifdef ASSERT
        assert(offset >= 0);
    #endif
    const string p_id = parse_pid(file_path, offset);
    char* tmp;
    
    if (constants::USE_BUFFER_POOL && buffer->get_from_buffer(p_id, tmp)) {
        data = tmp;
        return true;
    } else {
        tmp = (char*)new BTreeNode();
        int ret = pread(fd, tmp, constants::KEYS_PER_NODE * constants::PAIR_SIZE, offset);
        #ifdef ASSERT
            assert(ret == constants::KEYS_PER_NODE * constants::PAIR_SIZE);
        #endif
        // If a range query is long, we do not save it to the buffer pool (aka evict immediately)
        if (!isLongScan) {
            buffer->insert_to_buffer(p_id, isLeaf, tmp);
        }
        data = tmp;
        return false;
    }
}

// Given a level information, calculate the number of kv entries on the SST leaves
size_t LSMTree::calculate_sst_size(Level& cur_level) {
    size_t total_num_entries = constants::MEMTABLE_SIZE; // num entries in memtable
    total_num_entries *= pow(constants::LSMT_SIZE_RATIO, cur_level.level); // num entries in each SST on cur_level
    if (cur_level.last_level) {
        total_num_entries *= cur_level.cur_size; // For Dostoevsky, if it is at the last level, they will be a big contiguous run
    }
    return total_num_entries;
}

bool LSMTree::check_bloomFilter(const fs::path& filter_path, const int64_t& key, Level& cur_level) {
    size_t total_num_bits = (calculate_sst_size(cur_level) * constants::BLOOM_FILTER_NUM_BITS); // Convert total num of kv entries to num of BF bits

    int fd = open(filter_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
    for (uint32_t i = 0; i < constants::BLOOM_FILTER_NUM_HASHES; ++i) {
        size_t hash = murmur_hash(key, i) % total_num_bits;

        size_t page = hash >> (constants::BYTE_BIT_SHIFT + constants::PAGE_SIZE_SHIFT); // 4kB = 1<<12 bytes = 8<<12 bits = 1<<15 bits

        char* tmp = nullptr;
        read(filter_path, fd, tmp, page * constants::PAGE_SIZE, false, false);

        // One-page-large bitmap
        bitset<1<<(constants::BYTE_BIT_SHIFT + constants::PAGE_SIZE_SHIFT)>* bs = (bitset<1<<(constants::BYTE_BIT_SHIFT + constants::PAGE_SIZE_SHIFT)>*)tmp;

        size_t offset = hash & ((1<<(constants::BYTE_BIT_SHIFT + constants::PAGE_SIZE_SHIFT))-1); // This is the mask to the the offset bit

        if (!bs->test(offset)) {
            close(fd);
            return false;
        }
    }
    close(fd);
    return true;
}