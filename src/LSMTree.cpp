#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "LSMTree.h"
#include "BTree.h"
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
            assert(levels[i].sorted_dir.size() < constants::LSMT_SIZE_RATIO);
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
            for (size_t i = 0; i < num_levels; ++i) {
                Level level = levels[i];
                for (fs::path& file_name : level.sorted_dir) {
                    assert(fs::exists(sst_path / file_name));
                    assert(fs::exists(filter_path / file_name));
                    size_t which_level;
                    parse_SST_level(file_name, which_level);
                    assert(which_level == level.level);
                }
                if (!level.last_level) assert(level.cur_size == level.sorted_dir.size());
            }
            // Check Dostoevsky, where the last level needs to have only one big SST
            assert(levels[num_levels-1].last_level);
            assert(levels[num_levels-1].sorted_dir.size() == 1);
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

    // TODO: optimize the merge: move files from cur_level to next_level without compaction, and then merge
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

    // Rename the prefix of the file to indicate the new level
    new_filename[0] = '0' + next_level->level;
    next_level->sorted_dir.emplace_back(new_filename);
    rename((sst_path / cur_level->sorted_dir[0]).c_str(), (sst_path / new_filename).c_str());
    rename((filter_path / cur_level->sorted_dir[0]).c_str(), (filter_path / new_filename).c_str());

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
    vector<pair<int, int>> fds(num_sst); // pair<fd, ret> from open and pread

    for (int i = 0; i < num_sst; ++i) {
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
    BloomFilter bloom_filter(calculate_sst_size(*next_level), next_level->level, num_levels);
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

    // B-tree variables
    vector<int64_t> non_leaf_keys; // Stores all the keys in all B-Tree non-leaf nodes
    // Try to predict required memory using fan-out = KEYS_PER_NODE
    if (last_compaction) non_leaf_keys.reserve(calculate_sst_size(*next_level) / constants::KEYS_PER_NODE);
    pair<int64_t, int64_t> last_kv = {-1, -1}; // This records the last key in the leaf

    // Initialize the heap with the first element of each SSTs
    for (int i = 0; i < num_sst; ++i) {
        if (indices[i] < constants::KEYS_PER_NODE) {
            minHeap.push({leafNodes[i].data[indices[i]], i});
            ++indices[i];
        }
    }

    #ifdef ASSERT
        assert(!minHeap.empty());
    #endif
    // Get the minimum key in the SST, for generating SST file name
    int64_t min_key = constants::TOMBSTONE;
    if (last_compaction) min_key = minHeap.top().data.first;

    pair<bool,int64_t> found_tombstone; //(is_tombstone, key_tombstone)
    found_tombstone.first = false;
    while (!minHeap.empty()) {
        HeapNode node = minHeap.top();
        minHeap.pop();
        // If detect TOMBSTONE, reject any pairs with the same key until a new key appears
        if (largest_level && node.data.second == constants::TOMBSTONE){
            found_tombstone.first = true;
            found_tombstone.second = node.data.first;
        }
        if (largest_level && found_tombstone.second != node.data.first){
            found_tombstone.first = false;
        }
        if (!found_tombstone.first){
            // Add to result if it's the first element or a non-duplicate
            if ((total_count == 0) || // If it is the first element we ever inserted
                (output_buffer.size() == 0 && last_kv.first != node.data.first) || // Compare current node with the last element in the flushed buffer
                (output_buffer.size() != 0 && output_buffer.back().first != node.data.first)){ // Compare current node with the last element in the current buffer
                output_buffer.emplace_back(node.data);
                ++total_count;
                if (last_compaction)
                    bloom_filter.set(node.data.first);
                if (last_compaction && total_count % constants::KEYS_PER_NODE == 0) {
                    // This is an element in one of the non-leaf nodes in the B-Tree
                    non_leaf_keys.emplace_back(output_buffer.back().first);
                }
                if (output_buffer.isFull()) {
                    last_kv = output_buffer.flush_to_file(fd, SST_offset);
                }
                #ifdef DEBUG
                    cout << "insert: key {" << node.data.first << "," << node.data.second << "} to output buffer" << endl;
                #endif
            }
        }
        int index = node.arrayIndex;
        if (indices[index] < constants::KEYS_PER_NODE) {
            minHeap.push({leafNodes[index].data[indices[index]], index});
            ++indices[index];
        }

        // TODO: maybe we can skip the rest of the file whenever we encounter a padded key? (but it is not so important because a page can have 256 keys at max, which is not so big)
        if ((indices[index] * constants::PAIR_SIZE) >= fds[index].second && (pages_read[index] * constants::PAGE_SIZE) < leaf_ends[index]) { // read next page
            fds[index].second = pread(fds[index].first, (char*)&leafNodes[index], constants::PAGE_SIZE, pages_read[index] * constants::PAGE_SIZE);
            #ifdef ASSERT
                assert(fds[index].second == (int)(constants::PAGE_SIZE));
            #endif
            ++pages_read[index];
            indices[index] = 0;
        } else if ((pages_read[index] * constants::PAGE_SIZE) >= leaf_ends[index]) { // End of leaves
            fds[index].second = 0;
        }
    }

    // Get the maximum key in the SST, for generating SST file name
    int64_t max_key = constants::TOMBSTONE;
    if (last_compaction) {
        if (output_buffer.size() > 0) max_key = output_buffer.back().first;
        else max_key = last_kv.first;
    }

    // We pad repeated last element to form a complete 4kb node
    if (total_count % constants::KEYS_PER_NODE != 0) {
        int padding = output_buffer.add_padding();
        total_count += padding;
        #ifdef ASSERT
            assert(output_buffer.isFull());
        #endif
        if (last_compaction) non_leaf_keys.emplace_back(output_buffer.back().first);
        output_buffer.flush_to_file(fd, SST_offset);
    }

    // Build up the B-Tree if it is the last compaction
    int64_t leaf_end = total_count*constants::PAIR_SIZE;
    if (last_compaction) {
        BTree btree;
        // Build-up the non-leaf nodes
        btree.convertToBtree(non_leaf_keys, total_count);

        // Write non-leaf levels to file, starting from root
        int64_t offset = leaf_end;
        btree.write_non_leaf_nodes_to_storage(fd, offset);
    }

    int result = close(fd);
    #ifdef ASSERT
        assert(result != -1);
    #endif

    string output_filename = generate_filename(next_level->level, min_key, max_key, leaf_end);
    result = rename(temp_name.c_str(), (sst_path / output_filename).c_str());
    #ifdef ASSERT
        assert(result == 0);
    #endif
    // Write bloom filter to storage
    if (last_compaction)
        bloom_filter.writeToStorage(filter_path / output_filename);

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

void Level::clear_level() {
    cur_size = 0;
    sorted_dir.clear();
}

// Get min_key, max_key and leaf-end offset (end of leaf nodes & start of non-leaf nodes) from a SST file's name
// SST filename format: <LSMTree_level>_<time><clock>_<min_key>_<max_key>_<leaf_end>.bytes
void LSMTree::parse_SST_name(const string& file_name, int64_t& min_key, int64_t& max_key, size_t& leaf_end) {
    size_t first = file_name.find('_');
    size_t second = file_name.find('_', first + 1);
    size_t third = file_name.find('_', second + 1);
    size_t last = file_name.find_last_of('_');

    min_key = strtoll(file_name.substr(second + 1, third - second - 1).c_str(), nullptr, 10);
    max_key = strtoll(file_name.substr(third + 1, last - third - 1).c_str(), nullptr, 10);
    parse_SST_offset(file_name, leaf_end);

    #ifdef ASSERT
        assert(min_key < max_key);
    #endif
}

// Get only the leaf offset from a SST file's name
void LSMTree::parse_SST_offset(const string& file_name, size_t& leaf_end) {
    size_t start_pos = file_name.find_last_of("_");
    size_t end_pos = file_name.find('.', start_pos);
    leaf_end = stoi(file_name.substr(start_pos + 1, end_pos - start_pos - 1));
}

// Get only the level from a SST file's name
void LSMTree::parse_SST_level(const string& file_name, size_t& level) {
    size_t first = file_name.find('_');
    level = stoi(file_name.substr(0, first));
}

// Perform BTree search in SST
const int64_t* LSMTree::search_SST_BTree(int& fd, const fs::path& file_path, const int64_t& key
                                                            , const size_t& file_end, const size_t& non_leaf_start) {
    // Search BTree non-leaf nodes to find the offset of leaf
    const int32_t offset = BTree::search_BTree_non_leaf_nodes(*this, fd, file_path, key, file_end, non_leaf_start);
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
const int64_t* LSMTree::search_SST_Binary(int& fd, const fs::path& file_path, const int64_t& key
                                                            , const size_t& file_end, const size_t& non_leaf_start) {
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
const int64_t* LSMTree::search_SST(const fs::path& file_path, const int64_t& key, const size_t& file_end
                                                        , const size_t& non_leaf_start, const bool& use_btree) {
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

// Merge the scanned arrays from two SSTs (stored as two consecutive sorted ranges, separated by first_end in sorted_KV)
// The final sorted array is stored back in sorted_KV
void LSMTree::merge_scan_results(vector<pair<int64_t, int64_t>>*& sorted_KV, const size_t& first_end) {
    std::vector<std::pair<int64_t, int64_t>> tmp;
    tmp.reserve(sorted_KV->size());
    size_t i = 0, j = first_end;
    while (i < first_end && j < sorted_KV->size()) {
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
    while (i < first_end) {
        tmp.emplace_back((*sorted_KV)[i]);
        ++i;
    }
    while (j < sorted_KV->size()) {
        tmp.emplace_back((*sorted_KV)[j]);
        ++j;
    }
    *sorted_KV = std::move(tmp);
}

// Perform scan operation in SSTs
void LSMTree::scan(vector<pair<int64_t, int64_t>>*& sorted_KV, const int64_t& key1, const int64_t& key2, const bool& use_btree) {
    size_t len;
    // counts the number of pages that the scan accesses
    // Used for preventing sequential floodings
    size_t scanPageCount = 0;

    // Scan each SST
    for (int i = 0; i < (int)num_levels; ++i) {
        for (auto file_path_itr = levels[i].sorted_dir.rbegin(); file_path_itr != levels[i].sorted_dir.rend(); ++file_path_itr) {
            #ifdef DEBUG
                cout << "Scanning file: " << *file_path_itr << "..." << endl;
            #endif

            int64_t min_key, max_key;
            size_t file_end = fs::file_size(sst_path / *file_path_itr);
            size_t non_leaf_start;
            parse_SST_name(*file_path_itr, min_key, max_key, non_leaf_start);

            // EXTRA FEATURE
            // Since Bloom Filter does not help with scan(), we use min_key & max_key to
            // make db skip SSTs if the scan is not within the key range of the SST
            if (key2 < min_key || key1 > max_key) {
                #ifdef DEBUG
                    cout << "key is not in range of: " << *file_path_itr << endl;
                #endif
                continue;
            }

            // Store current size of array for merging
            len = sorted_KV->size();

            // Scan the SST
            scan_SST(*sorted_KV, sst_path / (*file_path_itr), key1, key2, file_end, non_leaf_start, scanPageCount, use_btree);

            // Merge into one sorted array
            merge_scan_results(sorted_KV, len);
        }
    }
}

const int32_t LSMTree::scan_helper_BTree(const int& fd, const fs::path& file_path, const int64_t& key1, const size_t& file_end
                                                                                            , const size_t& non_leaf_start) {
    int32_t offset = BTree::search_BTree_non_leaf_nodes(*this, fd, file_path, key1, file_end, non_leaf_start);
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

const int32_t LSMTree::scan_helper_Binary(const int& fd, const fs::path& file_path, const int64_t& key1
                                , const int32_t& num_elements, const size_t& file_end, const size_t& non_leaf_start) {
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
void LSMTree::scan_SST(vector<pair<int64_t, int64_t>>& sorted_KV, const string& file_path, const int64_t& key1
        , const int64_t& key2, const size_t& file_end, const size_t& non_leaf_start, size_t& scanPageCount, const bool& use_btree) {
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

    bool bufferHit = true;

    for (auto i=start; i < num_elements ; ++i) {
        // Record previous key to prevent reading redundant padded values
        if (i > start) {
            prev = cur.first;
        }
        // Iterate each element and push to vector
        int curPage = floor((i*constants::PAIR_SIZE) / constants::PAGE_SIZE);
        if (curPage != prevPage) {
            // Under long scan, since the page returned from read() is not in buffer pool,
            // no eviction anymore and we need to delte it manually
            if (scanPageCount >= constants::SEQUENTIAL_FLOODING_LIMIT && !bufferHit) {
                delete leafNode;
            }
            // If num of pages accessed by the scan is larger than SEQUENTIAL_FLOODING_LIMIT,
            // then do not put ANY following pages into buffer pool
            #ifdef DEBUG
                if (scanPageCount == constants::SEQUENTIAL_FLOODING_LIMIT) {
                        cout << "Long-Range Scan detected. Disabling buffer pool..." << endl;
                }
            #endif
            ++scanPageCount;

            char* tmp = nullptr;
            bufferHit = read(file_path.c_str(), fd, tmp, (curPage * constants::PAGE_SIZE), scanPageCount, true);
            leafNode = (BTreeLeafNode*)tmp;
            prevPage = curPage;
        }
        cur = leafNode->data[i - curPage * constants::KEYS_PER_NODE];

        if (cur.first <= key2) { 
            // Padding detection
            if (i > start && cur.first == prev) { // Our keys are unique
                break;
            }
            sorted_KV.emplace_back(cur);
        } else {
            break; // until meeting the first value out of range
        }
    }
    if (scanPageCount >= constants::SEQUENTIAL_FLOODING_LIMIT && !bufferHit) {
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
bool LSMTree::read(const string& file_path, const int& fd, char*& data, const off_t& offset, const size_t& scanPageCount
                                                                                                    , const bool& isLeaf) {
    #ifdef ASSERT
        assert(offset >= 0);
    #endif
    const string p_id = parse_pid(file_path, offset);
    char* tmp;
    
    if (constants::USE_BUFFER_POOL && buffer->get_from_buffer(p_id, tmp)) {
        data = tmp;
        return true;
    } else {
        tmp = (char*)new BTreeNonLeafNode();
        int ret = pread(fd, tmp, constants::KEYS_PER_NODE * constants::PAIR_SIZE, offset);
        #ifdef ASSERT
            assert(ret == constants::KEYS_PER_NODE * constants::PAIR_SIZE);
        #endif
        // If a range query is long, we do not save it to the buffer pool (aka evict immediately)
        if (scanPageCount < constants::SEQUENTIAL_FLOODING_LIMIT) {
            buffer->insert_to_buffer(p_id, isLeaf, tmp);
        }
        data = tmp;
        return false;
    }
}

// Given a level information, calculate the number of kv entries on the SST leaves
size_t LSMTree::calculate_sst_size(Level& cur_level) {
    // num entries in memtable
    size_t total_num_entries = constants::MEMTABLE_SIZE;
    // num entries in each SST on cur_level
    total_num_entries *= pow(constants::LSMT_SIZE_RATIO, cur_level.level); 
    if (cur_level.last_level) {
        // For Dostoevsky, if it is at the last level, they will be a big contiguous run
        total_num_entries *= cur_level.cur_size; 
    }
    return total_num_entries;
}

// Given a file path to a filter of a level, and a key, probing if the key is in the level
bool LSMTree::check_bloomFilter(const fs::path& filter_path, const int64_t& key, Level& cur_level) {
    float bits_per_entry = BloomFilter::calculate_num_bits_per_entry(cur_level.level, num_levels);
    // Convert total num of kv entries to num of BF bits
    size_t total_num_bits = (size_t)(calculate_sst_size(cur_level) * bits_per_entry);
    size_t total_num_cache_lines = total_num_bits >> constants::CACHE_LINE_SIZE_BITS_SHIFT;

    int fd = open(filter_path.c_str(), O_RDONLY | O_SYNC | O_DIRECT, 0777);
    #ifdef ASSERT
        assert(fd != -1);
    #endif

    // Get the cacheline index
    size_t cache_line_hash = murmur_hash(key, 0) % total_num_cache_lines;
    // To get the page index of the cacheline. One page has multiple cachelines
    size_t page = cache_line_hash >> constants::PAGE_CACHELINE_SHIFT;
    // To get the cacheline index on the page
    size_t cache_line_hash_on_page = cache_line_hash & ((1<<constants::PAGE_CACHELINE_SHIFT) - 1);

    char* tmp = nullptr;
    read(filter_path, fd, tmp, page << constants::PAGE_SIZE_SHIFT, false, false);

    // Array of bitset<constants::CACHE_LINE_SIZE>
    bitset<constants::CACHE_LINE_SIZE_BITS>* blocked_bitmaps = (bitset<constants::CACHE_LINE_SIZE_BITS>*)tmp; 

    for (uint32_t i = 1; i <= BloomFilter::calculate_num_of_hashes(bits_per_entry); ++i) {
        // Get the bit index
        size_t hash = murmur_hash(key, i) & ((1<<constants::CACHE_LINE_SIZE_BITS_SHIFT) - 1);
        // If any of the hash results in a negative in the probing, return DNE
        if (!blocked_bitmaps[cache_line_hash_on_page].test(hash)) {
            int close_res = close(fd);
            #ifdef ASSERT
                assert(close_res != -1);
            #endif
            return false;
        }
    }
    int close_res = close(fd);
    #ifdef ASSERT
        assert(close_res != -1);
    #endif
    return true;
}

// Generate the filename of a SST or a filter
// Format: <LSMT_level>_<time><clock>_<min_key_value>_<max_key_value>_<file_offset_of_non_leaf_nodes>.bytes
string LSMTree::generate_filename(const size_t& level, const int64_t& min_key, const int64_t& max_key, const int32_t& leaf_ends) {
    // We use time+clock to uniquely identify a timestamp
    string cur_time = to_string(time(0));
    string cur_clock = to_string(clock()); // In case there is a tie in time())
    string prefix = to_string(level).append("_").append(cur_time).append(cur_clock).append("_");
    string suffix = to_string(min_key).append("_").append(to_string(max_key)).append("_").append(to_string(leaf_ends)).append(".bytes");
    return prefix.append(suffix);
}

// Print the LSM-Tree, for debugging purpose
void LSMTree::print_lsmt() {
    for (size_t i = 0; i < num_levels; ++i) {
        cout << "level " << to_string(i) << " cur_size: " << levels[i].cur_size << endl;
        if (levels[i].cur_size > 0) {
            for (size_t j = 0; j < levels[i].sorted_dir.size(); ++j) {
                cout << " sorted_dir: " << levels[i].sorted_dir[j].c_str() << endl;
            }
        }
    }
}