#include <iostream>
#include "database.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include <random>
using namespace std;
namespace fs = std::filesystem;

// Delete all SSTs & Bloom Filters in data/ folder
void deleteSSTs(const fs::path& dir_path) {
    for (auto& file : fs::directory_iterator(dir_path)) {
        fs::remove_all(file);
    }
}

// Print memtable (rbtree) in the terminal (root is at left)
void inorderKey(Node* node, int indent=0)
{
    if (node != NULL) {
        if(node->right) inorderKey(node->right, indent+4);
        if (indent) {
            cout << setw(indent) << ' ';
        }
        cout<< node->key << endl;
        if(node->left) inorderKey(node->left, indent+4);
    }
}

// Print corresponding colour of each node
void inorderColor(Node* node, int indent=0)
{
    if (node != NULL) {
        if(node->right) inorderColor(node->right, indent+4);
        if (indent) {
            cout << setw(indent) << ' ';
        }
        cout<< node->color << endl;
        if(node->left) inorderColor(node->left, indent+4);
    }
}

// Test only on a small memtable
// This makes things easier when debugging
void test_put_small(string db_name)
{
    Database db(6);
    db.openDB(db_name);

    cout << "--- test case 1: Test put() without exceeding tree capacity ---" << endl;
    db.put(1, 10);
    db.put(5, 50);
    db.put(1, 30); // This is an update on memtable
    db.put(2, 20);
    db.put(4, 40);
    db.put(3, 30);
    db.put(8, 80);
    #ifdef DEBUG
        cout << "\ntree-graph for key: (Read from left to right)" << endl;
        inorderKey(db.memtable->root);
        cout << "\ntree-graph for color - 0 black, 1 red:" << endl;
        inorderColor(db.memtable->root);
    #endif
    assert(db.memtable->curr_size == 6);
    cout << "--- test case 2: Test put() with exceeding tree capacity ---" << endl;
    db.put(4, 400);
    db.put(7, 700);
    db.put(8, 800);
    db.put(9, 900);
    assert(db.memtable->curr_size == 4);
    assert(db.lsmtree->num_levels == 1);
    #ifdef DEBUG
        cout << "\ntree-graph for key: (Read from left to right)" << endl;
        inorderKey(db.memtable->root);
        cout << "\ntree-graph for color - 0 black, 1 red:" << endl;
        inorderColor(db.memtable->root);
    #endif
    db.closeDB();
}

// Test only on a small memtable
// This makes things easier when debugging
void test_get_small(const string& db_name, const bool& ifBtree)
{   
    Database db(2);
    db.openDB(db_name);

    cout << "--- test case 1: Test get() from memtable ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(1, 30); // This is an update on memtable
    db.put(5, 50);
    int64_t key = 1;
    const int64_t* value = db.get(key, ifBtree);
    if (value != nullptr) {
        #ifdef DEBUG
            cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        #endif
        assert(*value == 30);
        delete value;
    } else {
        #ifdef DEBUG
            cout << "Not found Key: " << key << endl;
        #endif
        assert(false);
    }

    cout << "--- test case 2: Test get() from SST ---" << endl;
    db.put(2, 80);
    value = db.get(key, ifBtree);
    if (value != nullptr) {
        #ifdef DEBUG
            cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        #endif
        assert(*value == 30);
    } else {
        #ifdef DEBUG
            cout << "Not found Key: " << key << endl;
        #endif
        assert(false);
    }
    delete value;
    // #ifdef DEBUG
    //     db.bufferpool->print();
    // #endif

    cout << "--- test case 3: Test get() from unexisted keys ---" << endl;
    key = -668;
    value = db.get(-668, ifBtree);
    if (value != nullptr) {
        #ifdef DEBUG
            cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        #endif
        assert(false);
        delete value;
    } else {
        #ifdef DEBUG
            cout << "Not found Key: " << key << endl;
        #endif
    }
    // #ifdef DEBUG
    //     db.bufferpool->print();
    // #endif

    db.closeDB();
}

// Test only on a small memtable
// This makes things easier when debugging
void test_scan_small(const string& db_name, const bool& ifBtree){
    Database db(6);
    db.openDB(db_name);

    cout << "--- test case 1: Test scan() from memtable ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(5, 50);
    db.put(2, 20);
    db.put(4, 40);
    db.put(3, 30);
    db.put(3, 60); // This is an update on memtable
    db.put(8, 80);
    int64_t key1 = 2;
    int64_t key2 = 4;
    const vector<pair<int64_t, int64_t>>* values = db.scan(key1, key2, ifBtree);
    vector<pair<int64_t, int64_t>> ans = {{2, 20}, {3, 60}, {4, 40}};
    #ifdef DEBUG
        for (const auto& pair : *values) {
            cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
        }
    #endif
    assert(is_sorted(values->begin(), values->end()));
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;
    // #ifdef DEBUG
    //     db.bufferpool->print();
    // #endif

    cout << "--- test case 2: Test scan() from SST ---" << endl;
    db.put(-1, -10);
    values = db.scan(key1, key2, ifBtree);
    #ifdef DEBUG
        for (const auto& pair : *values) {
            cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
        }
    #endif
    assert(is_sorted(values->begin(), values->end()));
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;
    // #ifdef DEBUG
    //     db.bufferpool->print();
    // #endif

    cout << "--- test case 3: Test scan() from both memtable and SST ---" << endl;
    db.put(7, 90);
    key1 = 2;
    key2 = 8;
    values = db.scan(key1, key2, ifBtree);
    #ifdef DEBUG
        for (const auto& pair : *values) {
            cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
        }
    #endif
    assert(is_sorted(values->begin(), values->end()));
    ans = {{2, 20}, {3, 60}, {4, 40}, {5, 50}, {7, 90}, {8, 80}};
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;
    // #ifdef DEBUG
    //     db.bufferpool->print();
    // #endif

    db.closeDB();
}

// Test put() with 32MB data + 1MB updates + 1MB deletion&re-insert + 1MB pure deletion
// The inserted data are saved for later testcases
void test_put_big(const string& db_name, Database& db, vector<pair<int64_t, int64_t>>*& memtable_data, vector<pair<int64_t, int64_t>>*& SST_data) {
    size_t SST_size = 32 * (1 << 20) / constants::PAIR_SIZE - constants::MEMTABLE_SIZE; // 31MB
    // This stores data that will eventually be saved in the memtable
    memtable_data = new vector<pair<int64_t, int64_t>>(constants::MEMTABLE_SIZE); // 1MB
    // This stores data that will eventually be saved in SSTs
    SST_data = new vector<pair<int64_t, int64_t>>(SST_size);
    // This stores data that will be inserted & deleted in this test
    vector<pair<int64_t, int64_t>>* delete_data = new vector<pair<int64_t, int64_t>>(constants::MEMTABLE_SIZE); // 1MB


    default_random_engine generator(443); // Fix seed for reproducibility
    uniform_int_distribution<int64_t> distrib(numeric_limits<int64_t>::min()+(int64_t)1, numeric_limits<int64_t>::max()); // Skip the tombstone

    for (int64_t i = 0; i < (int64_t)SST_size; ++i) {
        // Insert even numbers as keys
        SST_data->at(i) = {i * 2 , distrib(generator)};
    }
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        // Insert odd numbers as keys
        memtable_data->at(i) = {i * 2 + 1 , distrib(generator)};
    }
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        // Insert odd numbers as keys
        // These keys will be deleted eventually
        delete_data->at(i) = {(i + constants::MEMTABLE_SIZE) * 2 + 1 , distrib(generator)};
    }
    // Shuffle the data
    shuffle(memtable_data->begin(), memtable_data->end(), generator);
    shuffle(SST_data->begin(), SST_data->end(), generator);
    shuffle(delete_data->begin(), delete_data->end(), generator);

    cout << "--- test case 1: Test put() without exceeding tree capacity ---" << endl;
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        // Note: these data will be updated in the test case below
        db.put(SST_data->at(i).first, SST_data->at(i).second + 1);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);

    cout << "--- test case 2: Test put() with exceeding tree capacity ---" << endl;
    for (int64_t i = constants::MEMTABLE_SIZE; i < 2 * constants::MEMTABLE_SIZE; ++i) {
        // Note: these data will be deleted in the test case below
        db.put(SST_data->at(i).first, SST_data->at(i).second + 2);
    }
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        // Note: these data will be deleted in the test case below
        db.put(delete_data->at(i).first, delete_data->at(i).second);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);

    cout << "--- test case 3: Test Update & Deletion on SST ---" << endl;
    cout << "This may take a while..." << endl;
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        // Insert same data as in test case1 while using different values
        db.put(SST_data->at(i).first, SST_data->at(i).second);
        // Here we also delete the first several of entries inserted in test case 2
        db.del(SST_data->at(i + constants::MEMTABLE_SIZE).first);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);
    // Insert the deleted entries back
    for (int64_t i = constants::MEMTABLE_SIZE; i < 2 * constants::MEMTABLE_SIZE; ++i) {
        db.put(SST_data->at(i).first, SST_data->at(i).second);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);
    // Here we insert more data. This is for testing get() and scan() later-on
    for (int64_t i = 2 * constants::MEMTABLE_SIZE; i < (int64_t)SST_data->size(); ++i) {
        db.put(SST_data->at(i).first, SST_data->at(i).second);
    }
    // Delete entries from delete_data vector
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        db.del(delete_data->at(i).first);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);

    cout << "--- test case 4: Test Update & Deletion on Memtable ---" << endl;
    // Lastly, we insert 1MB data to fill up the memtable
    // So all previously inserted data should be in SSTs
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE - 1; ++i) {
        db.put(memtable_data->at(i).first, memtable_data->at(i).second + 4);
    }
    // Now memtable should only have one spot left
    assert(db.memtable->curr_size == db.memtable->memtable_size - 1);
    // We delete half of the entries in memtable, which should be equivalent to updates with TOMBSTONEs
    // Since the updates happen on memtable, they should be updated in-place
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE - 1; i += 2) {
        db.del(memtable_data->at(i).first);
    }
    // As a result, the memtable size should stay the same
    assert(db.memtable->curr_size == db.memtable->memtable_size - 1);
    // We re-insert the same keys to memtable, while using different values
    for (pair<int64_t, int64_t>& entry : *memtable_data) {
        db.put(entry.first, entry.second);
    }
    // Now memtable should be full
    assert(db.memtable->curr_size == db.memtable->memtable_size);

    delete delete_data;
}

// Test get() on big data size
void test_get_big(const string& db_name, Database& db, vector<pair<int64_t, int64_t>>*& memtable_data, vector<pair<int64_t, int64_t>>*& SST_data, const bool& ifBtree) {
    cout << "--- test case 1: Test get() from memtable ---" << endl;
    const int64_t* value = nullptr;
    for (pair<int64_t, int64_t>& entry : *memtable_data) {
        value = db.get(entry.first, ifBtree);
        assert(*value == entry.second);
        delete value;
    }

    cout << "--- test case 2: Test get() from SST ---" << endl;
    cout << "This may take a while..." << endl;
    int64_t count = 0;
    for (pair<int64_t, int64_t>& entry : *SST_data) {
        // To make the test finish quick, we only test every 500th entry
        if (count % 500 == 0){
            value = db.get(entry.first, ifBtree);
            assert(*value == entry.second);
            delete value;
        }
        ++count;
    }

    cout << "--- test case 3: Test get() from unexisted keys ---" << endl;
    cout << "This may take a while..." << endl;
    // Get the minimum and maximum keys in the current database
    int64_t minimum = min_element(SST_data->begin(), SST_data->end())->first;
    minimum = min(minimum, min_element(memtable_data->begin(), memtable_data->end())->first);
    int64_t maximum = max_element(SST_data->begin(), SST_data->end())->first;
    maximum = max(minimum, max_element(memtable_data->begin(), memtable_data->end())->first);
    // Randomly generate keys that are not in the database
    default_random_engine generator(443); // Fix seed for reproducibility
    uniform_int_distribution<int64_t> distrib_min(numeric_limits<int64_t>::min(), minimum);
    uniform_int_distribution<int64_t> distrib_max(maximum, numeric_limits<int64_t>::max());
    // We test 500 unexisted entries, and they should all return nullptr (NOT FOUND)
    for (int64_t i = SST_data->size() * 2; i < (int64_t)SST_data->size() * 2 + 500; ++i) {
        value = db.get(distrib_min(generator), ifBtree);
        assert(value == nullptr);
        value = db.get(distrib_max(generator), ifBtree);
        assert(value == nullptr);
    }
}

// Test scan() on big data size
void test_scan_big(const string& db_name, Database& db, vector<pair<int64_t, int64_t>>*& memtable_data, vector<pair<int64_t, int64_t>>*& SST_data, const bool& ifBtree) {
    // Merge to get all inserted data so far in sorted order
    vector<pair<int64_t, int64_t>> data(memtable_data->size() + SST_data->size());
    sort(memtable_data->begin(), memtable_data->end());
    sort(SST_data->begin(), SST_data->end());
    merge(memtable_data->begin(), memtable_data->end(), SST_data->begin(), SST_data->end(), data.begin());

    int64_t key1;
    int64_t key2;
    const vector<pair<int64_t, int64_t>>* values = nullptr;

    cout << "--- test case 1: Test scan() from DB ---" << endl;
    cout << "This may take a while..." << endl;
    default_random_engine generator(443); // Fix seed for reproducibility
    uniform_int_distribution<int64_t> distrib_index(0, data.size() - 1);
    // We test 50 scans, each with a incremental range
    for (int i = 1; i <= 100; i+=2) {
        int64_t index = distrib_index(generator);
        key1 = data[index].first;
        key2 = key1 + i * constants::PAGE_SIZE; // This may out of the large bound
        values = db.scan(key1, key2, ifBtree);
        assert(equal(values->begin(), values->end(), data.begin() + index));
        delete values;
    }
    
    cout << "--- test case 2: Test scan() with key1 < minimum element in DB ---" << endl;
    int64_t index = distrib_index(generator);
    key1 = -443;
    key2 = data[index].first;
    values = db.scan(key1, key2, ifBtree);
    assert(equal(values->begin(), values->end(), data.begin()));
    delete values;
    
    cout << "--- test case 3: Test scan() with both key1 & key2 out of bounds ---" << endl;
    key1 = -443;
    key2 = -100;
    values = db.scan(key1, key2, ifBtree);
    assert(values->size() == 0);
    delete values;
    key1 = data.back().first + 1;
    key2 = key1 + 100;
    values = db.scan(key1, key2, ifBtree);
    assert(values->size() == 0);
    delete values;

    cout << "--- test case 3: Test scan() with Sequential Flooding feature ---" << endl;
    key1 = data[0].first;
    // This should exceed the Sequential Scan range limit
    key2 = key1 + 2 * constants::SCAN_RANGE_LIMIT * constants::PAGE_SIZE;
    values = db.scan(key1, key2, ifBtree);
    assert(equal(values->begin(), values->end(), data.begin()));
    delete values;
}


int main(int argc, char **argv) {
    // Testing DB with small memtable capacities
    string db_name = "smallDB";
    cout << "\n===== Test Put(key, value) on small memtable =====\n" << endl;
    test_put_small(db_name);
    cout << "\nTest passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Get(key) on small memtable (BTree) =====\n" << endl;
    test_get_small(db_name, true);
    cout << "\nTest passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Get(key) on small memtable (Binary Search) =====\n" << endl;
    test_get_small(db_name, false);
    cout << "\nTest passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Scan(Key1, Key2) on small memtable (BTree) =====\n" << endl;
    test_scan_small(db_name, true);
    cout << "\nTest passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Scan(Key1, Key2) on small memtable (Binary Search) =====\n" << endl;
    test_scan_small(db_name, false);
    cout << "\nTest passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);

    // Testing DB with big (~32MB) memtable capacities
    db_name = "bigDB";
    vector<pair<int64_t, int64_t>>* memtable_data;
    vector<pair<int64_t, int64_t>>* SST_data;
    Database db(constants::MEMTABLE_SIZE);
    db.openDB(db_name);

    cout << "\n===== Test Put(key, value) on big memtable =====\n" << endl;
    test_put_big(db_name, db, memtable_data, SST_data);
    cout << "\nTest passed\n" << endl;
    cout << "\n===== Test Get(key) on big memtable (Btree) =====\n" << endl;
    test_get_big(db_name, db, memtable_data, SST_data, true);
    cout << "\nTest passed\n" << endl;
    cout << "\n===== Test Get(key) on big memtable (Binary Search) =====\n" << endl;
    test_get_big(db_name, db, memtable_data, SST_data, false);
    cout << "\nTest passed\n" << endl;
    cout << "\n===== Test Scan(key1, key2) on big memtable (Btree) =====\n" << endl;
    test_scan_big(db_name, db, memtable_data, SST_data, true);
    cout << "\nTest passed\n" << endl;
    cout << "\n===== Test Scan(key1, key2) on big memtable (Binary Search) =====\n" << endl;
    test_scan_big(db_name, db, memtable_data, SST_data, false);
    cout << "\nTest passed\n" << endl;

    db.closeDB();
    cout << "Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\nAll Tests Passed!\n" << endl;

    delete memtable_data;
    delete SST_data;
    return 0;
}