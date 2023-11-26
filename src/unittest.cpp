#include <iostream>
#include "database.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
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

void test_get(string db_name)
{   
    Database db(2); // Example with memtable_size 2
    db.openDB(db_name);

    cout << "--- test case 1: Test get() from memtable ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(1, 30); // This is an update on memtable
    db.put(5, 50);
    int64_t key = 1;
    const int64_t* value = db.get(key, true);
    if (value != nullptr) {
        cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        assert(*value == 30);
        delete value;
    } else {
        cout << "Not found Key: " << key << endl;
        assert(false);
    }

    cout << "\n--- test case 2: Test get() from SST ---" << endl;
    db.put(2, 80);
    value = db.get(key, true);
    if (value != nullptr) {
        cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        assert(*value == 30);
    } else {
        cout << "Not found Key: " << key << endl;
        assert(false);
    }
    delete value;
    cout << "\n--- test case 3: Test get() from unexisted key ---" << endl;
    key = -668;
    value = db.get(-668, true);
    if (value != nullptr) {
        cout << "Found {Key: " << key << ", Value: " << *value << "}" << endl;
        assert(false);
        delete value;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    db.closeDB();
}

void test_put(string db_name)
{
    Database db(6);
    db.openDB(db_name);

    cout << "--- test case 1:  Test put() without exceeding tree capacity ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(5, 50);
    db.put(2, 20);
    db.put(4, 40);
    db.put(3, 30);
    db.put(8, 80);
    cout << "\ntree-graph for key: (Read from left to right)" << endl;
    inorderKey(db.memtable->root);
    cout << "\ntree-graph for color - 0 black, 1 red:" << endl;
    inorderColor(db.memtable->root);
    cout << "\n--- test case 2: Test put() with exceeding tree capacity ---" << endl;
    // random insert
    db.put(4, 400);
    db.put(7, 700);
    db.put(8, 800);
    db.put(9, 900);
    db.closeDB();
}

void test_scan(string db_name){
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
    const vector<pair<int64_t, int64_t>>* values = db.scan(key1, key2, true);
    vector<pair<int64_t, int64_t>> ans = {{2, 20}, {3, 60}, {4, 40}};
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;

    cout << "\n--- test case 2: Test scan() from SST ---" << endl;
    db.put(-1, -10);
    values = db.scan(key1, key2, true);
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;

    cout << "\n--- test case 3: Test scan() from both memtable and SST ---" << endl;
    db.put(7, 90);
    key1 = 2;
    key2 = 8;
    values = db.scan(key1, key2, true);
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    ans = {{2, 20}, {3, 60}, {4, 40}, {5, 50}, {7, 90}, {8, 80}};
    assert(equal(values->begin(), values->end(), ans.begin()));

    delete values;
    db.closeDB();
}

void test_bufferpool(string db_name){
    Database db(constants::MEMTABLE_SIZE);
    db.openDB(db_name);
    int keys[2 * constants::MEMTABLE_SIZE];

    for (int i = 0; i < 2 * constants::MEMTABLE_SIZE; ++i) {
        keys[i] = (int64_t)rand();
        db.put(keys[i], 6);
    }
    int n = 0;
    // Find all existing keys
    for (int key : keys) {
        if(n == 1000){
            break;
        }
        const int64_t* value = db.get(key, true);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
        } else {
            assert(false);
        }
        delete value;
        n++;
    }
    for (int key : keys) {
        if(n == 500){
            break;
        }
        const int64_t* value = db.get(key, true);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
        } else {
            assert(false);
        }
        delete value;
        n--;
    }
    db.bufferpool->print();
    db.closeDB();
}

void test_bufferpool_scan_get_binary(string db_name){
    Database db(constants::MEMTABLE_SIZE);
    db.openDB(db_name);
    int keys[2 * constants::MEMTABLE_SIZE];

    for (int i = 0; i < 2 * constants::MEMTABLE_SIZE; ++i) {
        keys[i] = (int64_t)rand();
        db.put(keys[i], 6);
    }
    // test get with binary searching
    cout << "***********Test Get with Binary***********" << endl;
    int n = 0;
    for (int key : keys) {
        if(n == 1000){
            break;
        }
        const int64_t* value = db.get(key, false);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
            delete value;
        } else {
            assert(false);
        }
        n++;
    }
    for (int key : keys) {
        if(n == 500){
            break;
        }
        const int64_t* value = db.get(key, false);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
            delete value;
        } else {
            assert(false);
        }
        n--;
    }
    // test scan with binary
    cout << "***********Test Scan with Binary***********" << endl;
    int64_t key1 = 500000;
    int64_t key2 = 1000000;
    const vector<pair<int64_t, int64_t>>* values = db.scan(key1, key2, false);
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    delete values;

    db.bufferpool->print();
    db.closeDB();
}

// FIXME:: This is a draft
void test_sequential_flooding(string db_name) {
    Database db(constants::MEMTABLE_SIZE);
    db.openDB(db_name);
    cout << "\n--- test case 1: Test scan() from both single SST ---" << endl;
    for (int64_t i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        db.put(i, 0);
    }
    db.put(0, 1);
    const vector<pair<int64_t, int64_t>>* values = db.scan(1, 1 + constants::PAGE_SIZE * constants::SCAN_RANGE_LIMIT, true);
    delete values;

    cout << "\n--- test case 2: Test scan() across multiple SSTs ---" << endl;
    for (int64_t i = 1; i < constants::MEMTABLE_SIZE; ++i) {
        db.put(i, 1);
    }
    db.put(-1, -1);
    values = db.scan(1, 1 + constants::PAGE_SIZE * constants::SCAN_RANGE_LIMIT, true);
    delete values;

    // FIXME:: need Assertion
    // FIXME:: need 3rd test case
    db.closeDB();
}

void test_delete(string db_name)
{   
    Database db(2); // Example with memtable_size 2
    db.openDB(db_name);

    cout << "--- test case 1: Test del() from memtable ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(1, 30); // This is an update on memtable
    db.put(5, 50);
    int64_t key = 1;
    db.del(key);
    const int64_t* value = db.get(key, true);
    if (value != nullptr) {
        cout << "---NOT DELETE---" << endl;
        assert(false);
    } else {
        cout << "---DELETE---" << endl;
    }
    delete value;

    cout << "\n--- test case 2: Test del() from SST ---" << endl;
    db.put(2, 80);
    db.put(4, 80);
    const int64_t* value2 = db.get(key, true);
    if (value2 != nullptr) {
        cout << "NOT DELETE " << endl;
        assert(false);
    } else {
        cout << "---DELETE---" << endl;
    }
    delete value2;
    db.closeDB();
}

void test_delete_scan(string db_name){
    Database db(6);
    db.openDB(db_name);

    cout << "--- test case 1: Test del() with scan from memtable ---" << endl;
    // random insert
    db.put(1, 10);
    db.put(5, 50);
    db.put(2, 20);
    db.put(4, 40);
    db.put(3, 30);
    db.put(3, 60); // This is an update on memtable
    db.del(2);
    int64_t key1 = 2;
    int64_t key2 = 4;
    const vector<pair<int64_t, int64_t>>* values = db.scan(key1, key2, true);
    vector<pair<int64_t, int64_t>> ans = {{3, 60}, {4, 40}};
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;

    cout << "\n--- test case 2: Test del() with scan from SST ---" << endl;
    db.put(4, 50);
    db.put(11, -10);
    db.put(9, -10);
    values = db.scan(key1, key2, true);
    for (const auto& pair : *values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values->begin(), values->end()));
    ans = {{3, 60}, {4, 50}};
    assert(equal(values->begin(), values->end(), ans.begin()));
    delete values;
    db.closeDB();
}

int main(int argc, char **argv) {
    string db_name = "GaussDB";
    cout << "\n===== Test Put(key, value) =====\n" << endl;
    test_put(db_name);
    cout << "\nTest Put(Key, Value) passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Get(key) =====\n" << endl;
    test_get(db_name);
    cout << "\nTest Get(key) passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Delete(key) =====\n" << endl;
    test_delete(db_name);
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Delete(key) with scan =====\n" << endl;
    test_delete_scan(db_name);
    deleteSSTs(constants::DATA_FOLDER + db_name);

    // test with different DB:
    db_name = "GaussssDB";
    cout << "\n===== Test Scan(Key1, Key2) =====\n" << endl;
    test_scan(db_name);
    cout << "\nTest Scan(Key1, Key2) passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test BufferPool =====\n" << endl;
    test_bufferpool(db_name);
    cout << "\nTest BufferPool passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);

    // test with different DB:
    db_name = "GaussssDD";
    cout << "\n===== Test BufferPool =====\n" << endl;
    test_bufferpool_scan_get_binary(db_name);
    cout << "\nTest BufferPool passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Sequential Flooding =====\n" << endl;
    test_sequential_flooding(db_name);
    cout << "\nTest Sequential Flooding passed; Now deleting all SSTs & Bloom Filters...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\nAll Tests Passed!\n" << endl;
    return 0;
}