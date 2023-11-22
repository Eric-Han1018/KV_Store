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

// Delete all SSTs in data/ folder
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
    cout << "\n--- test case 2: Test put() with same key, update the value ---" << endl;
    db.put(1, 100);
    const int64_t* value = db.get(1, true);
    assert(*value == 100);
    delete value;
    cout << "\n--- test case 3: Test put() with exceeding tree capacity ---" << endl;
    db.put(-1, -10);

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
    int keys[constants::MEMTABLE_SIZE];

    for (int i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        keys[i] = (int64_t)rand();
        db.put(keys[i], 6);
    }
    db.put(-1, 6);
    int n = 0;
    // Find all existing keys
    for (int key : keys) {
        if(n == 1000){
            break;
        }
        const int64_t* value = db.get(key, true);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
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
    int keys[constants::MEMTABLE_SIZE];

    for (int i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        keys[i] = (int64_t)rand();
        db.put(keys[i], 6);
    }
    db.put(-1, 6);
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

void test_bufferpool(){
    Database db(constants::MEMTABLE_SIZE);
    int keys[constants::MEMTABLE_SIZE];

    for (int i = 0; i < constants::MEMTABLE_SIZE; ++i) {
        keys[i] = (int64_t)rand();
        db.put(keys[i], 6);
    }
    db.put(-1, 6);
    int n = 0;
    // Find all existing keys
    for (int key : keys) {
        if(n == 1000){
            break;
        }
        const int64_t* value = db.get(key, true);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
        }
        n++;
    }
    for (int key : keys) {
        if(n == 500){
            break;
        }
        const int64_t* value = db.get(key, true);
        if (value != nullptr){
            cout << "Found: " << key << "->" << *value << endl;
        }
        n--;
    }
    db.bufferpool->print();
}

int main(int argc, char **argv) {
    string db_name = "GaussDB";
    cout << "\n===== Test Put(key, value) =====\n" << endl;
    test_put(db_name);
    cout << "\nTest Put(Key, Value) passed; Now deleting all SSTs...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\n===== Test Get(key) =====\n" << endl;
    test_get(db_name);
    cout << "\nTest Get(key) passed; Now deleting all SSTs...\n" << endl;
    deleteSSTs(constants::DATA_FOLDER + db_name);

    // test with different DB:
    db_name = "GaussssDB";
    cout << "\n===== Test Scan(Key1, Key2) =====\n" << endl;
    test_scan(db_name);
    cout << "\nTest Scan(Key1, Key2) passed; Now deleting all SSTs...\n" << endl;
    cout << "\n===== Test BufferPool =====\n" << endl;
    test_bufferpool(db_name);
    deleteSSTs(constants::DATA_FOLDER + db_name);

    // test with different DB:
    db_name = "GaussssDD";
    cout << "\n===== Test BufferPool =====\n" << endl;
    test_bufferpool_scan_get_binary(db_name);
    deleteSSTs(constants::DATA_FOLDER + db_name);
    cout << "\nAll Tests Passed!\n" << endl;
    return 0;
}