#include <iostream>
#include "rbtree.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
using namespace std;

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

void test_get()
{   
    RBTree memtable(2); // Example with memtable_size 6
    cout << "--- test case 1: Test get() from memtable ---" << endl;
    // random insert
    memtable.put(1, 10);
    memtable.put(5, 50);
    int64_t key = 5;
    int64_t value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
        assert(value == 50);
    } else {
        cout << "Not found Key: " << key << endl;
        assert(false);
    }

    cout << "\n--- test case 2: Test get() from SST ---" << endl;
    memtable.put(2, 80);
    value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
        assert(value == 50);
    } else {
        cout << "Not found Key: " << key << endl;
        assert(false);
    }
    cout << "\n--- test case 3: Test get() from unexisted key ---" << endl;
    key = -668;
    value = memtable.get(-668);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
        assert(false);
    } else {
        cout << "Not found Key: " << key << endl;
    }
}

void test_put()
{
    RBTree memtable(6); // Example with memtable_size 6
    cout << "--- test case 1:  Test put() without exceeding tree capacity ---" << endl;
    // random insert
    memtable.put(1, 10);
    memtable.put(5, 50);
    memtable.put(2, 20);
    memtable.put(4, 40);
    memtable.put(3, 30);
    memtable.put(8, 80);
    cout << "\ntree-graph for key:" << endl;
    inorderKey(memtable.root);
    cout << "\ntree-graph for color - 0 black, 1 red:" << endl;
    inorderColor(memtable.root);
    cout << "\n--- test case 2: Test put() with exceeding tree capacity ---" << endl;
    memtable.put(-1, -10);
}

void test_scan(){
    RBTree memtable(6); // Example with memtable_size 6
    cout << "--- test case 1: Test scan() from memtable ---" << endl;
    // random insert
    memtable.put(1, 10);
    memtable.put(5, 50);
    memtable.put(2, 20);
    memtable.put(4, 40);
    memtable.put(3, 30);
    memtable.put(8, 80);
    int64_t key1 = 2;
    int64_t key2 = 4;
    vector<pair<int64_t, int64_t>> values = memtable.scan(key1, key2);
    vector<pair<int64_t, int64_t>> ans = {{2, 20}, {3, 30}, {4, 40}};
    for (const auto& pair : values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values.begin(), values.end()));
    assert(equal(values.begin(), values.end(), ans.begin()));

    cout << "\n--- test case 2: Test scan() from SST ---" << endl;
    memtable.put(-1, -10);
    values = memtable.scan(key1, key2);
    for (const auto& pair : values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values.begin(), values.end()));
    assert(equal(values.begin(), values.end(), ans.begin()));

    cout << "\n--- test case 3: Test scan() from both memtable and SST ---" << endl;
    memtable.put(7, 90);
    key1 = 2;
    key2 = 8;
    values = memtable.scan(key1, key2);
    for (const auto& pair : values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
    assert(is_sorted(values.begin(), values.end()));
    ans = {{2, 20}, {3, 30}, {4, 40}, {5, 50}, {7, 90}, {8, 80}};
    assert(equal(values.begin(), values.end(), ans.begin()));
}

int main(int argc, char **argv) {
    cout << "\n===== Test Put(key, value) =====\n" << endl;
    test_put();
    cout << "\n===== Test Get(key) =====\n" << endl;
    test_get();
    cout << "\n===== Test Scan(Key1, Key2) =====\n" << endl;
    test_scan();
    cout << "\nAll Tests Passed!\n" << endl;
    return 0;
}