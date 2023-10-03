#include <iostream>
#include "rbtree.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
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
    double key = 5;
    double value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }
    cout << "\n--- test case 2: Test get() from SST ---" << endl;
    memtable.put(2, 80);
    value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
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
    cout << "\n--- test case 2: Test put() with exceeding tree capacity ---" << endl;
    memtable.put(-1, 80);
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
    double key1 = 2;
    double key2 = 4;
    vector<pair<int64_t, int64_t>> values = memtable.scan(key1, key2);
    for (const auto& pair : values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }

    cout << "\n--- test case 2: Test scan() from SST ---" << endl;
    memtable.put(-1, 80);
    values = memtable.scan(key1, key2);
    for (const auto& pair : values) {
        cout << "Found {Key: " << pair.first << ", Value: " << pair.second << "}" << endl;
    }
}

void unit_test1()
{
    RBTree memtable(6); // Example with memtable_size 6

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

    // Search for key in the memtable
    cout << "\nTesting get() in memtable..." << endl;
    double key = 3;
    double value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    // Write memtable to SST
    cout << "\nTesting senario of reaching tree capacity..." << endl;
    memtable.put(-1, 80);

    // Testing SST search
    cout << "\nTesting get() from SST..." << endl;
    value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }
}

int main(int argc, char **argv) {
    cout << "\n===== Test Put(key, value) =====\n" << endl;
    test_put();
    cout << "\n===== Test Get(key) =====\n" << endl;
    test_get();
    cout << "\n===== Test Scan(Key1, Key2) =====\n" << endl;
    test_scan();
    return 0;
}