#include <iostream>
#include "rbtree.h"
#include <fstream>
#include <cassert>
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

int main(int argc, char **argv) {
    RBTree memtable(6); // Example with memtable_size 6

    // random insert
    memtable.put(1, 10);
    memtable.put(5, 50);
    memtable.put(2, 20);
    memtable.put(4, 40);
    memtable.put(3, 30);
    memtable.put(8, 80);

    double key = 3;
    double value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    cout << "tree-graph for key:" << endl;
    inorderKey(memtable.root);
    cout << "tree-graph for color - 0 black, 1 red:" << endl;
    inorderColor(memtable.root);

    string file_name = memtable.writeToSST();

    // Testing purpose: read a file
    cout << "Reading from " << file_name << " to test SST write..." << endl;
    vector<pair<int64_t, int64_t>> test(6);
    ifstream in(file_name, ios_base::binary);
    assert(in.read((char*)&test[0], 6*sizeof(pair<int64_t, int64_t>)));
    for (pair<int64_t, int64_t> i : test) {
        cout << i.first << ", " << i.second << endl;
    }
    in.close();

    return 0;
}