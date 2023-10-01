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

int main(int argc, char **argv) {
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
    memtable.put(11, 10);
    memtable.put(15, 50);
    memtable.put(12, 20);
    memtable.put(14, 40);
    memtable.put(13, 30);
    memtable.put(18, 80);
    memtable.put(-1, 80);

    // Testing SST search
    cout << "\nTesting get() from SST..." << endl;
    value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    return 0;
}