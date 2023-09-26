#include <iostream>
#include "rbtree.h"
using namespace std;

int main(int argc, char **argv) {
    // TODO: Make more throughout tests
    RBTree memtable(5); // Example with memtable_size 5

    memtable.put(1, 10);
    memtable.put(2, 20);
    memtable.put(3, 30);
    memtable.put(4, 40);
    memtable.put(5, 50);

    double key = 3;
    double value = memtable.get(key);
    if (value != -1) {
        cout << "Found {Key: " << key << ", Value: " << value << "}" << endl;
    } else {
        cout << "Not found Key: " << key << endl;
    }

    vector<pair<int, int>> sorted_KV = memtable.scan(3, 10);
    for (pair<int, int> KV: sorted_KV) {
        cout << KV.first << ", " << KV.second << endl;
    }

    return 0;
}