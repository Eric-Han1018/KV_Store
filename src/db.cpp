#include <iostream>
#include "rbtree.h"
#include <fstream>
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

    string file_name = memtable.writeToSST();

    // Testing purpose: read a file
    cout << "Reading from " << file_name << " to test SST write..." << endl;
    vector<pair<int64_t, int64_t>> test(5);
    ifstream in(file_name, ios_base::binary);
    assert(in.read((char*)&test[0], 5*sizeof(pair<int64_t, int64_t>)));
    for (pair<int64_t, int64_t> i : test) {
        cout << i.first << ", " << i.second << endl;
    }
    in.close();

    return 0;
}