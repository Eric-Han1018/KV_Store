#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "SST.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
#include "bufferpool.h"

using namespace std;

size_t Bufferpool::murmur_hash(const string& key){
    uint32_t seed = 443;
    const int length = key.length();
    uint32_t hash;
    // wolf:~$ uname -m => x86_64
    MurmurHash3_x64_128(key.c_str(), length, seed, &hash);
    return static_cast<size_t>(hash);
}

void Bufferpool::change_maximal_size(size_t new_size){
    if (new_size < current_size) {
        // TODO: evict
        cout << "change_maximal_size" << endl;
    }
    maximal_size = new_size;
}

void Bufferpool::insert_to_buffer(const string& p_id, bool leaf_page, char* data) {
    cout << "----------- INSERT TO BUFFER -------------- " << p_id << endl;
    size_t index = murmur_hash(p_id) % hash_directory.size();

    // TODO: update if exists

    // Allocate memory for data
    char* tmp = new char[constants::KEYS_PER_NODE * constants::PAIR_SIZE];
    tmp = data;

    // TODO: evict or extend is needed
    Bucket bucket(p_id, leaf_page, tmp);
    hash_directory[index].push_back(bucket);
    current_size++;
}

void Bufferpool::print() {
    cout << "----------- Print Bufferpool -------------- " << endl;
    for (int i = 0; i < 10; i++) {
        if (hash_directory[i].empty()){
            continue;
        }
        for (Bucket bucket : hash_directory[i]) {
            cout << "pid: " << bucket.p_id << endl;
            if (bucket.leaf_page) {
                BTreeLeafNode *leafNode = (BTreeLeafNode*) bucket.data;
                cout << "leafNode: " << leafNode->data[0].first << endl;
                cout << "leafNode data: " << &leafNode->data << endl;

            } else {
                BTreeNode *nonLeafNode = (BTreeNode*) bucket.data;
                cout << "nonLeafNode keys: " << nonLeafNode->keys[0] << endl;
                cout << "nonLeafNode data: " << &nonLeafNode->keys[0] << endl;

            }
        }
    }
    cout << "----------- Print Bufferpool END-------------- " << endl;
}

bool Bufferpool::get_from_buffer(const string& p_id, char*& data) {
    // TODO: Check if evict or extend is needed
    size_t index = murmur_hash(p_id) % hash_directory.size();
    if (hash_directory[index].empty()){
        cout << "----------- GET FROM BUFFER (EMPTY) -------------- " << p_id << endl;
        return false;
    }
    for (Bucket bucket : hash_directory[index]) {
        if (bucket.p_id == p_id) {
            data = bucket.data;
            bucket.clock_bit = true;
            cout << "----------- GET FROM BUFFER (FOUND) -------------- " << p_id << endl;
            return true;
        }
    }
    cout << "----------- GET FROM BUFFER (NOT FOUND) -------------- " << p_id << endl;
    return false;
}

void Bufferpool::evict_clock(int num_pages) {
    while (num_pages > 0 || num_pages > current_size) {
        for (auto bucket = hash_directory[clock_hand].begin(); bucket != hash_directory[clock_hand].end(); bucket++) {
            if (bucket->clock_bit) {
                bucket->clock_bit = false;
            } else {
                cout << "evict page: " << bucket->p_id << endl;
                hash_directory[clock_hand].erase(bucket);
                current_size--;
                num_pages--;
            }
        }
        clock_hand = (clock_hand + 1) % hash_directory.size();
    }
}