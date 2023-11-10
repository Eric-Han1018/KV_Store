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
    MurmurHash3_x86_32(key.c_str(), length, seed, &hash);
    return static_cast<size_t>(hash);
}

// TODO: BONUS - shrink or grow bufferpool
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

    // TODO: further determine when to evict and how many to evict?
    if(current_size > floor(maximal_size * 0.8)) {
        cout << "-------------- BEFORE EVICTION, SIZE: " << current_size << endl;
        evict_clock(floor(current_size * 0.3));
        cout << "-------------- AFTER EVICTION, SIZE: " << current_size << endl;
    }

    Frame frame(p_id, leaf_page, tmp);
    hash_directory[index].push_back(frame);
    current_size++;
}

bool Bufferpool::get_from_buffer(const string& p_id, char*& data) {
    // TODO: Check if evict or extend is needed
    size_t index = murmur_hash(p_id) % hash_directory.size();
    if (hash_directory[index].empty()){
        cout << "----------- GET FROM BUFFER (EMPTY) -------------- " << p_id << endl;
        return false;
    }
    for (Frame frame : hash_directory[index]) {
        if (frame.p_id == p_id) {
            data = frame.data;
            frame.clock_bit = true;
            cout << "----------- GET FROM BUFFER (FOUND) -------------- " << p_id << endl;
            return true;
        }
    }
    cout << "----------- GET FROM BUFFER (NOT FOUND) -------------- " << p_id << endl;
    return false;
}

void Bufferpool::evict_clock(int num_pages) {
    while (num_pages > 0 && current_size > 0) {
        for (auto frame = hash_directory[clock_hand].begin(); frame != hash_directory[clock_hand].end(); frame++) {
            if (frame->clock_bit) {
                frame->clock_bit = false;
            } else {
                // cout << "evict page: " << frame->p_id << endl;
                frame = hash_directory[clock_hand].erase(frame);
                current_size--;
                num_pages--;
            }
        }
        clock_hand = (clock_hand + 1) % hash_directory.size();
    }
}

void Bufferpool::print() {
    cout << "----------- Print Bufferpool -------------- " << endl;
    for (int i = 0; i < 10; i++) {
        if (hash_directory[i].empty()){
            continue;
        }
        for (Frame frame : hash_directory[i]) {
            cout << "pid: " << frame.p_id << endl;
            if (frame.leaf_page) {
                BTreeLeafNode *leafNode = (BTreeLeafNode*) frame.data;
                cout << "leafNode: " << leafNode->data[0].first << endl;
                cout << "leafNode data: " << &leafNode->data << endl;

            } else {
                BTreeNode *nonLeafNode = (BTreeNode*) frame.data;
                cout << "nonLeafNode keys: " << nonLeafNode->keys[0] << endl;
                cout << "nonLeafNode data: " << &nonLeafNode->keys[0] << endl;
            }
        }
    }
    cout << "----------- Print Bufferpool END-------------- " << endl;
}