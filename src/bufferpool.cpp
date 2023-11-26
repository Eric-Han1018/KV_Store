#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include "LSMTree.h"
#include <map>
#include <list>
#include <fstream>
#include <ctime>
#include <cassert>
#include <limits>
#include <string>
#include <sstream>
#include "bufferpool.h"
#include "util.h"

using namespace std;

// TODO: BONUS - shrink or grow bufferpool
void Bufferpool::change_maximal_size(size_t new_size){
    if (new_size < current_size) {
        // TODO: evict
        #ifdef DEBUG
            cout << "change_maximal_size" << endl;
        #endif
    }
    maximal_size = new_size;
}

void Bufferpool::insert_to_buffer(const string& p_id, bool leaf_page, char* data) {
    #ifdef DEBUG
        cout << "----------- INSERT TO BUFFER -------------- " << p_id << endl;
    #endif
    size_t index = murmur_hash(p_id) % hash_directory.size();

    // TODO: update if exists

    // Allocate memory for data
    char* tmp = data;

    // TODO: further determine when to evict and how many to evict?
    if(current_size > floor(maximal_size * 0.8)) {
        #ifdef DEBUG
            cout << "-------------- BEFORE EVICTION, SIZE: " << current_size << endl;
        #endif
        evict_clock(floor(current_size * 0.3));
        #ifdef DEBUG
            cout << "-------------- AFTER EVICTION, SIZE: " << current_size << endl;
        #endif
    }

    hash_directory[index].emplace_back(Frame(p_id, leaf_page, tmp));
    current_size++;
}

bool Bufferpool::get_from_buffer(const string& p_id, char*& data) {
    // TODO: Check if evict or extend is needed
    size_t index = murmur_hash(p_id) % hash_directory.size();
    if (hash_directory[index].empty()){
        #ifdef DEBUG
            cout << "----------- GET FROM BUFFER (EMPTY) -------------- " << p_id << endl;
        #endif
        return false;
    }
    for (Frame& frame : hash_directory[index]) {
        if (frame.p_id == p_id) {
            data = frame.data;
            frame.clock_bit = true;
            #ifdef DEBUG
                cout << "----------- GET FROM BUFFER (FOUND) -------------- " << p_id << endl;
            #endif
            return true;
        }
    }
    #ifdef DEBUG
        cout << "----------- GET FROM BUFFER (NOT FOUND) -------------- " << p_id << endl;
    #endif
    return false;
}

void Bufferpool::evict_clock(int num_pages) {
    while (num_pages > 0 && current_size > 0) {
        for (auto frame = hash_directory[clock_hand].begin(); frame != hash_directory[clock_hand].end(); frame++) {
            if (frame->clock_bit) {
                frame->clock_bit = false;
            } else {
                // cout << "evict page: " << frame->p_id << endl;
                delete (BTreeNode*)(frame->data);
                frame = hash_directory[clock_hand].erase(frame);
                current_size--;
                num_pages--;
            }
        }
        clock_hand = (clock_hand + 1) % hash_directory.size();
    }
}

void Bufferpool::print() {
    #ifdef DEBUG
        cout << "----------- Print Bufferpool -------------- " << endl;
    #endif
    for (int i = 0; i < (int)hash_directory.size(); i++) {
        if (hash_directory[i].empty()){
            continue;
        }
        for (Frame frame : hash_directory[i]) {
            #ifdef DEBUG
                cout << "pid: " << frame.p_id << endl;
            #endif
            if (frame.leaf_page) {
                BTreeLeafNode *leafNode = (BTreeLeafNode*) frame.data;
                #ifdef DEBUG
                    cout << "leafNode: " << leafNode->data[0].first << endl;
                    cout << "leafNode data: " << &leafNode->data << endl;
                #endif

            } else {
                BTreeNode *nonLeafNode = (BTreeNode*) frame.data;
                #ifdef DEBUG
                    cout << "nonLeafNode keys: " << nonLeafNode->keys[0] << endl;
                    cout << "nonLeafNode data: " << &nonLeafNode->keys[0] << endl;
                #endif
            }
        }
    }
    #ifdef DEBUG
        cout << "----------- Print Bufferpool END-------------- " << endl;
    #endif
}