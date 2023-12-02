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

// Insert the page into bufferpool, if the bufferpool is full, call evict_clock to evict cold pages
void Bufferpool::insert_to_buffer(const string& p_id, bool leaf_page, char* data) {
    size_t index = murmur_hash(p_id) % hash_directory.size();
    char* tmp = data;
    if (current_size > floor(maximal_size * 0.8)) {
        evict_clock(floor(current_size * 0.3));
    }
    hash_directory[index].emplace_back(Frame(p_id, leaf_page, tmp));
    current_size++;
}

// Get from bufferpool, and change the clock bit when needed
bool Bufferpool::get_from_buffer(const string& p_id, char*& data) {
    size_t index = murmur_hash(p_id) % hash_directory.size();
    if (hash_directory[index].empty()){
        return false;
    }
    for (Frame& frame : hash_directory[index]) {
        if (frame.p_id == p_id) {
            data = frame.data;
            frame.clock_bit = true;
            return true;
        }
    }
    return false;
}

// Evict num_pages from the bufferpool
void Bufferpool::evict_clock(int num_pages) {
    while (num_pages > 0 && current_size > 0) {
        for (auto frame = hash_directory[clock_hand].begin(); frame != hash_directory[clock_hand].end(); frame++) {
            if (frame->clock_bit) {
                frame->clock_bit = false;
            } else {
                delete (BTreeNonLeafNode*)(frame->data);
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
                BTreeNonLeafNode *nonLeafNode = (BTreeNonLeafNode*) frame.data;
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