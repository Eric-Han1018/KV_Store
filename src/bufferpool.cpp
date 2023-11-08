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
    size_t index = murmur_hash(p_id) % hash_directory.size();

    // TODO: update if exists

    // TODO: evict or extend is needed
    Bucket bucket(p_id, leaf_page, data);
    hash_directory[index].push_back(bucket);
    current_size++;
}

bool Bufferpool::get_from_buffer(const string& p_id, char*& data) {
    // TODO: Check if evict or extend is needed
    size_t index = murmur_hash(p_id) % hash_directory.size();
    for (Bucket bucket : hash_directory[index]) {
        if (bucket.p_id == p_id) {
            data = hash_directory[index].end()->data;
            bucket.clock_bit = true;
            return true;
        }
    }
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