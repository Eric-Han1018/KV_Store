#include <iostream>
#include "database.h"
#include <fstream>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include <time.h>
#include <chrono>
using namespace std;

// Taken from https://www.gormanalysis.com/blog/reading-and-writing-csv-files-with-cpp/
void write_csv(std::string filename, std::vector<std::pair<std::string, std::vector<int>>> dataset){
    // Make a CSV file with one or more columns of integer values
    // Each column of data is represented by the pair <column name, column data>
    //   as std::pair<std::string, std::vector<int>>
    // The dataset is represented as a vector of these columns
    // Note that all columns should be the same size
    
    // Create an output filestream object
    std::ofstream myFile(filename);
    
    // Send column names to the stream
    for(int j = 0; j < (int)dataset.size(); ++j)
    {
        myFile << dataset.at(j).first;
        if(j != (int)dataset.size() - 1) myFile << ","; // No comma at end of line
    }
    myFile << "\n";
    
    // Send data to the stream
    for(int i = 0; i < (int)dataset.at(0).second.size(); ++i)
    {
        for(int j = 0; j < (int)dataset.size(); ++j)
        {
            myFile << dataset.at(j).second.at(i);
            if(j != (int)dataset.size() - 1) myFile << ","; // No comma at end of line
        }
        myFile << "\n";
    }
    
    // Close the file
    myFile.close();
}

int main(int argc, char **argv) {
    srand (1);
    int64_t megabyte = 1 << 20;
    vector<int32_t> inputDataSize = {32, 64, 128};
    vector<bool> useBTrees = {true, false};

    cerr << "Running benchmarking..." << endl;
    vector<int32_t> put_ops_Btree;
    vector<int32_t> get_ops_Btree;
    vector<int32_t> scan_ops_Btree;
    vector<int32_t> put_ops_Binary;
    vector<int32_t> get_ops_Binary;
    vector<int32_t> scan_ops_Binary;

    for(bool useBtree : useBTrees) {
        if (useBtree) cerr << "============== Test Btree ==============" << endl;
        else cerr << "============== Test Binary Search ==============" << endl;

        for (int64_t inputSize : inputDataSize) {
            cerr << "-------------- Testing Input Size: " << inputSize << "MB..." << endl;
            inputSize *= megabyte;
            inputSize /= constants::PAIR_SIZE;

            cerr << "Creating Database with size: " << constants::MEMTABLE_SIZE << " memtable entries...";
            Database db(constants::MEMTABLE_SIZE);
            db.openDB("Benchmark");
            cerr << "Done" << endl;

            cerr << "Populating " << inputSize << " entries into DB...";
            for (int64_t i = 0; i < inputSize; ++i) {
                // FIXME: Confirm if use rand(), and if use % inputSize
                db.put((int64_t)rand() % inputSize, (int64_t)rand() % inputSize);
            }
            cerr << "Done" << endl;

            cerr << "Testing get()...";
            int64_t count = 0;
            auto start_time = chrono::high_resolution_clock::now();
            // FIXME: Confirm if use this way to determin 10sec
            while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
                // FIXME: Confirm if get by random
                delete db.get((int64_t)rand() % inputSize, useBtree);
                ++count;
            }
            if (useBtree) get_ops_Btree.emplace_back(count);
            else get_ops_Binary.emplace_back(count);
            cerr << "Done" << endl;

            cerr << "Testing scan()...";
            count = 0;
            start_time = chrono::high_resolution_clock::now();
            while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
                int64_t key1 = rand() % inputSize;
                delete db.scan(key1, key1 + constants::KEYS_PER_NODE, useBtree);
                ++count;
            }
            if (useBtree) scan_ops_Btree.emplace_back(count);
            else scan_ops_Binary.emplace_back(count);
            cerr << "Done" << endl;

            cerr << "Testing put()...";
            count = 0;
            start_time = chrono::high_resolution_clock::now();
            while ((chrono::high_resolution_clock::now() - start_time) / chrono ::milliseconds(1) <= 10000) {
                // FIXME: Confirm if use rand(), and if use % inputSize
                db.put((int64_t)rand() % inputSize, (int64_t)rand() % inputSize);
                ++count;
            }
            if (useBtree) put_ops_Btree.emplace_back(count);
            else put_ops_Binary.emplace_back(count);
            cerr << "Done" << endl;

            db.closeDB();
            count = 0;
            for (auto& path: fs::directory_iterator(constants::DATA_FOLDER + "Benchmark/sst")) {
                fs::remove_all(path);
                ++count;
            }
            cerr << "Deleted " << count << "SSTs" <<endl;
            count = 0;
            for (auto& path: fs::directory_iterator(constants::DATA_FOLDER + "Benchmark/filter")) {
                fs::remove_all(path);
                ++count;
            }
            cerr << "Deleted " << count << "Bloom Filters" <<endl;

        }
    }


    // Wrap into a vector
    std::vector<std::pair<std::string, std::vector<int>>> vals = {{"InputDataSize", inputDataSize}, {"Put_Btree", put_ops_Btree}, {"Get_Btree", get_ops_Btree}, {"Scan_Btree", scan_ops_Btree}, 
                                                                                                    {"Put_Binary", put_ops_Binary}, {"Get_Binary", get_ops_Binary}, {"Scan_Binary", scan_ops_Binary}};
    
    // Write the vector to CSV
    cerr << "Writing results to benchmark.csv...";
    write_csv("benchmark_step2.csv", vals);
    cerr << "Done" <<endl;
    
    return 0;
}