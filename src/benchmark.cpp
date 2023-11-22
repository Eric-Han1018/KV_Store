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

// Modified from https://www.gormanalysis.com/blog/reading-and-writing-csv-files-with-cpp/
void write_csv(std::string filename, std::vector<std::pair<std::string, std::vector<double>>> dataset){
    // Make a CSV file with one or more columns of integer values
    // Each column of data is represented by the pair <column name, column data>
    //   as std::pair<std::string, std::vector<double>>
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

// Return MB/sec
double calculate_throughput(const chrono::_V2::system_clock::time_point& start_time, const chrono::_V2::system_clock::time_point& end_time, const int& op_counts) {
    // in microsec
    int64_t delta = (end_time - start_time) / chrono::microseconds(1);
    // ops/microsec
    double op = (double)op_counts / delta;
    // ops/sec
    return op * 1000000;
}

int main(int argc, char **argv) {
    assert(argc == 2);
    srand (1);
    const int64_t num_ops = 500;
    const int64_t megabyte = 1 << 20;
    vector<double> inputDataSize = {1, 2};//, 4, 8, 16, 32, 64, 128, 256, 512, 1024};
    vector<bool> useBTrees = {true, false};

    cerr << "Running benchmarking..." << endl;
    vector<double> put_tps_Btree;
    vector<double> get_tps_Btree;
    vector<double> scan_tps_Btree;
    vector<double> put_tps_Binary;
    vector<double> get_tps_Binary;
    vector<double> scan_tps_Binary;

    for(bool useBtree : useBTrees) {
        if (useBtree) cerr << "============== Test Btree ==============" << endl;
        else cerr << "============== Test Binary Search ==============" << endl;

        cerr << "Creating Database with size: " << constants::MEMTABLE_SIZE << " memtable entries...";
        Database db(constants::MEMTABLE_SIZE);
        db.openDB("Benchmark");
        cerr << "Done" << endl;

        int64_t last_inputSize = 0;
        for (int64_t inputSize : inputDataSize) {
            cerr << "-------------- Testing Input Size: " << inputSize << "MB..." << endl;
            inputSize *= megabyte;
            inputSize /= constants::PAIR_SIZE;

            cerr << "Populating " << inputSize - last_inputSize << " more entries into DB...";
            auto start_time = chrono::high_resolution_clock::now();
            for (int64_t i = 0; i < inputSize - last_inputSize; ++i) {
                db.put((int64_t)rand(), (int64_t)rand());
            }
            double tps = calculate_throughput(start_time, chrono::high_resolution_clock::now(), inputSize);
            if (useBtree) put_tps_Btree.emplace_back(tps);
            else put_tps_Binary.emplace_back(tps);
            last_inputSize = inputSize;
            cerr << "Done: " << tps << "ops/sec" << endl;

            cerr << "Testing get()...";
            start_time = chrono::high_resolution_clock::now();
            for (int64_t i = 0; i < num_ops; ++i) {
                delete db.get((int64_t)rand() % inputSize, useBtree);
            }
            tps = calculate_throughput(start_time, chrono::high_resolution_clock::now(), num_ops);
            if (useBtree) get_tps_Btree.emplace_back(tps);
            else get_tps_Binary.emplace_back(tps);
            cerr << "Done: " << tps << "ops/sec" << endl;

            cerr << "Testing scan()...";
            start_time = chrono::high_resolution_clock::now();
            for (int64_t i = 0; i < num_ops; ++i) {
                int64_t key1 = rand();
                delete db.scan(key1, key1 + constants::KEYS_PER_NODE, useBtree);
            }
            tps = calculate_throughput(start_time, chrono::high_resolution_clock::now(), num_ops);
            if (useBtree) scan_tps_Btree.emplace_back(tps);
            else scan_tps_Binary.emplace_back(tps);
            cerr << "Done: " << tps << "ops/sec" << endl;
        }
        db.closeDB();
        int count = 0;
        for (auto& path: fs::directory_iterator(constants::DATA_FOLDER + "Benchmark")) {
            fs::remove_all(path);
            ++count;
        }
        cerr << "Deleted " << count << "SSTs" <<endl;
    }
    

    // Wrap into a vector
    vector<pair<string, vector<double>>> vals = {{"InputDataSize", inputDataSize}, {"Put_Btree", put_tps_Btree}, {"Get_Btree", get_tps_Btree}, {"Scan_Btree", scan_tps_Btree}, 
                                                                                                    {"Put_Binary", put_tps_Binary}, {"Get_Binary", get_tps_Binary}, {"Scan_Binary", scan_tps_Binary}};
    
    // Write the vector to CSV
    string file_name = argv[1];
    cerr << "Writing results to " << file_name << "..." << endl;;
    write_csv(file_name, vals);
    cerr << "Done" <<endl;
    
    return 0;
}