# CSC443-FinalProject

This is the CSC443/CSC2525H Database System Technology Project, which collaborated with **Weizhou Wang** (1004421262), **Dechen Han** (1006095636) and **Shi Tang** (1005930619).

In this project, we implemented an LSM-Tree based key-value store from scratch with the following simple API:
- Open(“database name”): opens database and prepares it to run
- Put(key, value): stores a key associated with a value
- Value = Get(key): retrieves a value associated with a given key
- KV-pairs= Scan(Key1, Key2): retrieves all KV-pairs in a key range in key order (key1 < key2)
- Close(): closes database

Project Status: all the required features and bonus features (such as Handling Sequential Flooding, Dostoevsky, Min-heap, Blocked Bloom Filters, and Monkey) have been implemented and thoroughly tested. Additionally, we have successfully run benchmarks with 1GB of data. Please see `CSC443_CSC2525H Project Report.pdf` for a detailed explanation of these design and implementations.

---

```include/```: Stores all .h files <br/>
```src/```: Stores all .cpp files <br/>
```bin/```: Stores executables after compilation <br/>
```data/```: Stores SSTs and Bloom Filters (**please make sure this folder exists before run any executables**) <br/>
