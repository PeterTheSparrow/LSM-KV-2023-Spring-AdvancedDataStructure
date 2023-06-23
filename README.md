# Project LSM-KV: KVStore using Log-structured Merge Tree

## Introduction1 (before implementation)
This is the course project of Advanced Data Structure in SJTU 2023 Spring.

The handout files include two main parts:

- The `KVStoreAPI` class in `kvstore_api.h` that specifies the interface of KVStore.
- Test files including correctness test (`correctness.cc`) and persistence test (`persistence.cc`).

Explanation of each handout file:

```text
.
├── Makefile  // Makefile if you use GNU Make
├── README.md // This readme file
├── correctness.cc // Correctness test, you should not modify this file
├── data      // Data directory used in our test
├── kvstore.cc     // your implementation
├── kvstore.h      // your implementation
├── kvstore_api.h  // KVStoreAPI, you should not modify this file
├── persistence.cc // Persistence test, you should not modify this file
├── utils.h         // Provides some cross-platform file/directory interface
├── MurmurHash3.h  // Provides murmur3 hash function
└── test.h         // Base class for testing, you should not modify this file
```


First have a look at the `kvstore_api.h` file to check functions you need to implement. Then modify the `kvstore.cc` and `kvstore.h` files and feel free to add new class files.

We will use all files with `.cc`, `.cpp`, `.cxx` suffixes to build correctness and persistence tests. Thus, you can use any IDE to finish this project as long as you ensure that all C++ source files are submitted.

For the test files, of course you could modify it to debug your programs. But remember to change it back when you are testing.

Good luck :)

## Introduction2 (after implementation)

```text
.
├── Makefile       // Makefile if you use GNU Make
├── README.md      // This readme file
├── correctness.cc // Correctness test, you should not modify this file
├── data           // Data directory used in our test
├── kvstore.cc     // your implementation
├── kvstore.h      // your implementation
├── kvstore_api.h  // KVStoreAPI, you should not modify this file
├── persistence.cc // Persistence test, you should not modify this file
├── utils.h        // Provides some cross-platform file/directory interface
├── MurmurHash3.h  // Provides murmur3 hash function
├── test.h         // Base class for testing, you should not modify this file
├── SkipList.h     // SkipList 
├── SkipList.cpp   // SkipList
├── Memtable.h     // Memtable
├── Memtable.cpp   // Memtable
├── SSTable.h      // SSTable
├── SSTable.cpp    // SSTable
├── BloomFilter.h  // BloomFilter

```

## Build

use make to build the project (please refer to the Makefile for details)

```bash
# make = make correctness + make persistence
make
# make the correctness test
make correctness
# make the persistence test
make persistence
```

## Run

```bash
# run correctness test
./correctness
# run persistence test
./persistence
```

## Design of the project

The compaction of the SSTable is the most interesting and important part of the project.
(refer to my code for detail!)

## Debug

Debug is a huge problem in the project.
Firstly i use the debug mode in clion to debug, it's really convenient, however 
it still fails when i run the test in the terminal.
So i asked TA, and i was informed that debug mode might change the memory layout, so it's not recommended.
(which means, if your code works in debug mode, it doesn't mean it will work in release mode, it might still be buggy.)

so i choose to write all the data into a file, and then read the file to check the correctness.
(actually i can use gdb as well, but i am not familiar with it, so i choose to write the data into a file.)

## Memory leak

memory leak is a big problem in the project, so we need to use valgrind to check the memory leak.
i did the check in Ubuntu22.04.
(memory leak might be a severe problem in your skip-list, be careful with that.)

```bash
# also in makefile
make valgrind_correctness
```
