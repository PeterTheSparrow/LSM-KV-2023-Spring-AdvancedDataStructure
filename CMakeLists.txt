cmake_minimum_required(VERSION 3.25)
project(LSM_clion)

set(CMAKE_CXX_STANDARD 17)

#add_executable(LSM_clion MyOwnTest.cpp SkipList.cpp SkipList.h)

#include_directories(LSM_clion)

add_executable(
        MurMurHash3.h
        BloomFilter.h
        correctness.cc
        kvstore.cpp kvstore.h kvstore_api.h
        Memtable.cpp Memtable.h
        SkipList.h SkipList.cpp
        SSTable.cpp SSTable.h test.h utils.h)