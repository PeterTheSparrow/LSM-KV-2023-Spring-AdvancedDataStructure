cmake_minimum_required(VERSION 3.25)
project(LSM_clion)

set(CMAKE_CXX_STANDARD 17)

add_executable(LSM_clion
        correctness.cc
        kvstore.h kvstore.cpp kvstore_api.h
        SSTable.cpp SSTable.h
        MemTable.cpp MemTable.h
        SkipList.cpp SkipList.h
        MurmurHash3.h
        BloomFilter.h)
