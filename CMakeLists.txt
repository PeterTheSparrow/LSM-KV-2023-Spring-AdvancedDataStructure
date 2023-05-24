cmake_minimum_required(VERSION 3.25)
project(LSM_clion)

set(CMAKE_CXX_STANDARD 14)

# 指定构建类型，否则会出很阴间的错误（debug）
set(CMAKE_BUILD_TYPE "Release")

add_executable(
        LSM_clion
        MurMurHash3.h
        BloomFilter.h
        correctness.cc
#        persistence.cc
        kvstore.cpp kvstore.h kvstore_api.h
        Memtable.cpp Memtable.h
        SkipList.h SkipList.cpp
        SSTable.cpp SSTable.h test.h utils.h
)