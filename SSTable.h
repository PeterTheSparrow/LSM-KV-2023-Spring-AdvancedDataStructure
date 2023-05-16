#pragma once

#include "BloomFilter.h"
#include <string>
#include <vector> // 数据区存放string用

/**
 * 32字节的header，包括：
 *  (1）SSTable的时间戳
 * （2）键值对的数量
 * （3）键的最小值和最大值
 */
struct Header
{
    uint64_t timeStamp;
    uint64_t keyValueNum;
    uint64_t minKey;
    uint64_t maxKey;

    Header()
    {
        timeStamp = 0;
        keyValueNum = 0;
        minKey = 0;
        maxKey = 0;
    }

    void setAllDataInHeader(uint64_t theTimeStamp, uint64_t theKeyValueNum, uint64_t theMinKey, uint64_t theMaxKey)
    {
        timeStamp = theTimeStamp;
        keyValueNum = theKeyValueNum;
        minKey = theMinKey;
        maxKey = theMaxKey;
    }
};

struct IndexData
{
    uint64_t key;
    uint32_t offset;
    IndexData(uint64_t key, uint32_t offset)
    {
        this->key = key;
        this->offset = offset;
    }
};

struct IndexArea
{
    std::vector<IndexData> indexDataList;
};

/**
 * @brief SSTable类
 * 结构：
 * （1）header
 * （2）bloom filter
 * （3）index area: key(i)+offset(i)
 * （4）data area: value
 *
 */
class SSTable
{
private:
    std::string fileName;
    Header *header = nullptr;
    BloomFilter *filter = nullptr; // 10240字节
    IndexArea *indexArea = nullptr;
    char *dataArea = nullptr;

    int dataSize = 0;


public:
    SSTable();
    ~SSTable();

    bool findInSSTable(std::string &answer, uint64_t key);

    void convertFileToSSTable(std::string routine);

    void writeIntoDisk();
};