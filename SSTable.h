#pragma once

#include "BloomFilter.h"
#include <string>
#include <vector> // 数据区存放string用

# define MAX_SSTABLE_SIZE 2 * 1024 * 1024

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


/**
 * offset: 某一个字符串到data area区域起始位置的偏移量
 * */
struct IndexData
{
    uint64_t key;
    uint32_t offset;
    IndexData(uint64_t key, uint32_t offset) {
        this->key = key;
        this->offset = offset;
    }
    ~IndexData() {}
};

struct IndexArea
{
    std::vector<IndexData> indexDataList;
    IndexArea() {}
    ~IndexArea()
    {
        indexDataList.clear();
        // swap
        std::vector<IndexData>().swap(indexDataList);
    }
};

/**
 * @brief SSTable类
 * 结构：
 * （1）header
 * （2）bloom filter
 * （3）index area: key(i)+offset(i)
 * （4）data area: value
 */

struct KVNode
{
    uint64_t key;
    std::string value;
    KVNode(uint64_t key, std::string value)
    {
        this->key = key;
        this->value = value;
    }
};

class SSTableCache
{
public:
    Header *header;
    BloomFilter *bloomFilter;
    IndexArea *indexArea;
    std::string fileRoutine;
    uint64_t timeStampIndex = 0; // 由于可能存在时间戳相同的文件，因此需要用index将其区分


    SSTableCache();
    void setAllData(uint64_t minKey, uint64_t maxKey, uint64_t numberOfPairs, uint64_t timeStamp, std::string fileName, uint64_t currentTime);
    ~SSTableCache();
    void readFileToFormCache(std::string routine, std::string fileName);

    static bool CompareSSTableCache(const SSTableCache* a, const SSTableCache* b) {
        return a->header->timeStamp > b->header->timeStamp;
    }
};



class SSTable
{
public:
    std::string fileName;
    Header *header = nullptr;
    BloomFilter *filter = nullptr; // 10240字节
    IndexArea *indexArea = nullptr;
    char *dataArea = nullptr;

    int dataSize = 0;

    // 用于合并SSTable的时候用（因为如果是char来存储数据的话，没有办法merge）
    std::vector<KVNode> KVPairs;


public:
    SSTable();
    ~SSTable();

    bool findInSSTable(std::string &answer, uint64_t key);

    // 说明：本查找函数现已废弃，因为现在已经将所有的信息从char * dataArea转移到了std::vector<KVNode> KVPairs中！
    bool findInSSTableAbandoned(std::string &answer, uint64_t key);

    // 读取所有char中的信息，生成键值对的vector，加速查找
    void formKVVector();

    void convertFileToSSTable(std::string routine);

    // 其实需要关注的是两张表的时间，但我也可以传参数的时候就把timeStamp大的传在前面
    // 其实也可以声明在KVStore类里面，但是其实声明在这里主要是为了代码风格，和SSTable相关的函数都写在一起
    static SSTable* mergeTwoTables(SSTable *&table1, SSTable *&table2);
//    static SSTable mergeTwoTables(SSTable &table1, SSTable &table2);

    // 实现把一个巨大的SSTable切开的函数
    std::vector<SSTableCache *> splitAndSave(std::string routine);

    // 切出来一个单独的SSTable，返回这个SSTable对应的SSTableCache
    SSTableCache * cutOutOneSSTable(int fileTag, std::string routine, int & currentSize);


    // 我传入的list中的时间戳一定是从大到小排布的！
    // 这个是留给外面的类调用的
    static void mergeTables(std::vector<SSTable*> &tableList);
//    static void mergeTables(std::vector<SSTable> &tableList);

private:
    static void mergeRecursively(std::vector<SSTable*> &tableList);
//    static void mergeRecursively(std::vector<SSTable> &tableList);
};

