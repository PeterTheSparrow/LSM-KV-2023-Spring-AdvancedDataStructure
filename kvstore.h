#pragma once

#include "kvstore_api.h"
#include "Memtable.h"
#include "SSTable.h"

#define MAX_MEMTABLE_SIZE 2 * 1024 * 1024

#define MAX_LEVEL 5
#define LEVEL_CHANGE 2
#define MAX_LEVEL_SIZE UINT_MAX

class SSTableCache
{
public:
    Header *header;
    BloomFilter *bloomFilter;
    IndexArea *indexArea;
    // 文件名其实是路径！
    std::string fileRoutine;
    uint64_t timeStamp = 0;


    SSTableCache();
    void setAllData(uint64_t minKey, uint64_t maxKey, uint64_t numberOfPairs, uint64_t timeStamp, std::string fileName, uint64_t currentTime);
    ~SSTableCache();
    void readFileToFormCache(std::string routine);
};

class KVStore : public KVStoreAPI
{
    // You can add your implementation here
private:
    MemTable * memTable0 = nullptr;
    uint64_t currentTimestamp = 0;
    std::string dataStoreDir;
    uint64_t currentLevel = 0;
    std::vector<std::string> levelDir;

    // TODO cache的实现，可以直接在这里放一个vector，里面存放cache
    std::vector<std::vector<SSTableCache*>> theCache;

    // TAG 适应不同的缓存策略：缓存里面存什么——其实这个在于查找，我存可以都存，测试不同的缓存策略的时候只需要修改查找的算法就可以了。

public:
    KVStore(const std::string &dir);

    /* INFO 系统在正常关闭时（可以实现在析构函数里面），应将 MemTable 中的所有数据以 SSTable 形式写回（类似于 MemTable 满了时的操作）。*/
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;
    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

private:
    // 将Memtable转化到磁盘中
    void convertMemTableIntoMemory();
    // 将Memtable转化到磁盘中，但是不往缓存中添加内容
    void convertMemTableIntoMemoryWithoutCache();

    // 根据不同的缓存策略，实现不同的在磁盘里搜索的代码
    // 不缓存
    bool findInDisk1(std::string & answer, uint64_t key);
    // 缓存index，使用二分查找
    bool findInDisk2(std::string & answer, uint64_t key);
    // 缓存index和bloom filter
    bool findInDisk3(std::string & answer, uint64_t key);

    // 遍历检测所有层是否需要归并
    void checkCompaction();
    // 归并某个特定的层
    void compactSingleLevel(int levelNum);

    // // 把文件转换为SSTable
    // SSTable * convertFileToSSTable(std::string routine);
};
