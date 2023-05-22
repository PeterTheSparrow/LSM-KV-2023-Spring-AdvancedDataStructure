#pragma once

#include "kvstore_api.h"
#include "Memtable.h"
#include "SSTable.h"


#define MAX_MEMTABLE_SIZE 2 * 1024 * 1024

#define LEVEL_CHANGE 2

//class SSTableCache
//{
//public:
//    Header *header;
//    BloomFilter *bloomFilter;
//    IndexArea *indexArea;
//    std::string fileRoutine;
//    uint64_t timeStampIndex = 0; // 由于可能存在时间戳相同的文件，因此需要用index将其区分
//
//
//    SSTableCache();
//    void setAllData(uint64_t minKey, uint64_t maxKey, uint64_t numberOfPairs, uint64_t timeStamp, std::string fileName, uint64_t currentTime);
//    ~SSTableCache();
//    void readFileToFormCache(std::string routine, std::string fileName);
//};


class KVStore : public KVStoreAPI
{
private:
    MemTable * memTable0 = nullptr;
    uint64_t currentTimestamp = 0;
    std::string dataStoreDir;
    std::vector<std::string> levelDir;  // 存储了每一层的目录的名称（注意是子目录的名称而非路径，如'level-0';

    // cache
    std::vector<std::vector<SSTableCache*>> theCache;

public:
    KVStore(const std::string &dir);

    /* 系统在正常关闭时（可以实现在析构函数里面），应将 MemTable 中的所有数据以 SSTable 形式写回（类似于 MemTable 满了时的操作）。*/
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

    // 合并SSTable
    SSTable * mergeSSTables(std::vector<SSTable *> tablesToMerge);
    /* 后面将SSTable写入硬盘的函数可以设计在SSTable类里面，我们直接对于每个SSTable * ptr调用就可以了
     * 以及SSTable的合并，本质上还是两两先开始合并，所以也可以在SSTable里面设计mergeTwoTables？
     * 噢其实这两个函数都可以写在SSTable类里面，传进来另一张表，我把它merge到我自己身上就可以了。
     *
     * 关于切分SSTable，并且将它们写入硬盘（写入硬盘的函数在SSTable类里面）
     * */

    std::vector<SSTable *> splitSSTables(SSTable * tableToSplit);
};
