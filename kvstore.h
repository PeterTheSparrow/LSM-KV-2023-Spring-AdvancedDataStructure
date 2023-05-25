#pragma once

#include "kvstore_api.h"
#include "Memtable.h"
#include "SSTable.h"


#define MAX_MEMTABLE_SIZE (2 * 1024 * 1024)

#define LEVEL_CHANGE 2

class KVStore : public KVStoreAPI
{
private:
public:
    MemTable * memTable0 = nullptr;
    uint64_t currentTimestamp = 0;
    std::string dataStoreDir;
    std::vector<std::string> levelDir;  // 存储了每一层的目录的名称（注意是子目录的名称而非路径，如'level-0'; 没有用处，只是函数需要这个参数

    // cache
    std::vector<std::vector<SSTableCache*>> theCache;

    void WriteAllCacheInfo(int whereEnter);

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
};
