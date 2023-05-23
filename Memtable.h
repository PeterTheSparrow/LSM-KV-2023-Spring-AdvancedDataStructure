# include "SkipList.h"
# include <string>
# include <iostream>
# include <vector>
# include <utility> // to use std::pair

class MemTable{
private:
    SkipList* skipList0 = nullptr;
    uint64_t currentSize = 0;
public:
    MemTable();
    ~MemTable();

    uint64_t getSize();

    // 当生成 SSTable 时，需要获取 MemTable 中的最大 key 和最小 key；
    // 注意：需要获得所有的key（无论是否被deleted）
    uint64_t getMinKey();
    uint64_t getMaxKey();

    // 插入
    void put(uint64_t key, const std::string &s);
    // 查询
    std::string get(uint64_t key);

    // 获得所有存储的键值对
    std::vector<std::pair<uint64_t, std::string>> getAllKVPairs();

    void clearMemTable();
};

