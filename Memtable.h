# include "SkipList.h"
# include <string>
# include <iostream>
# include <vector>
# include <utility> // to use std::pair

class MemTable{
private:
    SkipList* skiplist0 = nullptr;
    uint64_t currentSize = 0;
public:
    MemTable();
    ~MemTable();

    uint64_t getSize();
    // 当生成 SSTable 时，需要获取 MemTable 中的最大 key 和最小 key；注意，是所有的，无论有没有被删除，因为你往下merge的时候，都需要。
    uint64_t getMinKey();
    uint64_t getMaxKey();

    // NOTE 当爆炸的时候，需要往下将数据传入SSTable；这件事情留在KVSTORE里面去做，每次调用的时候判断体积的大小
    void put(uint64_t key, const std::string &s);
    std::string get(uint64_t key);
    bool del(uint64_t key);

    // 获得所有存储的键值对
    std::vector<std::pair<uint64_t, std::string>> getAllKVPairs();

    void clearMemTable();
};

