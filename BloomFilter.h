#pragma once
#include "MurMurHash3.h"

/* 大小为 10240 字节的布隆过滤器 */

/*
  MurmurHash3_x64_128 哈希算法：
具体来说，这段代码通过对输入的 key 进行分块，每个块长度为 16 字节，
然后对每个块分别进行哈希，最后将得到的哈希值组合起来得到最终的哈希值。
这个哈希值是一个 128 位的无符号整数，使用 4 个 unsigned int 来表示。
 */

class BloomFilter
{
private:
    uint32_t *hashTable;

public:
    bool *checkBits;

public:
    BloomFilter()
    {
        checkBits = new bool[10240];
        for(int i = 0; i < 10240; i++)
        {
            checkBits[i] = false;
        }
        hashTable = new uint32_t[4];
        for(int i = 0; i < 4; i++)
        {
            hashTable[i] = 0;
        }
    }
    ~BloomFilter()
    {
        delete [] checkBits;
        delete [] hashTable;
    }
    /**
     * @brief 将 key 加入到布隆过滤器中
     *
     * @param key
     */
    void addIntoFilter(uint64_t key)
    {
        MurmurHash3_x64_128(&key, sizeof(key), 1, hashTable);
        for(int i = 0; i < 4; i++)
        {
            checkBits[hashTable[i] % 10240] = true;
        }
    }

    /**
     * @brief 判断 key 是否在布隆过滤器中
     *
     * @param key
     * @return true
     * @return false
     */
    bool searchInFilter(uint64_t key)
    {
        MurmurHash3_x64_128(&key, sizeof(key), 1, hashTable);
        for(int i = 0; i < 4; i++)
        {
            if(checkBits[hashTable[i] % 10240] == false)
                return false;
        }
        return true;
    }
};