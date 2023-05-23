#include "Memtable.h"


MemTable::MemTable()
{
    // 32 字节的 header, 10240 字节的 bloom filter
    this->currentSize = 32 + 10240;
    skipList0 = new SkipList();
}

MemTable::~MemTable()
{
    delete skipList0;
}

uint64_t MemTable::getSize()
{
    return this->currentSize;
}

uint64_t MemTable::getMinKey()
{
    return this->skipList0->getMinKey();
}

uint64_t MemTable::getMaxKey()
{
    return this->skipList0->getMaxKey();
}

void MemTable::put(uint64_t key, const std::string &s)
{
    // 查找 key 是否存在
    SkipNode *node = skipList0->searchNode(key);
    int sizeChange = 0;
    if (node == nullptr)
    {
        // key + offset + value
        sizeChange = 8 + s.size() + 4;
    }
    else
    {
        sizeChange = s.size() - node->value.size();
    }
    this->currentSize += sizeChange;

    this->skipList0->insertNode(key, s);
}

std::string MemTable::get(uint64_t key)
{
    SkipNode *node0 = skipList0->searchNode(key);
    if (node0 == nullptr)
        return "";
    else if(node0->value == "~DELETED~")
        return "~DELETED~";
    else
        return node0->value;
}

std::vector<std::pair<uint64_t, std::string>> MemTable::getAllKVPairs()
{
    return this->skipList0->getAllKVPairs();
}

void MemTable::clearMemTable()
{
    if(this->skipList0 != nullptr)
    {
        delete this->skipList0;
        this->skipList0 = nullptr;
    }
    // resize the memtable
    this->currentSize = 32 + 10240;
    this->skipList0 = new SkipList();
}