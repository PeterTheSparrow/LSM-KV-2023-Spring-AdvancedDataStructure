#include "Memtable.h"


MemTable::MemTable()
{
    // 32 字节的 header, 10240 字节的 bloom filter
    this->currentSize = 32 + 10240;
    skiplist0 = new SkipList();
}

MemTable::~MemTable()
{
    delete skiplist0;
}

uint64_t MemTable::getSize()
{
    return this->currentSize;
}

uint64_t MemTable::getMinKey()
{
    return this->skiplist0->getMinKey();
}

uint64_t MemTable::getMaxKey()
{
    return this->skiplist0->getMaxKey();
}

void MemTable::put(uint64_t key, const std::string &s)
{
    // 查找 key 是否存在
    SkipNode *node = skiplist0->searchNode(key);
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

    this->skiplist0->insertNode(key, s);
}

std::string MemTable::get(uint64_t key)
{
    SkipNode *node0 = skiplist0->searchNode(key);
    if (node0 == nullptr)
        return "";
    else if(node0->value == "~DELETED~")
        return "~DELETED~";
    else
        return node0->value;
}

bool MemTable::del(uint64_t key)
{
    // 查找，若不存在，不需要删除，返回false
    SkipNode *node0 = skiplist0->searchNode(key);
    if (node0 == nullptr)
    {
        return false;
    }
    else if(node0->value == "~DELETED~")// 已经删除过了
    {
        return false;
    }
    else
    {
        // 删除
        this->put(key, "~DELETED~");
        return true;
    }
}

std::vector<std::pair<uint64_t, std::string>> MemTable::getAllKVPairs()
{
    return this->skiplist0->getAllKVPairs();
}

void MemTable::clearMemTable()
{
    if(this->skiplist0 != nullptr)
    {
        delete this->skiplist0;
        this->skiplist0 = nullptr;
    }
    // resize the memtable
    this->currentSize = 32 + 10240;
    this->skiplist0 = new SkipList();
}