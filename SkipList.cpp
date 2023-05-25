//
// Created by zqjpeter on 2023/2/24.
//

#include "SkipList.h"


/*
 * scale: the total amount of elements in the list
 */
SkipList::SkipList() {
    srand((unsigned)time(NULL));;//set the random seed so that the number we get verifies
    this->probabilityCoin = 0.367879441;

    headGuards = new SkipNode[maxHeight + 1];//the height is from 1 to n
    for(int i = maxHeight; i > 1; i --)
    {
        headGuards[i].underNode = & headGuards[i-1];
        headGuards[i].key = 142857;
    }
    headGuards[1].key = 142857;
    tailGuards = new SkipNode[maxHeight + 1];

    //initialization of successor-pointer
    for(int i = 0; i <= maxHeight; i++)
    {
        headGuards[i].successorNode = & tailGuards[i];
        headGuards[i].isGuard = true;
        tailGuards[i].successorNode = nullptr;
        tailGuards[i].isGuard = true;
        tailGuards[i].key = 142857;
    }

    rightestNode = new SkipNode*[maxHeight + 1];
}

/*
 * return the height of the tower to be built
 * */
int SkipList::coinFlipper() {
    /* generate a random number from 0 to 1, if no more than proba,
     * than grow
     * */
    int heightOfTower = 1;//min height is 1
    double randomNum = 0;

//    std::default_random_engine generator(time(0));
    while(heightOfTower < maxHeight)
    {
        //flip the coin

        randomNum = rand() % 10000 / double(10000);
//        std::uniform_real_distribution<double> distribution(0.0, 1.0);
//        randomNum = distribution(generator);
        if(randomNum > probabilityCoin)
            break;
        heightOfTower += 1;
    }

    return heightOfTower;
}

/*
 * 私有函数，不对外界开放
 * finished && tested
 * */
SkipNode *SkipList::searchNodePrivate(uint64_t keyToSearch) {
    //search from the current height
    if(currentHeight == 0)//the tree is empty
        return nullptr;

    int index = currentHeight;

    SkipNode * searchingNode = & headGuards[currentHeight];


    while(index > 0)
    {
        /* loop invariant: keyToSearch < searchingNode->successor->key */
        while(searchingNode->successorNode->isGuard != true && searchingNode->successorNode->key < keyToSearch)
        {
            searchingNode = searchingNode->successorNode;
        }
        //如果下一个是哨兵，直接往下一层
        if(searchingNode->successorNode->isGuard)
        {
            if(index != 1)
            {
                searchingNode = searchingNode->underNode;
            }
            else
            {
                return nullptr;
            }
        }
            //如果下一个值超过了，下一层——如果已经最底层了，返回空指针
        else if(keyToSearch < searchingNode->successorNode->key)
        {
            if(index == 1)
            {
                return nullptr;
            }
            else
            {
                searchingNode = searchingNode->underNode;
            }
        }
        else if(keyToSearch == searchingNode->successorNode->key)
        {
            searchingNode = searchingNode->successorNode;
            while(searchingNode->underNode->underNode != nullptr)
            {
                searchingNode = searchingNode->underNode;
            }

            return searchingNode;
        }
        index --;
    }
}

/*
 * public的搜索函数，对外界开放
 * */
SkipNode* SkipList::searchNode(uint64_t keyToSearch) {
//    // for debug
//    std::cout << "we begin to search in the skip list" << std::endl;
    SkipNode * answer = this->searchNodePrivate(keyToSearch);

    return answer;
}

SkipList::~SkipList() {
    SkipNode* currentNode = nullptr;
    SkipNode* nextNode = nullptr;
    for (int i = 0; i <= this->maxHeight; i++)
    {
        currentNode = headGuards[i].successorNode;
        while (!currentNode->isGuard)
        {
            nextNode = currentNode->successorNode;
            delete currentNode;
            currentNode = nextNode;
        }
    }

    delete[] headGuards;
    delete[] tailGuards;

    delete[] rightestNode;

    rightestNode = nullptr;

    headGuards = nullptr;
    tailGuards = nullptr;

    // 清理vector
    for(auto & i : this->towerButtonVec)
    {
        delete i;
    }
    // use swap to clear vector
    std::vector<SkipNode*>().swap(this->towerButtonVec);
}

/*
 * finished && not tested
 * */
std::vector<std::pair<uint64_t, std::string>> SkipList::getAllKVPairs()
{
    std::vector<std::pair<uint64_t, std::string>> allKVPairs;

    SkipNode * movingNode = & headGuards[1];

    while(movingNode->successorNode != nullptr && !movingNode->successorNode->isGuard)
    {
        movingNode = movingNode->successorNode;
        allKVPairs.push_back(std::make_pair(movingNode->key, movingNode->value));
    }

    return allKVPairs;
}

uint64_t SkipList::getMinKey()
{
    SkipNode * movingNode = & headGuards[1];

    // TAG 注意到，我们只有在跳表满了的时候才会调用这个函数，因此不会发生空指针的情况（虽然本身这个函数的逻辑上面是有问题的）
    // 第一个哨兵下一个节点就是最小值
    return movingNode->successorNode->key;
}

uint64_t SkipList::getMaxKey()
{
    SkipNode * movingNode = & headGuards[1];

    // TAG 注意到，我们只有在跳表满了的时候才会调用这个函数，因此不会发生直接返回头哨兵的情况（虽然本身这个函数的逻辑上面是有问题的）
    while(movingNode->successorNode != nullptr && movingNode->successorNode->isGuard == false)
    {
        movingNode = movingNode->successorNode;
    }

    return movingNode->key;
}

void SkipList::insertNode(uint64_t keyToSearch, std::string valueToReplace) {
    /*
     * rightestNode[i](i in range(1,n+1)):
     * [renew during the process of insertion]
     * contains a pointer to the rightmost SkipNode of
     * level i or higher that is to the left of the location of the insertion/deletion
     * */
//    delete rightestNode;//clear the previous data
    if (rightestNode != nullptr)
    {
        delete[] rightestNode;
        rightestNode = nullptr;
    }

    rightestNode = new SkipNode*[this->maxHeight + 1];

    for(int i = 0; i <= maxHeight; i++)
    {
        rightestNode[i] = nullptr;
    }

    int index = currentHeight;
    SkipNode * movingNode = nullptr;

    /*
    * 如何定义rightestNode
    * 有节点的层：
    * （1）从第一个有节点的层开始，往下遍历
    * 没有节点的层：
    * （1）直接定为最左边的哨兵
    * */
    for(int i = maxHeight; i >= 1; i--)
    {
        rightestNode[i] = & headGuards[i];
    }

    //initialize moving SkipNode
    if(index == 0)//no nodes in the list
    {
        movingNode = & headGuards[1];
        rightestNode[1] = & headGuards[1];
    }
    else
    {
        movingNode = & headGuards[currentHeight];
    }

    //开始查找。直到降到第一层为止（必须降到第一层，因为只有第一层存放了value
    while(index > 1)
    {
        //在每一行里面，下一个节点不是哨兵，下一个节点的值比目标key小，一直在当前行中往下
        while(!movingNode->successorNode->isGuard && movingNode->successorNode->key < keyToSearch)
        {
            movingNode = movingNode->successorNode;
        }
        rightestNode[index] = movingNode;
        movingNode = movingNode->underNode;
        index --;
    }
    //reach the last level of the tower(level 1)
    while(!movingNode->successorNode->isGuard && movingNode->successorNode->key < keyToSearch)
    {
        movingNode = movingNode->successorNode;
    }
    rightestNode[1] = movingNode;

    //哨兵里面value是垃圾值，加一个条件排除哨兵
    if (!movingNode->successorNode->isGuard && movingNode->successorNode->key == keyToSearch)
    {
        // // replace the old value with the new one
        // 每次查询的时候都降到塔底，value值只存在塔的第一层就可以了
        movingNode->successorNode->value = valueToReplace;
    }
    else//add a new tower in the skip-list
    {
        int towerLevel = this->coinFlipper();

        SkipNode ** newTower = new SkipNode * [towerLevel + 1];
        //initial of tower（把塔的上下层连接起来）
        for(int j = 0; j <= towerLevel; j++)
        {
            newTower[j] = new SkipNode;
            newTower[j]->key = keyToSearch;
            if(j != 0)
                newTower[j]->underNode = newTower[j-1]; //initialize the underNode pointer
            else
                newTower[j]->underNode = nullptr;
        }

        // 为了解决内存泄漏的问题
        this->towerButtonVec.push_back(newTower[0]);

//        //for debug
//        this->debugFunc();

        if (towerLevel > currentHeight)
        {
            for(int i = towerLevel; i > currentHeight; i--)
            {
                newTower[i]->successorNode = headGuards[i].successorNode;
                headGuards[i].successorNode = newTower[i];
            }
        }

//        // debug
//        this->debugFunc();

        int minLevel = currentHeight < towerLevel ? currentHeight:towerLevel;

        //here is current height!
        for(int i = minLevel; i >= 1; i--)
        {
            newTower[i]->successorNode = rightestNode[i]->successorNode;
            rightestNode[i]->successorNode = newTower[i];
        }
        newTower[1]->value = valueToReplace;//put the value at the bottom of the tower

        //renew the height of the list
        if(towerLevel > currentHeight)
            currentHeight = towerLevel;


        delete[] newTower;
    }
}

void SkipList::debugFunc() {
    std::cout << "---head-guards---" << std::endl;
    for(int i = maxHeight; i >= 1; i--)
    {
        std::cout << "guard[" << i << "]'s successor: " << (void*) headGuards[i].successorNode << std::endl;
    }
    std::cout << "---right---" << std::endl;
    for(int i = maxHeight; i >= 1; i--)
    {
        std::cout << "right[" << i << "]'s successor: " << (void*) headGuards[i].successorNode << std::endl;
    }
    std::cout << "---tail address---" << std::endl;
    for(int i = maxHeight; i >= 1;i --)
    {
        std::cout << "address of tail[" << i << "]:" << & tailGuards[i] << std::endl;
    }
}

/* 返回你搜索的次数 */
int SkipList::searchNum() {
    return this->numberOfSearch;
}

void SkipList::printTheList() {
    for(int i = maxHeight; i >= 1; i --)
    {
        std::cout << "level: " << i << std::endl;
        SkipNode * walkMan = & headGuards[i];
        while(walkMan->successorNode != nullptr)
        {
            std::cout << "[key: " << walkMan->key << "][curr_add:" << (void *)walkMan <<"] [next:" << (void*) walkMan->successorNode << "] [under: "<< (void * ) walkMan->underNode << "]" << std::endl;
            walkMan = walkMan->successorNode;
        }
        std::cout << "[key: " << walkMan->key << "][curr_add:" << (void *)walkMan <<"] [next:" << (void*) walkMan->successorNode << "](end guard)" << std::endl;
    }
}

int SkipList::getNumOfKVPairs()
{
    int count = 0;
    SkipNode * walkMan = & headGuards[1];
    walkMan = walkMan->successorNode;
    while(walkMan != nullptr && !walkMan->isGuard)
    {
        count ++;
        walkMan = walkMan->successorNode;
    }
    return count;
}