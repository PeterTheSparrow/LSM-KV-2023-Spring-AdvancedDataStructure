//
// Created by zqjpeter on 2023/5/25.
//
#include <iostream>
#include <string>
#include <vector>
#include "kvstore.h"
#include <chrono>
#include <fstream>

/**
 * @brief 测试吞吐量和延迟
 * 吞吐量：每秒能够处理的请求数量
 * 延迟：从请求发出到收到响应的时间
 *
 * 需要测量的操作
 * 1. 插入
 * 2. 查询
 * 3. 删除
 *
 * 测试k-v键值对中，当v的大小为256bytes, 512bytes, 1024bytes, 2048bytes, 4096bytes, 8192bytes
 * 的吞吐量
 * */
void testThroughPutAndLatency()
{
    // 把结果写入文件
    std::ofstream out;

    out.open("test_result-findindisk3.txt", std::ios::out);

    int numOfKVPairs = 20000;
    int size[4] = {512, 1024, 2048, 4096};

    for(int i = 0; i < 4; i++)
    {
        std::string value(size[i], 'a');
        KVStore kvStore("./data" + std::to_string(i));
        std::vector<uint64_t> keys;

        ////////////////////下面是测试插入///////////////////////////////////////

        // 记录总花费的时间，单位是us，微秒
        double totalDuration_put = 0;

        // 测量插入的时间
        auto start1 = std::chrono::steady_clock::now();
        for (int j = 0; j < numOfKVPairs; j++) {
            kvStore.put(j, value);

        }
        auto end1 = std::chrono::steady_clock::now();
        totalDuration_put = std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();

        // 计算吞吐量
        double throughput_put = numOfKVPairs / (totalDuration_put / 1000000.0);

        // 计算延迟
        double latency_put = totalDuration_put / numOfKVPairs;

        // 输出结果
        out << "size: " << size[i] << " throughput_put: " << throughput_put << " latency_put: " << latency_put << std::endl;

        ////////////////////下面是测试查询///////////////////////////////////////

        // 记录总花费的时间，单位是us，微秒
        double totalDuration_get = 0;

        // 测量查询的时间
        auto start2 = std::chrono::steady_clock::now();

        for (int j = 0; j < numOfKVPairs; j++) {
            std::string value = kvStore.get(j);
        }

        auto end2 = std::chrono::steady_clock::now();
        totalDuration_get = std::chrono::duration_cast<std::chrono::microseconds>(end2 - start2).count();

        // 计算吞吐量
        double throughput_get = numOfKVPairs / (totalDuration_get / 1000000.0);

        // 计算延迟
        double latency_get = totalDuration_get / numOfKVPairs;

        // 输出结果
        out << "size: " << size[i] << " throughput_get: " << throughput_get << " latency_get: " << latency_get << std::endl;


        ////////////////////下面是测试删除///////////////////////////////////////

        // 记录总花费的时间，单位是us，微秒

        double totalDuration_del = 0;

        // 测量删除的时间
        auto start3 = std::chrono::steady_clock::now();

        for (int j = 0; j < numOfKVPairs; j++) {
            kvStore.del(j);
        }

        auto end3 = std::chrono::steady_clock::now();

        totalDuration_del = std::chrono::duration_cast<std::chrono::microseconds>(end3 - start3).count();

        // 计算吞吐量
        double throughput_del = numOfKVPairs / (totalDuration_del / 1000000.0);

        // 计算延迟
        double latency_del = totalDuration_del / numOfKVPairs;

        // 输出结果
        out << "size: " << size[i] << " throughput_del: " << throughput_del << " latency_del: " << latency_del << std::endl;

    }

    out.close();
}


/**
 * @brief 测试不同层级文件数目对put时延的影响
 * */
void testMaxFileNum()
{
    // 插入10000个键值对，每个键值对的value大小为2048bytes，测试put的平均时延
    int numOfKVPairs = 100000;
    int size = 2048;
    std::string value(size, 'a');
    KVStore kvStore("./data-testMaxFileNum");

    // 记录总花费的时间，单位是us，微秒
    double totalDuration_put = 0;

    // 测量插入的时间
    auto start1 = std::chrono::steady_clock::now();
    for (int j = 0; j < numOfKVPairs; j++) {
        kvStore.put(j, value);
    }

    auto end1 = std::chrono::steady_clock::now();

    totalDuration_put = std::chrono::duration_cast<std::chrono::microseconds>(end1 - start1).count();

    // 计算delay
    double latency_put = totalDuration_put / numOfKVPairs;

    // 输出结果
    std::cout << "latency_put: " << latency_put << std::endl;
}


int main()
{
//    testThroughPutAndLatency();
    testMaxFileNum();
    return 0;
}