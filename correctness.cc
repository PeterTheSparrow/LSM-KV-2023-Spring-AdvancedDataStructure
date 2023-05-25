#include <iostream>
#include <cstdint>
#include <string>
#include <fstream>

#include "test.h"

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 64;
//     const uint64_t LARGE_TEST_MAX = 1024 * 10;

	void regular_test(uint64_t max)
	{
		uint64_t i;

		// Test a single key
		std::cout << "Test a single key" << std::endl;
		EXPECT(not_found, store.get(1));
		store.put(1, "SE");
		EXPECT("SE", store.get(1));
		EXPECT(true, store.del(1));
		EXPECT(not_found, store.get(1));
		EXPECT(false, store.del(1));
        std::cout << "Test a single key done" << std::endl;

		phase();

		// Test multiple key-value pairs
		std::cout << "Test multiple key-value pairs" << std::endl;
		for (i = 0; i < max; ++i) {
			store.put(i, std::string(i+1, 's'));
			EXPECT(std::string(i+1, 's'), store.get(i));
		}
		phase();

        // TODO for debug 所有缓存信息写到文件
        // 把theCache中每一层包含几个文件，所有文件名的信息写到theCache.txt中
        std::ofstream out("theCache-correctness-final.txt");
        out << "theCache size: " << store.theCache.size() << std::endl;

        for(int i = 0; i < store.theCache.size(); i++)
        {
            out << "level " << i << " size: " << store.theCache[i].size() << std::endl;
            for(int j = 0; j < store.theCache[i].size(); j++)
            {
                out << "level " << i << " file " << j << " name: " << store.theCache[i][j]->fileRoutine << " minKey: " << store.theCache[i][j]->header->minKey << " maxKey: " << store.theCache[i][j]->header->maxKey << std::endl;
            }
        }

		// Test after all insertions
		std::cout << "Test after all insertions" << std::endl;
		for (i = 0; i < max; ++i)
        {
//            std::cout <<" we begin " << i << std::endl;
            EXPECT(std::string(i+1, 's'), store.get(i));
        }

        // TODO for debug
        // 输出MemTable中的元素范围
        std::cout << "min in memtable: " << store.memTable0->getMinKey() << std::endl;
        std::cout << "max in memtable: " << store.memTable0->getMaxKey() << std::endl;

        // TODO for debug
        std::vector<std::pair<int, std::string>> result;
        for(int i = 0; i < max; i++)
        {
            std::string answer = store.get(i);
            if(answer != std::string(i+1, 's'))
            {
                result.push_back(std::make_pair(i, answer));
            }
        }

        // TODO for debug
        // write into file
        std::ofstream out2("result.txt");
        for(int i = 0; i < result.size(); i++)
        {
            out2 << "key: " << result[i].first << "  value: " << result[i].second << std::endl;
        }

        out2.close();

		phase();

		// // Test scan
		std::cout << "Test scan" << std::endl;
		// std::list<std::pair<uint64_t, std::string> > list_ans;
		// std::list<std::pair<uint64_t, std::string> > list_stu;

		// for (i = 0; i < max / 2; ++i) {
		// 	list_ans.emplace_back(std::make_pair(i, std::string(i+1, 's')));
		// }

		// store.scan(0, max / 2 - 1, list_stu);
		// EXPECT(list_ans.size(), list_stu.size());

		// auto ap = list_ans.begin();
		// auto sp = list_stu.begin();
		// while(ap != list_ans.end()) {
		// 	if (sp == list_stu.end()) {
		// 		EXPECT((*ap).first, -1);
		// 		EXPECT((*ap).second, not_found);
		// 		ap++;
		// 	}
		// 	else {
		// 		EXPECT((*ap).first, (*sp).first);
		// 		EXPECT((*ap).second, (*sp).second);
		// 		ap++;
		// 		sp++;
		// 	}
		// }

		phase();

		// Test deletions
		std::cout << "Test deletions" << std::endl;
        // delete all the even numbers
		for (i = 0; i < max; i+=2)
			EXPECT(true, store.del(i));

		for (i = 0; i < max; ++i)
			EXPECT((i & 1) ? std::string(i+1, 's') : not_found,
			       store.get(i));


		for (i = 1; i < max; ++i)
			EXPECT(i & 1, store.del(i));

		phase();




		report();

        store.WriteAllCacheInfo(10086);
	}

public:
	CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Correctness Test" << std::endl;

		store.reset();

		std::cout << "[Simple Test]" << std::endl;
		regular_test(SIMPLE_TEST_MAX);

		std::cout << "finished simple test" << std::endl;

		store.reset();

        std::cout << "[Large Test]" << std::endl;
        regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	test.start_test();

	return 0;
}
