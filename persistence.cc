#include <iostream>
#include <cstdint>
#include <string>
#include <cassert>
#include <fstream>

#include "test.h"
#include "utils.h"

/**
 * @brief 持久性测试
 *
 * @details
 * 1. void prepare:
 *      prepare函数用于准备数据。
 *      它首先清理存储（store），然后测试多个键值对的插入和获取操作。
 *      接下来，它测试插入操作后的获取操作，并进行一些键的删除操作。
 *      之后，它根据不同的模式准备数据。最后，它报告测试结果，并写入大量数据以将之前的数据从内存中清除。
 * 2. test
 *      函数用于测试数据。它根据不同的模式获取数据，并进行断言判断。
 * */
class PersistenceTest : public Test {
private:
	const uint64_t TEST_MAX = 1024 * 32;
//    const uint64_t TEST_MAX = 1024 * 8;
	void prepare(uint64_t max)
	{
		uint64_t i;

		// Clean up
		store.reset();

		// Test multiple key-value pairs
		for (i = 0; i < max; ++i) {
			store.put(i, std::string(i+1, 's'));
			EXPECT(std::string(i+1, 's'), store.get(i));
		}
		phase();

		// Test after all insertions
		for (i = 0; i < max; ++i)
			EXPECT(std::string(i+1, 's'), store.get(i));
		phase();

		// Test deletions
		for (i = 0; i < max; i+=2)
			EXPECT(true, store.del(i));

		// Prepare data for Test Mode
		for (i = 0; i < max; ++i) {
			switch (i & 3) {
			case 0:
				EXPECT(not_found, store.get(i));
				store.put(i, std::string(i+1, 't'));
				break;
			case 1:
				EXPECT(std::string(i+1, 's'), store.get(i));
				store.put(i, std::string(i+1, 't'));
				break;
			case 2:
				EXPECT(not_found, store.get(i));
				break;
			case 3:
				EXPECT(std::string(i+1, 's'), store.get(i));
				break;
			default:
				assert(0);
			}
		}

		phase();

		report();

        std::cout << "you have passed three stages" << std::endl;

		/**
		 * Write 10MB data to drain previous data out of memory.
		 */
		for (i = 0; i <= 10240; ++i)
        {
            store.put(max + i, std::string(1024, 'x'));
        }

		std::cout << "Data is ready, please press ctrl-c/ctrl-d to"
			" terminate this program!" << std::endl;
		std::cout.flush();

		while (true) {
			volatile int dummy;
			for (i = 0; i <= 1024; ++i) {
				// The loop slows down the program
				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.del(max + i);

				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.put(max + i, std::string(1024, '.'));

				for (i = 0; i <= 1000; ++i)
					dummy = i;

				store.put(max + i, std::string(512, 'x'));
			}
		}
	}

	void test(uint64_t max)
	{
		uint64_t i;

		for (i = 0; i < max; ++i) {
			switch (i & 3) {
			case 0:
				EXPECT(std::string(i+1, 't'), store.get(i));
				break;
			case 1:
				EXPECT(std::string(i+1, 't'), store.get(i));
				break;
			case 2:
				EXPECT(not_found, store.get(i));
				break;
			case 3:
				EXPECT(std::string(i+1, 's'), store.get(i));
				break;
			default:
				assert(0);
			}
		}

//        // for debug
//        // 如果存在搜索出的答案和实际情况不一致的时候，输出到txt文件
//        std::ofstream fout("persistence_error.txt");
//
//        std::string answer = "";
//        for (i = 0; i < max; ++i) {
//            if(i & 3 == 0)
//            {
//                answer = store.get(i);
//                if(answer != std::string(i+1, 't'))
//                {
//                    fout << "------> i = " << i << std::endl;
//                    fout << "error answer = " << answer << std::endl;
//                    fout << "std::string(i+1, 't') = " << std::string(i+1, 't') << std::endl;
//                }
//            }
//            else if(i & 3 == 1)
//            {
//                answer = store.get(i);
//                if(answer != std::string(i+1, 't'))
//                {
//                    fout << "------> i = " << i << std::endl;
//                    fout << "error answer = " << answer << std::endl;
//                    fout << "std::string(i+1, 't') = " << std::string(i+1, 't') << std::endl;
//                }
//            }
//            else if(i & 3 == 2)
//            {
//                answer = store.get(i);
//                if(answer != "")
//                {
//                    fout << "------> i = " << i << std::endl;
//                    fout << "error answer = " << answer << std::endl;
//                    fout << "not_found = " << not_found << std::endl;
//                }
//            }
//            else if(i & 3 == 3)
//            {
//                answer = store.get(i);
//                if(answer != std::string(i+1, 's'))
//                {
//                    fout << "------> i = " << i << std::endl;
//                    fout << "error answer = " << answer << std::endl;
//                    fout << "std::string(i+1, 's') = " << std::string(i+1, 's') << std::endl;
//                }
//            }
//        }
//
//        fout.close();
//
//        // for debug
//        // 输出此时所有缓存的信息，到文件里
//        store.WriteAllCacheInfo(142857);

		phase();

		report();
	}

public:
	PersistenceTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		bool testmode = (args && *static_cast<bool *>(args));

		std::cout << "KVStore Persistence Test" << std::endl;

		if (testmode) {
			std::cout << "<<Test Mode>>" << std::endl;
			test(TEST_MAX);
		} else {
			std::cout << "<<Preparation Mode>>" << std::endl;
			prepare(TEST_MAX);
		}
	}
};

void usage(const char *prog, const char *verb, const char *mode)
{
	std::cout << "Usage: " << prog  << " [-t] [-v]" << std::endl;
	std::cout << "  -t: test mode for persistence test,"
		" if -t is not given, the program only prepares data for test."
		" [currently " << mode << "]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << verb << "]" << std::endl;
	std::cout << std::endl;
	std::cout << " NOTE: A normal usage is as follows:" << std::endl;
	std::cout << "    1. invoke `" << prog << "`;" << std::endl;
	std::cout << "    2. terminate (kill) the program when data is ready;";
	std::cout << std::endl;
	std::cout << "    3. invoke `" << prog << " -t ` to test." << std::endl;
	std::cout << std::endl;
	std::cout.flush();
}


int main(int argc, char *argv[])
{
	bool verbose = false;
	bool testmode = false;

	if (argc == 2) {
		verbose = std::string(argv[1]) == "-v";
		testmode = std::string(argv[1]) == "-t";
	} else if (argc == 3) {
		verbose = std::string(argv[1]) == "-v" ||
			std::string(argv[2]) == "-v";
		testmode = std::string(argv[1]) == "-t" ||
			std::string(argv[2]) == "-t";
	} else if (argc > 3) {
		std::cerr << "Too many arguments." << std::endl;
		usage(argv[0], "OFF", "Preparation Mode");
		exit(-1);
	}
	usage(argv[0], verbose ? "ON" : "OFF",
	      testmode ? "Test Mode" : "Preparation Mode");

	PersistenceTest test("./data", verbose);

	test.start_test(static_cast<void *>(&testmode));

	return 0;
}
