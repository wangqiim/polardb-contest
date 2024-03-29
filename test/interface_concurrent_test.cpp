#include <stdint.h>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include "interface.h"
#include "test_util.h"
#include "spdlog/spdlog.h"
#include "util.h"
#include "def.h"

template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// for a thread, it should read the data it have write
void ReadAfterWriteHelper(void *ctx, int writeNumPerThread, 
                          __attribute__((unused)) uint64_t thread_itr) {
    TestUser user;
    memcpy(&user.name, "name", 5);

    char *res = new char[writeNumPerThread * 128];
    size_t read_cnt = 0;
    int64_t start = writeNumPerThread * thread_itr; // different thread write different key
    int64_t end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int64_t i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%lld", (long long)i);
      user.salary = -start; // negtive!
      engine_write(ctx, &user, sizeof(user));

      read_cnt = engine_read(ctx, Id, Userid, &user.user_id, 128, res);
      EXPECT_EQ(1, read_cnt);
      EXPECT_EQ(0, memcmp(res, (char *)&(user.id), 8));

      read_cnt = engine_read(ctx, Userid, Id, &user.id, 8, res);
      EXPECT_EQ(0, memcmp(res, user.user_id, 128));
      EXPECT_EQ(1, read_cnt);
      
      read_cnt = engine_read(ctx, Id, Salary, &user.salary, 8, res);
      EXPECT_EQ(i - start + 1, read_cnt);
      // if (i % 10000 == 0) {
      //   spdlog::info("i = {}", i);
      //   Util::print_resident_set_size();
      // }
    }
    delete res;
}

// for a thread, it only write
void WriteOnlyHelper(void *ctx, int writeNumPerThread, 
                          __attribute__((unused)) uint64_t thread_itr) {
    TestUser user;
    memcpy(&user.name, "name", 5);

    char *res = new char[writeNumPerThread * 128];
    int64_t start = writeNumPerThread * thread_itr; // different thread write different key
    int64_t end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int64_t i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%lld", (long long)i);
      user.salary = -start; // negtive!
      engine_write(ctx, &user, sizeof(user));
    }
    delete res;
}

// for a thread, it only write
void ReadOnlyHelper(void *ctx, int writeNumPerThread, 
                          __attribute__((unused)) uint64_t thread_itr) {
    TestUser user;
    memcpy(&user.name, "name", 5);

    char *res = new char[writeNumPerThread * 128];
    size_t read_cnt = 0;
    int64_t start = writeNumPerThread * thread_itr; // different thread write different key
    int64_t end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int64_t i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%lld", (long long)i);
      user.salary = -start; // negtive!
      read_cnt = engine_read(ctx, Id, Userid, &user.user_id, 128, res);
      EXPECT_EQ(1, read_cnt);
      EXPECT_EQ(0, memcmp(res, (char *)&(user.id), 8));

      read_cnt = engine_read(ctx, Userid, Id, &user.id, 8, res);
      EXPECT_EQ(0, memcmp(res, user.user_id, 128));
      EXPECT_EQ(1, read_cnt);
      
      read_cnt = engine_read(ctx, Id, Salary, &user.salary, 8, res);
      EXPECT_EQ(writeNumPerThread, read_cnt);
    }
    delete res;
}


// for a thread, it only write
void HackWriteOnlyHelper(void *ctx, int writeNumPerThread, 
                          __attribute__((unused)) uint64_t thread_itr) {
    TestUser user;
    memcpy(&user.name, "name", 5);

    char *res = new char[writeNumPerThread * 128];
    int64_t start = writeNumPerThread * thread_itr; // different thread write different key
    int64_t end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int64_t i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%lld", (long long)i);
      snprintf(user.name, sizeof(user.user_id), "%lld", (long long)i);
      user.salary = -i; // negtive!
      engine_write(ctx, &user, sizeof(user));
    }
    delete res;
}

// for a thread, it only read
void HackReadOnlyHelper(void *ctx, int writeNumPerThread, 
                          __attribute__((unused)) uint64_t thread_itr) {
    TestUser user;
    memcpy(&user.name, "name", 5);

    char *res = new char[writeNumPerThread * 128];
    size_t read_cnt = 0;
    int64_t start = writeNumPerThread * thread_itr; // different thread write different key
    int64_t end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int64_t i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%lld", (long long)i);
      snprintf(user.name, sizeof(user.name), "%lld", (long long)i);
      user.salary = -i; // negtive!
      read_cnt = engine_read(ctx, Name, Userid, &user.user_id, 128, res);
      EXPECT_EQ(1, read_cnt);
      EXPECT_EQ(0, memcmp(res, (char *)&(user.name), 128));

      read_cnt = engine_read(ctx, Userid, Id, &user.id, 8, res);
      EXPECT_EQ(0, memcmp(res, user.user_id, 128));
      EXPECT_EQ(1, read_cnt);
      
      read_cnt = engine_read(ctx, Id, Salary, &user.salary, 8, res);
      EXPECT_EQ(0, memcmp(res, &user.id, 8));
      EXPECT_EQ(1, read_cnt);
    }
    delete res;
}


// TEST(InterfaceConcurrentTest, BasicConcurrent) {
//   EXPECT_EQ(0, rmtree(disk_dir));
//   EXPECT_EQ(0, rmtree(aep_dir));
//   void* ctx = engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);

//   int threadNum = 10;
//   int writeNumPerThread = 100;
//   LaunchParallelTest(threadNum, ReadAfterWriteHelper, ctx, writeNumPerThread);

//   engine_deinit(ctx);
//   engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
//   engine_deinit(ctx);
//   engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
//   EXPECT_EQ(0, rmtree(disk_dir));
//   EXPECT_EQ(0, rmtree(aep_dir));
// }

// TEST(InterfaceConcurrentTest, WriteReadSeperateConcurrent) {
//   EXPECT_EQ(0, rmtree(disk_dir));
//   EXPECT_EQ(0, rmtree(aep_dir));
//   void* ctx = engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);

//   int threadNum = 10; // Don't change this unless you know what you're doing
//   int writeNumPerThread = 100;
//   LaunchParallelTest(threadNum, WriteOnlyHelper, ctx, writeNumPerThread);
//   LaunchParallelTest(threadNum, ReadOnlyHelper, ctx, writeNumPerThread);
//   engine_deinit(ctx);
//   engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
//   engine_deinit(ctx);
//   engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
//   engine_deinit(ctx);
//   EXPECT_EQ(0, rmtree(disk_dir));
//   EXPECT_EQ(0, rmtree(aep_dir));
// }

TEST(InterfaceConcurrentTest, HackReadConcurrent) {
  EXPECT_EQ(0, rmtree(disk_dir));
  EXPECT_EQ(0, rmtree(aep_dir));
  void* ctx = engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);

  int threadNum = ClientNum; // Don't change this unless you know what you're doing
  int writeNumPerThread = WritePerClient;
  LaunchParallelTest(threadNum, HackWriteOnlyHelper, ctx, writeNumPerThread);
  spdlog::info("performance Write phrase done!");
  LaunchParallelTest(threadNum, HackReadOnlyHelper, ctx, writeNumPerThread);
  spdlog::info("performance read phrase done!");
  engine_deinit(ctx);
  engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
  engine_deinit(ctx);
  engine_init(nullptr, nullptr, 0, aep_dir, disk_dir);
  engine_deinit(ctx);
  EXPECT_EQ(0, rmtree(disk_dir));
  EXPECT_EQ(0, rmtree(aep_dir));
}
