#include <stdint.h>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include "plate.h"
#include "interface.h"
#include "test_util.h"
#include "spdlog/spdlog.h"

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

    char res[3 * 128];
    size_t read_cnt = 0;
    int start = writeNumPerThread * thread_itr; // different thread write different key
    int end = writeNumPerThread * (thread_itr + 1);
    // spdlog::info("[Thread: {}], start = {}, end = {}", thread_itr, start, end);
    for (int i = start; i < end; i++) {
      user.id = i;
      snprintf(user.user_id, sizeof(user.user_id), "%d", i);
      user.salary = i;
      engine_write(ctx, &user, sizeof(user));

      read_cnt = engine_read(ctx, Id, Userid, &user.user_id, 128, res);
      EXPECT_EQ(1, read_cnt);
      EXPECT_EQ(0, memcmp(res, (char *)&(user.id), 8));

      read_cnt = engine_read(ctx, Userid, Id, &user.id, 8, res);
      EXPECT_EQ(0, memcmp(res, user.user_id, 128));
      EXPECT_EQ(1, read_cnt);
      
      read_cnt = engine_read(ctx, Id, Salary, &user.salary, 8, res);
      EXPECT_EQ(0, memcmp(res, (char *)&(user.id), 8));
      EXPECT_EQ(1, read_cnt);
    }
}

TEST(InterfaceConcurrentTest, BasicConcurrent) {
  EXPECT_EQ(0, rmtree(disk_dir));
  void* ctx = engine_init(nullptr, nullptr, 0, "/mnt/aep/", disk_dir);

  int threadNum = 30;
  int writeNumPerThread = 10000;
  LaunchParallelTest(threadNum, ReadAfterWriteHelper, ctx, writeNumPerThread);

  engine_deinit(ctx);
  EXPECT_EQ(0, rmtree(disk_dir));
}