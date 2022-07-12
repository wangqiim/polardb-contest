#include <stdint.h>
#include <thread>
#include <vector>
#include <gtest/gtest.h>
#include "test_util.h"
#include "util.h"
#include "log.h"

std::string pmem_file_name() {
  std::string path = aep_dir;
  path += "/DATA_00000000";
  return path;
}


TEST(LogTest, DiskBasic) {
  EXPECT_EQ(0, rmtree(disk_dir));
  spdlog::error("implement DiskBasic");
  EXPECT_EQ(0, rmtree(disk_dir));
}

class ReadCheckHelper {
  public:
    void check_data(const TestUser *user) {
      TestUser correct_user;
      correct_user.id = i_;
      sprintf(correct_user.user_id, "%ld", correct_user.id);
      sprintf(correct_user.name, "name%ld", correct_user.id);
      correct_user.salary = correct_user.id;
      EXPECT_EQ(true, correct_user == *user);
      i_++;
    }
  private:
    int i_ = 0;
    TestUser user_;
};

void check_record(const char *record, void *context) {
  const TestUser *user = reinterpret_cast<const TestUser *>(record);
  ReadCheckHelper *helper = reinterpret_cast<ReadCheckHelper *>(context);
  helper->check_data(user);
}

TEST(LogTest, PmemBasic) {
  EXPECT_EQ(0, rmtree(aep_dir));
  if (!Util::FileExists(aep_dir)) {
    EXPECT_EQ(0, mkdir(aep_dir, 0755));
  }

  std::string path = pmem_file_name();
  PmemWriter *writer = new PmemWriter(path, PoolSize);
  TestUser user;
  int n = 100; // must: n * size(user) < PoolSize
  for (int i = 0; i < n; i++) {
    user.id = i;
    sprintf(user.user_id, "%ld", user.id);
    sprintf(user.name, "name%ld", user.id);
    user.salary = user.id;
    writer->Append((const void*)&user, RecordSize);
  }
  delete writer;

  PmemReader *reader = new PmemReader(path, PoolSize);
  ReadCheckHelper check_helper;
  reader->Scan(check_record, &check_helper);
  delete reader;

  EXPECT_EQ(0, rmtree(aep_dir));
}