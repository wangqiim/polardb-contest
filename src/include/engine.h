#include <atomic>
#include <unordered_map>
#include <map>
#include <mutex>
#include <vector>

#include "user.h"
#include "log.h"


// id int64, user_id char(128), name char(128), salary int64
// pk : id 			    //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引

enum Phase{Hybrid=0, WriteOnly, ReadOnly};

using primary_key = std::unordered_map<int64_t, User>;
using unique_key  = std::unordered_map<UserIdWrapper, int64_t>;
using normal_key  = std::multimap<int64_t, int64_t>;

const int ShardNum = 50; // 对应客户端线程数量
const int WALNum = 50;  // 在lockfree情况下，必须ShardNum = WALNum
const int SSDNum = 50;  // 在lockfree情况下，必须ShardNum = WALNum
const int AEPNum = 0;  // 在lockfree情况下，必须ShardNum = WALNum

const int WritePerClient = 1000000; 
const int ClientNum = 50;

class Engine {
  public:
    Engine(const char* aep_dir, const char* disk_dir);
    ~Engine();

    int Init();

    int Append(const void *datas);

    size_t Read(void *ctx, int32_t select_column,
      int32_t where_column, const void *column_key, 
      size_t column_key_len, void *res);
    
  private:
    int replay_index(const std::vector<std::string> disk_path, const std::vector<std::string> pmem_path);
    int must_set_tid();
    // 为前n个map预留空间，避免插入过程中的rehash
    int pre_reserve_map(int n, size_t count);

    //read only mod
    size_t readOnly_read(void *ctx, int32_t select_column,
      int32_t where_column, const void *column_key, 
      size_t column_key_len, void *res);

  private:
    std::atomic<int> next_tid_;
    std::mutex mtx_;
    std::vector<std::string> disk_file_paths_;
    std::vector<std::string> pmem_file_paths_;
    const std::string aep_dir_;
    const std::string dir_;
    std::vector<Writer *> disk_logs_;
    std::vector<PmemWriter *> pmem_logs_;

    primary_key idx_id_list_[ShardNum];

    unique_key idx_user_id_list_[ShardNum];

    normal_key idx_salary_list_[ShardNum];
    
    // debug log
    std::chrono::_V2::system_clock::time_point start_;
    Phase phase_;
};