#include <unordered_map>
#include <map>
#include <atomic>
#include "user.h"
#include "log.h"


// id int64, user_id char(128), name char(128), salary int64
// pk : id 			    //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引

using primary_key = std::unordered_map<int64_t, User>;
using unique_key  = std::unordered_map<UserIdWrapper, int64_t>;
using normal_key  = std::multimap<int64_t, int64_t>;

const int ShardNum = 50;
const int WALNum = 50;

class Engine {
  public:
    Engine(const char* disk_dir);
    ~Engine();

    int Init();

    int Append(const void *datas);

    size_t Read(void *ctx, int32_t select_column,
      int32_t where_column, const void *column_key, 
      size_t column_key_len, void *res);
    
  private:
    int replay_index(const std::vector<std::string> paths);
    int must_set_tid();

    std::atomic<int> next_tid_;

    std::vector<std::string> file_paths_;
    const std::string dir_;
    std::vector<Writer *> log_;

    primary_key idx_id_list_[ShardNum];

    unique_key idx_user_id_list_[ShardNum];

    normal_key idx_salary_list_[ShardNum];
};