#include "plate.h"
#include <unordered_map>
#include <map>
#include <mutex>

// id int64, user_id char(128), name char(128), salary int64
// pk : id 			    //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引

using primary_key = std::unordered_map<int64_t, Location>;
using unique_key  = std::unordered_map<std::string, Location>;
using normal_key  = std::multimap<int64_t, Location>;

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
    std::mutex mtx_;

    const char* dir_;
    Plate* plate_;

    primary_key idx_id_;
    unique_key idx_user_id_;
    normal_key idx_salary_;
    
    // debug log
    int cnt1_ = 0;
    int cnt2_ = 0;
    int cnt3_ = 0;
    int cnt4_ = 0;
};