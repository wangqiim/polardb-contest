#include "plate.h"
#include <unordered_map>
#include <map>
#include <mutex>

// id int64, user_id char(128), name char(128), salary int64
// pk : id 			    //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引

class Engine {
  public:
    Engine(const char* disk_dir);
    ~Engine();

    int Wppend(const void *datas);

    size_t Read(void *ctx, int32_t select_column,
      int32_t where_column, const void *column_key, 
      size_t column_key_len, void *res);
    
  private:
    std::mutex mtx_;

    const char* dir_;
    Plate* plate_;

    std::unordered_map<int64_t, Location>     idx_id_;
    std::unordered_map<std::string, Location> idx_user_id_;
    std::multimap<int64_t, Location>          idx_salary_;
};