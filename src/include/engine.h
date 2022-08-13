#include <atomic>
#include <unordered_map>
#include <map>
#include <mutex>
#include <vector>

#include "unordered_dense.h"
#include "user.h"
#include "log.h"

// id int64, user_id char(128), name char(128), salary int64
// pk : id 			    //主键索引
// uk : user_id 		//唯一索引
// sk : salary			//普通索引

using primary_key = ankerl::unordered_dense::map<int64_t, size_t>;
using unique_key  = ankerl::unordered_dense::map<BlizardHashWrapper, size_t>;
using normal_key  = ankerl::unordered_dense::map<int64_t, LocationsWrapper>;

using cluster_primary_key = ankerl::unordered_dense::map<int64_t, UserIdWrapper>; // Id->Userid
using cluster_unique_key  = ankerl::unordered_dense::map<BlizardHashWrapper, NameWrapper>; // Userid->Name
using cluster_normal_key  = ankerl::unordered_dense::map<int64_t, int64_t>; // Salary->Id

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
    void warmUp();
    int replay_index(const std::vector<std::string> disk_path, const std::vector<std::string> pmem_path);
    int must_set_tid();

    void close_all_writers();
    int open_all_writers();

  private:
    int build_3_cluster_index(const std::vector<std::string> disk_path, const std::vector<std::string> pmem_path);
    size_t perf_Read(void *ctx, int32_t select_column,
      int32_t where_column, const void *column_key, 
      size_t column_key_len, void *res);
    

    std::atomic<bool> is_changing_;
    std::atomic<int> phase_;
    std::atomic<int> next_tid_;
    std::mutex mtx_;
    std::vector<std::string> disk_file_paths_;
    std::vector<std::string> pmem_file_paths_;
    const std::string aep_dir_;
    const std::string dir_;
    std::vector<MmapWriter *> disk_logs_;
    std::vector<PmapBufferWriter *> pmem_logs_;

    std::vector<User> users_;
    primary_key idx_id_;

    unique_key idx_user_id_;

    normal_key idx_salary_;

    // only use for performance read phase
    bool is_read_perf_ = false;
    cluster_primary_key cluster_idx_id_;
    cluster_unique_key  cluster_idx_user_id_;
    cluster_normal_key  cluster_idx_salary_;
    std::vector<std::mutex> read_mtxs_;
    std::vector<std::condition_variable> read_cvs_;
    // debug log
    std::chrono::_V2::system_clock::time_point start_;
};