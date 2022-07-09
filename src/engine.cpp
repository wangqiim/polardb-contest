#include "engine.h"
#include "spdlog/spdlog.h"

void add_res(const User &user, int32_t select_column, void **result) {
  switch(select_column) {
    case Id: 
      memcpy(*result, &user.id, 8); 
      *result = (char *)(*result) + 8; 
      break;
    case Userid: 
      memcpy(*result, user.user_id, 128); 
      *result = (char *)(*result)  + 128; 
      break;
    case Name: 
      memcpy(*result, user.name, 128); 
      *result = (char *)(*result) + 128; 
      break;
    case Salary: 
      memcpy(*result, &user.salary, 8); 
      *result = (char *)(*result) + 8; 
      break;
    default: 
      spdlog::error("unexpected here");
      break;
  }
}

// ------Index Builder-------------
class Index_Builder {
  public:
    Index_Builder(primary_key **idx_id,
      unique_key **idx_user_id, normal_key  **idx_salary, size_t len)
      : idx_id_(idx_id),
        idx_user_id_(idx_user_id),
        idx_salary_(idx_salary),
        len(len){ }

    void build(User *user, Location *location);

    primary_key **idx_id_;
    unique_key  **idx_user_id_;
    normal_key  **idx_salary_;
    size_t len;
};


void Index_Builder::build(User *user, Location *location) {
  // build pk index
  idx_id_[user->id % len]->insert({user->id, *user});
  // build uk index
  size_t hid = StrHash(user->user_id, sizeof(user->user_id));
  idx_user_id_[hid % len]->insert({std::string(user->user_id, sizeof(user->user_id)), user->id});
  // build nk index
  idx_salary_[user->salary % len]->insert({user->salary,user->id});
}

void build_index(void *record, void *location, void *context) {
    User *user = reinterpret_cast<User *>(record);
    Location *loc = reinterpret_cast<Location *>(location);
    Index_Builder *builder = reinterpret_cast<Index_Builder *>(context);
    builder->build(user, loc);
}

// ---------Name sequence scan------------------------
class EngineReader { // we should only use it when where_column isn't index
  public:
      EngineReader(int32_t select_column, int32_t where_column, const void *column_key, size_t column_key_len, void *res)
        : select_column_(select_column), 
        where_column_(where_column), 
        column_key_(column_key), 
        column_key_len_(column_key_len),
        res_(res),
        res_num_(0) {
          if (where_column != Name) {
            spdlog::error("we should only use it when where_column is name:{}, but it is {}", Name, where_column);
          }
        }
      
      void   read(User *user);
      size_t get_cnt() { return res_num_; }
      void   reset() { res_num_ = 0; }
  private:
    int32_t select_column_;
    int32_t where_column_;
    const void *column_key_;
    size_t column_key_len_;
    void *res_;

    size_t  res_num_;
};

void EngineReader::read(User *user) {
  bool b = true;
  switch(where_column_) {
      case Id: b = memcmp(column_key_, &user->id, column_key_len_) == 0; break;
      case Userid: b = memcmp(column_key_, user->user_id, column_key_len_) == 0; break;
      case Name: b = memcmp(column_key_, user->name, column_key_len_) == 0; break;
      case Salary: b = memcmp(column_key_, &user->salary, column_key_len_) == 0; break;
      default: b = false; break; // wrong
  }
  if (b) {
    ++res_num_;
    add_res(*user, select_column_, &res_);
  }
}

void read_record(void *record, void *location, void *context) {
    User *user = reinterpret_cast<User *>(record);
    EngineReader *reader = reinterpret_cast<EngineReader *>(context);
    reader->read(user);
}

// --------------------Engine-----------------------------
Engine::Engine(const char* disk_dir)
  : mtx_(), dir_(disk_dir), plate_(new Plate(dir_)),
    idx_id_list_(), idx_user_id_list_(), idx_salary_list_() {}

Engine::~Engine() {
  delete plate_;
}

int Engine::Init() {
  std::lock_guard<std::mutex> lock(mtx_);

  for(size_t i = 0; i < 8; i++) {
    idx_id_list_[i] = new primary_key;
    idx_user_id_list_[i] = new unique_key;
    idx_salary_list_[i] = new normal_key;
  }

  spdlog::info("engine start init");
  int ret = plate_->Init();
  if (ret != 0) {
    spdlog::error("plate init fail");
    return -1;
  }
  Index_Builder index_builder(idx_id_list_, idx_user_id_list_, idx_salary_list_, 8);
  ret = plate_->scan(build_index, &index_builder);
  if (ret != 0) {
    spdlog::error("plate scan fail");
    return -1;
  }
  spdlog::info("engine init done");
  return 0;
}

int Engine::Append(const void *datas) {
  std::lock_guard<std::mutex> lock(mtx_);
  if ((++write_cnt_) % 1000 == 0) {
    spdlog::debug("[wangqiim] write {}", ((User *)datas)->to_string());
  }
  // mtx_.lock();
  Location location;
  plate_->append(datas, location);
  // mtx_.unlock();
  const User *user = reinterpret_cast<const User *>(datas);
  // build pk index
  uint32_t id1 = (user->id) % 8;
  uint32_t id2 = (StrHash(user->user_id, sizeof(user->user_id))) % 8;
  uint32_t id3 = (user->salary) % 8;

  //  if (idx_id_list_[hid]->count(user->id) != 0) {
//    spdlog::error("insert dup id: {}", user->id);
//  } else if (idx_user_id_list_.count(std::string(user->user_id, 128)) != 0) {
//    spdlog::error("insert dup user_id: {}", user->user_id);
//  }

  idx_id_mtx_list_[id1].lock();
  idx_id_list_[id1]->insert({user->id, *user});
  idx_id_mtx_list_[id1].unlock();
  // build uk index
  idx_user_id_mtx_list_[id2].lock();
  idx_user_id_list_[id2]->insert({std::string(user->user_id, sizeof(user->user_id)), user->id}); // must use string(char* s, size_t n) construct funciton
  idx_user_id_mtx_list_[id2].unlock();

  // build nk index
  idx_salary_mtx_list_[id3].lock();
  idx_salary_list_[id3]->insert({user->salary, user->id});
  idx_salary_mtx_list_[id3].unlock();
  return 0;
}

size_t Engine::Read(void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, 
    size_t column_key_len, void *res) {
  std::lock_guard<std::mutex> lock(mtx_);
  User user;
  size_t res_num = 0;
  switch(where_column) {
      case Id: {
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }
        int64_t id = *((int64_t *)column_key);
        uint32_t id1 = id % 8;
        idx_id_mtx_list_[id1].lock();
        auto iter = idx_id_list_[id1]->find(id);
        if (iter != idx_id_list_[id1]->end()) {
          res_num = 1;
          user = iter->second;
          add_res(user, select_column, &res);
        }
        if ((++cnt1_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Id], user[{}]", select_column, user.to_string());
        }
        idx_id_mtx_list_[id1].unlock();
      }
      break;

      case Userid: {
        if (column_key_len != 128) {
          spdlog::error("read column_key_len is: {}, expcted: 128", column_key_len);
        }
        std::string user_id((char *)column_key, column_key_len); // todo: column_key_len is 128???
        uint32_t id2 = (StrHash((char *)column_key, column_key_len)) % 8;

        idx_user_id_mtx_list_[id2].lock();
        auto iter = idx_user_id_list_[id2]->find(user_id);
        if (iter != idx_user_id_list_[id2]->end()) {
          res_num = 1;
          int64_t id = iter->second;

          uint32_t id1 = id % 8;
          idx_id_mtx_list_[id1].lock();
          user = idx_id_list_[id1]->find(id)->second;
          idx_id_mtx_list_[id1].unlock();
          add_res(user, select_column, &res);
        }
        if ((++cnt2_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Userid], user[{}]", select_column, user.to_string());
        }
        idx_user_id_mtx_list_[id2].unlock();
      } 
      break;

      case Name: {
        if ((++cnt3_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Name]", select_column);
        }
        spdlog::info("select where Name is very slow (without index)");
        if (column_key_len != 128) {
          spdlog::error("read column_key_len is: {}, expcted: 128", column_key_len);
        }
        EngineReader reader(select_column, where_column, column_key, column_key_len, res);
        plate_->scan(read_record, reinterpret_cast<void *>(&reader));
        res_num = reader.get_cnt();
      } 
      break;

      case Salary: {
        if ((++cnt4_) % 1 == 0) {
          spdlog::debug("[wangqiim] read select_column[{}], where_column[Salary]", select_column);
        }
        if (column_key_len != 8) {
          spdlog::error("read column_key_len is: {}, expcted: 8", column_key_len);
        }
        int64_t salary = *((int64_t *)column_key);

        uint32_t id3 = salary % 8;
        idx_salary_mtx_list_[id3].lock();
        auto range = idx_salary_list_[id3]->equal_range(salary);
        auto iter = range.first;
        while (iter != range.second) {
          res_num += 1;
          int64_t id = iter->second;
          uint32_t id1 = id % 8;
          idx_id_mtx_list_[id1].lock();
          user = idx_id_list_[id1]->find(id)->second;
          idx_id_mtx_list_[id1].unlock();
          add_res(user, select_column, &res);
          iter++;
        }
        idx_salary_mtx_list_[id3].unlock();
      }
      break;

      default:
        spdlog::error("unexpected where_column: {}", where_column);
      break;
  }
  return res_num;
}