#include "interface.h"
#include <cstring>
#include "spdlog/spdlog.h"
#include <stdlib.h>
#include "leveldb/db.h"
#include "leveldb/write_batch.h"


class User
{
public:
    int64_t id;
    char user_id[128];
    char name[128];
    int64_t salary;
};

enum Column{Id=0,Userid,Name,Salary};

leveldb::DB* db;

void engine_write( void *ctx, const void *data, size_t len) {
    User user;
    memcpy(&user,data,len);
    leveldb::WriteBatch batch;
    leveldb::WriteOptions write_options;
    write_options.sync = true;
    leveldb::Slice value((char *) data, len);

    // key: id
    char k1[9];
    k1[0] = Id;
    memcpy((k1 + 1), &user.id, 8);
    batch.Put(leveldb::Slice(k1, 9), value);

    // key: user_id
    char k2[129];
    k2[0] = Userid;
    memcpy((k2 + 1), &user.user_id, 128);
    batch.Put(leveldb::Slice(k2, 129), value);

    // key: (name, id)
    char k3[137];
    k3[0] = Name;
    memcpy((k3 + 1), &user.name, 128);
    memcpy((k3 + 129), &user.id, 8);
    batch.Put(leveldb::Slice(k3, 137), value);

    // key: (salary_id, id)
    char k4[17];
    k4[0] = Salary;
    memcpy((k4 + 1), &user.salary, 8);
    memcpy((k4 + 9), &user.id, 8);
    batch.Put(leveldb::Slice(k4, 17), value);

    db->Write(write_options, &batch);
}

 size_t copy_to_res(int32_t select_column, const char* value, void* res) {
     User user;
     memcpy(&user, value, 272);
     switch (select_column) {
         case Id: {
             memcpy(res, &user.id, 8);
             return 8;
         }
         case Userid: {
             memcpy(res, &user.user_id, 128);
             return 128;
         }
         case Name: {
             memcpy(res, &user.name, 128);
             return 128;
         }
         default : {
             memcpy(res, &user.salary, 8);
             return 8;
         }
     }
}

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    size_t res_num = 0;
    std::string value;
    leveldb::Status status;
    leveldb::ReadOptions opt = leveldb::ReadOptions();
    switch (where_column) {
        case Id: {
            char k1[9];
            k1[0] = Id;
            memcpy((k1 + 1), column_key, 8);
            status = db->Get(opt, leveldb::Slice(k1, 9), &value);
            if (!status.ok()) {
                return 0;
            }
            copy_to_res(select_column, value.c_str(), res);
            res_num++;
            break;
        }
        case Userid: {
            char k2[129];
            k2[0] = Userid;
            memcpy((k2 + 1), column_key, 128);
            status = db->Get(opt, leveldb::Slice(k2, 129), &value);
            if (!status.ok()) {
                return 0;
            }
            copy_to_res(select_column, value.c_str(), res);
            res_num++;
            break;
        }

        case Name: {
            char k3[129];
            k3[0] = Name;
            memcpy((k3 + 1), column_key, 128);
            leveldb::Iterator* it = db->NewIterator(opt);
            leveldb::Slice s3(k3, 129);
            for (it->Seek(s3);
                 it->Valid() && it->key().starts_with(s3);
                 it->Next()) {
                size_t l = copy_to_res(select_column, it->value().data(), res);
                res = (char *)res + l;
                res_num++;
            }
            delete it;
            break;
        }

        case Salary: {
            char k4[9];
            k4[0] = Salary;
            memcpy((k4 + 1), column_key, 8);
            leveldb::Iterator* it = db->NewIterator(opt);
            leveldb::Slice s4(k4, 9);
            for (it->Seek(s4);
                 it->Valid() && it->key().starts_with(s4);
                 it->Next()) {
                size_t l = copy_to_res(select_column, it->value().data(), res);
                res = (char *)res + l;
                res_num++;
            }
            delete it;
            break;
        }
        default:
            spdlog::debug("[engine_read] [select_column:{0:d}] [where_column:{1:d}] [column_key_len:{2:d}]", select_column, where_column, column_key_len);
            break;
    }
    return res_num;
}

std::string gen_random(const int len) {
    static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
    std::string tmp_s;
    tmp_s.reserve(len);

    for (int i = 0; i < len; ++i) {
        tmp_s += alphanum[rand() % (sizeof(alphanum) - 1)];
    }

    return tmp_s;
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    spdlog::set_level(spdlog::level::debug);
    char db_path[30];
    if (strncmp(disk_dir, "/mnt", 4) != 0) {
        srand((unsigned)time(nullptr) * getpid());
        sprintf(db_path, "%s%s", disk_dir, gen_random(8).c_str());
    } else {
        sprintf(db_path, "%s%s", disk_dir, "test");
    }

    spdlog::info("[engine_init] [db_path:{0:s}]", db_path);
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, db_path, &db);
    assert(status.ok());
    return nullptr;
}

void engine_deinit(void *ctx) {
//    spdlog::info("[engine_deinit]");
    delete db;
}
