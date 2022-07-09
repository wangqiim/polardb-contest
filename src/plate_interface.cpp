#include "../inc/interface.h"
#include <vector>
#include <string.h>
#include "spdlog/spdlog.h"
#include "engine.h"

Engine *engine;

void engine_write( void *ctx, const void *data, size_t len) {
    User user;
    if (len != RECORDSIZE) {
        spdlog::error("engine_write len != {}", RECORDSIZE);
    }
    memcpy(&user,data,len);
    if (len != RECORDSIZE) {
      spdlog::error("engine_write len not equal to {:d}", RECORDSIZE);
    }
    engine->Append(data);
 }

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    // spdlog::debug("[engine_read] [select_column:{0:d}] [where_column:{1:d}] [column_key_len:{2:d}]", select_column, where_column, column_key_len); 
    size_t res_num = engine->Read(ctx, select_column, where_column, column_key, column_key_len, res);
    return res_num;
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    spdlog::set_level(spdlog::level::debug);
    spdlog::info("[plate engine_init 1]");
    engine = new Engine(disk_dir);
    engine->Init();
    return nullptr;
}

void engine_deinit(void *ctx) {
    spdlog::info("[plate engine_deinit]");
    delete engine;
}
