#include "../inc/interface.h"
#include <iostream>

void engine_write( void *ctx, const void *data, size_t len) { }

size_t engine_read( void *ctx, int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res) {return 1;}

void* engine_init(char* host_info, char** peer_host_info, size_t peer_host_info_num, const char* aep_dir) {return nullptr;}

void engine_deinit(void *ctx) {}
