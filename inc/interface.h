#pragma once

#include <stdint.h>
#include <stdio.h>

extern "C" {

/*
 * Table schema:
 * +-------------+-------------------+------------------+-----------------+
 * | Id(8 bytes) | Userid(128 bytes) |  Name(128 bytes) | Salary(8 bytes) |
 * +-------------+-------------------+------------------+-----------------+
 * And long uses small endian.
 */

/*
 * Writes incoming data to the engine
 */
void engine_write( void *ctx, const void *data, size_t len);

/*
 * Simulate queries to a relational database：
 * SELECT select_column FROM table_name WHERE where_column = column_key .
 * Since there is only one kind of table, the table_name is not provided in the parameters.
 * Column correspondence : Id=0, Userid=1, Name=2, Salary=3.
 * You need to read the data from engine into the res, and return the number of data tuples.
 */
size_t engine_read( void *ctx, int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res);

/*
 * Initialization interface, which is called when the engine starts.
 * You need to create or recover db from pmem-file.
 * host_info: Local machine information including ip and port. This value is nullptr in the preliminary round.
 * peer_host_info: Information about other machines in the distributed cluster. This value is nullptr in the preliminary round.
 * peer_host_info_num: The num of other machines in the distributed cluster.  This value is 0 in the preliminary round.
 * aep_dir: AEP file directory, eg : "/mnt/aep/"
 * disk_dir: Disk file directory, eg : "/mnt/disk/"
 */
void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir);

/*
 * Used to release resources when the engine exits normally.
 */
void engine_deinit(void *ctx);

}