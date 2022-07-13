#pragma once

// ------ env.h ----------
constexpr const size_t kWritableFileBufferSize = 65536;
constexpr const int RecordSize = 8 + 128 + 128 + 8;

// ------ log.h ----------
constexpr const int kBlockSize = 32768;
const char WALFileNamePrefix[] = "WAL";
const int PoolSize = 1 << 29; // 512MB

// ------ engine.h -------
const int ShardNum = 50; // 对应客户端线程数量
const int WALNum = 50;  // 在lockfree情况下，必须ShardNum = WALNum
const int SSDNum = 0;  // 在lockfree情况下，必须ShardNum = WALNum
const int AEPNum = 50;  // 在lockfree情况下，必须ShardNum = WALNum

enum Phase{Hybrid=0, WriteOnly, ReadOnly};
