#pragma once

const int OSPageSize = 4096;

// ------ env.h ----------
constexpr const int RecordSize = 8 + 128 + 128 + 8;

// ------ log.h ----------
const unsigned long CommitFlag = 0xFFFFFFFFFFFFFFFF;
const int CommitField = 8;
const char WALFileNamePrefix[] = "WAL";
const int PoolSize = 1 << 29; // 512MB can't exceed 1GB
// 272 * 5e7 * (24 / 50) / 50 = 130560000
// 130560000 + 8
const int MmapSize = 130560000 + 8; // 281018368 data: 272000000

const char PmapBufferWriterFileNameSuffix[] = "BUF";
const int PmapBufferWriterSize = 4352; // LCM(256, 272) write 256 per write pmem
const int PmapBufferWriterFileSize = PmapBufferWriterSize + 8; // 8 bytes is for commit_cnt

// ------ engine.h -------
const int WritePerClient = 1000000; 
const int ClientNum = 50;
const int SSDNum = 24;  // 在lockfree情况下，必须ClientNum = SSDNum + AEPNum
const int AEPNum = 26;  // 在lockfree情况下，必须ClientNum = SSDNum + AEPNum
#define IsWriteAEP(write_cnt) ((write_cnt) % 50 < AEPNum)

const int WaitChangeFinishSecond = 3;
const int FenceSecond = 10;

enum Phase{Hybrid=0, WriteOnly, ReadOnly};

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)