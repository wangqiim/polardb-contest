#pragma once

// ------ env.h ----------
constexpr const size_t kWritableFileBufferSize = 65536;
constexpr const int RecordSize = 8 + 128 + 128 + 8;

// ------ log.h ----------
const int CommitField = 8;
constexpr const int kBlockSize = 32768;
const char WALFileNamePrefix[] = "WAL";
const int PoolSize = 1 << 29; // 512MB can't exceed 1GB
const int MmapSize = 272800000; // 281018368 data: 272000000

const char PmapBufferWriterFileNameSuffix[] = "BUF";
const int PmapBufferWriterSize = 4352; // LCM(256, 272) write 256 per write pmem
const int PmapBufferWriterFileSize = PmapBufferWriterSize + 8; // 8 bytes is for commit_cnt

// ------ engine.h -------
const int WritePerClient = 1000000; 
const int ClientNum = 50;
const int SSDNum = 50;  // 在lockfree情况下，必须ClientNum = SSDNum + AEPNum
const int AEPNum = 0;  // 在lockfree情况下，必须ClientNum = SSDNum + AEPNum

const int WaitChangeFinishSecond = 3;
const int FenceSecond = 10;

enum Phase{Hybrid=0, WriteOnly, ReadOnly};
