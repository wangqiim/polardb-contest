#pragma once

constexpr const int RecordSize = 8 + 128 + 128 + 8;

// ------ log.h ----------
const char WALFileNameSuffix[] = "WAL";

// ------ engine.h -------
const int WritePerClient = 1000000; 
const int ClientNum = 50;

const int WaitChangeFinishSecond = 3;
const int FenceSecond = 10;

enum Phase{Hybrid=0, WriteOnly, ReadOnly};
