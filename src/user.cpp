// Copyright [2018] Alibaba Cloud All rights reserved

#include "user.h"
#include "robin_hood.h"

static const int kA = 54059;  // a prime
static const int kB = 76963;  // another prime
static const int kFinish = 37;  // also prime
uint32_t StrHash(const char* s, int size) {
  return robin_hood::hash_bytes(s, size);
}