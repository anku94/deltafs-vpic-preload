#pragma once

#define DEFAULT_PVTCNT 64

#define DEFAULT_OOBSZ 512

namespace pdlfs {
static const size_t kMaxPivots = 256;
static const size_t kMaxPartSize = 256;
static const size_t kRenegInterval = 250000;
static const float kFloatCompThreshold = 1e-3;
}  // namespace pdlfs
