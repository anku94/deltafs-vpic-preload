#include <algorithm>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "range_common.h"
#include "range_utils.h"
#include "pdlfs-common/testharness.h"
#include "pdlfs-common/testutil.h"

namespace pdlfs {

class RangeUtilsTest {
};

TEST(RangeUtilsTest, LoadBinsIntoRBVec) {
  std::vector<float> bins{ 0.0, 25.0, 50.0, 50.0, 75.0, 100.0 };
  std::vector<rb_item_t> rbvec;
  int num_ranks = 2, bins_per_rank = 3;

  load_bins_into_rbvec(bins, rbvec, num_ranks * bins_per_rank, num_ranks,
                       bins_per_rank);

  std::vector<rb_item_t> rbvec_check{
    { 0, 0.0, 25.0, true },
    { 0, 25.0, 0.0, false },
    { 0, 25.0, 50.0, true },
    { 0, 50.0, 25.0, false },
    { 1, 50.0, 75.0, true },
    { 1, 75.0, 50.0, false },
    { 1, 75.0, 100.0, true },
    { 1, 100.0, 75.0, false }
  };
  std::sort(rbvec_check.begin(), rbvec_check.end(), rb_item_lt);

  for (int i = 0; i < num_ranks * bins_per_rank; ++i) {
    const rb_item_t& A = rbvec[i], B = rbvec_check[i];
    ASSERT_EQ(A.rank, B.rank);
    ASSERT_EQ(A.bin_val, B.bin_val);
    ASSERT_EQ(A.bin_other, B.bin_other);
    ASSERT_EQ(A.is_start, B.is_start);
  }
}

TEST(RangeUtilsTest, ParticleCount) {
  ASSERT_EQ(get_particle_count(3, 5, 2), 4);
  ASSERT_EQ(get_particle_count(2, 5, 2), 6);
  ASSERT_EQ(get_particle_count(3, 3, 2), 0);
}

TEST(RangeUtilsTest, PivotCalc) {
  srand(time(NULL));
  pivot_ctx_t pctx;

  int oob_count = 256;
  pctx.oob_buffer_left.resize(oob_count);
  pctx.oob_count_left = oob_count;

  for (int oob_idx = 0; oob_idx < oob_count; oob_idx++) {
    float rand_val = rand() * 1.0f / RAND_MAX;
    pctx.oob_buffer_left[oob_idx].indexed_prop = rand_val;
  }

  int num_pivots = 64;
  pctx.mts_mgr.update_state(MainThreadState::MT_BLOCK);
  pivot_calculate(&pctx, num_pivots);

  size_t buf_sz = 2048;
  char buf[buf_sz];
  print_vector(buf, buf_sz, pctx.my_pivots, num_pivots, false);

  for (int pvt_idx = 1; pvt_idx < num_pivots; pvt_idx++) {
    float a = pctx.my_pivots[pvt_idx];
    float b = pctx.my_pivots[pvt_idx - 1];
//    printf("%d: %f %f %s\n", pvt_idx, a, b, a > b ? "ge" : "le");
    ASSERT_GT(a, b);
  }
}

} // namespace pdlfs

int main(int argc, char* argv[]) {
  return ::pdlfs::test::RunAllTests(&argc, &argv);
}
