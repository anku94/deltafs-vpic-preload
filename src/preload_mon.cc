/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "shuffle_internal.h"

#include "preload_internal.h"
#include "preload_mon.h"

static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

#ifdef PRELOAD_NEED_HISTO
/* clang-format off */
static const double BUCKET_LIMITS[MON_NUM_BUCKETS] = {
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
  50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
  500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
  3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
  16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
  70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
  250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
  900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
  3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
  9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
  25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
  70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
  180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
  450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
  1000000000, 2000000000, 4000000000.0, 8000000000.0,
  1e200, /* clang-format on */
};

static void hstg_reset_min(hstg_t& h) {
  h[2] = BUCKET_LIMITS[MON_NUM_BUCKETS - 1]; /* min */
}

static void hstg_add(hstg_t& h, double d) {
  int b = 0;
  while (b < MON_NUM_BUCKETS - 1 && BUCKET_LIMITS[b] <= d) {
    b++;
  }
  h[4 + b] += 1.0;
  h[0] += 1.0;            /* num */
  if (h[1] < d) h[1] = d; /* max */
  if (h[2] > d) h[2] = d; /* min */
  h[3] += d;              /* sum */
}

static double hstg_ptile(const hstg_t& h, double p) {
  double threshold = h[0] * (p / 100.0);
  double sum = 0;
  for (int b = 0; b < MON_NUM_BUCKETS; b++) {
    sum += h[4 + b];
    if (sum >= threshold) {
      double left_point = (b == 0) ? 0 : BUCKET_LIMITS[b - 1];
      double right_point = BUCKET_LIMITS[b];
      double left_sum = sum - h[4 + b];
      double right_sum = sum;
      double pos = (threshold - left_sum) / (right_sum - left_sum);
      double r = left_point + (right_point - left_point) * pos;
      if (r < h[2]) r = h[2]; /* min */
      if (r > h[1]) r = h[1]; /* max */

      return (r);
    }
  }

  return (h[1]); /* max */
}

static double hstg_max(const hstg_t& h) { return (h[1]); }

static double hstg_min(const hstg_t& h) { return (h[2]); }

static double hstg_avg(const hstg_t& h) {
  if (h[0] < 1.0) return (0);
  return (h[3] / h[0]);
}

static void hstg_reduce(const hstg_t& src, hstg_t& sum) {
  MPI_Reduce(const_cast<double*>(&src[0]), &sum[0], 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<double*>(&src[1]), &sum[1], 1, MPI_DOUBLE, MPI_MAX, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<double*>(&src[2]), &sum[2], 1, MPI_DOUBLE, MPI_MIN, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<double*>(&src[3]), &sum[3], 1, MPI_DOUBLE, MPI_SUM, 0,
             MPI_COMM_WORLD);

  MPI_Reduce(const_cast<double*>(&src[4]), &sum[4], MON_NUM_BUCKETS, MPI_DOUBLE,
             MPI_SUM, 0, MPI_COMM_WORLD);
}
#endif

int mon_fetch_plfsdir_stat(deltafs_plfsdir_t* dir, dir_stat_t* buf) {
  assert(dir != NULL);
  assert(buf != NULL);
#define get_property deltafs_plfsdir_get_integer_property

  buf->total_dblksz = get_property(dir, "sstable_filter_bytes");
  buf->total_iblksz = get_property(dir, "sstable_index_bytes");
  buf->total_dblksz = get_property(dir, "sstable_data_bytes");
  buf->num_sstables = get_property(dir, "num_sstables");
  buf->num_dropped_keys = get_property(dir, "num_dropped_keys");
  buf->num_keys = get_property(dir, "num_keys");

#undef get_property
  return 0;
}

int mon_preload_write(const char* fn, char* data, size_t n, int epoch) {
  size_t l;
  int rv;

  rv = preload_write(fn, data, n, epoch);

  if (rv == 0 && !pctx.nomon) {
    assert(fn != NULL && pctx.plfsdir != NULL);
    assert(strncmp(fn, pctx.plfsdir, pctx.len_plfsdir) == 0);
    assert(strlen(fn) > pctx.len_plfsdir + 1);

    l = strlen(fn) - pctx.len_plfsdir - 1;

    pthread_mutex_lock(&mtx);
    if (l > pctx.mctx.max_fnl) pctx.mctx.max_fnl = l;
    if (l < pctx.mctx.min_fnl) pctx.mctx.min_fnl = l;
    if (n > pctx.mctx.max_wsz) pctx.mctx.max_wsz = n;
    if (n < pctx.mctx.min_wsz) pctx.mctx.min_wsz = n;

    pctx.mctx.sum_fnl += l;
    pctx.mctx.sum_wsz += n;

    pctx.mctx.min_nw++;
    pctx.mctx.max_nw++;
    pctx.mctx.nw++;

    pthread_mutex_unlock(&mtx);
  }

  return (rv);
}

static void mon_shuffle_cb(int rv, void* arg1, void* arg2) {
  if (rv == 0 && !pctx.nomon) {
    pthread_mutex_lock(&mtx);
    pctx.mctx.min_nws++;
    pctx.mctx.max_nws++;
    pctx.mctx.nws++;

    pthread_mutex_unlock(&mtx);
  }
}

int mon_shuffle_write_send_async(void* write_in, int peer_rank) {
  int rv;

  rv = shuffle_write_send_async(static_cast<write_in_t*>(write_in), peer_rank,
                                mon_shuffle_cb, NULL, NULL);
  return (rv);
}

int mon_shuffle_write_send(void* write_in, int peer_rank) {
  int rv;

  rv = shuffle_write_send(static_cast<write_in_t*>(write_in), peer_rank);
  if (rv == 0 && !pctx.nomon) {
    pthread_mutex_lock(&mtx);
    pctx.mctx.min_nws++;
    pctx.mctx.max_nws++;
    pctx.mctx.nws++;

    pthread_mutex_unlock(&mtx);
  }

  return (rv);
}

int mon_shuffle_write_received() {
  if (!pctx.nomon) {
    pthread_mutex_lock(&mtx);
    pctx.mctx.min_nwr++;
    pctx.mctx.max_nwr++;
    pctx.mctx.nwr++;

    pthread_mutex_unlock(&mtx);
  }
}

void mon_reduce(const mon_ctx_t* src, mon_ctx_t* sum) {
  MPI_Reduce(const_cast<unsigned*>(&src->min_fnl), &sum->min_fnl, 1,
             MPI_UNSIGNED, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned*>(&src->max_fnl), &sum->max_fnl, 1,
             MPI_UNSIGNED, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->sum_fnl), &sum->sum_fnl, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned*>(&src->min_wsz), &sum->min_wsz, 1,
             MPI_UNSIGNED, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned*>(&src->max_wsz), &sum->max_wsz, 1,
             MPI_UNSIGNED, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->sum_wsz), &sum->sum_wsz, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nws), &sum->nws, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nws), &sum->min_nws, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nws), &sum->max_nws, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nwr), &sum->nwr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nwr), &sum->min_nwr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nwr), &sum->max_nwr, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->nw), &sum->nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->min_nw), &sum->min_nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<unsigned long long*>(&src->max_nw), &sum->max_nw, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);

  MPI_Reduce(const_cast<unsigned long long*>(&src->dura), &sum->dura, 1,
             MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->dir_stat.num_keys),
             &sum->dir_stat.num_keys, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->dir_stat.num_dropped_keys),
             &sum->dir_stat.num_dropped_keys, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->dir_stat.num_sstables),
             &sum->dir_stat.num_sstables, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);

  MPI_Reduce(const_cast<long long*>(&src->dir_stat.total_fblksz),
             &sum->dir_stat.total_fblksz, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->dir_stat.total_iblksz),
             &sum->dir_stat.total_iblksz, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
  MPI_Reduce(const_cast<long long*>(&src->dir_stat.total_dblksz),
             &sum->dir_stat.total_dblksz, 1, MPI_LONG_LONG, MPI_SUM, 0,
             MPI_COMM_WORLD);
}

#define DUMP(fd, buf, fmt, ...)                             \
  {                                                         \
    int n = snprintf(buf, sizeof(buf), fmt, ##__VA_ARGS__); \
    buf[n] = '\n';                                          \
    n = write(fd, buf, n + 1);                              \
  }

void mon_dumpstate(int fd, const mon_ctx_t* ctx) {
  char buf[1024];
  if (!ctx->global) {
    DUMP(fd, buf, "\n--- epoch-[%d] (rank %d) ---", ctx->epoch_seq,
         pctx.myrank);
    DUMP(fd, buf, "!!! NON GLOBAL !!!");
  } else {
    DUMP(fd, buf, "\n--- epoch-[%d] ---", ctx->epoch_seq);
  }
  DUMP(fd, buf, "[M] epoch dura: %llu us", ctx->dura);
  DUMP(fd, buf, "[M] observed epoch tput: %.2f bytes/s",
       double(ctx->sum_wsz) / ctx->dura * 1000000);
  DUMP(fd, buf, "[M] total sst filter bytes: %lld bytes",
       ctx->dir_stat.total_fblksz);
  DUMP(fd, buf, "[M] total sst indexes: %lld bytes",
       ctx->dir_stat.total_iblksz);
  DUMP(fd, buf, "[M] total sst data: %lld bytes", ctx->dir_stat.total_dblksz);
  DUMP(fd, buf, "[M] total num sst: %lld", ctx->dir_stat.num_sstables);
  DUMP(fd, buf, "[M] max fname len: %u chars", ctx->max_fnl);
  DUMP(fd, buf, "[M] min fname len: %u chars", ctx->min_fnl);
  DUMP(fd, buf, "[M] total fname len: %llu chars", ctx->sum_fnl);
  DUMP(fd, buf, "[M] max write size: %u bytes", ctx->max_wsz);
  DUMP(fd, buf, "[M] min write size: %u bytes", ctx->min_wsz);
  DUMP(fd, buf, "[M] total write size: %llu bytes", ctx->sum_wsz);
  DUMP(fd, buf, "[M] total rpc sent: %llu", ctx->nws);
  DUMP(fd, buf, "[M] min rpc sent per rank: %llu", ctx->min_nws);
  DUMP(fd, buf, "[M] max rpc sent per rank: %llu", ctx->max_nws);
  DUMP(fd, buf, "[M] total rpc received: %llu", ctx->nwr);
  DUMP(fd, buf, "[M] min rpc received per rank: %llu", ctx->min_nwr);
  DUMP(fd, buf, "[M] max rpc received per rank: %llu", ctx->max_nwr);
  DUMP(fd, buf, "[M] total writes: %llu", ctx->nw);
  DUMP(fd, buf, "[M] min writes per rank: %llu", ctx->min_nw);
  DUMP(fd, buf, "[M] max writes per rank: %llu", ctx->max_nw);
  if (!ctx->global) DUMP(fd, buf, "!!! NON GLOBAL !!!");
  DUMP(fd, buf, "--- end ---\n");

  return;
}

void mon_reinit(mon_ctx_t* ctx) {
  mon_ctx_t tmp = {0};
  tmp.min_fnl = 0xffffffff;
  tmp.min_wsz = 0xffffffff;
  *ctx = tmp;

  return;
}