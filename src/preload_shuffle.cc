/*
 * Copyright (c) 2019 Carnegie Mellon University,
 * Copyright (c) 2019 Triad National Security, LLC, as operator of
 *     Los Alamos National Laboratory.
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of CMU, TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.

 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <arpa/inet.h>
#include <assert.h>
#include <ifaddrs.h>

#include "preload_internal.h"
#include "preload_mon.h"
#include "preload_range.h"
#include "preload_shuffle.h"

#include "nn_shuffler.h"
#include "nn_shuffler_internal.h"
#include "xn_shuffler.h"

#include <ch-placement.h>
#include <mercury_config.h>
#include <pdlfs-common/xxhash.h>

#include "common.h"
#include "msgfmt.h"

namespace {
void shuffle_prepare_sm_uri(char* buf, const char* proto) {
  int min_port, max_port;
  const char* env;

  env = maybe_getenv("SHUFFLE_Min_port");
  if (env == NULL) {
    min_port = DEFAULT_MIN_PORT;
  } else {
    min_port = atoi(env);
  }

  env = maybe_getenv("SHUFFLE_Max_port");
  if (env == NULL) {
    max_port = DEFAULT_MAX_PORT;
  } else {
    max_port = atoi(env);
  }

  /* sanity check on port range */
  if (max_port - min_port < 0) ABORT("bad min-max port");
  if (min_port < 1) ABORT("bad min port");
  if (max_port > 65535) ABORT("bad max port");

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using port range [%d,%d]", min_port, max_port);
  }

  /* finalize uri */
  assert(strstr(proto, "sm") != NULL);
  sprintf(buf, "%s://%d:%d", proto, int(getpid()), min_port);
#ifndef NDEBUG
  if (pctx.verbose || pctx.my_rank == 0) {
    logf(LOG_INFO, "[hg] using %s (rank %d)", buf, pctx.my_rank);
  }
#endif
}

void shuffle_determine_ipaddr(char* ip, socklen_t iplen) {
  int family;
  struct ifaddrs *ifaddr, *cur;
  const char* subnet;

  subnet = maybe_getenv("SHUFFLE_Subnet");
  if (!subnet || !subnet[0] || strcmp("0.0.0.0", subnet) == 0) {
    subnet = NULL;
  }
  if (pctx.my_rank == 0) {
    if (!subnet) {
      logf(LOG_WARN,
           "subnet not specified\n>>> will use the 1st non-local ip...");
    } else {
      logf(LOG_INFO, "using subnet %s*", subnet);
    }
  }

  /* settle down an ip addr to use */
  if (getifaddrs(&ifaddr) == -1) {
    ABORT("getifaddrs");
  }

  for (cur = ifaddr; cur != NULL; cur = cur->ifa_next) {
    if (cur->ifa_addr != NULL) {
      family = cur->ifa_addr->sa_family;

      if (family == AF_INET) {
        if (getnameinfo(cur->ifa_addr, sizeof(struct sockaddr_in), ip, iplen,
                        NULL, 0, NI_NUMERICHOST) == -1)
          ABORT("getnameinfo");

        if (!subnet && strncmp("127", ip, strlen("127")) != 0) {
          break;
        } else if (subnet && strncmp(subnet, ip, strlen(subnet)) == 0) {
          break;
        } else {
#ifndef NDEBUG
          if (pctx.verbose || pctx.my_rank == 0) {
            logf(LOG_INFO, "[ip] skip %s (rank %d)", ip, pctx.my_rank);
          }
#endif
        }
      }
    }
  }

  if (cur == NULL) /* maybe a wrong subnet has been specified */
    ABORT("no ip addr");
  freeifaddrs(ifaddr);

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using ip %s (rank 0)", ip);
  }
}

}  // namespace

char _common_buf[255];
/* Print a string as hex
 * XXX: NOT THREAD SAFE
 */
char* print_hexstr(const char* s, int slen) {
  assert(slen < 100);
  for (int i = 0; i < slen; i++) {
    sprintf(&_common_buf[i * 2], "%02x", static_cast<unsigned int>(s[i]));
  }
  _common_buf[slen * 2] = '\0';
  return _common_buf;
}

void shuffle_prepare_uri(char* buf) {
  int port;
  const char* env;
  int min_port;
  int max_port;
  struct sockaddr_in addr;
  socklen_t addr_len;
  MPI_Comm comm;
  int rank;
  int size;
  const char* proto;
  char ip[50];
  int opt;
  int so;
  int rv;
  int n;

  proto = maybe_getenv("SHUFFLE_Mercury_proto");
  if (proto == NULL) {
    proto = DEFAULT_HG_PROTO;
  }
  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using %s", proto);
  }

  if (strstr(proto, "sm") != NULL) { /* special handling for sm addrs */
    shuffle_prepare_sm_uri(buf, proto);
  }

  env = maybe_getenv("SHUFFLE_Min_port");
  if (env == NULL) {
    min_port = DEFAULT_MIN_PORT;
  } else {
    min_port = atoi(env);
  }

  env = maybe_getenv("SHUFFLE_Max_port");
  if (env == NULL) {
    max_port = DEFAULT_MAX_PORT;
  } else {
    max_port = atoi(env);
  }

  /* sanity check on port range */
  if (max_port - min_port < 0) ABORT("bad min-max port");
  if (min_port < 1) ABORT("bad min port");
  if (max_port > 65535) ABORT("bad max port");

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "using port range [%d,%d]", min_port, max_port);
  }

#if MPI_VERSION >= 3
  rv = MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0,
                           MPI_INFO_NULL, &comm);
  if (rv != MPI_SUCCESS) {
    ABORT("MPI_Comm_split_type");
  }
#else
  comm = MPI_COMM_WORLD;
#endif
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &size);
  if (comm != MPI_COMM_WORLD) {
    MPI_Comm_free(&comm);
  }

  /* try and test port availability */
  port = min_port + (rank % (1 + max_port - min_port));
  for (; port <= max_port; port += size) {
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so != -1) {
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(port);
      opt = 1;
      setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
      n = bind(so, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
      close(so);
      if (n == 0) {
        break;
      }
    } else {
      ABORT("socket");
    }
  }

  if (port > max_port) {
    port = 0;
    logf(LOG_WARN,
         "no free ports available within the specified range\n>>> "
         "auto detecting ports ...");
    so = socket(PF_INET, SOCK_STREAM, 0);
    if (so != -1) {
      memset(&addr, 0, sizeof(addr));
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = INADDR_ANY;
      addr.sin_port = htons(0);
      opt = 0; /* do not reuse ports */
      setsockopt(so, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
      n = bind(so, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
      if (n == 0) {
        addr_len = sizeof(addr);
        n = getsockname(so, reinterpret_cast<struct sockaddr*>(&addr),
                        &addr_len);
        if (n == 0) {
          port = ntohs(addr.sin_port);
        }
      }
      close(so);
    } else {
      ABORT("socket");
    }
  }

  errno = 0;

  if (port == 0) /* maybe a wrong port range has been specified */
    ABORT("no free ports");

  /* finalize uri */
  shuffle_determine_ipaddr(ip, sizeof(ip));
  sprintf(buf, "%s://%s:%d", proto, ip, port);
#ifndef NDEBUG
  if (pctx.verbose || pctx.my_rank == 0) {
    logf(LOG_INFO, "[hg] using %s (rank %d)", buf, pctx.my_rank);
  }
#endif
}

void shuffle_epoch_pre_start(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffler_epoch_start(rep);
  } else {
    nn_shuffler_bgwait();
  }
}

/*
 * This function is called at the beginning of each epoch but before the epoch
 * really starts and before the final stats for the previous epoch are collected
 * and dumped. Therefore, this is a good time for us to copy xn_shuffler's
 * internal stats counters into preload's global mon context.
 */
void shuffle_epoch_start(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffler_epoch_start(rep);
    pctx.mctx.nlmr = rep->stat.local.recvs - rep->last_stat.local.recvs;
    pctx.mctx.min_nlmr = pctx.mctx.max_nlmr = pctx.mctx.nlmr;
    pctx.mctx.nlms = rep->stat.local.sends - rep->last_stat.local.sends;
    pctx.mctx.min_nlms = pctx.mctx.max_nlms = pctx.mctx.nlms;
    pctx.mctx.nlmd = pctx.mctx.nlms;
    pctx.mctx.nmr = rep->stat.remote.recvs - rep->last_stat.remote.recvs;
    pctx.mctx.min_nmr = pctx.mctx.max_nmr = pctx.mctx.nmr;
    pctx.mctx.nms = rep->stat.remote.sends - rep->last_stat.remote.sends;
    pctx.mctx.min_nms = pctx.mctx.max_nms = pctx.mctx.nms;
    pctx.mctx.nmd = pctx.mctx.nms;
  } else {
    nn_shuffler_bgwait();
  }
}

void shuffle_epoch_end(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    xn_shuffler_epoch_end(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    nn_shuffler_flushq(); /* flush rpc queues */
    if (!nnctx.force_sync) {
      /* wait for rpc replies */
      nn_shuffler_waitcb();
    }
  }
}

int shuffle_target(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz) {
  int world_sz;
  unsigned long target;
  int rv;

  assert(ctx != NULL);
  assert(buf_sz >= ctx->fname_len);

  world_sz = shuffle_world_sz(ctx);

  if (world_sz != 1) {
    if (IS_BYPASS_PLACEMENT(pctx.mode)) {
      rv = pdlfs::xxhash32(buf, ctx->fname_len, 0) % world_sz;
    } else {
      assert(ctx->chp != NULL);
      ch_placement_find_closest(
          ctx->chp, pdlfs::xxhash64(buf, ctx->fname_len, 0), 1, &target);
      rv = static_cast<int>(target);
    }
  } else {
    rv = shuffle_rank(ctx);
  }

  return (rv & ctx->receiver_mask);
}

float get_indexable_property(const char* data_buf, unsigned int dbuf_sz) {
  const float* prop = reinterpret_cast<const float*>(data_buf);
  return prop[0];
}

int shuffle_data_target(const float& indexed_prop) {
  auto rank_iter = std::lower_bound(pctx.rctx.rank_bins.begin(),
                                    pctx.rctx.rank_bins.end(), indexed_prop);

  int rank = rank_iter - pctx.rctx.rank_bins.begin() - 1;

  return rank;
}

namespace {
void shuffle_write_debug(shuffle_ctx_t* ctx, char* buf, unsigned char buf_sz,
                         int epoch, int src, int dst) {
  const int h = pdlfs::xxhash32(buf, buf_sz, 0);

  if (src != dst || ctx->force_rpc) {
    fprintf(pctx.trace, "[SH] %u bytes (ep=%d) r%d >> r%d (xx=%08x)\n", buf_sz,
            epoch, src, dst, h);
  } else {
    fprintf(pctx.trace,
            "[LO] %u bytes (ep=%d) "
            "(xx=%08x)\n",
            buf_sz, epoch, h);
  }
}
}  // namespace

/* Only to be used with preprocessed particle_mem_t structs
 * NOT for external use
 */
int shuffle_flush_oob(shuffle_ctx_t* ctx, range_ctx_t* rctx, int epoch) {
  logf(LOG_INFO, "Initiating OOB flush at rank %d\n", pctx.my_rank);
  int oob_count_left = rctx->oob_count_left;
  for (int oidx = oob_count_left - 1; oidx >= 0; oidx--) {
    particle_mem_t& p = rctx->oob_buffer_left[oidx];
    fprintf(stderr, "Flushing particle with energy %.1f\n", p.indexed_prop);
    if (p.indexed_prop > rctx->range_max || p.indexed_prop < rctx->range_min) {
      // should never happen since we flush after a reneg
      logf(LOG_ERRO,
           "Flushed particle lies out-of-bounds!"
           " Don't know what to do. Dropping particle");
      ABORT("panic");
      continue;  // drop this particle
    }
    int peer_rank = shuffle_data_target(p.indexed_prop);

    if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
      logf(LOG_ERRO,
           "Invalid shuffle_data_target for particle %.1f at %d: %d!\n",
           p.indexed_prop, pctx.my_rank, peer_rank);
      logf(LOG_ERRO, "INVALID %f\n",
           p.indexed_prop - rctx->rank_bins[pctx.comm_sz]);
    }

    rctx->rank_bin_count[peer_rank]++;

    fprintf(stderr, "Flushing ptcl rank: %d\n", peer_rank);
    // TODO: copy everything
    xn_shuffler_enqueue(static_cast<xn_ctx_t*>(ctx->rep), p.buf, p.buf_sz,
                        epoch, peer_rank, pctx.my_rank);
  }

  rctx->oob_count_left = 0;

  int oob_count_right = rctx->oob_count_right;
  for (int oidx = oob_count_right - 1; oidx >= 0; oidx--) {
    particle_mem_t& p = rctx->oob_buffer_right[oidx];
    fprintf(stderr, "Rank %d, flushing particle with energy %.1f\n",
            pctx.my_rank, p.indexed_prop);
    if (p.indexed_prop > rctx->range_max || p.indexed_prop < rctx->range_min) {
      // should never happen since we flush after a reneg
      logf(LOG_ERRO,
           "Flushed particle lies out-of-bounds!"
           " Don't know what to do. Dropping particle");
      ABORT("panic");
      continue;  // drop this particle
    }
    int peer_rank = shuffle_data_target(p.indexed_prop);
    rctx->rank_bin_count[peer_rank]++;

    if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
      return 0;
    }

    fprintf(stderr, "Flushing ptcl rank: %d\n", peer_rank);
    // TODO: copy everything
    xn_shuffler_enqueue(static_cast<xn_ctx_t*>(ctx->rep), p.buf, p.buf_sz,
                        epoch, peer_rank, pctx.my_rank);
  }

  rctx->oob_count_right = 0;

  fprintf(stderr, "Returning from flush-rank %d (%d/%d)\n", pctx.my_rank,
          rctx->oob_count_left, rctx->oob_count_right);
}

int shuffle_write(shuffle_ctx_t* ctx, const char* fname,
                  unsigned char fname_len, char* data, unsigned char data_len,
                  int epoch) {
  char buf[255];
  int peer_rank = -1;
  int rank;
  int rv;


  range_ctx_t* rctx = &pctx.rctx;

  rctx->ts_writes_received++;

  assert(ctx == &pctx.sctx);
  assert(ctx->extra_data_len + ctx->data_len < 255 - ctx->fname_len - 1);
  if (ctx->fname_len != fname_len) ABORT("bad filename len");
  if (ctx->data_len != data_len) ABORT("bad data len");

  rank = shuffle_rank(ctx);

  unsigned char base_sz = 1 + fname_len + data_len;
  unsigned char buf_sz = base_sz + ctx->extra_data_len;

  float indexed_property =
      static_cast<float>(get_indexable_property(data, data_len));

  // fprintf(stderr, "Rank %d, energy %f\n", rank, indexed_property);

  buf_type_t buf_type = buf_type_t::RB_NO_BUF;

  /* There's always space in the buffers. If buffer becomes full with
   * current write, it will be cleared within current loop itself
   * TODO: change this assertion to a wait/lock depending on multi
   * threading design
   */
  assert(rctx->oob_left_count + rctx->oob_right_count <
         RANGE_TOTAL_OOB_THRESHOLD);

  if (RANGE_LEFT_OOB_FULL(rctx) || RANGE_RIGHT_OOB_FULL(rctx)) {
    logf(LOG_ERRO,
         "OOB buffers full (%d/%d)... don't know what to do with particles"
         " at rank %d\n",
         rctx->oob_count_left, rctx->oob_count_right, pctx.my_rank);
    // return 0;
    return -10;// XXX: lie
  }

  if (rctx->range_state == range_state_t::RS_INIT) {
    /* In the init state, we always buffer particles into oob_left
     * We can buffer them into left + right for more sampling before
     * negotn is triggered, but that would require a cross-buffer sorting
     * phase, so we just use oob_left as long as state is RS_INIT
     */
    buf_type = buf_type_t::RB_BUF_LEFT;
  } else if (rctx->range_state == range_state_t::RS_RENEGO) {
    buf_type = buf_type_t::RB_NO_BUF;
  } else if (indexed_property < rctx->range_min) {
    buf_type = buf_type_t::RB_BUF_LEFT;
  } else if (indexed_property > rctx->range_max) {
    buf_type = buf_type_t::RB_BUF_RIGHT;
  } else {
    buf_type = buf_type_t::RB_UNDECIDED;
  }

  if (buf_type == buf_type_t::RB_BUF_LEFT) {
    // fprintf(stderr, "Writing to idx %d of oob_left\n", rctx->oob_count_left);
    rctx->oob_buffer_left[rctx->oob_count_left].indexed_prop = indexed_property;
    buf_sz = msgfmt_write_data(rctx->oob_buffer_left[rctx->oob_count_left].buf,
                               RANGE_MAX_PSZ, fname, fname_len, data, data_len,
                               ctx->extra_data_len);
    rctx->oob_buffer_left[rctx->oob_count_left].buf_sz = buf_sz;
    rctx->oob_count_left++;
  } else if (buf_type == buf_type_t::RB_BUF_RIGHT) {
    // fprintf(stderr, "Writing to idx %d of oob_right\n",
    // rctx->oob_count_left);
    rctx->oob_buffer_right[rctx->oob_count_right].indexed_prop =
        indexed_property;
    buf_sz = msgfmt_write_data(
        rctx->oob_buffer_right[rctx->oob_count_right].buf, RANGE_MAX_PSZ, fname,
        fname_len, data, data_len, ctx->extra_data_len);
    rctx->oob_buffer_right[rctx->oob_count_right].buf_sz = buf_sz;

    rctx->oob_count_right++;
  } else {
    buf_sz = msgfmt_write_data(buf, 255, fname, fname_len, data, data_len,
                               ctx->extra_data_len);
  }

  fprintf(stderr, "shuffle_write main: r%d (%d/%d), ip: %.1f\n", pctx.my_rank,
          rctx->oob_count_left, rctx->oob_count_right, indexed_property);

  // XXX: temp until range is working
  // buf_sz = msgfmt_write_data(buf, 255, fname, fname_len, data, data_len,
  // ctx->extra_data_len);

  if (RANGE_LEFT_OOB_FULL(rctx) || RANGE_RIGHT_OOB_FULL(rctx)) {
    /* Buffering caused OOB_MAX, renegotiate */
    // for (int iii = 0; iii < 100; iii++) {
    fprintf(stderr, "--------- NEED RENEGO @ R%d ----------\n", pctx.my_rank);
    // if (0 == pctx.my_rank) 
      range_init_negotiation(&pctx);
    std::unique_lock<std::mutex> ulock(rctx->bin_access_m);

    /* we change the range state to prevent the CV from returning
     * immediately */
    if(range_state_t::RS_READY == pctx.rctx.range_state) {
      pctx.rctx.range_state = range_state_t::RS_BLOCKED;
      pctx.rctx.range_state_prev = range_state_t::RS_READY;
    }

    rctx->block_writes_cv.wait(ulock, [] {
      /* having a condition ensures we ignore spurious wakes */
      return (pctx.rctx.range_state == range_state_t::RS_READY);
    });

    fprintf(stderr, "--------- DONE RENEGO @ R%d ----------\n", pctx.my_rank);
    shuffle_flush_oob(ctx, rctx, epoch);
    logf(LOG_INFO, "Rank %d flushed its OOB buffers\n", pctx.my_rank);
    fprintf(stderr, "Main fn from flush-rank %d (%d/%d)\n", pctx.my_rank,
            rctx->oob_count_left, rctx->oob_count_right);
    // }
  }

  if (RANGE_BUF_OOB(buf_type)) {
    return 0;
  }

  if (buf_type == buf_type_t::RB_NO_BUF) {
    peer_rank = shuffle_data_target(indexed_property);
    fprintf(stderr, "Current particle %f to %d\n", indexed_property, peer_rank);
  }

  // XXX: temp until range is working
  // peer_rank = shuffle_target(ctx, buf, buf_sz);
  // fprintf(stderr, "Rank %d, peer rank: %d\n", pctx.my_rank, peer_rank);

  /* write trace if we are in testing mode */
  if (pctx.testin && pctx.trace != NULL)
    shuffle_write_debug(ctx, buf, buf_sz, epoch, rank, peer_rank);

  /* bypass rpc if target is local */
  if (peer_rank == rank && !ctx->force_rpc) {
    rv = native_write(fname, fname_len, data, data_len, epoch);
    return rv;
  }

  if (peer_rank == -1 || peer_rank >= pctx.comm_sz) {
    return 0;
  }

  rctx->ts_writes_shuffled++;

  fprintf(stderr, "At SRC: %d, SEND to %d, P: %02x%02x%02x\n", rank, peer_rank,
          buf[0], buf[1], buf[2]);

  if (ctx->type == SHUFFLE_XN) {
    xn_shuffler_enqueue(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz, epoch,
                        peer_rank, rank);
    // xn_shuffler_priority_send(static_cast<xn_ctx_t*>(ctx->rep), buf, buf_sz,
    // epoch, peer_rank, rank);
  } else {
    nn_shuffler_enqueue(buf, buf_sz, epoch, peer_rank, rank);
  }

  return 0;
}

namespace {
void shuffle_handle_debug(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz,
                          int epoch, int src, int dst) {
  const int h = pdlfs::xxhash32(buf, buf_sz, 0);

  fprintf(pctx.trace,
          "[RM] %u bytes (ep=%d) r%d << r%d "
          "(xx=%08x)\n",
          buf_sz, epoch, dst, src, h);
}
}  // namespace

int shuffle_handle(shuffle_ctx_t* ctx, char* buf, unsigned int buf_sz,
                   int epoch, int src, int dst) {
  int rv;

  // fprintf(stderr, "At DEST: %d, RCVD from %d, P: %02x%02x%02x\n", dst, src,
          // buf[0], buf[1], buf[2]);

  char msg_type = msgfmt_get_msgtype(buf);
  switch (msg_type) {
    case MSGFMT_DATA:
      // fprintf(stderr, "At DEST: %d, received MSGFMT_DATA\n", dst);
      break;
    case MSGFMT_RENEG_BEGIN:
      fprintf(stderr, "At DEST: %d, received RENEG_BEGIN\n", dst);
      // shouldn't take much time, can handle inline
      range_handle_reneg_begin(buf, buf_sz);
      return 0;
    case MSGFMT_RENEG_PIVOTS:
      fprintf(stderr, "At DEST: %d, received RENEG_PIVOTS\n", dst);
      // XXX: this is slow, should move to its own thread to prevent
      // head-of-line blocking
      range_handle_reneg_pivots(buf, buf_sz, src);
      return 0;
  }

  ctx = &pctx.sctx;

  if (buf_sz !=
      msgfmt_get_data_size(ctx->fname_len, ctx->data_len, ctx->extra_data_len))
    ABORT("unexpected incoming shuffle request size");

  char *fname, *fdata;
  msgfmt_parse_data(buf, buf_sz, &fname, ctx->fname_len, &fdata, ctx->data_len);

  rv = exotic_write(fname, ctx->fname_len, fdata, ctx->data_len, epoch, src);

  if (pctx.testin && pctx.trace != NULL)
    shuffle_handle_debug(ctx, buf, buf_sz, epoch, src, dst);

  return rv;
}

void shuffle_finalize(shuffle_ctx_t* ctx) {
  if (pctx.my_rank == 0) logf(LOG_INFO, "SHUFFLE SHUTDOWN BEGIN");
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN && ctx->rep != NULL) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(ctx->rep);
    xn_shuffler_destroy(rep);
    if (ctx->finalize_pause > 0) {
      sleep(ctx->finalize_pause);
    }
#ifndef NDEBUG
    unsigned long long sum_rpcs[2];
    unsigned long long min_rpcs[2];
    unsigned long long max_rpcs[2];
    unsigned long long rpcs[2];
    rpcs[0] = rep->stat.local.sends;
    rpcs[1] = rep->stat.remote.sends;
    MPI_Reduce(rpcs, sum_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, min_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(rpcs, max_rpcs, 2, MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0,
               MPI_COMM_WORLD);
    if (pctx.my_rank == 0 && (sum_rpcs[0] + sum_rpcs[1]) != 0) {
      logf(LOG_INFO,
           "[rpc] total sends: %s intra-node + %s inter-node = %s overall "
           ".....\n"
           " -> intra-node: %s per rank (min: %s, max: %s)\n"
           " -> inter-node: %s per rank (min: %s, max: %s)\n"
           " //",
           pretty_num(sum_rpcs[0]).c_str(), pretty_num(sum_rpcs[1]).c_str(),
           pretty_num(sum_rpcs[0] + sum_rpcs[1]).c_str(),
           pretty_num(double(sum_rpcs[0]) / pctx.comm_sz).c_str(),
           pretty_num(min_rpcs[0]).c_str(), pretty_num(max_rpcs[0]).c_str(),
           pretty_num(double(sum_rpcs[1]) / pctx.comm_sz).c_str(),
           pretty_num(min_rpcs[1]).c_str(), pretty_num(max_rpcs[1]).c_str());
    }
#endif
    ctx->rep = NULL;
    free(rep);
  } else {
    hstg_t hg_intvl;
    int p[] = {10, 30, 50, 70, 90, 95, 96, 97, 98, 99};
    double d[] = {99.5,  99.7,   99.9,   99.95,  99.97,
                  99.99, 99.995, 99.997, 99.999, 99.9999};
#define NUM_RUSAGE (sizeof(nnctx.r) / sizeof(nn_rusage_t))
    nn_rusage_t total_rusage_recv[NUM_RUSAGE];
    nn_rusage_t total_rusage[NUM_RUSAGE];
    unsigned long long total_writes;
    unsigned long long total_msgsz;
    hstg_t iq_dep;
    nn_shuffler_destroy();
    if (ctx->finalize_pause > 0) {
      sleep(ctx->finalize_pause);
    }
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "[nn] per-thread cpu usage ... (s)");
      logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_rank",
           "SYS_per_rank", "TOTAL_per_rank");
    }
    for (size_t i = 0; i < NUM_RUSAGE; i++) {
      if (nnctx.r[i].tag[0] != 0) {
        MPI_Reduce(&nnctx.r[i].usr_micros, &total_rusage[i].usr_micros, 1,
                   MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        MPI_Reduce(&nnctx.r[i].sys_micros, &total_rusage[i].sys_micros, 1,
                   MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
        if (pctx.my_rank == 0) {
          logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
               double(total_rusage[i].usr_micros) / 1000000 / pctx.comm_sz,
               double(total_rusage[i].sys_micros) / 1000000 / pctx.comm_sz,
               double(total_rusage[i].usr_micros + total_rusage[i].sys_micros) /
                   1000000 / pctx.comm_sz);
        }
      }
    }
    if (!shuffle_is_everyone_receiver(ctx)) {
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_recv",
             "SYS_per_recv", "TOTAL_per_recv");
      }
      for (size_t i = 0; i < NUM_RUSAGE; i++) {
        if (nnctx.r[i].tag[0] != 0 && pctx.recv_comm != MPI_COMM_NULL) {
          MPI_Reduce(&nnctx.r[i].usr_micros, &total_rusage_recv[i].usr_micros,
                     1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, pctx.recv_comm);
          MPI_Reduce(&nnctx.r[i].sys_micros, &total_rusage_recv[i].sys_micros,
                     1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, pctx.recv_comm);
          if (pctx.my_rank == 0) {
            logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
                 double(total_rusage_recv[i].usr_micros) / 1000000 /
                     pctx.recv_sz,
                 double(total_rusage_recv[i].sys_micros) / 1000000 /
                     pctx.recv_sz,
                 double(total_rusage_recv[i].usr_micros +
                        total_rusage_recv[i].sys_micros) /
                     1000000 / pctx.recv_sz);
          }
        }
      }
      if (pctx.my_rank == 0) {
        logf(LOG_INFO, "                %-16s%-16s%-16s", "USR_per_nonrecv",
             "SYS_per_nonrecv", "TOTAL_per_nonrecv");
      }
      for (size_t i = 0; i < NUM_RUSAGE; i++) {
        if (nnctx.r[i].tag[0] != 0 && pctx.recv_comm != MPI_COMM_NULL) {
          if (pctx.my_rank == 0) {
            logf(LOG_INFO, "  %-8s CPU: %-16.3f%-16.3f%-16.3f", nnctx.r[i].tag,
                 double(total_rusage[i].usr_micros -
                        total_rusage_recv[i].usr_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz),
                 double(total_rusage[i].sys_micros -
                        total_rusage_recv[i].sys_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz),
                 double(total_rusage[i].usr_micros -
                        total_rusage_recv[i].usr_micros +
                        total_rusage[i].sys_micros -
                        total_rusage_recv[i].sys_micros) /
                     1000000 / (pctx.comm_sz - pctx.recv_sz));
            ;
          }
        }
      }
    }
    if (pctx.recv_comm != MPI_COMM_NULL) {
      memset(&hg_intvl, 0, sizeof(hstg_t));
      hstg_reset_min(hg_intvl);
      hstg_reduce(nnctx.hg_intvl, hg_intvl, pctx.recv_comm);
      if (pctx.my_rank == 0 && hstg_num(hg_intvl) >= 1.0) {
        logf(LOG_INFO, "[nn] hg_progress interval ... (ms)");
        logf(LOG_INFO, "  %s samples, avg: %.3f (min: %.0f, max: %.0f)",
             pretty_num(hstg_num(hg_intvl)).c_str(), hstg_avg(hg_intvl),
             hstg_min(hg_intvl), hstg_max(hg_intvl));
        for (size_t i = 0; i < sizeof(p) / sizeof(int); i++) {
          logf(LOG_INFO, "    - %d%% %-12.2f %.4f%% %.2f", p[i],
               hstg_ptile(hg_intvl, p[i]), d[i], hstg_ptile(hg_intvl, d[i]));
        }
      }
      memset(&iq_dep, 0, sizeof(hstg_t));
      hstg_reset_min(iq_dep);
      hstg_reduce(nnctx.iq_dep, iq_dep, pctx.recv_comm);
      MPI_Reduce(&nnctx.total_writes, &total_writes, 1, MPI_UNSIGNED_LONG_LONG,
                 MPI_SUM, 0, pctx.recv_comm);
      MPI_Reduce(&nnctx.total_msgsz, &total_msgsz, 1, MPI_UNSIGNED_LONG_LONG,
                 MPI_SUM, 0, pctx.recv_comm);
      if (pctx.my_rank == 0 && hstg_num(iq_dep) >= 1.0) {
        logf(LOG_INFO,
             "[nn] avg rpc size: %s (%s writes per rpc, %s per write)",
             pretty_size(double(total_msgsz) / hstg_sum(iq_dep)).c_str(),
             pretty_num(double(total_writes) / hstg_sum(iq_dep)).c_str(),
             pretty_size(double(total_msgsz) / double(total_writes)).c_str());
        logf(LOG_INFO, "[nn] rpc incoming queue depth ...");
        logf(LOG_INFO, "  %s samples, avg: %.3f (min: %.0f, max: %.0f)",
             pretty_num(hstg_num(iq_dep)).c_str(), hstg_avg(iq_dep),
             hstg_min(iq_dep), hstg_max(iq_dep));
        for (size_t i = 0; i < sizeof(p) / sizeof(int); i++) {
          logf(LOG_INFO, "    - %d%% %-12.2f %.4f%% %.2f", p[i],
               hstg_ptile(iq_dep, p[i]), d[i], hstg_ptile(iq_dep, d[i]));
        }
      }
    }
#undef NUM_RUSAGE
  }
  if (ctx->chp != NULL) {
    ch_placement_finalize(ctx->chp);
    ctx->chp = NULL;
  }
  if (pctx.my_rank == 0) logf(LOG_INFO, "SHUFFLE SHUTDOWN OVER");
}

namespace {
/* convert an integer number to an unsigned char */
unsigned char TOUCHAR(int input) {
  assert(input >= 0 && input <= 255);
  unsigned char rv = static_cast<unsigned char>(input);
  return rv;
}
}  // namespace

void shuffle_init(shuffle_ctx_t* ctx) {
  const char* proto;
  const char* env;
  int world_sz;
  int vf;
  int n;

  assert(ctx != NULL);

  ctx->fname_len = TOUCHAR(pctx.particle_id_size);
  ctx->extra_data_len = TOUCHAR(pctx.particle_extra_size);
  if (pctx.sideft) {
    ctx->data_len = 0;
  } else if (pctx.sideio) {
    ctx->data_len = 8;
  } else {
    ctx->data_len = TOUCHAR(pctx.particle_size);
  }
  if (ctx->extra_data_len + ctx->data_len > 255 - ctx->fname_len - 1)
    ABORT("bad shuffle conf: id + data exceeds 255 bytes");
  if (ctx->fname_len == 0) {
    ABORT("bad shuffle conf: id size is zero");
  }

  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "shuffle format: K = %u (+ 1) bytes, V = %u bytes",
         ctx->fname_len, ctx->extra_data_len + ctx->data_len);
  }

  ctx->receiver_rate = 1;
  ctx->receiver_mask = ~static_cast<unsigned int>(0);
  env = maybe_getenv("SHUFFLE_Recv_radix");
  if (env != NULL) {
    n = atoi(env);
    if (n > 8) n = 8;
    if (n > 0) {
      ctx->receiver_rate <<= n;
      ctx->receiver_mask <<= n;
    }
  }
  ctx->is_receiver = shuffle_is_rank_receiver(ctx, pctx.my_rank);
  if (pctx.my_rank == 0) {
    logf(LOG_INFO, "%u shuffle senders per receiver\n>>> receiver mask is %#x",
         ctx->receiver_rate, ctx->receiver_mask);
  }

  env = maybe_getenv("SHUFFLE_Finalize_pause");
  if (env != NULL) {
    ctx->finalize_pause = atoi(env);
    if (ctx->finalize_pause < 0) {
      ctx->finalize_pause = 0;
    }
  }
  if (pctx.my_rank == 0) {
    if (ctx->finalize_pause > 0) {
      logf(LOG_INFO, "shuffle finalize pause: %d secs", ctx->finalize_pause);
    }
  }
  if (is_envset("SHUFFLE_Force_rpc")) {
    ctx->force_rpc = 1;
  }
  if (pctx.my_rank == 0) {
    if (!ctx->force_rpc) {
      logf(LOG_WARN,
           "shuffle force_rpc is OFF (will skip shuffle if addr is "
           "local)\n>>> "
           "main thread may be blocked on writing");
    } else {
      logf(LOG_INFO,
           "shuffle force_rpc is ON\n>>> "
           "will always invoke shuffle even addr is local");
    }
  }
  if (is_envset("SHUFFLE_Use_multihop")) {
    ctx->type = SHUFFLE_XN;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO, "using the scalable multi-hop shuffler");
    }
  } else {
    ctx->type = SHUFFLE_NN;
    if (pctx.my_rank == 0) {
      logf(LOG_INFO,
           "using the default NN shuffler: code might not scale well\n>>> "
           "switch to the multi-hop shuffler for better scalability");
    }
  }
  if (ctx->type == SHUFFLE_XN) {
    xn_ctx_t* rep = static_cast<xn_ctx_t*>(malloc(sizeof(xn_ctx_t)));
    memset(rep, 0, sizeof(xn_ctx_t));
    xn_shuffler_init(rep);
    world_sz = xn_shuffler_world_size(rep);
    ctx->rep = rep;
  } else {
    nn_shuffler_init(ctx);
    world_sz = nn_shuffler_world_size();
  }

  if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
    env = maybe_getenv("SHUFFLE_Virtual_factor");
    if (env == NULL) {
      vf = DEFAULT_VIRTUAL_FACTOR;
    } else {
      vf = atoi(env);
    }

    proto = maybe_getenv("SHUFFLE_Placement_protocol");
    if (proto == NULL) {
      proto = DEFAULT_PLACEMENT_PROTO;
    }

    ctx->chp = ch_placement_initialize(proto, world_sz, vf /* vir factor */,
                                       0 /* hash seed */);
    if (ctx->chp == NULL) {
      ABORT("ch_init");
    }
  }

  if (pctx.my_rank == 0) {
    if (!IS_BYPASS_PLACEMENT(pctx.mode)) {
      logf(LOG_INFO,
           "ch-placement group size: %s (vir-factor: %s, proto: %s)\n>>> "
           "possible protocols are: "
           "static_modulo, hash_lookup3, xor, and ring",
           pretty_num(world_sz).c_str(), pretty_num(vf).c_str(), proto);
    } else {
      logf(LOG_INFO, "ch-placement bypassed");
    }
  }

  if (pctx.my_rank == 0 && pctx.verbose) {
    logf(LOG_INFO, "HG is configured as follows ...");
    fputs(" > HG_HAS_POST_LIMIT=", stderr);
#ifdef HG_HAS_POST_LIMIT
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);

    fputs(" > HG_HAS_SELF_FORWARD=", stderr);
#ifdef HG_HAS_SELF_FORWARD
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);

    fputs(" > HG_HAS_CHECKSUMS=", stderr);
#ifdef HG_HAS_CHECKSUMS
    fputs("ON", stderr);
#else
    fputs("NO", stderr);
#endif
    fputc('\n', stderr);
  }
}

int shuffle_is_everyone_receiver(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  return ctx->receiver_rate == 1;
}

int shuffle_is_rank_receiver(shuffle_ctx_t* ctx, int rank) {
  assert(ctx != NULL);
  if (ctx->receiver_rate == 1) return 1;
  return (rank & ctx->receiver_mask) == rank;
}

int shuffle_world_sz(shuffle_ctx* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    return xn_shuffler_world_size(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    return nn_shuffler_world_size();
  }
}

int shuffle_rank(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    return xn_shuffler_my_rank(static_cast<xn_ctx_t*>(ctx->rep));
  } else {
    return nn_shuffler_my_rank();
  }
}

void shuffle_resume(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    // TODO
  } else {
    nn_shuffler_wakeup();
  }
}

void shuffle_pause(shuffle_ctx_t* ctx) {
  assert(ctx != NULL);
  if (ctx->type == SHUFFLE_XN) {
    // TODO
  } else {
    nn_shuffler_sleep();
  }
}

void shuffle_msg_sent(size_t n, void** arg1, void** arg2) {
  pctx.mctx.min_nms++;
  pctx.mctx.max_nms++;
  pctx.mctx.nms++;
}

void shuffle_msg_replied(void* arg1, void* arg2) {
  pctx.mctx.nmd++; /* delivered */
}

void shuffle_msg_received() {
  pctx.mctx.min_nmr++;
  pctx.mctx.max_nmr++;
  pctx.mctx.nmr++;
}
