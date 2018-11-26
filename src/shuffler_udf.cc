//
// Created by Ankush J on 11/24/18.
//

#include "shuffler_udf.h"
#include <cstdio>

shuffler_udf ::shuffler_udf() {
  pctx = NULL;
}

shuffler_udf ::~shuffler_udf() {
  printf("bye world\n");
}

void shuffler_udf ::init(preload_ctx_t *pctx_arg) {
  pctx = pctx_arg;

  if (pctx->my_rank == 0) {
    logf(LOG_INFO, "shuffle starting ... (rank 0)");
    if (pctx->print_meminfo) {
      print_meminfo();
    }
  }
  shuffle_init(&pctx->sctx);
  /* ensures all peers have the shuffle ready */
  PRELOAD_Barrier(MPI_COMM_WORLD);
  if (pctx->my_rank == 0) {
    logf(LOG_INFO, "shuffle started (rank 0)");
    if (pctx->print_meminfo) {
      print_meminfo();
    }
  }
  if (!shuffle_is_everyone_receiver(&pctx->sctx)) {
    /* rank 0 must be a receiver */
    if (pctx->my_rank == 0)
      assert(shuffle_is_rank_receiver(&pctx->sctx, pctx->my_rank) != 0);
    int rv = MPI_Comm_split(
        MPI_COMM_WORLD,
        shuffle_is_rank_receiver(&pctx->sctx, pctx->my_rank) != 0
        ? 1
        : MPI_UNDEFINED,
        pctx->my_rank, &pctx->recv_comm);
    if (rv != MPI_SUCCESS) {
      ABORT("MPI_Comm_split");
    }
  }
  return;
}

int shuffler_udf ::process(const char* fname, unsigned char fname_len, char* data, unsigned char data_len, int epoch) {
  assert(pctx);
  int rv = shuffle_write(&pctx->sctx, fname, fname_len, data, data_len, epoch);
  return rv;
}

int shuffler_udf ::pause() {
  assert(pctx);
  shuffle_pause(&pctx->sctx);
  return 0;
}

int shuffler_udf ::resume() {
  assert(pctx);
  shuffle_resume(&pctx->sctx);
  return 0;
}

void shuffler_udf ::finalize() {
  uint64_t flush_start;
  uint64_t flush_end;

  assert(pctx);

  if (pctx->my_rank == 0) {
    logf(LOG_INFO, "shuffle shutting down ...");
  }
  /* ensures all peer messages are received */
  PRELOAD_Barrier(MPI_COMM_WORLD);
  /* shuffle flush */
  if (pctx->my_rank == 0) {
    flush_start = now_micros();
    logf(LOG_INFO, "flushing shuffle ... (rank 0)");
  }
  shuffle_epoch_start(&pctx->sctx);
  if (pctx->my_rank == 0) {
    flush_end = now_micros();
    logf(LOG_INFO, "flushing done %s",
        pretty_dura(flush_end - flush_start).c_str());
  }
  /*
   * ensures everyone has the flushing done before finalizing so we can get
   * up-to-date and consistent shuffle stats
   */
  PRELOAD_Barrier(MPI_COMM_WORLD);
  shuffle_finalize(&pctx->sctx);
  if (pctx->my_rank == 0) {
    logf(LOG_INFO, "shuffle off");
  }
  return;
}

int shuffler_udf ::epoch_end() {
  uint64_t flush_start;
  uint64_t flush_end;

  if (pctx->my_rank == 0) {
    flush_start = now_micros();
    logf(LOG_INFO, "flushing shuffle senders ... (rank 0)");
  }
  shuffle_epoch_end(&pctx->sctx);
  if (pctx->my_rank == 0) {
    flush_end = now_micros();
    logf(LOG_INFO, "sender flushing done %s",
        pretty_dura(flush_end - flush_start).c_str());
  }

  return 0;
}

int shuffler_udf ::epoch_pre_start() {
  uint64_t flush_start;
  uint64_t flush_end;

  if (pctx->my_rank == 0) {
    flush_start = now_micros();
    logf(LOG_INFO, "pre-flushing shuffle receivers ... (rank 0)");
  }
  shuffle_epoch_pre_start(&pctx->sctx);
  if (pctx->my_rank == 0) {
    flush_end = now_micros();
    logf(LOG_INFO, "receiver pre-flushing done %s",
        pretty_dura(flush_end - flush_start).c_str());
  }

  return 0;
}

int shuffler_udf ::epoch_start(int num_eps) {
  uint64_t flush_start;
  uint64_t flush_end;

  if (num_eps != 0) {
    if (pctx->my_rank == 0) {
      flush_start = now_micros();
      logf(LOG_INFO, "flushing shuffle receivers ... (rank 0)");
    }
    shuffle_epoch_start(&pctx->sctx);
    if (pctx->my_rank == 0) {
      flush_end = now_micros();
      logf(LOG_INFO, "receiver flushing done %s",
          pretty_dura(flush_end - flush_start).c_str());
    }
  }
  return 0;
}
