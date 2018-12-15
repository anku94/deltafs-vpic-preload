//
// Created by Ankush J on 11/24/18.
//

#include "shuffler_udf.h"
#include "loadbalance_util.h"
#include <cstdio>
#include <cmath>

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
  float f[10];
  memcpy(f, data, 40);

  double energy = compute_energy(f[5], f[6], f[7]);
  this->running_total += energy;
  this->running_square += (energy * energy);

  this->running_num++;

  // this->running_px += f[5];
  // this->running_px2 += (f[5] * f[5]);

  // this->running_py += f[6];
  // this->running_py2 += (f[6] * f[6]);

  // this->running_pz += f[7];
  // this->running_pz2 += (f[7] * f[7]);

  int rv;
  if (pctx->sctx.has_bins) {
    // printf("--> safe shuffle write at rank %d\n", pctx->my_rank);
    rv = shuffle_write(&pctx->sctx, fname, fname_len, data, data_len, epoch);
  } else {
    // printf("--> writing %s to buffer, rank %d\n", fname, pctx->my_rank);
    rv = buffer_write(&pctx->sctx, fname, fname_len, data, data_len, epoch);
  }
  // printf("------- particle %s -------\n", fname);
  // printf("step*dt: %f\n", f[0]);
  // printf("pdx: %f, dy: %f, dz: %f\n", f[1], f[2], f[3]);
  // printf("id: %d\n", (int) f[4]);
  // printf("pux: %f, uy: %f, uz: %f\n", f[5], f[6], f[7]);
  // printf("charge: %d\n", (int) f[8]);
  // printf("tag: %d\n", (long int) f[9]);
  //fprintf(this->dump_file, "!!step: %f, name: %s, traj: %f %f %f, ener: %f %f %f\n", f[0], fname, f[1], f[2], f[3], f[5], f[6], f[7]);
  fprintf(this->dump_file, "fname: %s, s: %f, e: %lf\n", fname, f[0], energy);

  if (this->running_num == 500) {
    double all_total = 0;
    double all_square = 0;

    // double all_px = 0;
    // double all_px2 = 0;

    // double all_py = 0;
    // double all_py2 = 0;

    // double all_pz = 0;
    // double all_pz2 = 0;

    long int all_num = 0;
    MPI_Reduce(const_cast<double *>(&this->running_total), 
        &all_total, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(const_cast<double *>(&this->running_square), 
        &all_square, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    // MPI_Reduce(const_cast<double *>(&this->running_px), 
        // &all_px, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    // MPI_Reduce(const_cast<double *>(&this->running_px2), 
        // &all_px2, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    // MPI_Reduce(const_cast<double *>(&this->running_py), 
        // &all_py, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    // MPI_Reduce(const_cast<double *>(&this->running_py2), 
        // &all_py2, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    // MPI_Reduce(const_cast<double *>(&this->running_pz), 
        // &all_pz, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    // MPI_Reduce(const_cast<double *>(&this->running_pz2), 
        // &all_pz2, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    MPI_Reduce(const_cast<long int *>(&this->running_num), 
        &all_num, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (this->pctx->my_rank == 0) {
      printf("---> Post Reduce at rank 0: %lf %lf %ld\n", all_total, all_square, all_num);
    }

    MPI_Bcast(&all_total, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&all_square, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // MPI_Bcast(&all_px, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    // MPI_Bcast(&all_px2, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // MPI_Bcast(&all_py, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    // MPI_Bcast(&all_py2, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    // MPI_Bcast(&all_pz, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    // MPI_Bcast(&all_pz2, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    MPI_Bcast(&all_num, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    printf("---> Post Reduce at all: %lf %lf %ld\n", all_total, all_square, all_num);
    // printf("---> Px px2: %lf %lf\n", all_px, all_px2);
    // printf("---> Py py2: %lf %lf\n", all_py, all_py2);
    // printf("---> Pz pz2: %lf %lf\n", all_pz, all_pz2);

    double mu = all_total / all_num;
    double sigma2 = (all_square / all_num) - (mu * mu);
    double sigma = sqrt(sigma2);

    // printf("---> Distribution looks like: %lf %lf\n", mu, sigma);

    // fill bins into pctx->sctx->dest_bins
    int ret = gaussian_buckets(mu, sigma, pctx->sctx.dest_bins, pctx->comm_sz);
    // int ret = get_buckets(all_px, all_px2, all_py, all_py2, all_pz, all_pz2,
        // pctx->sctx.dest_bins, all_num, pctx->comm_sz);

    if (pctx->my_rank == 0) {
      printf("--> bucket distrib: ");
      for(int gidx = 0; gidx <= pctx->comm_sz; gidx++) {
        printf("%lf ", pctx->sctx.dest_bins[gidx]);
      }
      printf("\n");
    }

    printf("---> Ret: %d\n", ret);

    assert(ret == 0);
    // if (ret == 0) {
      pctx->sctx.has_bins = true;
    // }

    // flush map
    int flush_count = 0;
    for (auto it = pctx->sctx.temp_buffer.begin(); it != pctx->sctx.temp_buffer.end(); it++) {
      std::string fname = it->first;
      std::string fdata = it->second;
      // printf("--> processing %s %s at %d\n", it->first.c_str(), it->second.c_str(), pctx->my_rank);
      int name_len = fname.length();
      int data_len = fdata.length();
      // assume same epoch
      char fdata_str[255];
      strncpy(fdata_str, fdata.c_str(), data_len);
      rv &= shuffle_write(&pctx->sctx, fname.c_str(), name_len, fdata_str, data_len, epoch);
      flush_count++;
    }

    printf("--> rank %d, epoch: %d, flush_count: %d\n", pctx->my_rank, epoch, flush_count);

    pctx->sctx.temp_buffer.clear();

  }

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
  fclose(this->dump_file);

  printf("Running numbers: total: %lf, square: %lf, num: %ld\n",
      this->running_total, this->running_square, this->running_num);

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
  char final_path[256];
  const char *home_dir = "/users/ankushj";
  snprintf(final_path, 256, "%s/%s/%s.%d.%d", home_dir, "all_dumps", "dump", this->pctx->my_rank, num_eps);
  printf("Dumping particle data to: %s\n", final_path);
  this->dump_file = fopen(final_path, "w+");
  assert(this->dump_file);

  this->running_total = 0;
  this->running_square = 0;
  this->running_num = 0;

  // this->running_px = 0;
  // this->running_px2 = 0;

  // this->running_py = 0;
  // this->running_py2 = 0;

  // this->running_pz = 0;
  // this->running_pz2 = 0;

  pctx->sctx.has_bins = false;

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
