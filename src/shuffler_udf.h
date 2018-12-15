//
// Created by Ankush J on 11/24/18.
//

#pragma once

#include <cassert>
#include "udf_interface.h"
#include "preload_internal.h"

class shuffler_udf : udf_interface {
  private:
    preload_ctx_t *pctx;
    double running_total;
    double running_square;

    // double running_px;
    // double running_px2;

    // double running_py;
    // double running_py2;

    // double running_pz;
    // double running_pz2;

    long int running_num;

    FILE *dump_file;
  public:
    shuffler_udf();
    ~shuffler_udf();
    void init(preload_ctx_t *pctx_arg);
    int process(const char* fname, unsigned char fname_len, char* data, unsigned char data_len, int epoch);
    int epoch_start(int num_eps);
    int epoch_end();
    int epoch_pre_start();
    int pause();
    int resume();
    void finalize();
};
