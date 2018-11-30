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
