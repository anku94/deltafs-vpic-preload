//
// Created by Ankush J on 11/24/18.
//

#pragma once

#include "preload_internal.h"

class udf_interface {
  public:
    virtual void init(preload_ctx_t *pctx_arg) = 0;
    virtual int process(const char* fname, unsigned char fname_len, char* data, unsigned char data_len, int epoch) = 0;
    virtual int pause() = 0;
    virtual int resume() = 0;
    virtual void finalize() = 0;
};

