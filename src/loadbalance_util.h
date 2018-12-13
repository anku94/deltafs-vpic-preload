#pragma once

int gaussian_buckets(double mu, double sigma, double *bucket_out, int n);

double compute_energy(double ux, double uy, double uz);

double compute_energy(const char *data_blob);

int buffer_write(shuffle_ctx_t* ctx, const char* fname,
                  unsigned char fname_len, char* data, unsigned char data_len,
                  int epoch);

int binary_search(double *buckets, int n, double energy);
