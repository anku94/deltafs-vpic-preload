#include "msgfmt.h"
#include "common.h"

uint32_t msgfmt_get_data_size(int fname_sz, int data_sz, int extra_data_sz) {
  return MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz + extra_data_sz;
}

uint32_t msgfmt_write_data(char *buf, int buf_sz, const char *fname,
                           int fname_sz, const char *fdata, int data_sz,
                           int extra_data_sz) {
  uint32_t base_sz = MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz;

  if (base_sz > buf_sz) {
    fprintf(stderr, "Base size: %u buf size: %u\n", base_sz, buf_sz);
    ABORT("Buffer overflow");
  }

  buf[0] = MSGFMT_DATA;

  memcpy(buf + MSGFMT_TYPE_SIZE, fname, fname_sz);
  buf[MSGFMT_TYPE_SIZE + fname_sz] = 0;
  memcpy(buf + MSGFMT_TYPE_SIZE + fname_sz + 1, fdata, data_sz);

  memset(buf + base_sz, 0, buf_sz - base_sz);

  return base_sz + extra_data_sz;
}

void msgfmt_parse_data(char *buf, int buf_sz, char **fname, int fname_sz,
                       char **fdata, int data_sz) {
  uint32_t base_sz = MSGFMT_TYPE_SIZE + fname_sz + 1 + data_sz;
  if (base_sz > buf_sz) {
    ABORT("Buffer overflow");
  }

  if (buf[0] != MSGFMT_DATA) {
    ABORT("Invalid data fmt");
  }

  (*fname) = &buf[MSGFMT_TYPE_SIZE];
  (*fdata) = &buf[MSGFMT_TYPE_SIZE + fname_sz + 1];

  return;
}

uint32_t msgfmt_encode_reneg_begin(char *buf, int buf_sz, int round_no,
                                   int my_rank) {
  assert(((uint32_t) buf_sz) >= MSGFMT_TYPE_SIZE + 2 * sizeof(int));

  fprintf(stderr, "ENCODE reneg begin round num: %d\n", round_no);
  fprintf(stderr, "ENCODE reneg begin RANK: %d\n", my_rank);

  buf[0] = MSGFMT_RENEG_BEGIN;
  memcpy(buf + 1, reinterpret_cast<void *>(&round_no), sizeof(int));
  memcpy(buf + 1 + sizeof(int), reinterpret_cast<void *>(&my_rank),
         sizeof(int));
  // memcpy(&buf[1], static_cast<void *>(&my_rank), sizeof(int));

  return MSGFMT_TYPE_SIZE + 2*sizeof(int);
}

void msgfmt_parse_reneg_begin(char *buf, int buf_sz, int *round_no,
                              int *my_rank) {
  assert((uint32_t)buf_sz >= MSGFMT_TYPE_SIZE + sizeof(int));
  assert(MSGFMT_RENEG_BEGIN == buf[0]);

  (*round_no) = *(reinterpret_cast<int *>(buf + 1));
  (*my_rank) = *(reinterpret_cast<int *>(buf + 1 + sizeof(int)));

  return;
}

unsigned char msgfmt_get_msgtype(char *buf) { return buf[0]; }

uint32_t msgfmt_nbytes_reneg_pivots(int num_pivots) {
  /* One extra float for pivot width */
  uint32_t data_bytes = (num_pivots + 1) * sizeof(float);
  /* One int for sender_rank, another for round_no  */
  uint32_t header = MSGFMT_TYPE_SIZE + 2 * sizeof(int);

  return header + data_bytes;
}

void msgfmt_encode_reneg_pivots(char *buf, int buf_sz, int round_no,
                                float *pivots, float pivot_width,
                                int num_pivots) {
  int bytes_reqd = msgfmt_nbytes_reneg_pivots(num_pivots);
  assert(buf_sz >= bytes_reqd);

  /* message type_id */
  buf[0] = MSGFMT_RENEG_PIVOTS;
  memcpy(buf + 1, &round_no, sizeof(int));
  /* num_pivots */
  memcpy(buf + 1 + sizeof(int), &num_pivots, sizeof(int));
  /* pivot width */
  memcpy(buf + 1 + 2 * sizeof(int), &pivot_width, sizeof(float));
  /* actual pivots */
  memcpy(buf + 1 + 2 * sizeof(int) + sizeof(float), pivots,
         sizeof(float) * num_pivots);
  memset(&buf[bytes_reqd], 0, buf_sz - bytes_reqd);

  return;
}

void msgfmt_parse_reneg_pivots(char *buf, int buf_sz, int *round_no,
                               float **pivots, float *pivot_width,
                               int *num_pivots) {
  assert(MSGFMT_RENEG_PIVOTS == buf[0]);

  int *round_num_ptr = reinterpret_cast<int *>(buf + 1);
  (*round_no) = (*round_num_ptr);

  int *num_pivots_ptr = reinterpret_cast<int *>(buf + 1 + sizeof(int));
  (*num_pivots) = (*num_pivots_ptr);

  float *pivot_width_ptr = reinterpret_cast<float *>(buf + 1 + 2 * sizeof(int));
  (*pivot_width) = (*pivot_width_ptr);

  int bytes_reqd = msgfmt_nbytes_reneg_pivots(*num_pivots);
  assert(buf_sz >= bytes_reqd);

  (*pivots) =
      reinterpret_cast<float *>(buf + 1 + 2 * sizeof(int) + sizeof(float));
}
