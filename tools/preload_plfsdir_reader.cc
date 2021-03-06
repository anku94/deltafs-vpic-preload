/*
 * Copyright (c) 2018, Carnegie Mellon University.
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

/*
 * preload_plfsdir_reader.cc
 *
 * a simple reader program for reading data out of a plfsdir.
 */

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <deltafs/deltafs_api.h>

#include <algorithm>
#include <string>
#include <vector>

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
static char* argv0;      /* argv[0], program name */
static deltafs_tp_t* tp; /* plfsdir worker thread pool */
static char cf[500];     /* plfsdir conf str */
static struct deltafs_conf {
  int num_epochs;
  int key_size;
  int value_size;
  char* filter_bits_per_key;
  char* memtable_size;
  int lg_parts;
  int skip_crc32c;
  int bypass_shuffle;
  int force_leveldb_format;
  int unordered_storage;
  int io_engine;
  int comm_sz;
} c; /* plfsdir conf */

/*
 * vcomplain/complain about something and exit.
 */
static void vcomplain(const char* format, va_list ap) {
  fprintf(stderr, "!!! ERROR !!! %s: ", argv0);
  vfprintf(stderr, format, ap);
  fprintf(stderr, "\n");
  exit(1);
}

static void complain(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vcomplain(format, ap);
  va_end(ap);
}

/*
 * print info messages.
 */
static void vinfo(const char* format, va_list ap) {
  printf("-INFO- ");
  vprintf(format, ap);
  printf("\n");
}

static void info(const char* format, ...) {
  va_list ap;
  va_start(ap, format);
  vinfo(format, ap);
  va_end(ap);
}

/*
 * now: get current time in micros
 */
static uint64_t now() {
  struct timeval tv;
  uint64_t rv;

  gettimeofday(&tv, NULL);
  rv = tv.tv_sec * 1000000LLU + tv.tv_usec;

  return rv;
}

/*
 * end of helper/utility functions.
 */

/*
 * default values
 */
#define DEF_TIMEOUT 300 /* alarm timeout */

/*
 * gs: shared global data (e.g. from the command line)
 */
struct gs {
  int a;         /* anti-shuffle mode (query rank 0 names across all ranks)*/
  int r;         /* number of ranks to read */
  int d;         /* number of names to read per rank */
  int bg;        /* number of background worker threads */
  char* in;      /* path to the input dir */
  char* dirname; /* dir name (path to dir storage) */
  int nobf;      /* ignore bloom filters */
  int crc32c;    /* verify checksums */
  int paranoid;  /* paranoid checks */
  int timeout;   /* alarm timeout */
  int v;         /* be verbose */
} g;

/*
 * ms: measurements
 */
struct ms {
  std::vector<uint64_t>* latencies;
  uint64_t partitions;     /* num data partitions touched */
  uint64_t ops;            /* num read ops */
  uint64_t okops;          /* num read ops that return non-empty data */
  uint64_t bytes;          /* total amount of data queries */
  uint64_t under_bytes;    /* total amount of underlying data retrieved */
  uint64_t under_files;    /* total amount of underlying files opened */
  uint64_t under_seeks;    /* total amount of underlying storage seeks */
  uint64_t table_seeks[3]; /* sum/min/max sstable opened */
  uint64_t seeks[3];       /* sum/min/max data block fetched */
#define SUM 0
#define MIN 1
#define MAX 2
} m;

/*
 * report: print performance measurements
 */
static void report() {
  if (m.ops == 0) return;
  printf("\n");
  printf("=== Query Results ===\n");
  printf("[R] Total Epochs: %d\n", c.num_epochs);
  printf("[R] Total Data Partitions: %d (%lu queried)\n", c.comm_sz,
         m.partitions);
  if (!c.io_engine)
    printf("[R] Total Data Subpartitions: %d\n", c.comm_sz * (1 << c.lg_parts));
  printf("[R] Total Query Ops: %lu (%lu ok ops)\n", m.ops, m.okops);
  if (m.okops != 0)
    printf("[R] Total Data Queried: %lu bytes (%lu per entry per epoch)\n",
           m.bytes, m.bytes / m.okops / c.num_epochs);
  if (!c.io_engine)
    printf("[R] SST Touched Per Query: %.3f (min: %lu, max: %lu)\n",
           double(m.table_seeks[SUM]) / m.ops, m.table_seeks[MIN],
           m.table_seeks[MAX]);
  if (!c.io_engine)
    printf("[R] SST Data Blocks Fetched Per Query: %.3f (min: %lu, max: %lu)\n",
           double(m.seeks[SUM]) / m.ops, m.seeks[MIN], m.seeks[MAX]);
  printf("[R] Total Under Storage Seeks: %lu\n", m.under_seeks);
  printf("[R] Total Under Data Read: %lu bytes\n", m.under_bytes);
  printf("[R] Total Under Files Opened: %lu\n", m.under_files);
  std::vector<uint64_t>* const lat = m.latencies;
  if (lat != NULL && lat->size() != 0) {
    std::sort(lat->begin(), lat->end());
    uint64_t sum = 0;
    std::vector<uint64_t>::iterator it = lat->begin();
    for (; it != lat->end(); ++it) sum += *it;
    printf("[R] Latency Per Query: %.3f (med: %3f, min: %.3f, max %.3f) ms\n",
           double(sum) / m.ops / 1000,
           double((*lat)[(lat->size() - 1) / 2]) / 1000,
           double((*lat)[0]) / 1000, double((*lat)[lat->size() - 1]) / 1000);
    printf("[R] Total Read Latency: %.6f s\n", double(sum) / 1000 / 1000);
  }
  printf("[R] Dir IO Engine: %d\n", c.io_engine);
  printf("[R] MemTable Size: %s\n", c.memtable_size);
  printf("[R] BF Bits: %s\n", c.filter_bits_per_key);
  printf("\n");
}

/*
 * alarm signal handler
 */
static void sigalarm(int foo) {
  fprintf(stderr, "!!! SIGALRM detected !!!\n");
  fprintf(stderr, "Alarm clock\n");
  exit(1);
}

/*
 * usage
 */
static void usage(const char* msg) {
  if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
  fprintf(stderr, "usage: %s [options] plfsdir infodir\n", argv0);
  fprintf(stderr, "\noptions:\n");
  fprintf(stderr, "\t-a        enable the special anti-shuffle mode\n");
  fprintf(stderr, "\t-r ranks  number of ranks to read\n");
  fprintf(stderr, "\t-d depth  number of names to read per rank\n");
  fprintf(stderr, "\t-j num    number of background worker threads\n");
  fprintf(stderr, "\t-t sec    timeout (alarm), in seconds\n");
  fprintf(stderr, "\t-i        ignore bloom filters\n");
  fprintf(stderr, "\t-c        verify crc32c (for both data and indexes)\n");
  fprintf(stderr, "\t-k        force paranoid checks\n");
  fprintf(stderr, "\t-v        be verbose\n");
  exit(1);
}

/*
 * get_manifest: parse the conf from the dir manifest file
 */
static void get_manifest() {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  snprintf(fname, sizeof(fname), "%s/MANIFEST", g.in);
  f = fopen(fname, "r");
  if (!f) complain("error opening %s: %s", fname, strerror(errno));

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
    if (strncmp(ch, "num_epochs=", strlen("num_epochs=")) == 0) {
      c.num_epochs = atoi(ch + strlen("num_epochs="));
      if (c.num_epochs < 0) complain("bad num_epochs from manifest");
    } else if (strncmp(ch, "key_size=", strlen("key_size=")) == 0) {
      c.key_size = atoi(ch + strlen("key_size="));
      if (c.key_size < 0) complain("bad key_size from manifest");
    } else if (strncmp(ch, "value_size=", strlen("value_size=")) == 0) {
      c.value_size = atoi(ch + strlen("value_size="));
      if (c.value_size < 0) complain("bad value_size from manifest");
    } else if (strncmp(ch, "filter_bits_per_key=",
                       strlen("filter_bits_per_key=")) == 0) {
      c.filter_bits_per_key = strdup(ch + strlen("filter_bits_per_key="));
      if (c.filter_bits_per_key[0] != 0 &&
          c.filter_bits_per_key[strlen(c.filter_bits_per_key) - 1] == '\n')
        c.filter_bits_per_key[strlen(c.filter_bits_per_key) - 1] = 0;
    } else if (strncmp(ch, "memtable_size=", strlen("memtable_size=")) == 0) {
      c.memtable_size = strdup(ch + strlen("memtable_size="));
      if (c.memtable_size[0] != 0 &&
          c.memtable_size[strlen(c.memtable_size) - 1] == '\n')
        c.memtable_size[strlen(c.memtable_size) - 1] = 0;
    } else if (strncmp(ch, "lg_parts=", strlen("lg_parts=")) == 0) {
      c.lg_parts = atoi(ch + strlen("lg_parts="));
      if (c.lg_parts < 0) complain("bad lg_parts from manifest");
    } else if (strncmp(ch, "skip_checksums=", strlen("skip_checksums=")) == 0) {
      c.skip_crc32c = atoi(ch + strlen("skip_checksums="));
      if (c.skip_crc32c < 0) complain("bad skip_checksums from manifest");
    } else if (strncmp(ch, "bypass_shuffle=", strlen("bypass_shuffle=")) == 0) {
      c.bypass_shuffle = atoi(ch + strlen("bypass_shuffle="));
      if (c.bypass_shuffle < 0) complain("bad bypass_shuffle from manifest");
    } else if (strncmp(ch, "force_leveldb_format=",
                       strlen("force_leveldb_format=")) == 0) {
      c.force_leveldb_format = atoi(ch + strlen("force_leveldb_format="));
      if (c.force_leveldb_format < 0)
        complain("bad force_leveldb_format from manifest");
    } else if (strncmp(ch, "unordered_storage=",
                       strlen("unordered_storage=")) == 0) {
      c.unordered_storage = atoi(ch + strlen("unordered_storage="));
      if (c.unordered_storage < 0)
        complain("bad unordered_storage from manifest");
    } else if (strncmp(ch, "io_engine=", strlen("io_engine=")) == 0) {
      c.io_engine = atoi(ch + strlen("io_engine="));
      if (c.io_engine < 0) complain("bad io_engine from manifest");
    } else if (strncmp(ch, "comm_sz=", strlen("comm_sz=")) == 0) {
      c.comm_sz = atoi(ch + strlen("comm_sz="));
      if (c.comm_sz < 0) complain("bad comm_sz from manifests");
    }
  }

  if (ferror(f)) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  if (c.key_size == 0 || c.comm_sz == 0)
    complain("bad manifest: key_size or comm_sz is 0?!");

  fclose(f);
}

/*
 * prepare_conf: generate plfsdir conf
 */
static void prepare_conf(int rank, int* io_engine, int* unordered,
                         int* force_leveldb_fmt) {
  int n;

  if (g.bg && !tp) tp = deltafs_tp_init(g.bg);
  if (g.bg && !tp) complain("fail to init thread pool");

  n = snprintf(cf, sizeof(cf), "rank=%d", rank);
  n += snprintf(cf + n, sizeof(cf) - n, "&key_size=%d", c.key_size);
  n += snprintf(cf + n, sizeof(cf) - n, "&memtable_size=%s", c.memtable_size);
  n += snprintf(cf + n, sizeof(cf) - n, "&bf_bits_per_key=%s",
                c.filter_bits_per_key);

  if (!c.io_engine) {
    n += snprintf(cf + n, sizeof(cf) - n, "&num_epochs=%d", c.num_epochs);
    n += snprintf(cf + n, sizeof(cf) - n, "&skip_checksums=%d", c.skip_crc32c);
    n += snprintf(cf + n, sizeof(cf) - n, "&verify_checksums=%d", g.crc32c);
    n += snprintf(cf + n, sizeof(cf) - n, "&paranoid_checks=%d", g.paranoid);
    n += snprintf(cf + n, sizeof(cf) - n, "&parallel_reads=%d", g.bg != 0);
    n += snprintf(cf + n, sizeof(cf) - n, "&ignore_filters=%d", g.nobf);
    snprintf(cf + n, sizeof(cf) - n, "&lg_parts=%d", c.lg_parts);
  }

  *force_leveldb_fmt = c.force_leveldb_format;
  *unordered = c.unordered_storage;
  *io_engine = c.io_engine;
#ifndef NDEBUG
  info(cf);
#endif
}

/*
 * do_read: read from plfsdir and measure the performance.
 */
static void do_read(deltafs_plfsdir_t* dir, const char* name) {
  char* data;
  uint64_t start;
  uint64_t end;
  size_t table_seeks;
  size_t seeks;
  size_t sz;

  table_seeks = seeks = 0;
  start = now();

  data = static_cast<char*>(
      deltafs_plfsdir_read(dir, name, -1, &sz, &table_seeks, &seeks));
  if (data == NULL) {
    complain("error reading %s: %s", name, strerror(errno));
  } else if (sz == 0 && !g.a && !c.bypass_shuffle && c.value_size != 0) {
    complain("file %s is empty!!", name);
  }

  end = now();

  free(data);

  m.latencies->push_back(end - start);
  m.table_seeks[SUM] += table_seeks;
  m.table_seeks[MIN] = std::min<uint64_t>(table_seeks, m.table_seeks[MIN]);
  m.table_seeks[MAX] = std::max<uint64_t>(table_seeks, m.table_seeks[MAX]);
  m.seeks[SUM] += seeks;
  m.seeks[MIN] = std::min<uint64_t>(seeks, m.seeks[MIN]);
  m.seeks[MAX] = std::max<uint64_t>(seeks, m.seeks[MAX]);
  m.bytes += sz;
  if (sz != 0) m.okops++;
  m.ops++;
}

/*
 * get_names: load names from an input source.
 */
static void get_names(int rank, std::vector<std::string>* results) {
  char* ch;
  char fname[PATH_MAX];
  char tmp[100];
  FILE* f;

  results->clear();

  snprintf(fname, sizeof(fname), "%s/NAMES-%07d.txt", g.in, rank);
  f = fopen(fname, "r");
  if (!f) complain("error opening %s: %s", fname, strerror(errno));

  while ((ch = fgets(tmp, sizeof(tmp), f)) != NULL) {
    results->push_back(std::string(ch, strlen(ch) - 1));
  }

  if (ferror(f)) {
    complain("error reading %s: %s", fname, strerror(errno));
  }

  fclose(f);
}

/*
 * run_queries: open plfsdir and do reads on a specific rank.
 */
static void run_queries(int rank) {
  std::vector<std::string> names;
  deltafs_plfsdir_t* dir;
  int unordered;
  int force_leveldb_fmt;
  int io_engine;
  int r;

  get_names((g.a || c.bypass_shuffle) ? 0 : rank, &names);
  std::random_shuffle(names.begin(), names.end());
  prepare_conf(rank, &io_engine, &unordered, &force_leveldb_fmt);

  dir = deltafs_plfsdir_create_handle(cf, O_RDONLY, io_engine);
  if (!dir) complain("fail to create dir handle");
  deltafs_plfsdir_enable_io_measurement(dir, 1);
  deltafs_plfsdir_force_leveldb_fmt(dir, force_leveldb_fmt);
  deltafs_plfsdir_set_unordered(dir, unordered);
  deltafs_plfsdir_set_fixed_kv(dir, 1);
  if (tp) deltafs_plfsdir_set_thread_pool(dir, tp);

  r = deltafs_plfsdir_open(dir, g.dirname);
  if (r) complain("error opening plfsdir: %s", strerror(errno));

  if (g.v)
    info("rank %d (%d reads) ...\t\t(%d samples available)", rank,
         std::min(g.d, int(names.size())), int(names.size()));
  for (int i = 0; i < g.d && i < int(names.size()); i++) {
    do_read(dir, names[i].c_str());
  }

  m.under_bytes +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_bytes_read");
  m.under_files +=
      deltafs_plfsdir_get_integer_property(dir, "io.total_read_open");
  m.under_seeks += deltafs_plfsdir_get_integer_property(dir, "io.total_seeks");
  deltafs_plfsdir_free_handle(dir);

  m.partitions++;
}

/*
 * main program
 */
int main(int argc, char* argv[]) {
  std::vector<int> ranks;
  int nranks;
  int ch;

  argv0 = argv[0];
  memset(cf, 0, sizeof(cf));
  tp = NULL;

  /* we want lines, even if we are writing to a pipe */
  setlinebuf(stdout);

  /* setup default to zero/null, except as noted below */
  memset(&g, 0, sizeof(g));
  g.timeout = DEF_TIMEOUT;
  while ((ch = getopt(argc, argv, "ar:d:j:t:ickv")) != -1) {
    switch (ch) {
      case 'a':
        g.a = 1;
        break;
      case 'r':
        g.r = atoi(optarg);
        if (g.r < 0) usage("bad rank number");
        break;
      case 'd':
        g.d = atoi(optarg);
        if (g.d < 0) usage("bad depth");
        break;
      case 'j':
        g.bg = atoi(optarg);
        if (g.bg < 0) usage("bad bg number");
        break;
      case 't':
        g.timeout = atoi(optarg);
        if (g.timeout < 0) usage("bad timeout");
        break;
      case 'i':
        g.nobf = 1;
        break;
      case 'c':
        g.crc32c = 1;
        break;
      case 'k':
        g.paranoid = 1;
        break;
      case 'v':
        g.v = 1;
        break;
      default:
        usage(NULL);
    }
  }
  argc -= optind;
  argv += optind;

  if (argc != 2) /* plfsdir and infodir must be provided on cli */
    usage("bad args");
  g.dirname = argv[0];
  g.in = argv[1];

  if (access(g.dirname, R_OK) != 0)
    complain("cannot access %s: %s", g.dirname, strerror(errno));
  if (access(g.in, R_OK) != 0)
    complain("cannot access %s: %s", g.in, strerror(errno));

  memset(&c, 0, sizeof(c));
  get_manifest();

  printf("\n%s\n==options:\n", argv0);
  printf("\tqueries: %d x %d (ranks x reads)\n", g.r, g.d);
  printf("\tnum bg threads: %d (reader thread pool)\n", g.bg);
  printf("\tanti-shuffle: %d\n", g.a);
  printf("\tinfodir: %s\n", g.in);
  printf("\tplfsdir: %s\n", g.dirname);
  printf("\ttimeout: %d s\n", g.timeout);
  printf("\tignore bloom filters: %d\n", g.nobf);
  printf("\tverify crc32: %d\n", g.crc32c);
  printf("\tparanoid checks: %d\n", g.paranoid);
  printf("\tverbose: %d\n", g.v);
  printf("\n==dir manifest\n");
  printf("\tio engine: %d\n", c.io_engine);
  printf("\tforce leveldb format: %d\n", c.force_leveldb_format);
  printf("\tunordered storage: %d\n", c.unordered_storage);
  printf("\tnum epochs: %d\n", c.num_epochs);
  printf("\tkey size: %d bytes\n", c.key_size);
  printf("\tvalue size: %d bytes\n", c.value_size);
  printf("\tmemtable size: %s\n", c.memtable_size);
  printf("\tfilter bits per key: %s\n", c.filter_bits_per_key);
  printf("\tskip crc32c: %d\n", c.skip_crc32c);
  printf("\tbypass shuffle: %d\n", c.bypass_shuffle);
  printf("\tlg parts: %d\n", c.lg_parts);
  printf("\tcomm sz: %d\n", c.comm_sz);
  printf("\n");

  signal(SIGALRM, sigalarm);
  alarm(g.timeout);

  memset(&m, 0, sizeof(m));
  m.latencies = new std::vector<uint64_t>;
  m.table_seeks[MIN] = ULONG_LONG_MAX;
  m.seeks[MIN] = ULONG_LONG_MAX;
  for (int i = 0; i < c.comm_sz; i++) {
    ranks.push_back(i);
  }
  std::random_shuffle(ranks.begin(), ranks.end());
  nranks = (g.a || c.bypass_shuffle) ? c.comm_sz : g.r;
  if (g.v) info("start queries (%d ranks) ...", std::min(nranks, c.comm_sz));
  for (int i = 0; i < nranks && i < c.comm_sz; i++) {
    run_queries(ranks[i]);
  }
  report();

  if (tp) deltafs_tp_close(tp);
  if (c.memtable_size) free(c.memtable_size);
  if (c.filter_bits_per_key) free(c.filter_bits_per_key);
  delete m.latencies;

  if (g.v) info("all done!");
  if (g.v) info("bye");

  exit(0);
}
