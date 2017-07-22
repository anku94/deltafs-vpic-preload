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

#pragma once

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* a set of utilities for probing important system configurations. */

void maybe_warn_rlimit(int myrank, int worldsz);
void maybe_warn_cpuaffinity();
void try_scan_procfs();
void try_scan_sysfs();

/* get the number of cpu cores that we may use */
int my_cpu_cores();

/* get the current time in us. */
uint64_t now_micros();

/* log message into a given file using unbuffered io. */
inline void log(int fd, const char* fmt, ...) {
  char tmp[500];
  va_list va;
  int n;
  va_start(va, fmt);
  n = vsnprintf(tmp, sizeof(tmp), fmt, va);
  n = write(fd, tmp, n);
  va_end(va);
  errno = 0;
}

inline void info(const char* msg) { log(fileno(stderr), "-INFO- %s\n", msg); }
inline void warn(const char* msg) {
  log(fileno(stderr), "++ WARN ++ %s\n", msg);
}

inline void error(const char* msg) {
  if (errno != 0) {
    log(fileno(stderr), "!!! ERROR !!! %s: %s\n", msg, strerror(errno));
  } else {
    log(fileno(stderr), "!!! ERROR !!! %s\n", msg);
  }
}

inline void msg_abort(const char* msg) {
  if (errno != 0) {
    log(fileno(stderr), "*** ABORT *** %s: %s\n", msg, strerror(errno));
  } else {
    log(fileno(stderr), "*** ABORT *** %s\n", msg);
  }

  abort();
}

inline const char* maybe_getenv(const char* key) {
  const char* env = getenv(key);
  errno = 0;
  return (env);
}

inline bool is_envset(const char* key) {
  const char* env = getenv(key);
  errno = 0;
  if (env == NULL) {
    return (false);
  } else if (env[0] == '\0') {
    return (false);
  } else if (env[0] == '0') {
    return (false);
  } else {
    return (true);
  }
}

#ifndef PRELOAD_MUTEX_LOCKING

typedef int maybe_mutex_t;
typedef int maybe_mutexattr_t;
static inline int maybe_mutex_lock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_unlock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_trylock(maybe_mutex_t* __mut) { return 0; }
static inline int maybe_mutex_init(maybe_mutex_t* __mut,
                                   maybe_mutexattr_t* __attr) {
  return 0;
}
static inline int maybe_mutex_destroy(maybe_mutex_t* __mut) { return 0; }
#define MAYBE_MUTEX_INITIALIZER 0

#else

typedef pthread_mutex_t maybe_mutex_t;
typedef pthread_mutexattr_t maybe_mutexattr_t;
#define maybe_mutex_lock(__mut) pthread_mutex_lock(__mut)
#define maybe_mutex_unlock(__mut) pthread_mutex_unlock(__mut)
#define maybe_mutex_trylock(__mut) pthread_mutex_trylock(__mut)
#define maybe_mutex_init(__mut, __attr) pthread_mutex_init(__mut, __attr)
#define maybe_mutex_destroy(__mut) pthread_mutex_destroy(__mut)
#define MAYBE_MUTEX_INITIALIZER PTHREAD_MUTEX_INITIALIZER

#endif

inline void must_maybelockmutex(maybe_mutex_t* __mut) {
  int r = maybe_mutex_lock(__mut);
  if (r != 0) {
    msg_abort("mtx_lock");
  }
}

inline void must_maybeunlock(maybe_mutex_t* __mut) {
  int r = maybe_mutex_unlock(__mut);
  if (r != 0) {
    msg_abort("mtx_unlock");
  }
}

#include <string>

/* print a human-readable time duration. */
inline std::string pretty_dura(double us) {
  char tmp[100];
  if (us >= 1000000) {
    snprintf(tmp, sizeof(tmp), "%.3f s", us / 1000000.0);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f ms", us / 1000.0);
  }

  return (tmp);
}

/* print a human-readable integer number. */
inline std::string pretty_num(double num) {
  char tmp[100];
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (num >= 1099511627776.0) {
    num /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.1f Ti", num);
  } else if (num >= 1073741824.0) {
    num /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.1f Gi", num);
  } else if (num >= 1048576.0) {
    num /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.1f Mi", num);
  } else if (num >= 1024.0) {
    num /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.1f Ki", num);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f", num);
  }
#else
  if (num >= 1000000000000.0) {
    num /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f T", num);
  } else if (num >= 1000000000.0) {
    num /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f G", num);
  } else if (num >= 1000000.0) {
    num /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f M", num);
  } else if (num >= 1000.0) {
    num /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.1f K", num);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f", num);
  }
#endif

  return (tmp);
}

/* print a human-readable I/O throughput number. */
inline std::string pretty_tput(double ops, double us) {
  char tmp[100];
  double ops_per_s = ops / us * 1000000;
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (ops_per_s >= 1099511627776.0) {
    ops_per_s /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.3f Tiop/s", ops_per_s);
  } else if (ops_per_s >= 1073741824.0) {
    ops_per_s /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.3f Giop/s", ops_per_s);
  } else if (ops_per_s >= 1048576.0) {
    ops_per_s /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.3f Miop/s", ops_per_s);
  } else if (ops_per_s >= 1024.0) {
    ops_per_s /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.3f Kiop/s", ops_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f op/s", ops_per_s);
  }
#else
  if (ops_per_s >= 1000000000000.0) {
    ops_per_s /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Top/s", ops_per_s);
  } else if (ops_per_s >= 1000000000.0) {
    ops_per_s /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Gop/s", ops_per_s);
  } else if (ops_per_s >= 1000000.0) {
    ops_per_s /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Mop/s", ops_per_s);
  } else if (ops_per_s >= 1000.0) {
    ops_per_s /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.3f Kop/s", ops_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f op/s", ops_per_s);
  }
#endif

  return (tmp);
}

/* print a human-readable I/O size. */
inline std::string pretty_size(double size) {
  char tmp[100];
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (size >= 1099511627776.0) {
    size /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.1f TiB", size);
  } else if (size >= 1073741824.0) {
    size /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.1f GiB", size);
  } else if (size >= 1048576.0) {
    size /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.1f MiB", size);
  } else if (size >= 1024.0) {
    size /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.1f KiB", size);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f bytes", size);
  }
#else
  if (size >= 1000000000000.0) {
    size /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f TB", size);
  } else if (size >= 1000000000.0) {
    size /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f GB", size);
  } else if (size >= 1000000.0) {
    size /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.1f MB", size);
  } else if (size >= 1000.0) {
    size /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.1f KB", size);
  } else {
    snprintf(tmp, sizeof(tmp), "%.0f bytes", size);
  }
#endif

  return (tmp);
}

/* print a human-readable data bandwidth number. */
inline std::string pretty_bw(double bytes, double us) {
  char tmp[100];
  double bytes_per_s = bytes / us * 1000000;
#if defined(PRELOAD_PRETTY_USE_BINARY)
  if (bytes_per_s >= 1099511627776.0) {
    bytes_per_s /= 1099511627776.0;
    snprintf(tmp, sizeof(tmp), "%.3f TiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1073741824.0) {
    bytes_per_s /= 1073741824.0;
    snprintf(tmp, sizeof(tmp), "%.3f GiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1048576.0) {
    bytes_per_s /= 1048576.0;
    snprintf(tmp, sizeof(tmp), "%.3f MiB/s", bytes_per_s);
  } else if (bytes_per_s >= 1024.0) {
    bytes_per_s /= 1024.0;
    snprintf(tmp, sizeof(tmp), "%.3f KiB/s", bytes_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f bytes/s", bytes_per_s);
  }
#else
  if (bytes_per_s >= 1000000000000.0) {
    bytes_per_s /= 1000000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f TB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000000000.0) {
    bytes_per_s /= 1000000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f GB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000000.0) {
    bytes_per_s /= 1000000.0;
    snprintf(tmp, sizeof(tmp), "%.3f MB/s", bytes_per_s);
  } else if (bytes_per_s >= 1000.0) {
    bytes_per_s /= 1000.0;
    snprintf(tmp, sizeof(tmp), "%.3f KB/s", bytes_per_s);
  } else {
    snprintf(tmp, sizeof(tmp), "%.3f bytes/s", bytes_per_s);
  }
#endif

  return (tmp);
}