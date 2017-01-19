/*
 * Copyright (c) 2016 Carnegie Mellon University.
 * Chuck Cranor <chuck@ece.cmu.edu>
 * George Amvrosiadis <gamvrosi@cs.cmu.edu>
 *
 * All rights reserved.
 *
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file. See the AUTHORS file for names of contributors.
 */

#ifdef NDEBUG
#undef NDEBUG    /* we always want to assert */
#endif

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include <mpi.h>

int main(int argc, char **argv) {
    FILE *fp;
    char fname[32], buf[33];
    int fd;

    memset(fname, 0, 32);
    memset(buf, 0, 33);

    /* No arguments. We will create a temporary file in /tmp. */
    if (argc != 1) {
        fprintf(stderr, "usage: %s\n", *argv);
        exit(1);
    }

    /* Generate a temporary filename */
    assert(strncpy(fname, "/tmp/preload-test.XXXXXX", 24));
    fd = mkstemp(fname);
    assert(fd > 0);
    close(fd);

    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        perror("Error - MPI_Init failed");
        exit(1);
    }

    fp = fopen(fname, "w");
    if (!fp) {
        perror("Error - fopen failed");
        goto error;
    }

    /* Write 32b of data */
    assert(fwrite("1234", 4, 1, fp) == 1);
    assert(fwrite("5678", 1, 4, fp) == 4);
    assert(fwrite("9", 1, 1, fp) == 1);
    assert(fwrite("0", 1, 1, fp) == 1);
    assert(fwrite("abcdefghijklmnopqrstuv", 1, 22, fp) == 22);

    assert(fclose(fp) == 0);

    MPI_Finalize();

    /* Check persisted data. Use unbuffered I/O (not preloaded). */
    fd = open(fname, O_RDONLY);
    if (fd < 0) {
        perror("Error - open failed");
        exit(1);
    }

    if (read(fd, buf, 32) != 32) {
        perror("Error - read failed");
        close(fd);
        exit(1);
    }

    close(fd);

    if (strcmp(buf, "1234567890abcdefghijklmnopqrstuv")) {
        fprintf(stderr, "Error: output did not match\n"
                "Want: 1234567890abcdefghijklmnopqrstuv\n"
                "Got:  %s\n", buf);
        exit(1);
    }

    exit(0);

error:
    MPI_Finalize();
    exit(1);
}
