#ifndef MASTER_H
#define MASTER_H

#include <stdbool.h>
#include <sys/types.h>

#include "config.h"
#include "event_loop.h"
#include "server.h"
#include "task.h"

typedef struct {
  int port;
  int tcp_backlog;
  int hz;
  int client_timeout_s;
  int n_reduce;
  int task_timeout_ms;
  char *input_dir;
  char *aof_path;
} master_config_t;

typedef struct {
  master_config_t config;
  server_t server;

  /* Application spine. */
  job_t job;
  task_t maps[MAX_MAP_TASKS];
  task_t reduces[MAX_REDUCE_TASKS];
  worker_t workers[MAX_FDS]; /* application state per fd */

  int64_t now_realtime_ms; /* wall clock, for persisted timestamps */
  int aof_fd;
} master_t;

extern master_t master;

int master_main(const char *configfile);

#endif
