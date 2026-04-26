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
  int client_timeout_check_hz;
} master_config_t;

typedef struct {
  master_config_t config;
  int listen_fd; /* TCP listener fd */
  event_loop_t el;
  client_t clients[MAX_FDS]; /* transport state per fd */
  worker_t workers[MAX_FDS]; /* application state per fd */
  size_t clients_count;
  int pipe[2];
  int64_t cronloops;
  int64_t now_ms;          /* monotonic, for elapsed time */
  int64_t now_realtime_ms; /* wall clock, for persisted timestamps */
} master_t;

extern master_t master;

int master_main(const char *configfile);

#endif
