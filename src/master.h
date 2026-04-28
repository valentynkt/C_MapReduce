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
} master_config_t;

typedef struct {
  master_config_t config;
  worker_t workers[MAX_FDS]; /* application state per fd */
  server_t server;
  int64_t now_realtime_ms; /* wall clock, for persisted timestamps */
} master_t;

extern master_t master;

int master_main(const char *configfile);

#endif
