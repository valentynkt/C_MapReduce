#include "master.h"
#include "config.h"
#include "server.h"
#include "util.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

master_t master;

static int master_on_message(int fd, const char *payload, size_t len,
                             void *ud) {
  master_t *m = ud;
  /* TODO: rpc_dispatch */
  fprintf(stderr, "[master] message from fd=%d (%zu bytes)\n", fd, len);
  (void)m;
  (void)payload;
  return 0;
}

static void master_on_disconnect(int fd, void *ud) {
  master_t *m = ud;
  /* timeout sweep handles task reset, just clear the slot */
  m->workers[fd] = (worker_t){0};
}

static void master_periodic(void *ud) {
  master_t *m = ud;
  m->now_realtime_ms = realtime_ms();
  /* TODO: task_timeouts_check */
}
static int master_init(void) {
  master.now_realtime_ms = realtime_ms();

  if (server_init(&master.server, master.config.port, master.config.tcp_backlog,
                  master.config.hz, master.config.client_timeout_s) != 0) {
    return EXIT_FAILURE;
  }
  server_set_on_message_cb(&master.server, master_on_message, &master);
  server_set_on_disconnect_cb(&master.server, master_on_disconnect, &master);
  server_set_on_periodic_cb(&master.server, master_periodic, &master);

  printf("[mapreduce-master] listening on port %d (hz=%d)\n",
         master.config.port, master.config.hz);
  return EXIT_SUCCESS;
}

static void master_shutdown(void) { server_shutdown(&master.server); }

int master_main(const char *configfile) {
  master_config_init();

  if (configfile) {
    if (load_config(configfile) == -1)
      return EXIT_FAILURE;
  }

  if (master_init() == EXIT_FAILURE) {
    return EXIT_FAILURE;
  }

  server_run(&master.server);
  master_shutdown();
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
  const char *configfile = (argc >= 2) ? argv[1] : NULL;
  return master_main(configfile);
}
