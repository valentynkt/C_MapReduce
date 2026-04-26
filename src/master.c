
#ifndef SERVER_H
#define SERVER_H

#include <stdbool.h>
#include <sys/types.h>

#include "config.h"
#include "event_loop.h"
#include "master.h"
#include "networking.h"
#include "util.h"
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
master_t master;

/* --- housekeeping --- */

static void before_sleep(void) {
  master.now_ms = now_ms();
  master.now_realtime_ms = realtime_ms();
  master.cronloops++;

  client_timeouts_cron(&master.el);
}

/* --- signal handling --- */

static volatile sig_atomic_t shutdown_signaled = 0;

static void sig_shutdown_handler(int sig) {
  if (shutdown_signaled && sig == SIGINT) {
    static const char msg[] = "FORCE SHUTDOWN\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
  }
  shutdown_signaled = 1;

  unsigned char byte = (unsigned char)sig;
  write(master.pipe[1], &byte, 1);
}

static void setup_signal_handlers(void) {
  struct sigaction sig_act;
  sigemptyset(&sig_act.sa_mask);
  sig_act.sa_flags = 0;
  sig_act.sa_handler = sig_shutdown_handler;
  sigaction(SIGINT, &sig_act, NULL);
  sigaction(SIGTERM, &sig_act, NULL);
}

static void on_shutdown(event_loop_t *el, int fd) {
  unsigned char buf[16];
  for (;;) {
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n <= 0)
      break;
    for (ssize_t i = 0; i < n; i++) {
      switch (buf[i]) {
      case SIGINT:
        fprintf(stderr, "Received SIGINT, shutting down...\n");
        break;
      case SIGTERM:
        fprintf(stderr, "Received SIGTERM, shutting down...\n");
        break;
      default:
        fprintf(stderr, "Received signal %d, shutting down...\n", buf[i]);
        break;
      }
    }
  }
  el->stop = 1;
}

/* --- lifecycle --- */
static int shutdown_pipe_setup(int master_fd) {
  if (pipe(master.pipe) == -1) {
    perror("pipe");
    close(master_fd);
    return EXIT_FAILURE;
  }

  if (set_non_blocking(master.pipe[0]) == -1 ||
      set_non_blocking(master.pipe[1]) == -1) {
    close(master_fd);
    close(master.pipe[0]);
    close(master.pipe[1]);
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}

static int init_master(void) {
  master.now_ms = now_ms();
  master.now_realtime_ms = realtime_ms();
  // master.db = db_create(master.now_ms);

  setup_signal_handlers();

  int master_fd =
      create_listener(master.config.port, master.config.tcp_backlog);
  if (master_fd == -1)
    return EXIT_FAILURE;

  printf("[bytekv] listening on port %d (hz=%d)\n", master.config.port,
         master.config.hz);

  if (el_init(&master.el) == -1) {
    close(master_fd);
    return EXIT_FAILURE;
  }
  master.el.before_sleep_proc = before_sleep;

  if (shutdown_pipe_setup(master_fd) == EXIT_FAILURE) {
    return EXIT_FAILURE;
  }

  master.master_fd = master_fd;

  if (el_add(&master.el, master.pipe[0], on_shutdown) == -1)
    goto fail;

  if (el_add(&master.el, master_fd, on_accept) == -1)
    goto fail;

  return EXIT_SUCCESS;

fail:
  close(master_fd);
  el_cleanup(&master.el);
  return EXIT_FAILURE;
}

static void shutdown_server(void) {
  for (int fd = 0; fd < MAX_FDS; fd++) {
    if (master.workers[fd].active) {
      shutdown(fd, SHUT_WR);
      close(fd);
    }
  }
  close(master.master_fd);
  close(master.pipe[0]);
  close(master.pipe[1]);
  el_cleanup(&master.el);
  // db_free(master.db);
}

int server_main(const char *configfile) {
  init_server_config();

  if (configfile) {
    if (load_config(configfile) == -1)
      return EXIT_FAILURE;
  }

  if (init_master() == EXIT_FAILURE) {
    // db_free(master.db);
    return EXIT_FAILURE;
  }

  el_run(&master.el);
  shutdown_server();
  return EXIT_SUCCESS;
}

#endif
