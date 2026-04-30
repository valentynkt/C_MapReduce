#ifndef SERVER_H
#define SERVER_H

#include "config.h"
#include "event_loop.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct {
  bool active;
  int64_t last_active_ms;
  char buf[MSG_MAX + FRAME_HDR_SIZE]; /* read buffer */
  size_t len;                         /* read buffer: bytes accumulated */
  char wbuf[WBUF_SIZE];               /* write buffer */
  size_t wlen;                        /* write buffer: total bytes queued */
  size_t woff;                        /* write buffer: bytes already sent */
} client_t;
typedef int (*server_on_message_fn)(int fd, const char *payload, size_t len,
                                    void *user_data);
typedef void (*server_on_connect_fn)(int fd, void *user_data);
typedef void (*server_on_disconnect_fn)(int fd, void *user_data);
typedef void (*server_on_periodic_fn)(void *user_data);

typedef struct {
  int listen_fd;
  event_loop_t el;
  client_t clients[MAX_FDS];
  size_t clients_count;
  int pipe[2];

  int64_t cronloops;
  int64_t now_ms;
  int64_t last_timeout_check_ms;
  int client_timeout_s;

  server_on_message_fn on_message;
  void *on_message_data;
  server_on_connect_fn on_connect;
  void *on_connect_data;
  server_on_disconnect_fn on_disconnect;
  void *on_disconnect_data;
  server_on_periodic_fn on_periodic;
  void *on_periodic_data;
} server_t;

int server_init(server_t *s, int port, int tcp_backlog, int hz,
                int client_timeout_s);
int server_run(server_t *s);
void server_shutdown(server_t *s);

void server_set_on_message_cb(server_t *s, server_on_message_fn fn,
                              void *user_data);
void server_set_on_connect_cb(server_t *s, server_on_connect_fn fn,
                              void *user_data);
void server_set_on_disconnect_cb(server_t *s, server_on_disconnect_fn fn,
                                 void *user_data);
void server_set_on_periodic_cb(server_t *s, server_on_periodic_fn fn,
                               void *user_data);

int server_send(server_t *s, int fd, const char *payload, uint32_t payload_len);

#endif
