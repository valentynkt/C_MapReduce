#include "server.h"
#include "config.h"
#include "event_loop.h"
#include "util.h"
#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static void on_write(event_loop_t *el, int fd);

static void server_before_sleep(event_loop_t *el) {
  server_t *s = container_of(el, server_t, el);
  s->now_ms = now_ms();
  s->cronloops++;
  // client_timeouts_cron(s, el);
  if (s->on_periodic) {
    s->on_periodic(s->on_disconnect_data);
  }
}

static void remove_client(server_t *s, event_loop_t *el, int fd) {
  printf("client disconnected (fd=%d)\n", fd);
  el_remove(el, fd);
  close(fd);

  s->clients[fd] = (client_t){0};
  s->clients_count--;
  if (s->on_disconnect) {
    s->on_disconnect(fd, s->on_disconnect_data);
  }
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
static bool queue_write(server_t *s, event_loop_t *el, int fd, const char *data,
                        size_t len) {
  client_t *c = &s->clients[fd];

  if (c->wlen + len > WBUF_SIZE && c->woff > 0) {
    size_t pending = c->wlen - c->woff;
    memmove(c->wbuf, c->wbuf + c->woff, pending);
    c->wlen = pending;
    c->woff = 0;
  }

  if (c->wlen + len > WBUF_SIZE)
    return false;

  bool buf_was_empty = (c->wlen == 0);

  memcpy(c->wbuf + c->wlen, data, len);
  c->wlen += len;

  /* Fast path: try direct write if no prior data was pending */
  if (buf_was_empty) {
    ssize_t n = write(fd, c->wbuf + c->woff, c->wlen - c->woff);
    if (n == -1) {
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        perror("write");
        remove_client(s, el, fd);
        return false;
      }
      /* EAGAIN/EINTR: write nothing, fall through to register handler */
    } else {
      c->woff += n;
      if (c->woff == c->wlen) {
        /* everything sent, skip kqueue */
        c->woff = 0;
        c->wlen = 0;
        return true;
      }
    }
  }

  /* Slow path: data remains unsent, let the event loop drain it */
  el_add_write(el, fd, on_write);
  return true;
}

static bool send_framed(server_t *s, event_loop_t *el, int fd,
                        const char *payload, uint32_t payload_len) {
  char frame[FRAME_HDR_SIZE + MSG_MAX];
  uint32_t net_len = htonl(payload_len);
  memcpy(frame, &net_len, FRAME_HDR_SIZE);
  memcpy(frame + FRAME_HDR_SIZE, payload, payload_len);
  return queue_write(s, el, fd, frame, FRAME_HDR_SIZE + payload_len);
}

static void process_buffer(server_t *s, event_loop_t *el, int fd) {
  client_t *c = &s->clients[fd];

  while (c->len >= FRAME_HDR_SIZE) {
    uint32_t net_len;
    memcpy(&net_len, c->buf, FRAME_HDR_SIZE);
    uint32_t payload_len = ntohl(net_len);

    if (payload_len > MSG_MAX) {
      remove_client(s, el, fd);
      return;
    }
    if (c->len < FRAME_HDR_SIZE + payload_len)
      return;

    char *payload = c->buf + FRAME_HDR_SIZE;
    fprintf(stderr, "[net] received %u bytes from fd=%d (echo)\n", payload_len,
            fd);
    /* int rc = s->on_message(fd, payload, palyoda_len, s->on_message_data) */
    if (s->on_message) {
      int rc = s->on_message(fd, payload, payload_len, s->on_message_data);
      if (rc != 0) {
        remove_client(s, el, fd);
      }
    }
    // if (!send_framed(s, el, fd, payload, payload_len))
    // return;

    size_t frame_size = FRAME_HDR_SIZE + payload_len;
    memmove(c->buf, c->buf + frame_size, c->len - frame_size);
    c->len -= frame_size;
  }
}

static void on_write(event_loop_t *el, int fd) {
  server_t *s = container_of(el, server_t, el);
  client_t *c = &s->clients[fd];

  ssize_t n = write(fd, c->wbuf + c->woff, c->wlen - c->woff);
  if (n == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
      return;
    perror("write");
    remove_client(s, el, fd);
    return;
  }

  c->woff += n;

  if (c->woff == c->wlen) {
    c->woff = 0;
    c->wlen = 0;
    el_remove_write(el, fd);

    process_buffer(s, el, fd);
  }
}

static void on_read(event_loop_t *el, int fd) {

  server_t *s = container_of(el, server_t, el);
  client_t *c = &s->clients[fd];

  if (!c->active)
    return;

  ssize_t n = read(fd, c->buf + c->len, sizeof(c->buf) - c->len);

  if (n == 0) {
    remove_client(s, el, fd);
    return;
  }
  if (n == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
      return;
    perror("read");
    remove_client(s, el, fd);
    return;
  }

  c->last_active_ms = s->now_ms;
  c->len += n;
  process_buffer(s, el, fd);
}

void on_accept(event_loop_t *el, int server_fd) {
  server_t *s = container_of(el, server_t, el);

  int fd = accept(server_fd, NULL, NULL);
  if (fd == -1) {
    if (errno != EAGAIN && errno != EWOULDBLOCK)
      perror("accept");
    return;
  }
  if (fd >= MAX_FDS) {
    fprintf(stderr, "fd %d >= MAX_FDS, rejecting\n", fd);
    close(fd);
    return;
  }
  if (set_non_blocking(fd) == -1) {
    close(fd);
    return;
  }

  s->clients[fd] = (client_t){.active = true, .last_active_ms = s->now_ms};
  s->clients_count++;
  printf("client connected (fd=%d)\n", fd);

  if (el_add(el, fd, on_read) == -1) {
    close(fd);
    s->clients[fd] = (client_t){0};
    s->clients_count--;
  }
}

static void check_client_timeouts(server_t *s, event_loop_t *el) {
  size_t found = 0;
  for (int i = 0; i < MAX_FDS && found < s->clients_count; i++) {
    if (!s->clients[i].active)
      continue;
    found++;
    int64_t idle_s = (s->now_ms - s->clients[i].last_active_ms) / 1000;
    if (idle_s >= s->config.client_timeout_s) {
      printf("client timed out (fd=%d, idle=%llds)\n", i, (long long)idle_s);
      remove_client(s, el, i);
    }
  }
}

void client_timeouts_cron(server_t *s, event_loop_t *el) {
  if (s->now_ms - s->last_timeout_check_ms < CLIENT_TIMEOUT_CHECK_INTERVAL_MS)
    return;
  s->last_timeout_check_ms = s->now_ms;
  check_client_timeouts(s, el);
}

int create_listener(int port, int backlog) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    perror("socket");
    return -1;
  }

  int opt = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
    perror("setsockopt");
    goto fail;
  }

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
      .sin_addr.s_addr = INADDR_ANY,
  };

  if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("bind");
    goto fail;
  }

  if (listen(fd, backlog) == -1) {
    perror("listen");
    goto fail;
  }

  if (set_non_blocking(fd) == -1) {
    close(fd);
    return -1;
  }
  return fd;

fail:
  close(fd);
  return -1;
}

int server_init(server_t *s, int port, int tcp_backlog, int hz,
                int client_timeout_s) {}
int server_run(server_t *s) {}
void server_shutdown(server_t *s) {}
