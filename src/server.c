#include "server.h"
#include "config.h"
#include "event_loop.h"
#include "util.h"
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
static void on_write(event_loop_t *el, int fd);

static volatile sig_atomic_t shutdown_signaled = 0;
static volatile sig_atomic_t shutdown_pipe_fd = -1;

static void sig_shutdown_handler(int sig) {
  if (shutdown_signaled && sig == SIGINT) {
    static const char msg[] = "FORCE SHUTDOWN\n";
    write(STDERR_FILENO, msg, sizeof(msg) - 1);
    _exit(1);
  }
  shutdown_signaled = 1;

  if (shutdown_pipe_fd >= 0) {
    unsigned char byte = (unsigned char)sig;
    write(shutdown_pipe_fd, &byte, 1);
  }
}

static void setup_signal_handlers(void) {
  struct sigaction sig_act;
  sigemptyset(&sig_act.sa_mask);
  sig_act.sa_flags = 0;
  sig_act.sa_handler = sig_shutdown_handler;
  sigaction(SIGINT, &sig_act, NULL);
  sigaction(SIGTERM, &sig_act, NULL);
}
/* 2. Generic infrastructure */

static int create_listener(int port, int backlog) {
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

/* 3. Per-client lifecycle */

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

/* 4. Framing layer */

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
    if (s->on_message) {
      int rc = s->on_message(fd, payload, payload_len, s->on_message_data);
      if (rc != 0) {
        remove_client(s, el, fd);
        return; /* client_t zeroed; do not touch c->buf / c->len */
      }
    }

    size_t frame_size = FRAME_HDR_SIZE + payload_len;
    memmove(c->buf, c->buf + frame_size, c->len - frame_size);
    c->len -= frame_size;
  }
}

/* 5. Event-loop callbacks */

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

static void on_accept(event_loop_t *el, int server_fd) {
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
    return;
  }

  if (s->on_connect) {
    s->on_connect(fd, s->on_connect_data);
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

/* 6. Housekeeping */

static void check_client_timeouts(server_t *s, event_loop_t *el) {
  size_t found = 0;
  for (int i = 0; i < MAX_FDS && found < s->clients_count; i++) {
    if (!s->clients[i].active)
      continue;
    found++;
    int64_t idle_s = (s->now_ms - s->clients[i].last_active_ms) / 1000;
    if (idle_s >= s->client_timeout_s) {
      printf("client timed out (fd=%d, idle=%llds)\n", i, (long long)idle_s);
      remove_client(s, el, i);
    }
  }
}

static void client_timeouts_cron(server_t *s, event_loop_t *el) {
  if (s->now_ms - s->last_timeout_check_ms < CLIENT_TIMEOUT_CHECK_INTERVAL_MS)
    return;
  s->last_timeout_check_ms = s->now_ms;
  check_client_timeouts(s, el);
}

static void server_before_sleep(event_loop_t *el) {
  server_t *s = container_of(el, server_t, el);
  s->now_ms = now_ms();
  s->cronloops++;
  client_timeouts_cron(s, el);
  if (s->on_periodic) {
    s->on_periodic(s->on_periodic_data);
  }
}

/* 7. Public API */

int server_init(server_t *s, int port, int tcp_backlog, int hz,
                int client_timeout_s) {
  /* server_t is ~13MB (clients[]). compound literal would blow the stack. */
  memset(s, 0, sizeof(*s));

  s->client_timeout_s = client_timeout_s;
  s->now_ms = now_ms();
  s->last_timeout_check_ms = s->now_ms;

  s->listen_fd = create_listener(port, tcp_backlog);
  if (s->listen_fd == -1) {
    return -1;
  }

  if (el_init(&s->el, 1000 / hz) == -1) {
    close(s->listen_fd);
    return -1;
  }
  s->el.before_sleep_proc = server_before_sleep;

  if (pipe(s->pipe) == -1 || set_non_blocking(s->pipe[0]) == -1 ||
      set_non_blocking(s->pipe[1]) == -1) {
    close(s->listen_fd);
    el_cleanup(&s->el);
    return -1;
  }

  shutdown_pipe_fd = s->pipe[1];
  setup_signal_handlers();

  if (el_add(&s->el, s->pipe[0], on_shutdown) == -1)
    goto fail;
  if (el_add(&s->el, s->listen_fd, on_accept) == -1)
    goto fail;
  return 0;
fail:
  server_shutdown(s);
  return -1;
}
int server_run(server_t *s) { return el_run(&s->el); }
void server_shutdown(server_t *s) {
  for (int fd = 0; fd < MAX_FDS; fd++) {
    if (s->clients[fd].active) {
      shutdown(fd, SHUT_WR);
      close(fd);
    }
  }
  close(s->listen_fd);
  close(s->pipe[0]);
  close(s->pipe[1]);
  el_cleanup(&s->el);
}

void server_set_on_message_cb(server_t *s, server_on_message_fn fn,
                              void *user_data) {
  s->on_message = fn;
  s->on_message_data = user_data;
}

void server_set_on_connect_cb(server_t *s, server_on_connect_fn fn,
                              void *user_data) {
  s->on_connect = fn;
  s->on_connect_data = user_data;
}

void server_set_on_disconnect_cb(server_t *s, server_on_disconnect_fn fn,
                                 void *user_data) {
  s->on_disconnect = fn;
  s->on_disconnect_data = user_data;
}

void server_set_on_periodic_cb(server_t *s, server_on_periodic_fn fn,
                               void *user_data) {
  s->on_periodic = fn;
  s->on_periodic_data = user_data;
}

int server_send(server_t *s, int fd, const char *payload,
                uint32_t payload_len) {
  return send_framed(s, &s->el, fd, payload, payload_len) ? 0 : -1;
}
