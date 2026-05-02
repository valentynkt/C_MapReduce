#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "config.h"
#include "rpc.h"
#include "util.h"
#include "worker_map.h"

#define MAX_TOKENS_WORD 100

/* ----- helpers ----- */

static void sleep_ms(uint32_t ms) {
  struct timespec ts = {
      .tv_sec = ms / 1000,
      .tv_nsec = (long)(ms % 1000) * 1000000L,
  };
  nanosleep(&ts, NULL);
}

/* ----- framing primitives ----- */

static int send_framed(int fd, const uint8_t *payload, size_t len) {
  if (len > MSG_MAX)
    return -1;

  char frame[FRAME_HDR_SIZE + MSG_MAX];
  uint32_t net_len = htonl(len);
  memcpy(frame, &net_len, FRAME_HDR_SIZE);
  memcpy(frame + FRAME_HDR_SIZE, payload, len);
  if (write_all(fd, frame, FRAME_HDR_SIZE + len) !=
      (ssize_t)(FRAME_HDR_SIZE + len)) {
    perror("write");
    return -1;
  }
  return 0;
}

static int recv_framed(int fd, uint8_t *buf, size_t cap, size_t *out_len) {
  char hdr[FRAME_HDR_SIZE];
  if (read_exact(fd, hdr, FRAME_HDR_SIZE) != FRAME_HDR_SIZE)
    return -1;

  uint32_t net_len;
  memcpy(&net_len, hdr, FRAME_HDR_SIZE);
  uint32_t payload_len = ntohl(net_len);
  if ((size_t)payload_len > cap)
    return -1;
  if (read_exact(fd, (char *)buf, payload_len) != (ssize_t)payload_len)
    return -1;
  *out_len = payload_len;
  return 0;
}

/* ----- decoded GetTask response -----

   Tagged union: `kind` is the discriminator; `as` holds the per-kind decoded
   body. DONE_RESP has no payload, so it isn't represented in `as` and the
   union members are not read for that kind. */
typedef struct {
  rpc_kind_t kind;
  union {
    rpc_task_map_resp_t map;
    rpc_task_reduce_resp_t reduce;
    rpc_wait_resp_t wait;
  } as;
} task_response_t;

/* Send GET_TASK_REQ, recv the response, decode into *out.
   On success, *out is fully populated and ready to dispatch on. */
static int get_task_req(int fd, task_response_t *out) {
  uint8_t buf[MSG_MAX];
  size_t out_len;
  if (rpc_encode_get_task_req(buf, sizeof buf, &out_len) != 0)
    return -1;
  if (send_framed(fd, buf, out_len) != 0)
    return -1;

  uint8_t resp[MSG_MAX];
  size_t resp_len;
  if (recv_framed(fd, resp, sizeof resp, &resp_len) != 0)
    return -1;
  if (rpc_peek_kind(resp, resp_len, &out->kind) != 0)
    return -1;

  const uint8_t *body = resp + 1;
  size_t body_len = resp_len - 1;
  switch (out->kind) {
  case RPC_TASK_MAP_RESP:
    return rpc_decode_task_map_resp(body, body_len, &out->as.map);
  case RPC_TASK_REDUCE_RESP:
    return rpc_decode_task_reduce_resp(body, body_len, &out->as.reduce);
  case RPC_WAIT_RESP:
    return rpc_decode_wait_resp(body, body_len, &out->as.wait);
  case RPC_DONE_RESP:
    return 0; /* no body to decode */
  default:
    return -1;
  }
}

/* Send TASK_DONE_REQ for the given (task_id, attempt_id, result). */
static int send_task_done(int fd, uint32_t task_id, uint32_t attempt_id,
                          uint8_t result) {
  rpc_task_done_req_t msg = {
      .task_id = task_id,
      .attempt_id = attempt_id,
      .result = result,
  };
  uint8_t buf[MSG_MAX];
  size_t out_len;
  if (rpc_encode_task_done_req(buf, sizeof buf, &msg, &out_len) != 0)
    return -1;
  if (send_framed(fd, buf, out_len) != 0)
    return -1;
  return 0;
}

/* ----- task runners (stubs for slice 2; real I/O lands in slice 3) ----- */

static uint8_t run_reduce_task(void) {
  printf("[worker] run reduce task\n");
  return 0;
}

/* ----- connection setup -----
   Returns the connected fd on success, -1 on error. */
static int client_init(int port, const char *host) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    perror("socket");
    return -1;
  }

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
  };
  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    fprintf(stderr, "[worker] invalid host: %s\n", host);
    close(fd);
    return -1;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("connect");
    close(fd);
    return -1;
  }
  printf("[worker] connected to %s:%d (fd=%d)\n", host, port, fd);
  return fd;
}

/* ----- main loop ----- */

int main(int argc, char *argv[]) {
  const char *host = (argc >= 2) ? argv[1] : "127.0.0.1";
  int port = (argc >= 3) ? atoi(argv[2]) : CONFIG_DEFAULT_PORT;

  int fd = client_init(port, host);
  if (fd == -1)
    return EXIT_FAILURE;

  int rc = EXIT_SUCCESS;
  bool loop = true;
  while (loop) {
    task_response_t resp;
    if (get_task_req(fd, &resp) != 0) {
      fprintf(stderr, "[worker] get_task_req failed\n");
      rc = EXIT_FAILURE;
      break;
    }

    switch (resp.kind) {
    case RPC_TASK_MAP_RESP: {
      printf("[worker] TASK_MAP_RESP: task_id=%u attempt=%u n_reduce=%u "
             "path=%s\n",
             resp.as.map.task_id, resp.as.map.attempt_id, resp.as.map.n_reduce,
             resp.as.map.input_path);
      int rv = worker_map_run(resp.as.map.task_id, resp.as.map.attempt_id,
                              resp.as.map.n_reduce, resp.as.map.input_path);
      uint8_t result = (rv == 0) ? 0 : 1;
      if (send_task_done(fd, resp.as.map.task_id, resp.as.map.attempt_id,
                         result) != 0) {
        fprintf(stderr, "[worker] send_task_done failed\n");
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      uint8_t ack_buf[MSG_MAX];
      size_t ack_buf_len;
      if (recv_framed(fd, ack_buf, sizeof ack_buf, &ack_buf_len) != 0) {
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      rpc_kind_t map_rc;
      if (rpc_peek_kind(ack_buf, ack_buf_len, &map_rc) != 0) {

        rc = EXIT_FAILURE;
        loop = false;
        break;
      }
      if (map_rc != RPC_TASK_DONE_ACK) {
        // What we do? Master does'nt send ACK back.
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      break;
    }
    case RPC_TASK_REDUCE_RESP: {
      printf("[worker] TASK_REDUCE_RESP: task_id=%u attempt=%u n_map=%u\n",
             resp.as.reduce.task_id, resp.as.reduce.attempt_id,
             resp.as.reduce.n_map);
      uint8_t result = run_reduce_task();
      if (send_task_done(fd, resp.as.reduce.task_id, resp.as.reduce.attempt_id,
                         result) != 0) {
        fprintf(stderr, "[worker] send_task_done failed\n");
        rc = EXIT_FAILURE;
        loop = false;
      }
      break;
    }
    case RPC_WAIT_RESP:
      printf("[worker] WAIT_RESP: wait_ms=%u\n", resp.as.wait.wait_ms);
      sleep_ms(resp.as.wait.wait_ms);
      break;
    case RPC_DONE_RESP:
      printf("[worker] DONE_RESP\n");
      loop = false;
      break;
    default:
      fprintf(stderr, "[worker] unexpected kind=%d\n", (int)resp.kind);
      rc = EXIT_FAILURE;
      loop = false;
      break;
    }
  }

  close(fd);
  return rc;
}
