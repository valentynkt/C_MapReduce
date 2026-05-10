#include <arpa/inet.h>
#include <errno.h>
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
#include "log.h"
#include "rpc.h"
#include "util.h"
#include "worker_map.h"
#include "worker_reduce.h"

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
    LOG_ERROR("worker", "send_framed write_all failed: %s", strerror(errno));
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

/* ----- connection setup -----
   Returns the connected fd on success, -1 on error. */
static int client_init(int port, const char *host) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    LOG_ERROR("worker", "socket: %s", strerror(errno));
    return -1;
  }

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
  };
  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    LOG_ERROR("worker", "invalid host: %s", host);
    close(fd);
    return -1;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    LOG_ERROR("worker", "connect %s:%d: %s", host, port, strerror(errno));
    close(fd);
    return -1;
  }
  LOG_INFO("worker", "connected to %s:%d fd=%d pid=%d", host, port, fd,
           (int)getpid());
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
      LOG_ERROR("worker", "get_task_req failed (master gone?)");
      rc = EXIT_FAILURE;
      break;
    }

    switch (resp.kind) {
    case RPC_TASK_MAP_RESP: {
      LOG_INFO("worker", "got MAP task=%u attempt=%u n_reduce=%u input=%s",
               resp.as.map.task_id, resp.as.map.attempt_id,
               resp.as.map.n_reduce, resp.as.map.input_path);
      int64_t t0 = now_ms();
      int rv = worker_map_run(resp.as.map.task_id, resp.as.map.attempt_id,
                              resp.as.map.n_reduce, resp.as.map.input_path);
      uint8_t result = (rv == 0) ? 0 : 1;
      LOG_INFO("worker",
               "MAP task=%u attempt=%u finished result=%u elapsed_ms=%lld",
               resp.as.map.task_id, resp.as.map.attempt_id, result,
               (long long)(now_ms() - t0));
      if (send_task_done(fd, resp.as.map.task_id, resp.as.map.attempt_id,
                         result) != 0) {
        LOG_ERROR("worker", "send_task_done MAP task=%u failed",
                  resp.as.map.task_id);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      uint8_t ack_buf[MSG_MAX];
      size_t ack_buf_len;
      if (recv_framed(fd, ack_buf, sizeof ack_buf, &ack_buf_len) != 0) {
        LOG_ERROR("worker", "recv ACK for MAP task=%u failed",
                  resp.as.map.task_id);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      rpc_kind_t ack_kind;
      if (rpc_peek_kind(ack_buf, ack_buf_len, &ack_kind) != 0) {
        LOG_ERROR("worker", "peek_kind ACK failed for MAP task=%u",
                  resp.as.map.task_id);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }
      if (ack_kind != RPC_TASK_DONE_ACK) {
        LOG_ERROR("worker",
                  "expected RPC_TASK_DONE_ACK for MAP task=%u, got kind=%d",
                  resp.as.map.task_id, (int)ack_kind);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      break;
    }
    case RPC_TASK_REDUCE_RESP: {
      rpc_task_reduce_resp_t reduce = resp.as.reduce;
      LOG_INFO("worker", "got REDUCE task=%u attempt=%u n_map=%u",
               reduce.task_id, reduce.attempt_id, reduce.n_map);
      int rv =
          worker_reduce_run(reduce.task_id, reduce.attempt_id, reduce.n_map);
      uint8_t result = (rv == 0) ? 0 : 1;
      if (send_task_done(fd, resp.as.reduce.task_id, resp.as.reduce.attempt_id,
                         result) != 0) {
        LOG_ERROR("worker", "send_task_done REDUCE task=%u failed",
                  resp.as.reduce.task_id);
        rc = EXIT_FAILURE;
        loop = false;
      }

      uint8_t ack_buf[MSG_MAX];
      size_t ack_buf_len;
      if (recv_framed(fd, ack_buf, sizeof ack_buf, &ack_buf_len) != 0) {
        LOG_ERROR("worker", "recv ACK for REDUCE task=%u failed",
                  resp.as.reduce.task_id);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }

      rpc_kind_t ack_kind;
      if (rpc_peek_kind(ack_buf, ack_buf_len, &ack_kind) != 0) {
        LOG_ERROR("worker", "peek_kind ACK failed for REDUCE task=%u",
                  resp.as.reduce.task_id);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }
      if (ack_kind != RPC_TASK_DONE_ACK) {
        LOG_ERROR("worker",
                  "expected RPC_TASK_DONE_ACK for REDUCE task=%u, got kind=%d",
                  resp.as.reduce.task_id, (int)ack_kind);
        rc = EXIT_FAILURE;
        loop = false;
        break;
      }
      break;
    }
    case RPC_WAIT_RESP:
      LOG_INFO("worker", "WAIT wait_ms=%u", resp.as.wait.wait_ms);
      sleep_ms(resp.as.wait.wait_ms);
      break;
    case RPC_DONE_RESP:
      LOG_INFO("worker", "DONE — exiting cleanly");
      loop = false;
      break;
    default:
      LOG_ERROR("worker", "unexpected RPC kind=%d", (int)resp.kind);
      rc = EXIT_FAILURE;
      loop = false;
      break;
    }
  }

  close(fd);
  LOG_INFO("worker", "exit rc=%d", rc);
  return rc;
}
