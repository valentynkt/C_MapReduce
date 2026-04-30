#include "rpc.h"

#include <arpa/inet.h>
#include <string.h>

/* ----- byte-buffer primitives -----
   Pointer-to-pointer pattern: each call advances *p on success.
   Return 0 on success, -1 if the operation would overrun [.., end). */

static int buf_write_u8(uint8_t **p, const uint8_t *end, uint8_t v) {
  if ((size_t)(end - *p) < 1)
    return -1;
  **p = v;
  *p += 1;
  return 0;
}

static int buf_write_u16(uint8_t **p, const uint8_t *end, uint16_t v) {
  if ((size_t)(end - *p) < 2)
    return -1;
  uint16_t net = htons(v);
  memcpy(*p, &net, 2);
  *p += 2;
  return 0;
}

static int buf_write_u32(uint8_t **p, const uint8_t *end, uint32_t v) {
  if ((size_t)(end - *p) < 4)
    return -1;
  uint32_t net = htonl(v);
  memcpy(*p, &net, 4);
  *p += 4;
  return 0;
}

static int buf_write_bytes(uint8_t **p, const uint8_t *end, const void *src,
                           size_t n) {
  if ((size_t)(end - *p) < n)
    return -1;
  memcpy(*p, src, n);
  *p += n;
  return 0;
}

static int buf_read_u8(const uint8_t **p, const uint8_t *end, uint8_t *out) {
  if ((size_t)(end - *p) < 1)
    return -1;
  *out = **p;
  *p += 1;
  return 0;
}

static int buf_read_u16(const uint8_t **p, const uint8_t *end, uint16_t *out) {
  if ((size_t)(end - *p) < 2)
    return -1;
  uint16_t net;
  memcpy(&net, *p, 2);
  *out = ntohs(net);
  *p += 2;
  return 0;
}

static int buf_read_u32(const uint8_t **p, const uint8_t *end, uint32_t *out) {
  if ((size_t)(end - *p) < 4)
    return -1;
  uint32_t net;
  memcpy(&net, *p, 4);
  *out = ntohl(net);
  *p += 4;
  return 0;
}

static int buf_read_bytes(const uint8_t **p, const uint8_t *end, void *dst,
                          size_t n) {
  if ((size_t)(end - *p) < n)
    return -1;
  memcpy(dst, *p, n);
  *p += n;
  return 0;
}

/* ----- kind peek ----- */

int rpc_peek_kind(const uint8_t *buf, size_t len, rpc_kind_t *out_kind) {
  if (len < 1)
    return -1;
  uint8_t k = buf[0];
  if (k < RPC_GET_TASK_REQ || k > RPC_TASK_DONE_ACK)
    return -1;
  *out_kind = (rpc_kind_t)k;
  return 0;
}

/* ----- encoders ----- */

int rpc_encode_get_task_req(uint8_t *buf, size_t cap, size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_GET_TASK_REQ) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_task_map_resp(uint8_t *buf, size_t cap,
                             const rpc_task_map_resp_t *msg, size_t *out_len) {
  /* Reserve one byte for the receiver's null terminator. */
  if (msg->input_path_len >= MAPREDUCE_PATH_MAX)
    return -1;
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_TASK_MAP_RESP) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->task_id) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->attempt_id) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->n_reduce) != 0)
    return -1;
  if (buf_write_u16(&p, end, msg->input_path_len) != 0)
    return -1;
  if (buf_write_bytes(&p, end, msg->input_path, msg->input_path_len) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_task_reduce_resp(uint8_t *buf, size_t cap,
                                const rpc_task_reduce_resp_t *msg,
                                size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_TASK_REDUCE_RESP) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->task_id) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->attempt_id) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->n_map) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_wait_resp(uint8_t *buf, size_t cap, const rpc_wait_resp_t *msg,
                         size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_WAIT_RESP) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->wait_ms) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_done_resp(uint8_t *buf, size_t cap, size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_DONE_RESP) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_task_done_req(uint8_t *buf, size_t cap,
                             const rpc_task_done_req_t *msg, size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_TASK_DONE_REQ) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->task_id) != 0)
    return -1;
  if (buf_write_u32(&p, end, msg->attempt_id) != 0)
    return -1;
  if (buf_write_u8(&p, end, msg->result) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

int rpc_encode_task_done_ack(uint8_t *buf, size_t cap, size_t *out_len) {
  uint8_t *p = buf;
  const uint8_t *end = buf + cap;
  if (buf_write_u8(&p, end, RPC_TASK_DONE_ACK) != 0)
    return -1;
  *out_len = (size_t)(p - buf);
  return 0;
}

/* ----- decoders -----
   Receive the body AFTER the kind byte; caller has already peeked.
   Reject trailing bytes (p must reach end exactly). */

int rpc_decode_task_map_resp(const uint8_t *buf, size_t len,
                             rpc_task_map_resp_t *out) {
  const uint8_t *p = buf;
  const uint8_t *end = buf + len;
  if (buf_read_u32(&p, end, &out->task_id) != 0)
    return -1;
  if (buf_read_u32(&p, end, &out->attempt_id) != 0)
    return -1;
  if (buf_read_u32(&p, end, &out->n_reduce) != 0)
    return -1;
  if (buf_read_u16(&p, end, &out->input_path_len) != 0)
    return -1;
  if (out->input_path_len >= MAPREDUCE_PATH_MAX)
    return -1;
  if (buf_read_bytes(&p, end, out->input_path, out->input_path_len) != 0)
    return -1;
  if (p != end)
    return -1;
  out->input_path[out->input_path_len] = '\0';
  return 0;
}

int rpc_decode_task_reduce_resp(const uint8_t *buf, size_t len,
                                rpc_task_reduce_resp_t *out) {
  const uint8_t *p = buf;
  const uint8_t *end = buf + len;
  if (buf_read_u32(&p, end, &out->task_id) != 0)
    return -1;
  if (buf_read_u32(&p, end, &out->attempt_id) != 0)
    return -1;
  if (buf_read_u32(&p, end, &out->n_map) != 0)
    return -1;
  if (p != end)
    return -1;
  return 0;
}

int rpc_decode_wait_resp(const uint8_t *buf, size_t len, rpc_wait_resp_t *out) {
  const uint8_t *p = buf;
  const uint8_t *end = buf + len;
  if (buf_read_u32(&p, end, &out->wait_ms) != 0)
    return -1;
  if (p != end)
    return -1;
  return 0;
}

int rpc_decode_task_done_req(const uint8_t *buf, size_t len,
                             rpc_task_done_req_t *out) {
  const uint8_t *p = buf;
  const uint8_t *end = buf + len;
  if (buf_read_u32(&p, end, &out->task_id) != 0)
    return -1;
  if (buf_read_u32(&p, end, &out->attempt_id) != 0)
    return -1;
  if (buf_read_u8(&p, end, &out->result) != 0)
    return -1;
  if (p != end)
    return -1;
  return 0;
}
