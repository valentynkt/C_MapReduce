#ifndef RPC_H
#define RPC_H

#include <stddef.h>
#include <stdint.h>

#include "config.h" /* for MAPREDUCE_PATH_MAX */

typedef enum {
  RPC_GET_TASK_REQ = 1,
  RPC_TASK_MAP_RESP = 2,
  RPC_TASK_REDUCE_RESP = 3,
  RPC_WAIT_RESP = 4,
  RPC_DONE_RESP = 5,
  RPC_TASK_DONE_REQ = 6,
  RPC_TASK_DONE_ACK = 7
} rpc_kind_t;

typedef struct {
  uint32_t task_id;
  uint32_t attempt_id;
  uint32_t n_reduce;
  uint16_t input_path_len;
  char input_path[MAPREDUCE_PATH_MAX];
} rpc_task_map_resp_t;

typedef struct {
  uint32_t task_id;
  uint32_t attempt_id;
  uint32_t n_map;
} rpc_task_reduce_resp_t;

typedef struct {
  uint32_t task_id;
  uint32_t attempt_id;
  uint8_t result; // 0=sucesse, 1=failure
} rpc_task_done_req_t;

typedef struct {
  uint32_t wait_ms;
} rpc_wait_resp_t;

/* Encoder hepler */
/* Client Example:
  uint8_t buf[256];
  size_t n;
  if (rpc_encode_task_map_resp(buf, sizeof buf, &msg, &n) == 0) {
      server_send(&master.server, fd, (const char *)buf, (uint32_t)n);
  }
*/
typedef struct {
  uint32_t M;   /* number of map tasks; <= MAX_MAP_TASKS */
  uint32_t R;   /* number of reduce tasks; <= MAX_REDUCE_TASKS */
  uint8_t *buf; /* M * (input_path_len + input_path) */
} aof_job_t;

int rpc_encode_get_task_req(uint8_t *buf, size_t cap, size_t *out_len);

int rpc_encode_task_map_resp(uint8_t *buf, size_t cap,
                             const rpc_task_map_resp_t *msg, size_t *out_len);

int rpc_encode_task_reduce_resp(uint8_t *buf, size_t cap,
                                const rpc_task_reduce_resp_t *msg,
                                size_t *out_len);

int rpc_encode_wait_resp(uint8_t *buf, size_t cap, const rpc_wait_resp_t *msg,
                         size_t *out_len);
int rpc_encode_done_resp(uint8_t *buf, size_t cap, size_t *out_len);
int rpc_encode_task_done_req(uint8_t *buf, size_t cap,
                             const rpc_task_done_req_t *msg, size_t *out_len);

int rpc_encode_task_done_ack(uint8_t *buf, size_t cap, size_t *out_len);

/* Decoder helper*/
int rpc_decode_task_map_resp(const uint8_t *buf, size_t len,
                             rpc_task_map_resp_t *out);

int rpc_decode_task_reduce_resp(const uint8_t *buf, size_t len,
                                rpc_task_reduce_resp_t *out);

int rpc_decode_wait_resp(const uint8_t *buf, size_t len, rpc_wait_resp_t *out);

int rpc_decode_task_done_req(const uint8_t *buf, size_t len,
                             rpc_task_done_req_t *out);

int rpc_peek_kind(const uint8_t *buf, size_t len, rpc_kind_t *out_kind);
/*
  rpc_get_task_req_t        (empty)
  rpc_task_map_resp_t       (task_id, attempt_id, n_reduce, input_path,
  input_path_len) rpc_task_reduce_resp_t    (task_id, attempt_id, n_map)
  rpc_wait_resp_t           (wait_ms)
  rpc_done_resp_t           (empty)
  rpc_task_done_req_t       (task_id, attempt_id, result)
  rpc_task_done_ack_t       (empty)
*/

#endif
