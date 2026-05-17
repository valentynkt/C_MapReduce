#ifndef AOF_H
#define AOF_H
// TODO: Circular Dependency? on master <> aof?
#include "master.h"
#include "rpc.h"

#include "task.h"

/* --- AOF FILE-LEVEL header (offset 0, written once per file) ---
   ┌────────────┬─────────┬─────────┬───────────────┬──────────────┐
   │ 8B magic   │ 4B ver  │ 4B flag │ 8B created_ms │ 8B header CRC│
   └────────────┴─────────┴─────────┴───────────────┴──────────────┘
   header CRC64 covers bytes 0..23. All multi-byte fields big-endian. */
#define AOF_FILE_MAGIC "MAPREDUC"
#define AOF_FILE_MAGIC_LEN 8
#define AOF_FILE_VERSION 1
#define AOF_FILE_VERSION_LEN 4
#define AOF_FILE_CRC_LEN 8
#define AOF_FILE_HEADER_SIZE 32

/* --- AOF RECORD layout (per completed task) ---
   ┌─────────┬─────────────┬────────────────┬──────────┐
   │ 1B kind │ 4B task_id  │ 4B attempt_id  │ 8B crc64 │
   └─────────┴─────────────┴────────────────┴──────────┘
   record CRC64 covers bytes [0 .. SIZE - CRC_LEN).
   All multi-byte fields big-endian. */
#define AOF_RECORD_KIND_LEN 1
#define AOF_RECORD_TASK_ID_LEN 4
#define AOF_RECORD_ATTEMPT_ID_LEN 4
#define AOF_RECORD_CRC_LEN 8
#define AOF_RECORD_SIZE                                                        \
  (AOF_RECORD_KIND_LEN + AOF_RECORD_TASK_ID_LEN + AOF_RECORD_ATTEMPT_ID_LEN +  \
   AOF_RECORD_CRC_LEN)

int aof_init(master_t *master, const char *path);
int aof_load(master_t *master, const char *path);
int aof_open(const char *path);
int aof_close(master_t *master);
int aof_append_completed(const master_t *master, const rpc_task_done_req_t *msg,
                         task_kind_e kind);

#endif // !DEBUG
