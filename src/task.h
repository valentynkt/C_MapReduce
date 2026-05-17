#ifndef TASK_H
#define TASK_H

#include <stdbool.h>
#include <stdint.h>

#include "config.h" /* for MAPREDUCE_PATH_MAX */

/* ----- Compile-time bounds ----- */

/* Maximum number of map / reduce tasks the master can host in a single job.
   Concrete M and R per job come from config and must satisfy M <= MAX_MAP_TASKS
   and R <= MAX_REDUCE_TASKS. */
#define MAX_MAP_TASKS 256
#define MAX_REDUCE_TASKS 64

/* Per-task retry cap. Once a task hits MAX_ATTEMPTS failures, the job aborts
   (job.phase -> JOB_FAILED). Sizes the per-task history buffer. */
#define MAX_ATTEMPTS 3

/* ----- Task kind ----- */

typedef enum { TASK_KIND_MAP = 1, TASK_KIND_REDUCE = 2 } task_kind_e;

/* ----- Task lifecycle state ----- */

typedef enum {
  TASK_PENDING,     /* not assigned; eligible for the next GetTask */
  TASK_IN_PROGRESS, /* currently assigned to owner_fd at current_attempt */
  TASK_COMPLETED,   /* terminal: outputs exist on disk */
  TASK_FAILED,      /* terminal: hit MAX_ATTEMPTS; drives job -> JOB_FAILED */
} task_state_e;

/* ----- Attempt history ----- */

/* How an attempt ended. RUNNING means the attempt is the live one. */
typedef enum {
  ATTEMPT_RUNNING,
  ATTEMPT_SUCCESS,
  ATTEMPT_FAILED,     /* worker reported result=failure */
  ATTEMPT_TIMEOUT,    /* now - started_at_ms > task_timeout_ms */
  ATTEMPT_DISCONNECT, /* fd dropped before TaskDone */
} attempt_end_status_e;

/* One row in a task's bounded history. Entries 0..attempts_count-1 are valid;
   later slots are zero. */
typedef struct {
  uint32_t attempt_id;
  int worker_fd;
  int64_t started_at_ms;
  int64_t ended_at_ms; /* 0 while end_status == ATTEMPT_RUNNING */
  attempt_end_status_e end_status;
} attempt_record_t;

/* ----- Task ----- */

/* Per-task state. Two arrays of these on master_t: maps[M] and reduces[R].
   For reduce tasks, the array index IS the partition_id; input_path is unused.

   Invariants:
     state == TASK_IN_PROGRESS  iff  owner_fd != -1
     current_attempt > 0        iff  attempts_count > 0
     attempts_count <= MAX_ATTEMPTS
     history[i].end_status valid for i < attempts_count */
typedef struct {
  task_state_e state;
  uint32_t current_attempt; /* 0 means never assigned */
  uint32_t attempts_count;
  int owner_fd;          /* -1 unless TASK_IN_PROGRESS */
  int64_t started_at_ms; /* current attempt start; for timeout sweep */

  /* Map-only (unused for reduce tasks). */
  char input_path[MAPREDUCE_PATH_MAX];
  uint16_t input_path_len;

  attempt_record_t history[MAX_ATTEMPTS];
} task_t;

/* Reset a task slot to its fresh-PENDING shape.

   Used by both fresh init (master_init_job) and AOF replay (aof_load) as the
   canonical "uncompleted slot" baseline. After this call, a slot is in the
   exact state the scheduler expects for an unassigned task; callers may then
   overlay map-only fields (input_path) or state transitions (COMPLETED). */
static inline void task_reset_pending(task_t *t) {
  *t = (task_t){
      .state = TASK_PENDING,
      .owner_fd = -1,
  };
}

/* ----- Job phase ----- */

/* Drives the master's GetTask scheduling decision. */
typedef enum {
  JOB_MAP,    /* hand out map tasks; WAIT if all in flight */
  JOB_REDUCE, /* hand out reduce tasks; WAIT if all in flight */
  JOB_DONE,   /* terminal success: reply DONE_RESP */
  JOB_FAILED, /* terminal failure: a task hit MAX_ATTEMPTS */
} job_phase_e;

/* ----- Job ----- */

/* Aggregate per-job state. Single instance on master_t (single-job design). */
typedef struct {
  uint32_t M; /* number of map tasks; <= MAX_MAP_TASKS */
  uint32_t R; /* number of reduce tasks; <= MAX_REDUCE_TASKS */
  job_phase_e phase;
  uint32_t maps_done;    /* count of maps in TASK_COMPLETED */
  uint32_t reduces_done; /* count of reduces in TASK_COMPLETED */
  int64_t task_timeout_ms;
} job_t;

/* ----- Per-fd worker bookkeeping ----- */

/* Indexed by fd, parallel to server's clients[]. Lifetime tied to the TCP
   connection: zeroed on disconnect, populated on accept.

   has_task is set on TASK_*_RESP and cleared on TASK_DONE_REQ or disconnect.
   The current_* fields are valid only when has_task == true. */
typedef struct {
  int64_t connected_at_ms;
  bool has_task;
  task_kind_e current_kind;
  uint32_t current_task_id;
  uint32_t current_attempt_id;
} worker_t;

#endif /* TASK_H */
