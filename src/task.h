#ifndef TASK_H
#define TASK_H

#include <stdbool.h>
#include <stdint.h>

/* per-worker app state, indexed by fd.
   `active` is redundant with client_t.active. drop later. */
typedef struct {
  bool active;
  int current_task_id; /* -1 if no task */
  int64_t assigned_at_ms;
} worker_t;

#endif
