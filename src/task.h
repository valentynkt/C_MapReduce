#ifndef TASK_H
#define TASK_H

#include <stdbool.h>
#include <stdint.h>

/* Per-worker application state, indexed by fd.
   `active` here is redundant with networking.h's client_t.active —
   client_t.active is the canonical source of truth for "is this slot live."
   Drop in a follow-up cleanup once call sites are updated. */
typedef struct {
  bool active;
  int current_task_id; /* -1 if no task */
  int64_t assigned_at_ms;
} worker_t;

#endif
