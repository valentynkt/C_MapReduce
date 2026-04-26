
#ifndef TASK_H
#define TASK_H
#include <sys/types.h>
typedef struct {
  bool active;
  int current_task_id; // -1 if no task
  int64_t assigned_at_ms;
} worker_t;

#endif
