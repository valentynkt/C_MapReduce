#ifndef WORKER_MAP_H
#define WORKER_MAP_H

#include <stdint.h>
#include <stdio.h>

/* Borrowed for the emit call only. */
typedef struct {
  FILE **handles; /* R open temp files, indexed by partition Y */
  uint32_t R;
  const char *doc_name;
} worker_map_emit_ctx_t;

typedef void (*worker_map_emit_fn)(const char *token, void *ud);

void emit_word_count(const char *token, void *ud);
/* TODO:(phase-6): emit_inverted_index */

/* Run map task: read input_path, tokenize, partition by hash, atomically
   publish R files mr-{task_id}-{Y}. 0 on success, -1 on error. */
int worker_map_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_reduce,
                   const char *input_path, worker_map_emit_fn emit);

#endif
