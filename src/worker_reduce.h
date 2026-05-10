#ifndef WORKER_REDUCE_H
#define WORKER_REDUCE_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

typedef void (*worker_reduce_fold_fn)(const char *key,
                                      const char **values, size_t n_values,
                                      FILE *out);

void fold_word_count(const char *key,
                     const char **values, size_t n_values,
                     FILE *out);
/* TODO(phase-6): fold_inverted_index */

/* Run reduce task: read M files mr-{X}-{task_id}, group by key, fold each
   group, atomically publish output/mr-out-{task_id}. 0 on success, -1 on error. */
int worker_reduce_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_map,
                      worker_reduce_fold_fn fold);

#endif
