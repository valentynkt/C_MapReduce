#ifndef WORKER_MAP_H
#define WORKER_MAP_H

#include <stdint.h>

/* Run the map function on the input split, producing R intermediate files
 * mr-{task_id}-{Y} for Y in [0, R]. 0 - Success, -1 on Error */

int worker_map_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_reduce,
                   const char *input_path);

#endif
