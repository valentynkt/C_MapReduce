
#ifndef WORKER_REDUCE_H
#define WORKER_REDUCE_H

#include <stdint.h>

/* Run the reduce function on the map intermediate files mr-{X}-{Y} for X [0,
 * n_map), Y task_id producing one output/mr-out-{y} where Y = task_id . 0
 * -Success, -1 on Error
 */

int worker_reduce_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_map);

#endif
