#include "worker_map.h"
#include "config.h"
#include "log.h"
#include "task.h"
#include "util.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define WORKER_MAP_LINE_MAX 8192

/* ----- input parsing ----- */

static int tokenize_file(const char *path, worker_map_emit_fn emit, void *ud) {
  FILE *f = fopen(path, "r");
  if (!f) {
    LOG_ERROR("worker.map", "fopen(%s): %s", path, strerror(errno));
    return -1;
  }

  char line[WORKER_MAP_LINE_MAX];
  int rc = 0;
  while (fgets(line, sizeof line, f)) {
    char *p = line;
    char *tok;
    while ((tok = strsep(&p, " \t\n\r")) != NULL) {
      if (*tok != '\0') emit(tok, ud);
    }
  }
  if (ferror(f)) {
    LOG_ERROR("worker.map", "read error on %s", path);
    rc = -1;
  }
  if (fclose(f) != 0) {
    LOG_ERROR("worker.map", "fclose(%s): %s", path, strerror(errno));
    rc = -1;
  }
  return rc;
}

/* ----- paths ----- */

static int build_paths(uint32_t task_id, uint32_t y, uint32_t attempt_id,
                       char *temp_out, char *final_out) {
  int n = snprintf(temp_out, MAPREDUCE_PATH_MAX,
                   "intermediate/mr-%u-%u.tmp.%d.%u",
                   task_id, y, (int)getpid(), attempt_id);
  if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) return -1;

  n = snprintf(final_out, MAPREDUCE_PATH_MAX,
               "intermediate/mr-%u-%u", task_id, y);
  if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) return -1;
  return 0;
}

/* ----- handle cleanup ----- */

/* Close handles[0..count); any NULL slot is skipped. */
static void close_handles(FILE **handles, uint32_t count) {
  for (uint32_t i = 0; i < count; i++) {
    if (handles[i]) {
      fclose(handles[i]);
      handles[i] = NULL;
    }
  }
}

/* ----- workload: word count ----- */

void emit_word_count(const char *token, void *ud) {
  worker_map_emit_ctx_t *ctx = ud;
  uint32_t y = get_hash(token) % ctx->R;
  fprintf(ctx->handles[y], "%s\t1\n", token);
}

/* ----- public entry ----- */

int worker_map_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_reduce,
                   const char *input_path, worker_map_emit_fn emit) {
  LOG_INFO("worker.map",
           "run start task=%u attempt=%u n_reduce=%u input=%s pid=%d",
           task_id, attempt_id, n_reduce, input_path, (int)getpid());

  FILE *handles[MAX_REDUCE_TASKS]                     = {0};
  char  temp_paths[MAX_REDUCE_TASKS][MAPREDUCE_PATH_MAX];
  char  final_paths[MAX_REDUCE_TASKS][MAPREDUCE_PATH_MAX];
  int   rc = -1;

  /* Open R temp files. On any failure mid-loop, close the ones already opened
     and bail. */
  for (uint32_t y = 0; y < n_reduce; y++) {
    if (build_paths(task_id, y, attempt_id, temp_paths[y], final_paths[y]) != 0) {
      LOG_ERROR("worker.map", "path truncation task=%u y=%u", task_id, y);
      close_handles(handles, y);
      goto done;
    }
    handles[y] = fopen(temp_paths[y], "w");
    if (!handles[y]) {
      LOG_ERROR("worker.map", "fopen(%s): %s", temp_paths[y], strerror(errno));
      close_handles(handles, y);
      goto done;
    }
  }

  const char *slash = strrchr(input_path, '/');
  worker_map_emit_ctx_t ctx = {
      .handles  = handles,
      .R        = n_reduce,
      .doc_name = slash ? slash + 1 : input_path,
  };

  rc = tokenize_file(input_path, emit, &ctx);
  if (rc != 0) {
    LOG_WARN("worker.map", "tokenize_file failed task=%u", task_id);
  }

  /* Close pass: always runs, even if tokenize failed, so we don't leak fds.
     ferror catches buffered write failures that fprintf swallowed. */
  for (uint32_t y = 0; y < n_reduce; y++) {
    if (ferror(handles[y])) {
      LOG_ERROR("worker.map", "ferror on temp y=%u (%s)", y, temp_paths[y]);
      rc = -1;
    }
    if (fclose(handles[y]) != 0) {
      LOG_ERROR("worker.map", "fclose(%s): %s", temp_paths[y], strerror(errno));
      rc = -1;
    }
    handles[y] = NULL;
  }

  if (rc != 0) {
    LOG_WARN("worker.map", "task=%u — temp files orphaned", task_id);
    goto done;
  }

  /* Rename pass: only on a clean tokenize+close. */
  for (uint32_t y = 0; y < n_reduce; y++) {
    if (rename(temp_paths[y], final_paths[y]) != 0) {
      LOG_ERROR("worker.map", "rename(%s -> %s): %s",
                temp_paths[y], final_paths[y], strerror(errno));
      rc = -1;
    }
  }
  if (rc == 0)
    LOG_INFO("worker.map", "published %u files for task=%u", n_reduce, task_id);

done:
  /* Defensive: handles[] entries are NULL'd above, but if a future path
     forgets, this catches it. */
  close_handles(handles, n_reduce);
  LOG_INFO("worker.map", "run end task=%u attempt=%u rc=%d",
           task_id, attempt_id, rc);
  return rc;
}
