#include "config.h"
#include "log.h"
#include "task.h"
#include "util.h"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> /* pid_t */
#include <unistd.h>    /* getpid */

typedef void (*emit_token_fn)(const char *token, void *ud);

typedef struct {
  FILE **handles;
  uint32_t R;
  const char *doc_name;
} emit_ctx_t;

static void emit_word_count(const char *token, void *ud) {
  emit_ctx_t *ctx = (emit_ctx_t *)ud;
  uint32_t y = get_hash(token) % ctx->R;
  fprintf(ctx->handles[y], "%s\t1\n", token);
}

/*
static void emit_inverted_index(const char *token, void *ud) {
  emit_ctx_t *ctx = (emit_ctx_t *)ud;
  uint32_t y = get_hash(token) % ctx->R;
  fprintf(ctx->handles[y], "%s\t%s\n", token, ctx->doc_name);
}
*/

static int tokenize_file(const char *path, emit_token_fn emit, void *ud) {
  FILE *f = fopen(path, "r");
  if (f == NULL) {
    LOG_ERROR("worker.map", "tokenize_file: fopen(%s): %s", path,
              strerror(errno));
    return -1;
  }
  char line[8192];
  while (fgets(line, sizeof line, f) != NULL) {
    char *p = line;
    char *tok;
    while ((tok = strsep(&p, " \t\n\r")) != NULL) {
      if (*tok != '\0') {
        emit(tok, ud);
      }
    }
  }
  int rc = 0;
  if (ferror(f)) {
    LOG_ERROR("worker.map", "tokenize_file: read error on %s: %s", path,
              strerror(errno));
    rc = -1;
  }
  if (fclose(f) != 0) {
    LOG_ERROR("worker.map", "tokenize_file: fclose(%s): %s", path,
              strerror(errno));
    rc = -1;
  }
  return rc;
}

int worker_map_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_reduce,
                   const char *input_path) {
  LOG_INFO("worker.map",
           "run start task=%u attempt=%u n_reduce=%u input=%s pid=%d", task_id,
           attempt_id, n_reduce, input_path, (int)getpid());

  FILE *handles[MAX_REDUCE_TASKS] = {0};
  char temp_paths[MAX_REDUCE_TASKS][MAPREDUCE_PATH_MAX];
  char final_paths[MAX_REDUCE_TASKS][MAPREDUCE_PATH_MAX];

  for (uint32_t i = 0; i < n_reduce; i++) {
    int n = snprintf(temp_paths[i], MAPREDUCE_PATH_MAX,
                     "intermediate/mr-%u-%u.tmp.%d.%u", task_id /* X */,
                     i /* Y */, (int)getpid(), attempt_id);
    if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) {
      LOG_ERROR("worker.map", "temp path truncation for task=%u y=%u", task_id,
                i);
      for (uint32_t j = 0; j < i; j++)
        fclose(handles[j]);
      return -1;
    }

    n = snprintf(final_paths[i], MAPREDUCE_PATH_MAX, "intermediate/mr-%u-%u",
                 task_id, i);
    if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) {
      LOG_ERROR("worker.map", "final path truncation for task=%u y=%u", task_id,
                i);
      for (uint32_t j = 0; j < i; j++)
        fclose(handles[j]);
      return -1;
    }

    handles[i] = fopen(temp_paths[i], "w");
    if (!handles[i]) {
      LOG_ERROR("worker.map", "fopen(%s): %s", temp_paths[i], strerror(errno));
      for (uint32_t j = 0; j < i; j++)
        fclose(handles[j]);
      return -1;
    }
  }
  LOG_INFO("worker.map", "opened %u temp files for task=%u", n_reduce, task_id);

  const char *slash = strrchr(input_path, '/');
  const char *doc_name = slash ? slash + 1 : input_path;

  emit_ctx_t ctx = (emit_ctx_t){
      .handles = handles,
      .R = n_reduce,
      .doc_name = doc_name,
  };

  int rc = tokenize_file(input_path, emit_word_count, &ctx);
  if (rc != 0)
    LOG_WARN("worker.map", "tokenize_file returned %d for task=%u", rc,
             task_id);

  /* Close pass: always run, regardless of rc, so we don't leak fds. */
  for (uint32_t y = 0; y < n_reduce; y++) {
    if (ferror(handles[y])) {
      LOG_ERROR("worker.map", "ferror set on temp y=%u (path=%s) — write "
                              "failure during tokenize",
                y, temp_paths[y]);
      rc = -1;
    }
    if (fclose(handles[y]) != 0) {
      LOG_ERROR("worker.map", "fclose(%s): %s", temp_paths[y], strerror(errno));
      rc = -1;
    }
  }

  /* Rename pass: only run if everything was clean. */
  if (rc == 0) {
    for (uint32_t y = 0; y < n_reduce; y++) {
      if (rename(temp_paths[y], final_paths[y]) != 0) {
        LOG_ERROR("worker.map", "rename(%s -> %s): %s", temp_paths[y],
                  final_paths[y], strerror(errno));
        rc = -1;
      }
    }
    if (rc == 0)
      LOG_INFO("worker.map", "renamed %u files to canonical for task=%u",
               n_reduce, task_id);
  } else {
    LOG_WARN("worker.map",
             "skipping rename for task=%u due to earlier errors (rc=%d) — "
             "temp files orphaned",
             task_id, rc);
  }

  LOG_INFO("worker.map", "run end task=%u attempt=%u rc=%d", task_id,
           attempt_id, rc);
  return rc;
}
