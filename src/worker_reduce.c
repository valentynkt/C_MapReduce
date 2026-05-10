#include "worker_reduce.h"
#include "config.h"
#include "log.h"

#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define WORKER_REDUCE_LINE_MAX           8192
#define WORKER_REDUCE_INITIAL_CAPACITY   4
#define WORKER_REDUCE_MAX_VALUES_PER_KEY 1024

/* ----- record list ----- */

typedef struct {
  char *key;
  char *value;
} record_t;

/* Owns the records buffer and every record's key+value strings. */
typedef struct {
  record_t *records;   /* inline storage; valid for i in [0, count) */
  size_t    count;
  size_t    capacity;
} record_list_t;

static record_list_t *record_list_create(void) {
  record_list_t *list = malloc(sizeof(*list));
  if (!list) return NULL;
  list->capacity = WORKER_REDUCE_INITIAL_CAPACITY;
  list->count = 0;
  list->records = calloc(list->capacity, sizeof(record_t));
  if (!list->records) {
    free(list);
    return NULL;
  }
  return list;
}

static int record_list_push(record_list_t *list,
                            const char *key, const char *value) {
  if (list->count == list->capacity) {
    size_t new_capacity = list->capacity * 2;
    record_t *grown = realloc(list->records, sizeof(record_t) * new_capacity);
    if (!grown) return -1;
    list->records = grown;
    list->capacity = new_capacity;
  }
  char *k = strdup(key);
  if (!k) return -1;
  char *v = strdup(value);
  if (!v) { free(k); return -1; }
  list->records[list->count].key = k;
  list->records[list->count].value = v;
  list->count++;
  return 0;
}

static void record_list_destroy(record_list_t *list) {
  if (!list) return;
  for (size_t i = 0; i < list->count; i++) {
    free(list->records[i].key);
    free(list->records[i].value);
  }
  free(list->records);
  free(list);
}

/* ----- input parsing ----- */

static int read_records_file(const char *path, record_list_t *list) {
  FILE *f = fopen(path, "r");
  if (!f) {
    LOG_ERROR("worker.reduce", "fopen(%s): %s", path, strerror(errno));
    return -1;
  }

  char line[WORKER_REDUCE_LINE_MAX];
  int rc = 0;
  while (fgets(line, sizeof line, f)) {
    char *p = line;
    char *key   = strsep(&p, "\t");
    char *value = strsep(&p, "\n");
    if (!key || !value || *key == '\0') {
      LOG_WARN("worker.reduce", "skipping malformed line in %s", path);
      continue;
    }
    if (record_list_push(list, key, value) != 0) {
      LOG_ERROR("worker.reduce", "OOM pushing record from %s", path);
      rc = -1;
      break;
    }
  }
  if (ferror(f)) {
    LOG_ERROR("worker.reduce", "read error on %s", path);
    rc = -1;
  }
  if (fclose(f) != 0) {
    LOG_ERROR("worker.reduce", "fclose(%s): %s", path, strerror(errno));
    rc = -1;
  }
  return rc;
}

/* ----- sort ----- */

static int compare_by_key(const void *a, const void *b) {
  const record_t *ra = a;
  const record_t *rb = b;
  return strcmp(ra->key, rb->key);
}

/* ----- paths ----- */

static int build_paths(uint32_t task_id, uint32_t attempt_id,
                       char *temp_out, char *final_out) {
  int n = snprintf(temp_out, MAPREDUCE_PATH_MAX,
                   "output/mr-out-%u.tmp.%d.%u",
                   task_id, (int)getpid(), attempt_id);
  if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) return -1;

  n = snprintf(final_out, MAPREDUCE_PATH_MAX, "output/mr-out-%u", task_id);
  if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) return -1;
  return 0;
}

/* ----- workload: word count ----- */

void fold_word_count(const char *key,
                     const char **values, size_t n_values,
                     FILE *out) {
  (void)values;   /* word count: values are always "1", we just need the count */
  fprintf(out, "%s\t%zu\n", key, n_values);
}

/* ----- public entry ----- */

int worker_reduce_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_map,
                      worker_reduce_fold_fn fold) {
  LOG_INFO("worker.reduce", "run start task=%u attempt=%u n_map=%u pid=%d",
           task_id, attempt_id, n_map, (int)getpid());
  assert(n_map > 0);
  assert(fold != NULL);

  FILE *out = NULL;
  char temp_path[MAPREDUCE_PATH_MAX];
  char final_path[MAPREDUCE_PATH_MAX];
  int rc = -1;

  record_list_t *list = record_list_create();
  if (!list) {
    LOG_ERROR("worker.reduce", "record_list_create: out of memory");
    goto done;
  }

  if (build_paths(task_id, attempt_id, temp_path, final_path) != 0) {
    LOG_ERROR("worker.reduce", "path truncation for task=%u", task_id);
    goto done;
  }

  for (uint32_t x = 0; x < n_map; x++) {
    char input_path[MAPREDUCE_PATH_MAX];
    int n = snprintf(input_path, sizeof input_path,
                     "intermediate/mr-%u-%u", x, task_id);
    if (n < 0 || (size_t)n >= (int)sizeof input_path) {
      LOG_ERROR("worker.reduce", "input path truncation x=%u y=%u", x, task_id);
      goto done;
    }
    if (read_records_file(input_path, list) != 0) goto done;
  }

  qsort(list->records, list->count, sizeof(record_t), compare_by_key);

  out = fopen(temp_path, "w");
  if (!out) {
    LOG_ERROR("worker.reduce", "fopen(%s): %s", temp_path, strerror(errno));
    goto done;
  }

  /* Group walk: emit fold(key, all-its-values) at every key boundary. */
  const char *vals[WORKER_REDUCE_MAX_VALUES_PER_KEY];
  const char *cur_key = NULL;
  size_t n_vals = 0;

  for (size_t i = 0; i < list->count; i++) {
    record_t *r = &list->records[i];
    if (cur_key == NULL || strcmp(r->key, cur_key) != 0) {
      if (n_vals > 0) fold(cur_key, vals, n_vals, out);
      cur_key = r->key;
      n_vals = 0;
    }
    if (n_vals >= WORKER_REDUCE_MAX_VALUES_PER_KEY) {
      LOG_ERROR("worker.reduce", "group '%s' exceeded %d values",
                cur_key, WORKER_REDUCE_MAX_VALUES_PER_KEY);
      goto done;
    }
    vals[n_vals++] = r->value;
  }
  if (n_vals > 0) fold(cur_key, vals, n_vals, out);

  if (ferror(out)) {
    LOG_ERROR("worker.reduce", "ferror on %s after fold pass", temp_path);
    goto done;
  }

  int close_rc = fclose(out);
  out = NULL;
  if (close_rc != 0) {
    LOG_ERROR("worker.reduce", "fclose(%s): %s", temp_path, strerror(errno));
    goto done;
  }

  if (rename(temp_path, final_path) != 0) {
    LOG_ERROR("worker.reduce", "rename(%s -> %s): %s",
              temp_path, final_path, strerror(errno));
    goto done;
  }

  rc = 0;
  LOG_INFO("worker.reduce", "renamed %s -> %s", temp_path, final_path);

done:
  if (out) fclose(out);
  record_list_destroy(list);
  LOG_INFO("worker.reduce", "run end task=%u attempt=%u rc=%d",
           task_id, attempt_id, rc);
  return rc;
}
