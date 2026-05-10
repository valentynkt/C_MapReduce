
#include "config.h"
#include "log.h"
#include "task.h"
#include "util.h"
#include <assert.h>
#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h> /* pid_t */
#include <unistd.h>    /* getpid */

#define INITIAL_SIZE 4

typedef struct {
  char *key;
  char *value;
} record_t;

typedef struct {
  record_t **records;
  size_t count;
  size_t capacity;
} d_array_t;

static d_array_t *init_array(void) {
  d_array_t *array = malloc(sizeof(*array));
  if (!array) {
    return NULL;
  }
  array->capacity = INITIAL_SIZE;
  array->count = 0;
  array->records = calloc(array->capacity, sizeof(record_t *));
  if (!array->records) {
    free(array);
    return NULL;
  }
  return array;
}
static int add_record(d_array_t *array, char *key, char *value) {
  if (array->count == array->capacity) {
    size_t new_capacity = array->capacity * 2;
    record_t **new_records =
        realloc(array->records, sizeof(record_t *) * new_capacity);
    if (!new_records) {
      return -1;
    }
    array->records = new_records;
    array->capacity = new_capacity;
  }
  record_t *record = malloc(sizeof(*record));
  if (!record) {
    return -1;
  }
  record->key = strdup(key);
  if (!record->key) {
    free(record);
    return -1;
  }
  record->value = strdup(value);
  if (!record->value) {
    free(record->key);
    free(record);
    return -1;
  }
  array->records[array->count] = record;
  array->count += 1;
  return 0;
}

static void arr_free(d_array_t *array) {
  for (size_t i = 0; i < array->count; i++) {
    record_t *record = array->records[i];
    if (record == NULL) {
      continue;
    }
    free(record->key);
    free(record->value);
    free(record);
  }
  free(array->records);
  free(array);
}

static int read_records_file(const char *path, d_array_t *array) {
  FILE *f = fopen(path, "r");

  if (f == NULL) {
    LOG_ERROR("worker.reduce", "read_records_file: fopen(%s): %s", path,
              strerror(errno));
    return -1;
  }

  char line[8192];
  int rc = 0;
  while (fgets(line, sizeof line, f) != NULL) {
    char *p = line;
    char *key = strsep(&p, "\t");
    char *value = strsep(&p, "\n");
    if (key == NULL || value == NULL || *key == '\0') {
      LOG_WARN("worker.reduce", "skipping malformed line in %s: %.40s", path,
               line);
      continue;
    }
    if (add_record(array, key, value) != 0) {
      LOG_ERROR("worker.reduce", "add_record failed (OOM?) reading %s", path);
      rc = -1;
      break;
    }
  }
  if (ferror(f)) {
    LOG_ERROR("worker.reduce", "read_records_file: read error on %s", path);
    rc = -1;
  }
  if (fclose(f) != 0) {
    LOG_ERROR("worker.reduce", "read_records_file: fclose(%s): %s", path,
              strerror(errno));
    rc = -1;
  }
  return rc;
}

static int comp(const void *a, const void *b) {
  const record_t *ra = *(record_t *const *)a;
  const record_t *rb = *(record_t *const *)b;
  return strcmp(ra->key, rb->key);
}

static void fold_word_count(char *key, const char **values, size_t n_value,
                            FILE *out) {
  (void)values;
  // fsync or any operation to force write to a file?
  // probably it just waiting in buffer
  fprintf(out, "%s\t%zu\n", key, n_value);
}

int worker_reduce_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_map) {

  LOG_INFO("worker.reduce", "run start task=%u attempt=%u n_map=%u  pid=%d",
           task_id, attempt_id, n_map, (int)getpid());
  assert(n_map > 0);
  FILE *handle = NULL;
  char input_paths[MAX_MAP_TASKS][MAPREDUCE_PATH_MAX];
  char temp_path[MAPREDUCE_PATH_MAX];
  char final_path[MAPREDUCE_PATH_MAX];
  d_array_t *array = init_array();
  if (!array) {
    LOG_ERROR("worker.reduce", "init_array returned null");
    return -1;
  }

  // temp_path
  int temp_path_n =
      snprintf(temp_path, MAPREDUCE_PATH_MAX, "output/mr-out-%u.tmp.%d.%u",
               task_id /* Y */, (int)getpid(), attempt_id);

  if (temp_path_n < 0 || (size_t)temp_path_n >= MAPREDUCE_PATH_MAX) {
    LOG_ERROR("worker.reduce", "temp path truncation for task=%u", task_id);
    goto cleanup;
  }
  // final_path
  int final_path_n = snprintf(final_path, MAPREDUCE_PATH_MAX,
                              "output/mr-out-%u", task_id /* Y */);

  if (final_path_n < 0 || (size_t)final_path_n >= MAPREDUCE_PATH_MAX) {
    LOG_ERROR("worker.reduce", "final path truncation for task=%u", task_id);
    goto cleanup;
  }

  for (uint32_t i = 0; i < n_map; i++) {
    int n = snprintf(input_paths[i], MAPREDUCE_PATH_MAX,
                     "intermediate/mr-%u-%u", i /* X */, task_id /* Y */);

    if (n < 0 || (size_t)n >= MAPREDUCE_PATH_MAX) {
      LOG_ERROR("worker.reduce", "input path truncation for task=%u x=%u",
                task_id, i);
      goto cleanup;
    }

    if (read_records_file(input_paths[i], array) != 0) {
      goto cleanup;
    }
  }
  qsort(array->records, array->count, sizeof(record_t *), comp);

  handle = fopen(temp_path, "w");
  if (!handle) {
    LOG_ERROR("worker.reduce", "fopen(%s): %s", temp_path, strerror(errno));
    goto cleanup;
  }

  // Group walk + fold
  const char *vals[1024];
  char *cur_key;
  size_t n_vals = 0;

  for (size_t i = 0; i < array->count; i++) {
    record_t *record = array->records[i];
    if (cur_key == NULL) {
      cur_key = record->key;
      vals[n_vals] = record->key;
      n_vals += 1;
    } else if (strcmp(record->key, cur_key) == 0) {
      vals[n_vals] = record->key;
      n_vals += 1;
    } else {
      fold_word_count(cur_key, vals, n_vals, handle);
      cur_key = record->key;
      vals[0] = record->key;
      n_vals = 1;
    }
  }
  fold_word_count(cur_key, vals, n_vals, handle);
  if (handle) {
    fclose(handle);
  }
  arr_free(array);

  /* Rename pass: only run if everything was clean. */
  if (rename(temp_path, final_path) != 0) {
    LOG_ERROR("worker.reduce", "rename(%s -> %s): %s", temp_path, final_path,
              strerror(errno));
    return -1;
  }
  LOG_INFO("worker.reduce", "renamed temp file:%s to final file:%s for task=%u",
           temp_path, final_path, task_id);

  LOG_INFO("worker.reduce", "run end task=%u attempt=%u ", task_id, attempt_id);
  return 0;

cleanup:
  if (handle) {
    fclose(handle);
  }
  arr_free(array);
  return -1;
}
