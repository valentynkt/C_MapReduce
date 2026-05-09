
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

static d_array_t *init_container(void) {
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
