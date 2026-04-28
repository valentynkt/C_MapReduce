
#include "config.h"
#include "master.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OFFSET(field) offsetof(master_config_t, field)

static config_entry_t config_table[] = {
    {"port", CONFIG_TYPE_INT, OFFSET(port), 1, 65535, NULL},
    {"tcp-backlog", CONFIG_TYPE_INT, OFFSET(tcp_backlog), 1, 65535, NULL},
    {"hz", CONFIG_TYPE_INT, OFFSET(hz), 1, 500, NULL},
    {"client-timeout", CONFIG_TYPE_INT, OFFSET(client_timeout_s), 0, 86400,
     NULL},
    {NULL, 0, 0, 0, 0, NULL},
};

void master_config_init(void) {
  master.config.port = CONFIG_DEFAULT_PORT;
  master.config.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
  master.config.hz = CONFIG_DEFAULT_HZ;
  master.config.client_timeout_s = CONFIG_DEFAULT_CLIENT_TIMEOUT_S;
}

static config_entry_t *find_config(const char *name) {
  for (int i = 0; config_table[i].name != NULL; i++) {
    if (strcmp(config_table[i].name, name) == 0)
      return &config_table[i];
  }
  return NULL;
}

static int apply_config(config_entry_t *entry, const char *value) {
  void *target = (char *)&master.config + entry->offset;

  switch (entry->type) {
  case CONFIG_TYPE_INT: {
    char *end;
    long val = strtol(value, &end, 10);
    if (*end != '\0') {
      fprintf(stderr, "config error: '%s' expects integer, got '%s'\n",
              entry->name, value);
      return -1;
    }
    if (val < entry->min || val > entry->max) {
      fprintf(stderr, "config error: '%s' must be %d-%d, got %ld\n",
              entry->name, entry->min, entry->max, val);
      return -1;
    }
    *(int *)target = (int)val;
    break;
  }
  case CONFIG_TYPE_BOOL: {
    if (strcmp(value, "yes") == 0)
      *(bool *)target = true;
    else if (strcmp(value, "no") == 0)
      *(bool *)target = false;
    else {
      fprintf(stderr, "config error: '%s' expects yes/no, got '%s'\n",
              entry->name, value);
      return -1;
    }
    break;
  }
  case CONFIG_TYPE_STRING: {
    char *copy = strdup(value);
    if (!copy) {
      fprintf(stderr, "config error: out of memory for '%s'\n", entry->name);
      return -1;
    }
    free(*(char **)target);
    *(char **)target = copy;
    break;
  }
  case CONFIG_TYPE_ENUM: {
    for (int i = 0; entry->enum_values[i] != NULL; i++) {
      if (strcmp(entry->enum_values[i], value) == 0) {
        *(int *)target = i;
        return 0;
      }
    }
    fprintf(stderr, "config error: '%s' invalid value '%s', expected one of:",
            entry->name, value);
    for (int i = 0; entry->enum_values[i] != NULL; i++) {
      fprintf(stderr, " %s", entry->enum_values[i]);
    }
    fprintf(stderr, "\n");
    return -1;
  }
  }
  return 0;
}

int load_config(const char *filename) {
  FILE *fp = fopen(filename, "r");
  if (!fp) {
    fprintf(stderr, "cannot open config file: %s\n", filename);
    return -1;
  }

  char line[1024];
  int lineno = 0;
  int errors = 0;

  while (fgets(line, sizeof(line), fp)) {
    lineno++;

    /* strip newline */
    size_t len = strlen(line);
    if (len > 0 && line[len - 1] == '\n')
      line[len - 1] = '\0';

    /* skip empty lines and comments */
    if (line[0] == '\0' || line[0] == '#')
      continue;

    char key[256], value[256];
    if (sscanf(line, "%255s %255s", key, value) != 2) {
      fprintf(stderr, "config error: malformed line %d: %s\n", lineno, line);
      errors++;
      continue;
    }

    config_entry_t *entry = find_config(key);
    if (!entry) {
      fprintf(stderr, "config error: unknown key '%s' at line %d\n", key,
              lineno);
      errors++;
      continue;
    }

    if (apply_config(entry, value) == -1) {
      fprintf(stderr, "  at line %d: %s\n", lineno, line);
      errors++;
    }
  }

  fclose(fp);

  if (errors > 0) {
    fprintf(stderr, "config: %d error(s) in %s\n", errors, filename);
    return -1;
  }

  printf("[mapreduce-master] loaded config from %s\n", filename);
  return 0;
}
