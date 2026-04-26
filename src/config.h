
#ifndef CONFIG_H
#define CONFIG_H

#include <stdbool.h>
#include <stddef.h>

/* Compile-time limits */
#define MAX_FDS 1024
#define MAX_EVENTS 64
#define MSG_MAX 4096
#define FRAME_HDR_SIZE 4
#define WBUF_SIZE ((MSG_MAX + FRAME_HDR_SIZE) * 2)

/* Default values for runtime config */
#define CONFIG_DEFAULT_PORT 9999
#define CONFIG_DEFAULT_TCP_BACKLOG 511
#define CONFIG_DEFAULT_HZ 10
#define CONFIG_DEFAULT_CLIENT_TIMEOUT_S 300
#define CONFIG_DEFAULT_CLIENT_TIMEOUT_CHECK_HZ 10

/* Config entry types */
typedef enum {
  CONFIG_TYPE_INT,
  CONFIG_TYPE_BOOL,
  CONFIG_TYPE_ENUM,
  CONFIG_TYPE_STRING,
} config_type_t;

typedef struct {
  const char *name;
  config_type_t type;
  size_t offset;
  int min;
  int max;
  /* NULL-terminated list of accepted strings, only for CONFIG_TYPE_ENUM.
     The index of the matching string becomes the stored int value. */
  const char **enum_values;
} config_entry_t;

void init_server_config(void);
int load_config(const char *filename);

#endif
