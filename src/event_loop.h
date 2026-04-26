#ifndef EVENT_LOOP_H
#define EVENT_LOOP_H
#include "config.h"
#include <stdbool.h>

typedef struct event_loop event_loop_t;
typedef void (*el_handler_fn)(event_loop_t *el, int fd);
typedef void el_before_sleep_fn(event_loop_t *el);

struct event_loop {
  int kq;
  el_handler_fn read_handlers[MAX_FDS];
  el_handler_fn write_handlers[MAX_FDS];
  el_before_sleep_fn *before_sleep_proc;
  int poll_timeout_ms;
  bool stop;
};

int el_init(event_loop_t *el, int poll_timeout_ms);
int el_add(event_loop_t *el, int fd, el_handler_fn handler);
int el_add_write(event_loop_t *el, int fd, el_handler_fn handler);
void el_remove(event_loop_t *el, int fd);
void el_remove_write(event_loop_t *el, int fd);
int el_run(event_loop_t *el);
void el_cleanup(event_loop_t *el);

#endif
