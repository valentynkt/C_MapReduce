#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <time.h>

/* One-line structured logging to stderr.

   Format: <unix_ms> [LEVEL] [component] message
   Example: 1714742400123 [INFO ] [master] worker connected fd=12 active=1

   Stderr is unbuffered (last line survives a crash) and redirectable
   independently from stdout. Grep across one stream, no interleaving.

   Usage:
       LOG_INFO ("master", "phase advance: JOB_MAP -> JOB_REDUCE");
       LOG_WARN ("master", "task %u attempt %u timed out", task_id, attempt);
       LOG_ERROR("worker", "rename(%s -> %s): %s", tmp, fin, strerror(errno));

   The format is plain printf-compatible. No GNU `##__VA_ARGS__` quirks: the
   format string is itself part of __VA_ARGS__, so passing zero extra args
   still expands cleanly under -std=c17. */

#define LOG_(level, component, ...)                                            \
  do {                                                                         \
    struct timespec _log_ts;                                                   \
    clock_gettime(CLOCK_REALTIME, &_log_ts);                                   \
    long long _log_ms =                                                        \
        (long long)_log_ts.tv_sec * 1000 + _log_ts.tv_nsec / 1000000;          \
    fprintf(stderr, "%lld [%s] [%s] ", _log_ms, level, component);             \
    fprintf(stderr, __VA_ARGS__);                                              \
    fputc('\n', stderr);                                                       \
  } while (0)

#define LOG_INFO(component, ...)  LOG_("INFO ", component, __VA_ARGS__)
#define LOG_WARN(component, ...)  LOG_("WARN ", component, __VA_ARGS__)
#define LOG_ERROR(component, ...) LOG_("ERROR", component, __VA_ARGS__)

#endif /* LOG_H */
