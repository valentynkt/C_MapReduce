#include "master.h"
#include "aof.h"
#include "config.h"
#include "log.h"
#include "rpc.h"
#include "server.h"
#include "task.h"
#include "util.h"
#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

master_t master;

static const char *phase_name(job_phase_e p) {
  switch (p) {
  case JOB_MAP:
    return "JOB_MAP";
  case JOB_REDUCE:
    return "JOB_REDUCE";
  case JOB_DONE:
    return "JOB_DONE";
  case JOB_FAILED:
    return "JOB_FAILED";
  }
  return "?";
}

static const char *attempt_end_name(attempt_end_status_e s) {
  switch (s) {
  case ATTEMPT_RUNNING:
    return "RUNNING";
  case ATTEMPT_SUCCESS:
    return "SUCCESS";
  case ATTEMPT_FAILED:
    return "FAILED";
  case ATTEMPT_TIMEOUT:
    return "TIMEOUT";
  case ATTEMPT_DISCONNECT:
    return "DISCONNECT";
  }
  return "?";
}

/* Close the current attempt of `t` without success.
   Appends a history record with the given end_status, then decides:
     - if attempts_count >= MAX_ATTEMPTS:
         state -> TASK_FAILED, job.phase -> JOB_FAILED.
     - else:
         state -> TASK_PENDING (ready for reassignment).
   Always clears owner_fd; the invariant "state == IN_PROGRESS iff owner_fd !=
   -1" is maintained.

   Does NOT touch worker bookkeeping (`has_task`). The trigger sites own that:
     - master_handle_task_done clears it after the ACK is sent.
     - sweep_timeouts clears it when it decides to time the worker out.
     - master_on_disconnect zeroes the whole worker slot. */
static int task_attempt_ended(task_t *t, attempt_end_status_e end_status,
                              int fd, int64_t now_ms, const char *kind_label,
                              uint32_t task_id) {
  t->history[t->attempts_count - 1] = (attempt_record_t){
      .attempt_id = t->current_attempt,
      .worker_fd = fd,
      .end_status = end_status,
      .ended_at_ms = now_ms,
  };
  t->owner_fd = -1;

  if (t->attempts_count >= MAX_ATTEMPTS) {
    t->state = TASK_FAILED;
    master.job.phase = JOB_FAILED;
    LOG_ERROR("master",
              "%s task=%u FAILED terminal after %u attempts (%s); phase -> "
              "JOB_FAILED",
              kind_label, task_id, t->attempts_count,
              attempt_end_name(end_status));
    return -1;
  } else {
    t->state = TASK_PENDING;
    LOG_WARN(
        "master", "%s task=%u attempt=%u %s fd=%d — reset to PENDING (%u/%u)",
        kind_label, task_id, t->current_attempt, attempt_end_name(end_status),
        fd, t->attempts_count, (uint32_t)MAX_ATTEMPTS);
  }
  return 0;
}

/* Walk an array of tasks; for each TASK_IN_PROGRESS whose current attempt has
   exceeded task_timeout_ms, close it via task_attempt_ended() with TIMEOUT.
   Phase-agnostic — caller selects the right array (maps[] or reduces[]). */
static void sweep_timeouts(task_t *tasks, uint32_t n, const char *kind_label) {
  int64_t now = master.server.now_ms;
  int64_t timeout_ms = master.config.task_timeout_ms;

  for (uint32_t i = 0; i < n; i++) {
    task_t *t = &tasks[i];
    if (t->state != TASK_IN_PROGRESS)
      continue;
    if (now - t->started_at_ms <= timeout_ms)
      continue;

    int fd = t->owner_fd;
    LOG_WARN("master",
             "%s task=%u attempt=%u timed out fd=%d (elapsed_ms=%lld)",
             kind_label, i, t->current_attempt, fd,
             (long long)(now - t->started_at_ms));

    // -1 from task_attempt_ended means the job is failed, so no need to
    // continue the loop;

    if (task_attempt_ended(t, ATTEMPT_TIMEOUT, fd, now, kind_label, i) != 0) {
      break;
    }
  }
}

static void maybe_advance_phase(void) {
  if (master.job.phase == JOB_MAP && master.job.maps_done == master.job.M) {
    LOG_INFO("master", "phase: JOB_MAP -> JOB_REDUCE (maps_done=%u/%u)",
             master.job.maps_done, master.job.M);
    master.job.phase = JOB_REDUCE;
  }
  if (master.job.phase == JOB_REDUCE &&
      master.job.reduces_done == master.job.R) {
    LOG_INFO("master", "phase: JOB_REDUCE -> JOB_DONE (reduces_done=%u/%u)",
             master.job.reduces_done, master.job.R);
    master.job.phase = JOB_DONE;
  }
}
static int choose_available_task(int fd, task_kind_e kind) {
  int choosen = -1;
  assert(kind == TASK_KIND_MAP || kind == TASK_KIND_REDUCE);
  uint32_t n_tasks = (kind == TASK_KIND_MAP) ? master.job.M : master.job.R;
  task_t *tasks = (kind == TASK_KIND_MAP) ? master.maps : master.reduces;

  for (uint32_t i = 0; i < n_tasks; i++) {
    task_t *task = &tasks[i];
    int64_t now_ms = master.server.now_ms;
    if (task->state == TASK_PENDING) {
      task->state = TASK_IN_PROGRESS;
      task->current_attempt += 1;
      task->attempts_count += 1;
      task->owner_fd = fd;
      task->started_at_ms = now_ms;
      task->history[task->attempts_count - 1] =
          (attempt_record_t){.attempt_id = task->current_attempt,
                             .worker_fd = fd,
                             .started_at_ms = now_ms,
                             .end_status = ATTEMPT_RUNNING,
                             .ended_at_ms = 0};
      worker_t *worker = &master.workers[fd];
      worker->has_task = true;
      worker->current_kind = kind;
      worker->current_task_id = i;
      worker->current_attempt_id = task->current_attempt;
      choosen = (int)i;
      break;
    }
  }
  return choosen;
}

static int wait_for_available_tasks(int fd, uint8_t *buf, size_t cap,
                                    size_t *out_len, task_kind_e kind) {
  /* No PENDING. If maps_done < M, some are still running -> WAIT.
     If maps_done == M, TaskDone failed to advance the phase -> bug. */
  bool assert_guard =
      (kind == TASK_KIND_MAP && master.job.maps_done < master.job.M) ||
      (kind == TASK_KIND_REDUCE && master.job.reduces_done < master.job.R);

  assert(assert_guard);
  rpc_wait_resp_t msg = (rpc_wait_resp_t){
      .wait_ms = (uint32_t)(master.job.task_timeout_ms / 2),
  };
  if (rpc_encode_wait_resp(buf, cap, &msg, out_len) != 0)
    return -1;
  if (kind == TASK_KIND_MAP) {
    LOG_INFO("master",
             "WAIT for available tasks fd=%d phase=JOB_MAP wait_ms=%u "
             "maps_done=%u/%u",
             fd, msg.wait_ms, master.job.maps_done, master.job.M);
  } else {
    LOG_INFO("master",
             "WAIT for available tasks fd=%d phase=JOB_REDUCE wait_ms=%u "
             "reduces_done=%u/%u",
             fd, msg.wait_ms, master.job.reduces_done, master.job.R);
  }
  return 0;
}

static int master_handle_get_task(int fd) {
  uint8_t buf[MSG_MAX];
  size_t out_len;
  switch (master.job.phase) {
  case JOB_MAP: {
    int choosen = choose_available_task(fd, TASK_KIND_MAP);
    if (choosen >= 0) {
      task_t *t = &master.maps[choosen];
      rpc_task_map_resp_t msg = (rpc_task_map_resp_t){
          .task_id = (uint32_t)choosen,
          .attempt_id = t->current_attempt,
          .input_path_len = t->input_path_len,
          .n_reduce = master.job.R,
      };
      memcpy(msg.input_path, t->input_path, t->input_path_len);
      if (rpc_encode_task_map_resp(buf, sizeof buf, &msg, &out_len) != 0)
        return -1;

      LOG_INFO("master",
               "assigned MAP task=%d attempt=%u fd=%d input=%s n_reduce=%u",
               choosen, t->current_attempt, fd, t->input_path, master.job.R);
    } else {
      if (wait_for_available_tasks(fd, buf, sizeof buf, &out_len,
                                   TASK_KIND_MAP) != 0) {
        return -1;
      }
    }
    break;
  }
  case JOB_REDUCE: {
    int choosen = choose_available_task(fd, TASK_KIND_REDUCE);

    if (choosen >= 0) {
      task_t *t = &master.reduces[choosen];
      rpc_task_reduce_resp_t msg =
          (rpc_task_reduce_resp_t){.task_id = (uint32_t)choosen,
                                   .attempt_id = t->current_attempt,
                                   .n_map = master.job.M};

      if (rpc_encode_task_reduce_resp(buf, sizeof buf, &msg, &out_len) != 0)
        return -1;

      LOG_INFO("master", "assigned REDUCE task=%d attempt=%u fd=%d  n_map=%u",
               choosen, t->current_attempt, fd, master.job.M);

    } else {

      if (wait_for_available_tasks(fd, buf, sizeof buf, &out_len,
                                   TASK_KIND_REDUCE) != 0) {
        return -1;
      }
    }
    break;
  }
  case JOB_DONE:
    if (rpc_encode_done_resp(buf, sizeof buf, &out_len) != 0) {
      return -1;
    }

    LOG_INFO("master", "GetTask fd=%d phase=JOB_DONE", fd);
    break;
  case JOB_FAILED:
    if (rpc_encode_done_resp(buf, sizeof buf, &out_len) != 0) {
      return -1;
    }
    LOG_INFO("master", "GetTask fd=%d phase=JOB_FAILED", fd);
    break;
  default:
    LOG_ERROR("master", "GetTask fd=%d invalid phase=%d", fd,
              (int)master.job.phase);
    return -1;
  }

  if (server_send(&master.server, fd, (const char *)buf, (uint32_t)out_len) !=
      0)
    return -1;
  return 0;
}

static int task_done_helper(int fd, const rpc_task_done_req_t *msg) {
  job_phase_e phase = master.job.phase;
  assert(phase == JOB_MAP || phase == JOB_REDUCE);
  int current_task_id = master.workers[fd].current_task_id;
  task_t *t;

  if (phase == JOB_MAP) {
    t = &master.maps[current_task_id];
  } else {

    t = &master.reduces[current_task_id];
  }

  uint8_t buf[MSG_MAX];
  size_t out_len;

  if (rpc_encode_task_done_ack(buf, sizeof buf, &out_len) != 0)
    return -1;

  // ignore the result, zombie process, send ack.
  if (msg->attempt_id != t->current_attempt) {
    LOG_WARN("master",
             "stale TaskDone fd=%d task=%u attempt=%u (current=%u) — ack and "
             "drop",
             fd, msg->task_id, msg->attempt_id, t->current_attempt);

    if (server_send(&master.server, fd, (const char *)buf, (uint32_t)out_len) !=
        0) {

      // ignore the result, zombie process;
      master.workers[fd].has_task = false;
      return -1;
    }
    return 0;
  }
  // Task Success
  if (msg->result == 0) {
    t->owner_fd = -1;
    t->state = TASK_COMPLETED;

    t->history[t->attempts_count - 1] = (attempt_record_t){
        .attempt_id = t->current_attempt,
        .worker_fd = fd,
        .end_status = ATTEMPT_SUCCESS,
        .ended_at_ms = master.server.now_ms,
    };

    if (phase == JOB_MAP) {
      master.job.maps_done += 1;
      LOG_INFO("master",
               "MAP task=%d attempt=%u COMPLETED fd=%d (maps_done=%u/%u)",
               current_task_id, t->current_attempt, fd, master.job.maps_done,
               master.job.M);
    } else {
      master.job.reduces_done += 1;
      LOG_INFO("master",
               "REDUCE task=%d attempt=%u COMPLETED fd=%d (reduces_done=%u/%u)",
               current_task_id, t->current_attempt, fd, master.job.reduces_done,
               master.job.R);
    }
    maybe_advance_phase();
  } else {
    if (phase == JOB_MAP)
      task_attempt_ended(t, ATTEMPT_FAILED, fd, master.server.now_ms, "MAP",
                         (uint32_t)current_task_id);
    else

      task_attempt_ended(t, ATTEMPT_FAILED, fd, master.server.now_ms, "REDUCE",
                         (uint32_t)current_task_id);
  }

  // Send ACK
  if (server_send(&master.server, fd, (const char *)buf, (uint32_t)out_len) !=
      0)
    return -1;

  master.workers[fd].has_task = false;
  return 0;
}

static int master_handle_task_done(int fd, const rpc_task_done_req_t *msg) {
  switch (master.job.phase) {
  case JOB_MAP:
  case JOB_REDUCE:
    return task_done_helper(fd, msg);
  case JOB_DONE:
    LOG_WARN("master", "TaskDone fd=%d phase=JOB_DONE — unexpected", fd);
    return -1;
  case JOB_FAILED:
    LOG_WARN("master", "TaskDone fd=%d phase=JOB_FAILED — ignoring", fd);
    return -1;
  }
  return 0;
}

static int master_on_message(int fd, const uint8_t *payload, size_t len,
                             void *ud) {
  (void)ud;
  rpc_kind_t kind;
  if (rpc_peek_kind(payload, len, &kind) != 0)
    return -1;
  switch (kind) {
  case RPC_GET_TASK_REQ:
    if (master_handle_get_task(fd) != 0)
      return -1;
    break;
  case RPC_TASK_DONE_REQ: {

    rpc_task_done_req_t msg;
    if (rpc_decode_task_done_req(payload + 1, len - 1, &msg) != 0)
      return -1;
    if (master_handle_task_done(fd, &msg) != 0)
      return -1;
    break;
  }
  default:
    return -1;
  }
  return 0;
}

static void master_on_connect(int fd, void *ud) {
  master_t *m = ud;
  m->workers[fd] = (worker_t){
      .connected_at_ms = m->server.now_ms,
      .has_task = false,
  };
  LOG_INFO("master", "worker connected fd=%d", fd);
}

static void master_on_disconnect(int fd, void *ud) {
  master_t *m = ud;
  worker_t *w = &m->workers[fd];
  if (w->has_task) {
    LOG_WARN("master",
             "worker disconnected fd=%d had in-flight task kind=%d id=%u "
             "attempt=%u (timeout sweep will reset)",
             fd, (int)w->current_kind, w->current_task_id,
             w->current_attempt_id);
  } else {
    LOG_INFO("master", "worker disconnected fd=%d", fd);
  }
  /* timeout sweep handles task reset, just clear the slot */
  m->workers[fd] = (worker_t){0};
}
static void master_periodic(void *ud) {
  master_t *m = ud;
  m->now_realtime_ms = realtime_ms();

  /* Sweep at ~1 Hz when server hz=10. Sweep period is decoupled-via-modulo
     from server tick rate; revisit if hz changes — see roadmap §2.4. */
  if (m->server.cronloops % 10 != 0)
    return;

  switch (m->job.phase) {
  case JOB_MAP:
    sweep_timeouts(m->maps, m->job.M, "MAP");
    break;
  case JOB_REDUCE:
    sweep_timeouts(m->reduces, m->job.R, "REDUCE");
    break;
  case JOB_DONE:
  case JOB_FAILED:
    /* Nothing IN_PROGRESS to time out in terminal phases. */
    break;
  }
}

static int input_filter(const struct dirent *e) { return e->d_name[0] != '.'; }

static void free_scandir_entries(struct dirent **entries, int n) {
  for (int i = 0; i < n; i++)
    free(entries[i]);
  free(entries);
}

static int master_init_job(void) {
  struct dirent **entries = NULL;
  int n = scandir(master.config.input_dir, &entries, input_filter, alphasort);
  if (n < 0) {
    LOG_ERROR("master", "scandir(%s): %s", master.config.input_dir,
              strerror(errno));
    return -1;
  }

  if (mkdir("intermediate", 0755) != 0 && errno != EEXIST) {
    LOG_ERROR("master", "mkdir(intermediate): %s", strerror(errno));
    return -1;
  }
  if (mkdir("output", 0755) != 0 && errno != EEXIST) {
    LOG_ERROR("master", "mkdir(output): %s", strerror(errno));
    return -1;
  }

  uint32_t M = 0;
  for (int i = 0; i < n; i++) {
    char path[MAPREDUCE_PATH_MAX];
    int written = snprintf(path, sizeof path, "%s/%s", master.config.input_dir,
                           entries[i]->d_name);
    if (written < 0 || (size_t)written >= sizeof path) {
      LOG_ERROR("master", "input path too long: %s/%s", master.config.input_dir,
                entries[i]->d_name);
      free_scandir_entries(entries, n);
      return -1;
    }

    struct stat sb;
    if (stat(path, &sb) == -1) {
      LOG_ERROR("master", "stat(%s): %s", path, strerror(errno));
      free_scandir_entries(entries, n);
      return -1;
    }
    if (!S_ISREG(sb.st_mode))
      continue;

    if (M >= MAX_MAP_TASKS) {
      LOG_ERROR("master", "too many input splits in %s (>%d)",
                master.config.input_dir, MAX_MAP_TASKS);
      free_scandir_entries(entries, n);
      return -1;
    }

    master.maps[M] = (task_t){
        .state = TASK_PENDING,
        .owner_fd = -1,
    };
    memcpy(master.maps[M].input_path, path, (size_t)written);
    master.maps[M].input_path[written] = '\0';
    master.maps[M].input_path_len = (uint16_t)written;
    M++;
  }

  free_scandir_entries(entries, n);

  if (M == 0) {
    LOG_ERROR("master", "no input splits found in %s", master.config.input_dir);
    return -1;
  }

  /* Backstop: config validator caps n_reduce at MAX_REDUCE_TASKS, but defend
     here too against stale/uninitialized config. */
  uint32_t R = (uint32_t)master.config.n_reduce;
  if (R == 0 || R > MAX_REDUCE_TASKS) {
    LOG_ERROR("master", "invalid n_reduce=%u (must be 1..%d)", R,
              MAX_REDUCE_TASKS);
    return -1;
  }

  for (uint32_t i = 0; i < R; i++) {
    master.reduces[i] = (task_t){
        .state = TASK_PENDING,
        .owner_fd = -1,
    };
  }

  master.job = (job_t){
      .M = M,
      .R = R,
      .phase = JOB_MAP,
      .maps_done = 0,
      .reduces_done = 0,
      .task_timeout_ms = master.config.task_timeout_ms,
  };

  LOG_INFO("master",
           "job init: M=%u R=%u input_dir=%s task_timeout_ms=%lld phase=%s", M,
           R, master.config.input_dir, (long long)master.config.task_timeout_ms,
           phase_name(master.job.phase));
  return 0;
}

static int master_init(void) {
  master.now_realtime_ms = realtime_ms();

  if (server_init(&master.server, master.config.port, master.config.tcp_backlog,
                  master.config.hz, master.config.client_timeout_s) != 0) {
    return EXIT_FAILURE;
  }
  server_set_on_message_cb(&master.server, master_on_message, &master);
  server_set_on_connect_cb(&master.server, master_on_connect, &master);
  server_set_on_disconnect_cb(&master.server, master_on_disconnect, &master);
  server_set_on_periodic_cb(&master.server, master_periodic, &master);

  LOG_INFO("master", "listening on port %d hz=%d client_timeout_s=%d",
           master.config.port, master.config.hz,
           master.config.client_timeout_s);
  return EXIT_SUCCESS;
}

static void master_shutdown(void) {
  aof_close(&master);
  server_shutdown(&master.server);
}

int master_main(const char *configfile) {
  master_config_init();

  if (configfile) {
    if (load_config(configfile) == -1)
      return EXIT_FAILURE;
  }

  if (master_init_job() != 0) {
    return EXIT_FAILURE;
  }

  if (master_init() == EXIT_FAILURE) {
    return EXIT_FAILURE;
  }

  server_run(&master.server);
  master_shutdown();
  return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
  const char *configfile = (argc >= 2) ? argv[1] : NULL;
  return master_main(configfile);
}
