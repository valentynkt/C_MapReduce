#include "master.h"
#include "config.h"
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
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

master_t master;

static void maybe_advance_phase(void) {
  if (master.job.phase == JOB_MAP && master.job.maps_done == master.job.M) {
    master.job.phase = JOB_REDUCE;
  }
  if (master.job.phase == JOB_REDUCE &&
      master.job.reduces_done == master.job.R) {
    master.job.phase = JOB_DONE;
  }
}

static int master_handle_get_task(int fd) {
  switch (master.job.phase) {
  case JOB_MAP: {
    int choosen = -1;
    for (uint32_t i = 0; i < master.job.M; i++) {
      task_t *task = &master.maps[i];
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
        worker->current_kind = TASK_KIND_MAP;
        worker->current_task_id = i;
        worker->current_attempt_id = task->current_attempt;
        choosen = (int)i;
        break;
      }
    }

    uint8_t buf[MSG_MAX];
    size_t out_len;
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
      if (server_send(&master.server, fd, (const char *)buf,
                      (uint32_t)out_len) != 0)
        return -1;
    } else {
      /* No PENDING. If maps_done < M, some are still running -> WAIT.
         If maps_done == M, TaskDone failed to advance the phase -> bug. */
      assert(master.job.maps_done < master.job.M);
      rpc_wait_resp_t msg = (rpc_wait_resp_t){
          .wait_ms = (uint32_t)(master.job.task_timeout_ms / 2),
      };
      if (rpc_encode_wait_resp(buf, sizeof buf, &msg, &out_len) != 0)
        return -1;
      if (server_send(&master.server, fd, (const char *)buf,
                      (uint32_t)out_len) != 0)
        return -1;
    }
    break;
  }
  case JOB_REDUCE:
    break;
  case JOB_DONE:
    break;
  case JOB_FAILED:
    break;
  default:
    perror("wrong job phase");
    return -1;
  }
  return 0;
}

static int master_handle_task_done(int fd, const rpc_task_done_req_t *msg) {

  switch (master.job.phase) {
  case JOB_MAP: {
    // ToDo: Probably could be extracted or moved above, to not duplicate in
    // Reduce
    int current_task_id = master.workers[fd].current_task_id;
    task_t *t = &master.maps[current_task_id];

    uint8_t buf[MSG_MAX];
    size_t out_len;

    if (rpc_encode_task_done_ack(buf, sizeof buf, &out_len) != 0)
      return -1;

    if (msg->attempt_id != t->current_attempt) {

      if (server_send(&master.server, fd, (const char *)buf,
                      (uint32_t)out_len) != 0) {

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

      master.job.maps_done += 1;
      maybe_advance_phase();

    } else {
      t->history[t->attempts_count - 1] = (attempt_record_t){
          .attempt_id = t->current_attempt,
          .worker_fd = fd,
          .end_status = ATTEMPT_FAILED,
          .ended_at_ms = master.server.now_ms,
      };
      if (t->attempts_count >= MAX_ATTEMPTS) {
        t->state = TASK_FAILED;
        master.job.phase = JOB_FAILED;
      } else {
        t->state = TASK_PENDING;
      }
    }

    // Send ACK
    if (server_send(&master.server, fd, (const char *)buf, (uint32_t)out_len) !=
        0)
      return -1;
    master.workers[fd].has_task = false;
    break;
  }
  case JOB_REDUCE:
    return -1;
  case JOB_DONE:
    return -1;
  case JOB_FAILED:
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
}

static void master_on_disconnect(int fd, void *ud) {
  master_t *m = ud;
  /* timeout sweep handles task reset, just clear the slot */
  m->workers[fd] = (worker_t){0};
}

static void master_periodic(void *ud) {
  master_t *m = ud;
  m->now_realtime_ms = realtime_ms();
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
    fprintf(stderr, "scandir(%s): %s\n", master.config.input_dir,
            strerror(errno));
    return -1;
  }

  uint32_t M = 0;
  for (int i = 0; i < n; i++) {
    char path[MAPREDUCE_PATH_MAX];
    int written = snprintf(path, sizeof path, "%s/%s", master.config.input_dir,
                           entries[i]->d_name);
    if (written < 0 || (size_t)written >= sizeof path) {
      fprintf(stderr, "input path too long: %s/%s\n", master.config.input_dir,
              entries[i]->d_name);
      free_scandir_entries(entries, n);
      return -1;
    }

    struct stat sb;
    if (stat(path, &sb) == -1) {
      fprintf(stderr, "stat(%s): %s\n", path, strerror(errno));
      free_scandir_entries(entries, n);
      return -1;
    }
    if (!S_ISREG(sb.st_mode))
      continue;

    if (M >= MAX_MAP_TASKS) {
      fprintf(stderr, "too many input splits in %s (>%d)\n",
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
    fprintf(stderr, "no input splits found in %s\n", master.config.input_dir);
    return -1;
  }

  /* Backstop: config validator caps n_reduce at MAX_REDUCE_TASKS, but defend
     here too against stale/uninitialized config. */
  uint32_t R = (uint32_t)master.config.n_reduce;
  if (R == 0 || R > MAX_REDUCE_TASKS) {
    fprintf(stderr, "invalid n_reduce=%u (must be 1..%d)\n", R,
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

  printf("[mapreduce-master] job init: M=%u R=%u input_dir=%s\n", M, R,
         master.config.input_dir);
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

  printf("[mapreduce-master] listening on port %d (hz=%d)\n",
         master.config.port, master.config.hz);
  return EXIT_SUCCESS;
}

static void master_shutdown(void) { server_shutdown(&master.server); }

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
