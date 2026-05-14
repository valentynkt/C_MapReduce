#include "aof.h"
#include "config.h"
#include "crc64.h"
#include "log.h"
#include "rpc.h"
#include "task.h"
#include "util.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <sys/endian.h>
#endif
#include "master.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

/* ToDo */
/* File-level header */
int aof_write_file_header(int fd) {
  size_t off = 0;
  uint8_t hdr[AOF_FILE_HEADER_SIZE];
  memcpy(hdr, AOF_FILE_MAGIC, AOF_FILE_MAGIC_LEN);
  off += AOF_FILE_MAGIC_LEN;

  uint32_t version_net = htonl(AOF_FILE_VERSION);
  memcpy(hdr + off, &version_net, AOF_FILE_VERSION_LEN);
  off += AOF_FILE_VERSION_LEN;

  uint32_t flags_net = 0;
  memcpy(hdr + off, &flags_net, 4);
  off += 4;

  uint64_t created_net = htobe64((uint64_t)realtime_ms());
  memcpy(hdr + off, &created_net, 8);
  off += 8;

  uint64_t crc = crc64(0, hdr, off);
  uint64_t crc_net = htobe64(crc);
  memcpy(hdr + off, &crc_net, 8);

  ssize_t written_n = write_all(fd, (const char *)hdr, AOF_FILE_HEADER_SIZE);
  if (written_n != AOF_FILE_HEADER_SIZE) {
    LOG_ERROR("aof_write_file_header",
              "write_all didn't wrote all data: wrote: %zu from: %d\n",
              written_n, AOF_FILE_HEADER_SIZE);
    return -1;
  }
  return 0;
}

/* Read+validate the 32-byte file header from `fd`'s current offset.
   On success the fd is positioned at the first record (offset = HEADER_SIZE).
   Returns 0 on success, -1 on bad magic / unknown version / corrupt CRC. */
static int aof_read_file_header(int fd) {
  uint8_t hdr[AOF_FILE_HEADER_SIZE];
  ssize_t n = read_exact(fd, (char *)hdr, AOF_FILE_HEADER_SIZE);

  if (n != AOF_FILE_HEADER_SIZE) {
    LOG_ERROR("aof.aof_read_file_header",
              "header read short (%zd of %d) - file too small\n", n,
              AOF_FILE_HEADER_SIZE);
    return -1;
  }
  if (memcmp(hdr, AOF_FILE_MAGIC, AOF_FILE_MAGIC_LEN) != 0) {
    LOG_ERROR("aof.aof_read_file_header",
              "Bad magic. This file is not version or Legacy/Corrupt. \n "
              "Delete it and restart to begin fresh. \n");
    return -1;
  }
  uint32_t version_net;
  memcpy(&version_net, hdr + AOF_FILE_MAGIC_LEN, AOF_FILE_VERSION_LEN);
  uint32_t version = ntohl(version_net);
  if (version != AOF_FILE_VERSION) {
    LOG_ERROR("aof.aof_read_file_header",
              "Unsupported file version %u (this build expects %u)\n", version,
              AOF_FILE_VERSION);
    return -1;
  }
  uint64_t crc_stored_net;
  memcpy(&crc_stored_net, hdr + (AOF_FILE_HEADER_SIZE - AOF_FILE_CRC_LEN),
         AOF_FILE_CRC_LEN);
  uint64_t crc_stored = be64toh(crc_stored_net);
  uint64_t crc_computed =
      crc64(0, hdr, (AOF_FILE_HEADER_SIZE - AOF_FILE_CRC_LEN));
  if (crc_stored != crc_computed) {
    LOG_ERROR("aof.aof_read_file_header", "header CRC mismatch \n");
    return -1;
  }
  return 0;
}
/* kind (1), task_id 4, attemptId 4, crc 8 */
/* ToDo: Align to 32? */

static ssize_t aof_write_record(int fd, uint8_t *buf,
                                const rpc_task_done_req_t *msg,
                                task_kind_e kind) {
  size_t off = 0;
  buf[off] = kind;
  off += 1;
  uint32_t task_id_net = htonl(msg->task_id);
  memcpy(buf + off, &task_id_net, 4);
  off += 4;

  uint32_t attempt_id_net = htonl(msg->attempt_id);
  memcpy(buf + off, &attempt_id_net, 4);
  off += 4;
  uint64_t crc = crc64(0, buf, off);
  uint64_t crc_net = htobe64(crc);
  memcpy(buf + off, &crc_net, 8);
  off += 8;

  ssize_t written_n = write_all(fd, (const char *)buf, off);
  if (written_n == -1 || written_n != (ssize_t)off) {
    return -1;
  }

  return off;
}
// Input_pathes = M * (input_path_len, input path)
static ssize_t aof_write_job(int fd, uint8_t *buf, size_t buf_sz, uint32_t M,
                             uint32_t R, const task_t *maps) {
  size_t off = 0;

  uint32_t M_net = htonl(M);
  uint32_t R_net = htonl(R);

  if (off + sizeof(M_net) + sizeof(R_net) > buf_sz) {
    LOG_ERROR("aof_serialize_job", "buf_sz is too small: %zu\n", buf_sz);
    return -1;
  }
  memcpy(buf + off, &M_net, sizeof(M_net));
  off += sizeof(M_net);

  memcpy(buf + off, &R_net, sizeof(R_net));
  off += sizeof(R_net);

  for (uint32_t i = 0; i < M; i++) {
    uint16_t path_len = maps[i].input_path_len;
    uint16_t path_len_net = htons(path_len);

    if (off + sizeof(path_len_net) + path_len > buf_sz) {
      LOG_ERROR("aof_serialize_job", "buf_sz is too small: %zu\n", buf_sz);
      return -1;
    }

    memcpy(buf + off, &path_len_net, sizeof(path_len_net));
    off += sizeof(path_len_net);
    memcpy(buf + off, maps[i].input_path, path_len);

    off += path_len;
  }

  ssize_t written_n = write_all(fd, (const char *)buf, off);
  if (written_n == -1 || written_n != (ssize_t)off) {
    return -1;
  }
  return (ssize_t)off;
}

int aof_open(const char *path) {
  if (path == NULL) {
    LOG_ERROR("aof.aof_open", "path is NULL\n");
    return -1;
  }

  int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd == -1) {
    LOG_ERROR("aof.aof_open", "open fd: %d failedr. \n", fd);
    return -1;
  }
  return fd;
}

int aof_load(const char *path) {
  struct stat st;
  if (stat(path, &st) == -1 || st.st_size == 0)
    return 0;

  if (st.st_size < AOF_FILE_HEADER_SIZE) {
    LOG_ERROR("aof.load",
              "aof: file too small for v1 header (%lld bytes < %d)\n",
              (long long)st.st_size, AOF_FILE_HEADER_SIZE);
  }

  int fd = open(path, O_RDWR);
  if (fd == -1) {
    LOG_ERROR("aof.load", "open fd: %d\n", fd);
    return -1;
  }
  off_t total_size = st.st_size;
  off_t valid_end = 0;
  size_t records = 0;
}
/* We have to design the record that we will serialize
 * we def need to save some part of task_t so we could replay the completed
 tasks.
 * and than we could move the phase based on the number of completed tasks
 etc..
 * BUT do we need to have a seperate file for each job, maybe adding the auto
 generated id, for each job, so we could save state of the job itself
 * especially the M - R configuration of it, so during replay, we could now,
 to what M - R are we targetting etc..
 * so it could be a seperate file with filename mr-{job-id}.aof (it will be
 byte storage)
 * having in the beginning the file header with version, checksum etc.. for
 validation.
 * than the "job header" with the number of M and R and some other files, and
 than individual records of tasks after it?
 * LAYOUT:
 * FILE HEADER (MAGIC, VERSION, FLAGS, CRC etc...)
 * JOB: M, R, Input Pathes? (Variable Lenght, M * MAPREDUCE_PATH_MAX);
 *
  char input_path[MAPREDUCE_PATH_MAX];
  uint16_t input_path_len;
 * record (Fixed Sized vs Lengh prefixed? I prefer fixed sized for now)
 * task_kind_e, task_id, attemptId, timestamp, crc.
*/
int aof_append_completed(const master_t *master, const rpc_task_done_req_t *msg,
                         task_kind_e kind, const char *path) {
  const job_t job = master->job;
  int fd = aof_open(path);
  if (fd == -1) {
    LOG_ERROR("aof.aof_append_completed", "aof_open failed\n");
    return -1;
  }
  struct stat st;
  if (fstat(fd, &st) == -1) {
    LOG_ERROR("aof.aof_append_completed", "fstat error, fd: %d, path: %s\n", fd,
              path);
    return -1;
  }
  // File Empty we have to add file header to it
  if (st.st_size == 0) {
    if (aof_write_file_header(fd) == -1) {
      return -1;
    }

    size_t job_buf_size = sizeof(job.M) + sizeof(job.R);
    for (uint32_t i = 0; i < job.M; i++) {
      task_t cur_t = master->maps[i];
      job_buf_size += sizeof(cur_t.input_path_len) + cur_t.input_path_len;
    }

    uint8_t *job_buf = malloc(job_buf_size);
    if (aof_write_job(fd, job_buf, job_buf_size, job.M, job.R, master->maps) ==
        -1) {

      return -1;
    }
    free(job_buf);
  }

  uint8_t rec[AOF_RECORD_SIZE];
  if (aof_write_record(fd, rec, msg, kind) == -1) {
    return -1;
  }

  fsync(fd);
  close(fd);
}
