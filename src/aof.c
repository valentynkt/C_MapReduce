#include "aof.h"
#include "config.h"
#include "crc64.h"
#include "log.h"
#include "master.h"
#include "rpc.h"
#include "task.h"
#include "util.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <sys/endian.h>
#endif

/* ----- file-level header ----- */
/* layout: 8B magic | 4B version | 4B flags | 8B created_ms | 8B crc64 */

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
    LOG_ERROR("aof_write_file_header", "write_all short: wrote %zd of %d",
              written_n, AOF_FILE_HEADER_SIZE);
    return -1;
  }
  return 0;
}

/* Read+validate the file header from `fd`'s current offset.
   On success the fd is positioned at the first byte after the header. */
static int aof_read_file_header(int fd) {
  uint8_t hdr[AOF_FILE_HEADER_SIZE];
  ssize_t n = read_exact(fd, (char *)hdr, AOF_FILE_HEADER_SIZE);

  if (n != AOF_FILE_HEADER_SIZE) {
    LOG_ERROR("aof_read_file_header", "header read short (%zd of %d)", n,
              AOF_FILE_HEADER_SIZE);
    return -1;
  }
  if (memcmp(hdr, AOF_FILE_MAGIC, AOF_FILE_MAGIC_LEN) != 0) {
    LOG_ERROR("aof_read_file_header", "bad magic — not an AOF file or corrupt");
    return -1;
  }
  uint32_t version_net;
  memcpy(&version_net, hdr + AOF_FILE_MAGIC_LEN, AOF_FILE_VERSION_LEN);
  uint32_t version = ntohl(version_net);
  if (version != AOF_FILE_VERSION) {
    LOG_ERROR("aof_read_file_header",
              "unsupported version %u (build expects %u)", version,
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
    LOG_ERROR("aof_read_file_header", "header CRC mismatch");
    return -1;
  }
  return 0;
}
/* TODO: read record */
static int aof_read_record(int fd, rpc_task_done_req_t *out,
                           task_kind_e *kind) {
  uint8_t rec[AOF_RECORD_SIZE];
  ssize_t n = read_exact(fd, (char *)rec, AOF_RECORD_SIZE);
  if (n != AOF_RECORD_SIZE) {
    LOG_ERROR("aof_read_job", "header read short (%zd of %d)", n,
              AOF_RECORD_SIZE);
    return -1;
  }
  size_t off = 0;
  *kind = rec[off];
  off += 1;
  uint32_t task_id_net;
  memcpy(&task_id_net, rec + off, 4);
  off += 4;
  out->task_id = ntohl(task_id_net);

  uint32_t attempt_id_net;
  memcpy(&attempt_id_net, rec + off, 4);
  off += 4;
  out->attempt_id = ntohl(attempt_id_net);
  uint64_t crc_stored_net;
  memcpy(&crc_stored_net, rec + off, 8);
  off += 8;
  uint64_t crc_stored = be64toh(crc_stored_net);
  uint64_t crc_computed = crc64(0, rec, off - 8);
  if (crc_stored != crc_computed) {
    LOG_ERROR("aof_read_record", "CRC record mismatch");
    return -1;
  }

  return 0;
}

/* ----- record ----- */
/* layout: 1B kind | 4B task_id | 4B attempt_id | 8B crc64 */

static ssize_t aof_write_record(int fd, uint8_t *buf,
                                const rpc_task_done_req_t *msg,
                                task_kind_e kind) {
  size_t off = 0;
  buf[off] = (uint8_t)kind;
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
  return (ssize_t)off;
}

// What we return? read is probably not enough, we have to return the extracted
// data like M/R * input_path + input_path_len;
// I have to return the m r + pointer to malloc buf (which will have the len +
// path itself);
// returs off (aof_job_t out buf) off = size of buf
//
static ssize_t aof_read_job(int fd, aof_job_t *out) {
  ssize_t off = 0;
  // M 4b, R 4b
  ssize_t job_static_hdr = 8;

  uint8_t job_hdr[job_static_hdr];
  ssize_t n = read_exact(fd, (char *)job_hdr, job_static_hdr);

  if (n != job_static_hdr) {
    LOG_ERROR("aof_read_job", "header read short (%zd of %zd)", n,
              job_static_hdr);
    off = -1;
    goto done;
  }
  uint32_t m_net;
  memcpy(&m_net, job_hdr, sizeof(m_net));
  uint32_t m = ntohl(m_net);

  uint32_t r_net;
  memcpy(&r_net, job_hdr + sizeof(m_net), sizeof(r_net));
  uint32_t r = ntohl(r_net);

  size_t job_buf_size =
      sizeof(m_net) + sizeof(r_net) + (m * (2 + MAPREDUCE_PATH_MAX)) + 8;

  uint8_t *job_buf = malloc(job_buf_size);
  if (job_buf == NULL) {
    LOG_ERROR("aof_read_job", "job_buf malloc returned NULL");
    off = -1;
    goto done;
  }
  memcpy(job_buf, job_hdr, 8);
  off += 8;

  if (job_buf == NULL) {
    LOG_ERROR("aof_read_job", "malloc(%zu) failed", job_buf_size);
    off = -1;
    goto done;
  }
  for (uint32_t i = 0; i < m; i++) {
    ssize_t path_len_n = read_exact(fd, (char *)job_buf + off, 2);
    if (path_len_n != 2) {
      LOG_ERROR("aof_read_job", "path_len_n header read short (%zd of %d)",
                path_len_n, 2);
      off = -1;
      goto done;
    }

    off += path_len_n;
    uint16_t path_len_net;
    // getting the last path_len from buf;
    memcpy(&path_len_net, job_buf + off - 2, 2);
    uint16_t path_len = ntohs(path_len_net);
    ssize_t path_n = read_exact(fd, (char *)job_buf + off, path_len);
    if (path_n != path_len) {

      LOG_ERROR("aof_read_job", "path_n header read short (%zd of %d)", path_n,
                path_len);
      off = -1;
      goto done;
    }
    off += path_n;
  }
  uint64_t crc_stored_net;
  ssize_t crc_n = read_exact(fd, (char *)job_buf + off, 8);
  off += 8;
  if (crc_n != 8) {
    LOG_ERROR("aof_read_job", "crc_n header read short (%zd of %d)", crc_n, 8);
    off = -1;
    goto done;
  }

  memcpy(&crc_stored_net, job_buf + off - 8, 8);
  uint64_t crc_stored = be64toh(crc_stored_net);
  uint64_t crc_computed = crc64(0, job_buf, off - 8);
  if (crc_stored != crc_computed) {
    LOG_ERROR("aof_read_job", "job CRC mismatch");
    off = -1;
    goto done;
  }

  *out = (aof_job_t){.M = m, .R = r, .buf = job_buf};

done:
  /* Job buf is used externally so no cleanup on success path */
  if (off == -1) {

    if (job_buf) {
      free(job_buf);
    }
  }
  return off;
}

/* ----- job header ----- */
/* layout: 4B M | 4B R | M times: 2B path_len | path_len bytes of path */

static ssize_t aof_write_job(int fd, uint8_t *buf, size_t buf_sz, uint32_t M,
                             uint32_t R, const task_t *maps) {
  size_t off = 0;

  uint32_t M_net = htonl(M);
  uint32_t R_net = htonl(R);

  if (off + sizeof(M_net) + sizeof(R_net) > buf_sz) {
    LOG_ERROR("aof_write_job", "buf_sz too small: %zu", buf_sz);
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
      LOG_ERROR("aof_write_job", "buf_sz too small: %zu", buf_sz);
      return -1;
    }

    memcpy(buf + off, &path_len_net, sizeof(path_len_net));
    off += sizeof(path_len_net);
    memcpy(buf + off, maps[i].input_path, path_len);
    off += path_len;
  }

  uint64_t crc = crc64(0, buf, off);
  uint64_t crc_net = htobe64(crc);
  memcpy(buf + off, &crc_net, sizeof(crc_net));

  ssize_t written_n = write_all(fd, (const char *)buf, off);
  if (written_n == -1 || written_n != (ssize_t)off) {
    return -1;
  }
  return (ssize_t)off;
}

/* ----- open / load / close ----- */

int aof_open(const char *path) {
  if (path == NULL) {
    LOG_ERROR("aof_open", "path is NULL");
    return -1;
  }

  int fd = open(path, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd == -1) {
    LOG_ERROR("aof_open", "open(%s): %s", path, strerror(errno));
    return -1;
  }
  return fd;
}
/* TODO: */
int aof_load(master_t *master, const char *path) {
  struct stat st;
  if (stat(path, &st) == -1 || st.st_size == 0)
    return 0;

  if (st.st_size < AOF_FILE_HEADER_SIZE) {
    LOG_ERROR("aof_load", "file too small for v1 header (%lld bytes < %d)",
              (long long)st.st_size, AOF_FILE_HEADER_SIZE);
    return -1;
  }

  int fd = open(path, O_RDONLY | O_CLOEXEC);
  if (fd == -1) {
    LOG_ERROR("aof_load", "open(%s): %s", path, strerror(errno));
    return -1;
  }

  int rc = 0;
  if (aof_read_file_header(fd) != 0) {

    rc = -1;
    goto done;
  }

  aof_job_t aof_job = {0};

  ssize_t job_n = aof_read_job(fd, &aof_job);
  if (job_n == -1) {
    rc = -1;
    goto done;
  };
  for (uint32_t i = 0; i < aof_job.M; i++) {
    rpc_task_done_req_t task_done;
    task_kind_e kind;
    if (aof_read_record(fd, &task_done, &kind) == -1) {
      rc = -1;
      goto done;
    }
    if (kind == TASK_KIND_MAP) {
      master->maps[i] = (task_t) {
        .state = TASK_COMPLETED, .owner_fd = -1,
        .current_attempt = task_done.attempt_id,
      }

    } else if (kind == TASK_KIND_REDUCE) {
    }
    /* TODO: something */
  }
  /* TODO: read job header, walk records, populate master state */

done:
  if (aof_job.buf) {
    free(aof_job.buf);
  }
  close(fd);
  return rc;
}

int aof_close(master_t *master) {
  if (master->aof_fd < 0)
    return 0;
  int fd = master->aof_fd;
  master->aof_fd = -1;
  if (close(fd) != 0) {
    LOG_ERROR("aof_close", "close fd=%d: %s", fd, strerror(errno));
    return -1;
  }
  return 0;
}

/* ----- public entry points ----- */

int aof_init(master_t *master, const char *path) {
  int fd = aof_open(path);
  if (fd == -1) {
    LOG_ERROR("aof_init", "aof_open(%s) failed", path);
    master->aof_fd = -1;
    return -1;
  }
  master->aof_fd = fd;

  int rc = 0;
  uint8_t *job_buf = NULL;

  struct stat st;
  if (fstat(fd, &st) == -1) {
    LOG_ERROR("aof_init", "fstat fd=%d path=%s: %s", fd, path, strerror(errno));
    rc = -1;
    goto end;
  }

  /* First-time init: write file header + job header. On subsequent inits the
     file already exists and we keep its contents (replay via aof_load). */
  if (st.st_size == 0) {
    if (aof_write_file_header(fd) == -1) {
      rc = -1;
      goto end;
    }

    size_t job_buf_size = sizeof(master->job.M) + sizeof(master->job.R);
    for (uint32_t i = 0; i < master->job.M; i++) {
      job_buf_size += sizeof(master->maps[i].input_path_len) +
                      master->maps[i].input_path_len;
    }

    job_buf = malloc(job_buf_size);
    if (job_buf == NULL) {
      LOG_ERROR("aof_init", "malloc(%zu) failed", job_buf_size);
      rc = -1;
      goto end;
    }

    if (aof_write_job(fd, job_buf, job_buf_size, master->job.M, master->job.R,
                      master->maps) == -1) {
      rc = -1;
      goto end;
    }

    if (durable_flush(fd) == -1) {
      LOG_ERROR("aof_init", "durable_flush after init writes failed");
      rc = -1;
      goto end;
    }
  }

end:
  free(job_buf);
  if (rc != 0) {
    close(fd);
    master->aof_fd = -1;
  }
  return rc;
}

int aof_append_completed(const master_t *master, const rpc_task_done_req_t *msg,
                         task_kind_e kind) {
  int fd = master->aof_fd;
  if (fd < 0) {
    LOG_ERROR("aof_append_completed", "aof_fd is not open");
    return -1;
  }
  uint8_t rec[AOF_RECORD_SIZE];
  if (aof_write_record(fd, rec, msg, kind) == -1) {
    return -1;
  }
  if (durable_flush(fd) == -1) {
    return -1;
  }
  return 0;
}
