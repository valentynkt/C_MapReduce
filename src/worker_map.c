#include "util.h"
#include <errno.h>
#include <libgen.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <util.h>

typedef int (*emit_token_fn)(const char *token, void *ud);

typedef struct {
  FILE **handles;
  uint32_t R;
  const char *doc_name;
} emit_ctx_t;

static int emit_word_count(const char *token, void *ud) {
  emit_ctx_t *ctx = (emit_ctx_t *)ud;
  uint32_t y = get_hash(token) % ctx->R;
  if (fprintf(ctx->handles[y], "%s\t1\n", token) != strlen(token)) {
    fprintf(stderr, "emit_word_count: fprintf token(%s): %s\n", token,
            strerror(errno));
    return -1;
  }
  return 0;
}

static int emit_inverted_index(const char *token, void *ud) {
  emit_ctx_t *ctx = (emit_ctx_t *)ud;
  uint32_t y = get_hash(token) % ctx->R;
  if (fprintf(ctx->handles[y], "%s\t%s\n", token, ctx->doc_name) !=
      strlen(token)) {
    fprintf(stderr, "emit_word_count: fprintf token(%s): %s\n", token,
            strerror(errno));
    return -1;
  }
  return 0;
}

static int tokenize_file(const char *path, emit_token_fn emit, void *ud) {
  FILE *f = fopen(path, "r");
  if (f == NULL) {
    fprintf(stderr, "tokenize_file: fopen(%s): %s\n", path, strerror(errno));
    return -1;
  }
  char line[8192];
  while (fgets(line, sizeof line, f) != NULL) {
    char *p = line;
    char *tok;
    while ((tok = strsep(&p, " \t\n\r")) != NULL) {
      if (*tok != '\0') {
        emit(tok, ud);
      }
    }
  }
  int rc = 0;
  if (ferror(f)) {
    fprintf(stderr, "tokenize_file: read error on %s: %s\n", path,
            strerror(errno));
    rc = -1;
  }
  if (fclose(f) != 0) {
    fprintf(stderr, "tokenize_file: fclose(%s): %s\n", path, strerror(errno));
    rc = -1;
  }
  return rc;
}

int worker_map_run(uint32_t task_id, uint32_t attempt_id, uint32_t n_reduce,
                   const char *input_path) {
  printf("[worker] run map task\n");

  FILE **handles;
  emit_ctx_t ctx = (emit_ctx_t){
      .handles = handles,
      .R = n_reduce,
      .doc_name = basename((char *)input_path),
  };

  // we could add some configuration here to switch between normal index and
  // inverted index.
  tokenize_file(input_path, emit_word_count, &ctx);
  // 1. Open the input file, read it bytes.
  // 2. Tokenize the bytes into records (whole input file is one record.)
  // 3. emit zero or more key, value pairs.
  // 4. Hash each emitted key, compute partition Y = hash(key) % R
  // 5. Write the (key, value) (APPEND flag), to mr-X-Y

  // is input_path null terminated? Or we should add null termination so fopen
  // will work properly if it expects null terminated string.

  return 0;
}
