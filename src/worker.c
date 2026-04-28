#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "config.h"
#include "util.h"

int client_init(int port, const char *host) {

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1) {
    perror("socket");
    return EXIT_FAILURE;
  }

  struct sockaddr_in addr = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
  };
  if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
    fprintf(stderr, "[worker] invalid host: %s\n", host);
    return EXIT_FAILURE;
  }

  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    perror("connect");
    return EXIT_FAILURE;
  }
  printf("[worker] connected to %s:%d (fd=%d)\n", host, port, fd);
  return fd;
}

int main(int argc, char *argv[]) {
  const char *host = (argc >= 2) ? argv[1] : "127.0.0.1";
  int port = (argc >= 3) ? atoi(argv[2]) : CONFIG_DEFAULT_PORT;

  int client_fd = client_init(port, host);

  if (client_fd == EXIT_FAILURE) {
    return EXIT_FAILURE;
  }

  /* one framed payload, just to confirm master's on_message fires */
  const char payload[] = "hello";
  uint32_t payload_len = sizeof(payload) - 1;
  char frame[FRAME_HDR_SIZE + sizeof(payload) - 1];
  uint32_t net_len = htonl(payload_len);
  memcpy(frame, &net_len, FRAME_HDR_SIZE);
  memcpy(frame + FRAME_HDR_SIZE, payload, payload_len);

  if (write_all(client_fd, frame, sizeof(frame)) != (ssize_t)sizeof(frame)) {
    perror("write");
    close(client_fd);
    return EXIT_FAILURE;
  }
  printf("[worker] sent %u bytes\n", payload_len);

  sleep(1);
  close(client_fd);
  return EXIT_SUCCESS;
}
