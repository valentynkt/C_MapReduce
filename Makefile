CC = gcc
CFLAGS = -Wall -Wextra -Werror -std=c17 -g -fsanitize=undefined -MMD -MP
LDFLAGS = -fsanitize=undefined -g

SRC_DIR = src
BUILD_DIR = build

# shared between binaries
COMMON_SRCS = $(SRC_DIR)/util.c \
              $(SRC_DIR)/rpc.c

# master only
MASTER_SRCS = $(SRC_DIR)/master.c \
              $(SRC_DIR)/server.c \
              $(SRC_DIR)/event_loop.c \
              $(SRC_DIR)/config.c \
              $(COMMON_SRCS)

# worker only (sequential client, no event loop)
WORKER_SRCS = $(SRC_DIR)/worker.c \
              $(SRC_DIR)/worker_map.c \
              $(SRC_DIR)/worker_reduce.c \
              $(COMMON_SRCS)

MASTER_OBJS = $(MASTER_SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)
WORKER_OBJS = $(WORKER_SRCS:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

DEPS = $(MASTER_OBJS:.o=.d) $(WORKER_OBJS:.o=.d)

all: master worker

master: $(MASTER_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

worker: $(WORKER_OBJS)
	$(CC) $(LDFLAGS) -o $@ $^

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf $(BUILD_DIR) master worker mr

-include $(DEPS)

.PHONY: all clean
