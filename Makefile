CC 	:= gcc
FLAGS 	:= -D_GNU_SOURCE -Wall -Wextra -Isrc/lib -Isrc/include
SRC 	:= $(wildcard src/**/*.c)
OBJ 	:= $(SRC:src/%.c=obj/%.o)
DEP 	:= $(OBJ:.o=.d)

ifdef DEBUG
	FLAGS += -g -O0
endif

all: server client

obj/%.o: src/%.c
	@ mkdir -p $(dir $@)
	$(CC) $< -o $@ -c $(FLAGS) -MMD -MP

server: obj/server/main.o obj/server/suffix.o obj/io.o
	$(CC) $^ -o $@ $(FLAGS)

client: obj/client/main.o obj/client/queue.o obj/io.o
	$(CC) $^ -o $@ $(FLAGS) -lvlc -lpthread

compile_commands:
	bear -- make -B

clean:
	@ rm -rf obj server client

-include $(DEP)

.PHONY: all clean

