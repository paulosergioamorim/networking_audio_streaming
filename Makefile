CC 		:= gcc
FLAGS 	:= -Wall
SRC 	:= $(wildcard src/*.c)
OBJ 	:= $(SRC:src/%.c=obj/%.o)
DEP 	:= $(OBJ:.o=.d)

ifdef DEBUG
	FLAGS += -g -O0
endif

all: server client

objFolder:
	mkdir -p obj

obj/%.o: src/%.c | objFolder
	$(CC) $< -o $@ -c $(FLAGS) -MMD -MP

server: obj/server.o obj/signals.o obj/suffix.o obj/logger.o
	$(CC) $^ -o $@ $(FLAGS)

client: obj/client.o obj/signals.o obj/queue.o obj/logger.o
	$(CC) $^ -o $@ $(FLAGS) -lvlc -lpthread

clean:
	rm -rf obj server client

-include $(DEP)

.PHONY: all clean objFolder

