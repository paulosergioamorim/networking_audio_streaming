CC 		:= gcc
FLAGS 	:= -Wall -MMD -MP
SRC 	:= $(wildcard src/*.c)
TEST	:= $(wildcard test/*.c)
OBJ 	:= $(SRC:src/%.c=obj/%.o)
DEP 	:= $(OBJ:.o=.d)

ifdef DEBUG
	FLAGS += -g -O0
endif

all: server client

objFolder:
	mkdir -p obj

obj/%_test.o: test/%.c | objFolder
	$(CC) $< -o $@ -c $(FLAGS)

obj/%.o: src/%.c | objFolder
	$(CC) $< -o $@ -c $(FLAGS)

server: obj/server.o
	$(CC) $< -o $@ $(FLAGS) -lpthread

client: obj/client.o
	$(CC) $< -o $@ $(FLAGS) -l vlc -lpthread

clean:
	rm -rf obj

-include $(DEP)

.PHONY: all clean objFolder

