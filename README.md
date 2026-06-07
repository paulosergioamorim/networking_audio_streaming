## Trabalho Prático - Redes de Computadores 2026/1

### Componentes

- Letícia Mariana da Silva Ferreira
- Paulo Sérgio Amorim Mônico

### 1. Introdução

O trabalho visa implementar uma arquitetura cliente-servidor para streaming de áudio
por meio do protocolo TCP. A arquitetura é baseada em apenas uma conexão tcp entre
cliente e servidor (destinada a comandos e a streaming de áudio), event-driven programming 
utilizando epoll, multithreading utilizando a biblioteca pthread.

### 2. Como executar

#### 2.1 Compile os programas

Execute este comando:

```shell
make
```

Esse comando gerará dois programas: `client` e `server`.

#### 2.2 Execute o servidor

Primeiro, execute este comando para iniciar o servidor:

```shell
./server <ip-address> <port>
```

#### 2.3 Execute o cliente

Em seguida, execute este comando para iniciar o cliente:

```shell
./client <server-ip-address> <server-port>
```
