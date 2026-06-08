## Trabalho Prático - Redes de Computadores 2026/1

### Componentes

- Letícia Mariana da Silva Ferreira
- Paulo Sérgio Amorim Mônico

### 1. Introdução

O trabalho visa implementar uma arquitetura cliente-servidor para streaming de áudio
por meio do protocolo TCP em C. A arquitetura é baseada em apenas uma conexão tcp entre
cliente e servidor (destinada a comandos e a streaming de áudio), event-driven programming 
utilizando epoll, multithreading utilizando a biblioteca pthread.

### 2. Como executar

#### 2.1 Baixe o código-fonte

Execute este comando para clonar o repositório:

```shell
git clone git@github.com:paulosergioamorim/networking_audio_streaming.git
```

#### 2.2 Compile os programas

Execute este comando:

```shell
make
```

Esse comando gerará dois programas: `client` e `server`.

#### 2.3 Execute o servidor

Primeiro, execute este comando para iniciar o servidor:

```shell
./server <ip-address> <port>
```

#### 2.4 Execute o cliente

Em seguida, execute este comando para iniciar o cliente:

```shell
./client <server-ip-address> <server-port>
```

Execute o comando `/help` para listar todos os comandos possíveis.

### 3. Dependências e APIs utilizadas

- `libvlc`: para reprodução de áudio no cliente. `libvlc-dev` em distros Debian based.
- `std_ds.h`: single-header library para hash tables em C.
- `logger.h`: biblioteca para logging.

### 4. Escolhas de Implementação

- Multiplexação de IO no servidor: isso permitiu um baixo de consumo de recursos
por conexão no servidor. Não é criado um novo processo nem uma nova thread. Os sockets
são consumidos sobre demanda utilizando `epoll`, API específica do Linux.
- Único socket TCP entre cliente e servidor: permitiu que o file descriptor do socket
de conexão do cliente fosse uma chave num hashmap de estados do cliente. Mapeamento mais simples.
Houve alguns problemas com uma implementação de duplo aceite (dois `accept()`) como geração de 
uma chave única e "linkagem" entre os dois sockets.
- Multiplexação de IO no cliente: utilização de apenas uma única thread. Utilização do `epoll`
para consumir a entrada padrão e o socket TCP no loop. Evitou possíveis dificuldades com condições
de corrida e integração com a biblioteca `libvlc`.
- Mensagens de tamanho variável: o cliente e servidor se comunicam entre si com mensagens de tamanho
enxuto, graças ao header da requisição e resposta. Somente mensagens que realmente necessitam do campo
`buf` o enviam.

### 5. Funcionalidades Implementadas

- Criação de um protocolo de aplicação em cima do TCP para streaming de áudio
- Criação de uma fila bloqueante
- Envio e resposta de comandos
- Envio e streaming de áudio no cliente
- Gerenciamento de conexões por meio de multiplexação de IO
- Análise de métricas no cliente utilizando one-way delay

### 6. Captura de pacotes com Wireshark

- Amostra obtida numa VPN com tráfego zero.
- `pcap/command_list.pcapng`: cliente se conecta, executa o `/list` e `/exit`.
- `pcap/command_start.pcapng`: cliente se conecta, executa o `/start`, `/stop`, `/resume` e `/exit`.

### 7. Possíveis Melhorias

1. Melhorar multiplexação de IO e sockets não bloqueantes
- Uso de Edge-Triggered no `epoll` e uso total de sockets não bloqueantes.
2. Melhorar a implementação com TCP
- Criação de dois canais tcp separados: um para comandos e outro para áudio. Pensar numa melhor
estratégia de "linkar" os dois sockets indicando que são de um mesmo cliente.
- Melhor tratamento ao ocorrer TCP Zero Window. 
3. Utilizar outros protocolos
- Usar UDP no streaming de áudio. Algumas dificuldades como entrega em ordem, confiável e controle de congestionamento.
- Usar QUIC no streaming de áudio. Reimplementar a arquitetura do zero.
