# Projeto Atlas: sistema de troca de mensagens instantâneas

Projeto da disciplina **CC7261 - Sistemas Distribuídos** (FEI). É uma versão distribuída de um BBS/IRC em que bots (clientes) fazem login, criam canais, publicam mensagens e trocam dados em tempo real, contando com relógios lógicos de Lamport, eleição de líder (Bully/Rank), sincronização de relógio (Berkeley), persistência via SQLite e replicação ativa de dados.

**Integrantes**

| Nome | RA | Linguagem |
|---|---|---|
| Brayan André da Costa | 22124005-4 | Python |
| Gustavo Souza Alvarenga | 22124058-3 | C |

> **Partes implementadas:**
> - **Parte 1:** Login de usuário, criação e listagem de canais.
> - **Parte 2:** Troca de mensagens públicas (`publicar`) com broadcast via proxy PUB/SUB.
> - **Parte 3:** Relógios lógicos de Lamport integrados a todas as mensagens.
> - **Parte 4:** Serviço de Referência para atribuição de ranks, algoritmo de eleição de coordenador e sincronização de relógio (Berkeley).
> - **Parte 5:** Persistência relacional local via **SQLite** (`atlas.db`) e replicação ativa em tempo real entre todos os servidores.

## Como executar

docker compose up --build


O comando sobe todos os containers: 1 serviço de referência, 1 broker, 1 proxy PUB/SUB, 4 servidores (2 em Python e 2 em C) e 4 bots (2 em Python e 2 em C). Nenhuma interação com o usuário é necessária. Para parar, use `Ctrl+C` e depois `docker compose down`.

Os dados e bancos SQLite de cada servidor ficam na pasta `dados/<nome_do_servidor>/` do host. Para começar do zero, apague a pasta `dados/`.

## Arquitetura

mermaid
flowchart LR
  subgraph Bots
    BP1[bot_py_1] & BP2[bot_py_2] & BC1[bot_c_1] & BC2[bot_c_2]
  end
  B["broker<br/>ROUTER :5555 / DEALER :5556"]
  subgraph Servidores
    SP1[servidor_py_1] & SP2[servidor_py_2] & SC1[servidor_c_1] & SC2[servidor_c_2]
  end
  BP1 & BP2 & BC1 & BC2 -- REQ --> B
  B -- REP --> SP1 & SP2 & SC1 & SC2

- **broker** (`broker/broker.py`): modelo Request-Reply. Os bots se conectam com `REQ` na porta 5555 (`ROUTER`) e os servidores com `REP` na porta 5556 (`DEALER`). O `DEALER` distribui as requisições entre os servidores por *round-robin*.
- **proxy** (`proxy/proxy.py`): gerencia o padrão PUB/SUB (portas 5557 e 5558) para broadcast de publicações nos canais, anúncios de coordenadores e replicação de dados entre os nós.
- **referência** (`referencia/referencia.py`): centraliza a atribuição de ranks iniciais aos servidores, listagem de nós ativos e serviço de heartbeat (porta 5559).
- **servidores** (`python/servidor.py` e `c/servidor.c`): atendem requisições, executam eleições, sincronizam relógios via Berkeley, persistem em SQLite e replicam dados.
- **bots** (`python/cliente.py` e `c/cliente.c`): fazem login, criam/listam canais e publicam mensagens ciclicamente.

## Escolhas do projeto

### Linguagens
- **Python**: biblioteca `pyzmq`.
- **C**: `libzmq` e `protobuf-c`.

As duas implementações seguem exatamente o mesmo protocolo Protobuf, permitindo interoperabilidade total entre nós Python e C.

### Serialização: Protocol Buffers
As mensagens são serializadas em binário com **Protobuf** (`proto/mensagens.proto`), compilado automaticamente no build dos containers. O protocolo contempla requisições de clientes, respostas, publicações, mensagens de controle de referência, eleições, sincronização de relógio e o canal de replicação.

### Persistência e Replicação (Parte 5)
Cada servidor possui o seu próprio arquivo de banco de dados relacional **SQLite** (`/data/atlas.db`), além de manter compatibilidade com arquivos TSV para histórico textual. 

Para solucionar o problema do balanceamento por *round-robin* (onde cada servidor receberia apenas uma parte dos dados), implementamos **replicação ativa em tempo real**:
- Toda operação de escrita (`login`, `criar_canal` ou `publicar`) validada por um servidor é imediatamente gravada no seu SQLite local e difundida para os demais servidores através de um tópico PUB/SUB dedicado (`"replica"`).
- Ao receber a mensagem de replicação, os outros servidores atualizam os seus bancos SQLite locais. Desse modo, **todos os servidores possuem cópias idênticas e completas de todos os dados**, garantindo consistência e tolerância a falhas.

### Tolerância a Falhas e Relógios
- **Relógios Lógicos de Lamport**: incrementados a cada envio/recebimento de mensagem para assegurar ordenação causal.
- **Eleição de Coordenador**: baseada em ranks e mensagens diretas via socket P2P na porta 5560.
- **Sincronização de Relógio (Berkeley)**: executada periodicamente entre os servidores e o coordenador eleito para ajustar desvios de relógio físico.

## Estrutura do repositório


├── docker-compose.yaml
├── proto/mensagens.proto     
├── broker/broker.py
├── proxy/proxy.py
├── referencia/referencia.py
├── python/                 
│   ├── Dockerfile            
│   ├── comum.py
│   ├── servidor.py
│   └── cliente.py
└── c/                      
    ├── Dockerfile
    ├── Makefile
    ├── comum.h / comum.c
    ├── servidor.c
    └── cliente.c