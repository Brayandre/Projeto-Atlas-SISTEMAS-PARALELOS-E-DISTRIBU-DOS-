#include <errno.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zmq.h>

#include "comum.h"

static char NOME[64];
static char coordenador[64] = "";
static char ARQ_DB[512];
static char ARQ_LOGINS[512];
static char ARQ_CANAIS[512];
static char ARQ_PUBLICACOES[512];

static void *ctx;
static void *pub_socket;
static void *sub_socket;
static void *ref_socket;
static void *por_rep;
static sqlite3 *db = NULL;
static int PORTASERVER = 5560;

#define TAM_MAX_MENSAGEM 500

static char canais[MAX_CANAIS][TAM_NOME];
static size_t n_canais = 0;

static int64_t relogio_contador = 0;
static int64_t offset_relogio = 0;
static int meu_rank = 0;
static int msgs_recebidas = 0;

static void att_relogio(int64_t relogio_rec)
{
    if (relogio_rec > relogio_contador) {
        relogio_contador = relogio_rec;
    }
}

static int64_t agora_ajst_ms(void)
{
    return agora_ms() + offset_relogio;
}

static int canal_existe(const char *nome)
{
    for (size_t i = 0; i < n_canais; i++)
        if (strcmp(canais[i], nome) == 0) return 1;
    return 0;
}

static void adicionar_canal(const char *nome)
{
    if (n_canais < MAX_CANAIS && !canal_existe(nome)) {
        strncpy(canais[n_canais], nome, TAM_NOME - 1);
        canais[n_canais][TAM_NOME - 1] = '\0';
        n_canais++;
    }
}

static void criar_pasta(const char *caminho)
{
    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s", caminho);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) perror("mkdir");
}

/* --- SQLITE E PERSISTÊNCIA LOCAL --- */

static void iniciar_db(void)
{
    if (sqlite3_open(ARQ_DB, &db) != SQLITE_OK) {
        fprintf(stderr, "Erro ao abrir SQLite: %s\n", sqlite3_errmsg(db));
        return;
    }
    const char *sql =
        "CREATE TABLE IF NOT EXISTS logins ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  timestamp INTEGER, data_hora TEXT, usuario TEXT, origem TEXT);"
        "CREATE TABLE IF NOT EXISTS canais ("
        "  nome TEXT PRIMARY KEY, timestamp INTEGER, data_hora TEXT, criador TEXT, origem TEXT);"
        "CREATE TABLE IF NOT EXISTS publicacoes ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  timestamp INTEGER, data_hora TEXT, ts_requisicao INTEGER,"
        "  canal TEXT, usuario TEXT, mensagem TEXT, servidor TEXT);";
    sqlite3_exec(db, sql, 0, 0, NULL);
}

static void carregar_canais(void)
{
    if (db) {
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(db, "SELECT nome FROM canais", -1, &stmt, NULL) == SQLITE_OK) {
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *c = (const char *)sqlite3_column_text(stmt, 0);
                if (c && canal_valido(c)) adicionar_canal(c);
            }
            sqlite3_finalize(stmt);
        }
    }
    FILE *f = fopen(ARQ_CANAIS, "r");
    if (!f) return;
    char linha[1024];
    while (fgets(linha, sizeof linha, f)) {
        linha[strcspn(linha, "\n")] = '\0';
        char *col = strtok(linha, "\t");
        for (int i = 0; col && i < 2; i++) col = strtok(NULL, "\t");
        if (col && canal_valido(col)) adicionar_canal(col);
    }
    fclose(f);
}

static int gravar_login_local(int64_t ts_val, const char *usuario, const char *origem)
{
    char ts[32];
    fmt_ts(ts_val, ts, sizeof ts);

    if (db) {
        sqlite3_stmt *stmt;
        const char *sql = "INSERT INTO logins (timestamp, data_hora, usuario, origem) VALUES (?, ?, ?, ?)";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, ts_val);
            sqlite3_bind_text(stmt, 2, ts, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 3, usuario, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, origem, -1, SQLITE_STATIC);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    FILE *f = fopen(ARQ_LOGINS, "a");
    if (!f) { perror("logins"); return 0; }
    fprintf(f, "%lld\t%s\t%s\n", (long long)ts_val, ts, usuario);
    fclose(f);
    return 1;
}

static int gravar_canal_local(int64_t ts_val, const char *canal, const char *criador, const char *origem)
{
    adicionar_canal(canal);
    char ts[32];
    fmt_ts(ts_val, ts, sizeof ts);

    if (db) {
        sqlite3_stmt *stmt;
        const char *sql = "INSERT OR IGNORE INTO canais (nome, timestamp, data_hora, criador, origem) VALUES (?, ?, ?, ?, ?)";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, canal, -1, SQLITE_STATIC);
            sqlite3_bind_int64(stmt, 2, ts_val);
            sqlite3_bind_text(stmt, 3, ts, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 4, criador, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 5, origem, -1, SQLITE_STATIC);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    FILE *f = fopen(ARQ_CANAIS, "a");
    if (!f) { perror("canais"); return 0; }
    fprintf(f, "%lld\t%s\t%s\t%s\n", (long long)ts_val, ts, canal, criador);
    fclose(f);
    return 1;
}

static int gravar_publicacao_local(int64_t ts_pub, int64_t ts_req, const char *canal,
                                   const char *usuario, const char *mensagem, const char *origem)
{
    char ts[32];
    fmt_ts(ts_pub, ts, sizeof ts);

    char msg_limpa[TAM_MAX_MENSAGEM + 1];
    strncpy(msg_limpa, mensagem, TAM_MAX_MENSAGEM);
    msg_limpa[TAM_MAX_MENSAGEM] = '\0';
    for (char *c = msg_limpa; *c; c++) {
        if (*c == '\t' || *c == '\n' || *c == '\r') *c = ' ';
    }

    if (db) {
        sqlite3_stmt *stmt;
        const char *sql = "INSERT INTO publicacoes (timestamp, data_hora, ts_requisicao, canal, usuario, mensagem, servidor) VALUES (?, ?, ?, ?, ?, ?, ?)";
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, ts_pub);
            sqlite3_bind_text(stmt, 2, ts, -1, SQLITE_STATIC);
            sqlite3_bind_int64(stmt, 3, ts_req);
            sqlite3_bind_text(stmt, 4, canal, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 5, usuario, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 6, msg_limpa, -1, SQLITE_STATIC);
            sqlite3_bind_text(stmt, 7, origem, -1, SQLITE_STATIC);
            sqlite3_step(stmt);
            sqlite3_finalize(stmt);
        }
    }

    FILE *f = fopen(ARQ_PUBLICACOES, "a");
    if (!f) { perror("publicacoes"); return 0; }
    fprintf(f, "%lld\t%s\t%lld\t%s\t%s\t%s\n",
            (long long)ts_pub, ts, (long long)ts_req, canal, usuario, msg_limpa);
    fclose(f);
    return 1;
}

static void enviar_replica(const char *tipo, const char *usuario, const char *canal,
                           const char *mensagem, int64_t timestamp, int64_t ts_requisicao)
{
    relogio_contador++;
    Replica rep_msg = REPLICA__INIT;
    rep_msg.tipo = (char *)tipo;
    rep_msg.servidor = NOME;
    rep_msg.usuario = (char *)(usuario ? usuario : "");
    rep_msg.canal = (char *)(canal ? canal : "");
    rep_msg.mensagem = (char *)(mensagem ? mensagem : "");
    rep_msg.timestamp = timestamp;
    rep_msg.ts_requisicao = ts_requisicao;
    rep_msg.relogio = relogio_contador;

    size_t tam = replica__get_packed_size(&rep_msg);
    uint8_t *buf = malloc(tam ? tam : 1);
    replica__pack(&rep_msg, buf);
    zmq_send(pub_socket, "replica", 7, ZMQ_SNDMORE);
    zmq_send(pub_socket, buf, tam, 0);
    free(buf);
}

/* --- REFERÊNCIA, ELEIÇÃO E BERKELEY --- */

static RepReferencia *comunicar_referencia(const char *operacao)
{
    relogio_contador++;
    ReqReferencia req = REQ_REFERENCIA__INIT;
    req.operacao = (char *)operacao;
    req.nome = NOME;
    req.timestamp = agora_ajst_ms();
    req.relogio = relogio_contador;

    size_t tam = req_referencia__get_packed_size(&req);
    uint8_t *buf = malloc(tam ? tam : 1);
    req_referencia__pack(&req, buf);
    zmq_send(ref_socket, buf, tam, 0);
    free(buf);

    zmq_msg_t msg;
    zmq_msg_init(&msg);
    if (zmq_msg_recv(&msg, ref_socket, 0) < 0) {
        zmq_msg_close(&msg);
        return NULL;
    }

    RepReferencia *rep = rep_referencia__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
    zmq_msg_close(&msg);

    if (rep) {
        att_relogio(rep->relogio);
        if (strcmp(operacao, "rank") == 0) {
            meu_rank = rep->rank;
            printf("%s: registrado na ref com rank=%d\n", NOME, meu_rank);
        }
    }
    return rep;
}

static void anuncia_coord(void)
{
    snprintf(coordenador, sizeof coordenador, "%s", NOME);
    relogio_contador++;

    Publicacao pub = PUBLICACAO__INIT;
    pub.canal = "servers";
    pub.usuario = NOME;
    pub.mensagem = NOME;
    pub.servidor = NOME;
    pub.timestamp = agora_ajst_ms();
    pub.relogio = relogio_contador;

    size_t tam = publicacao__get_packed_size(&pub);
    uint8_t *buf = malloc(tam ? tam : 1);
    publicacao__pack(&pub, buf);
    zmq_send(pub_socket, "servers", 7, ZMQ_SNDMORE);
    zmq_send(pub_socket, buf, tam, 0);
    free(buf);

    printf("%s: novo coordenador (rank=%d)\n", NOME, meu_rank);
}

static RepServidor *requisitar_server(const char *alvo_nome, const char *operacao, int64_t *t_envio_out)
{
    void *sock = zmq_socket(ctx, ZMQ_REQ);
    int timeout = 1500;
    int linger = 0;
    zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof timeout);
    zmq_setsockopt(sock, ZMQ_SNDTIMEO, &timeout, sizeof timeout);
    zmq_setsockopt(sock, ZMQ_LINGER, &linger, sizeof linger);

    char end[128];
    if (strchr(alvo_nome, ':') != NULL) {
        snprintf(end, sizeof end, "tcp://%s", alvo_nome);
    } else {
        snprintf(end, sizeof end, "tcp://%s:%d", alvo_nome, PORTASERVER);
    }
    zmq_connect(sock, end);

    relogio_contador++;
    ReqServidor req = REQ_SERVIDOR__INIT;
    req.operacao = (char *)operacao;
    req.remetente = NOME;
    req.rank = meu_rank;
    req.timestamp = agora_ajst_ms();
    req.relogio = relogio_contador;
    if (t_envio_out) *t_envio_out = req.timestamp;

    size_t tam = req_servidor__get_packed_size(&req);
    uint8_t *buf = malloc(tam ? tam : 1);
    req_servidor__pack(&req, buf);
    int rc = zmq_send(sock, buf, tam, 0);
    free(buf);

    if (rc < 0) {
        zmq_close(sock);
        return NULL;
    }

    zmq_msg_t msg;
    zmq_msg_init(&msg);
    if (zmq_msg_recv(&msg, sock, 0) < 0) {
        zmq_msg_close(&msg);
        zmq_close(sock);
        return NULL;
    }

    RepServidor *rep = rep_servidor__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
    zmq_msg_close(&msg);
    zmq_close(sock);

    if (rep) att_relogio(rep->relogio);
    return rep;
}

static void iniciar_eleicao(void)
{
    RepReferencia *rep_lista = comunicar_referencia("list");
    if (!rep_lista) return;

    int alguem_acima_respondeu = 0;
    for (size_t i = 0; i < rep_lista->n_servidores; i++) {
        InfoServidor *srv = rep_lista->servidores[i];
        if (strcmp(srv->nome, NOME) == 0) continue;
        if (srv->rank < meu_rank) {
            RepServidor *rep = requisitar_server(srv->nome, "eleicao", NULL);
            if (rep && strcmp(rep->status, "OK") == 0) {
                alguem_acima_respondeu = 1;
            }
            if (rep) rep_servidor__free_unpacked(rep, NULL);
        }
    }
    rep_referencia__free_unpacked(rep_lista, NULL);

    if (!alguem_acima_respondeu) {
        anuncia_coord();
    }
}

static void sincronizar_relogio(void)
{
    if (coordenador[0] == '\0') {
        iniciar_eleicao();
    }
    if (strcmp(coordenador, NOME) == 0) {
        return;
    }

    int64_t t_envio = 0;
    RepServidor *rep = requisitar_server(coordenador, "relogio", &t_envio);
    if (!rep) {
        printf("%s: coordenador '%s' indisponivel | iniciando nova eleicao\n", NOME, coordenador);
        iniciar_eleicao();
        return;
    }

    int64_t agora_local = agora_ms();
    int64_t rtt = agora_local - t_envio;
    int64_t hora_coord = rep->timestamp + (rtt / 2);
    offset_relogio = hora_coord - agora_local;
    printf("%s: relogio sincronizado com '%s' (offset=%lldms)\n",
           NOME, coordenador, (long long)offset_relogio);

    rep_servidor__free_unpacked(rep, NULL);
}

static void tratarpor_req(void)
{
    zmq_msg_t msg;
    zmq_msg_init(&msg);
    if (zmq_msg_recv(&msg, por_rep, 0) < 0) {
        zmq_msg_close(&msg);
        return;
    }

    ReqServidor *req = req_servidor__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
    zmq_msg_close(&msg);

    if (req) att_relogio(req->relogio);

    relogio_contador++;
    RepServidor rep = REP_SERVIDOR__INIT;
    rep.operacao = req ? req->operacao : "";
    rep.status = "OK";
    rep.timestamp = agora_ajst_ms();
    rep.relogio = relogio_contador;

    size_t tam = rep_servidor__get_packed_size(&rep);
    uint8_t *buf = malloc(tam ? tam : 1);
    rep_servidor__pack(&rep, buf);
    zmq_send(por_rep, buf, tam, 0);
    free(buf);

    if (req) {
        if (strcmp(req->operacao, "eleicao") == 0) {
            printf("%s: recebi pedido de ELEICAO de '%s' (rank=%d) -> respondi OK\n",
                   NOME, req->remetente, req->rank);
            if (meu_rank < req->rank && strcmp(coordenador, NOME) != 0) {
                iniciar_eleicao();
            }
        } else if (strcmp(req->operacao, "relogio") == 0) {
            printf("%s: forneci hora para '%s'\n", NOME, req->remetente);
        }
        req_servidor__free_unpacked(req, NULL);
    }
}

static void tratar_subservers(void)
{
    while (1) {
        zmq_msg_t topico_msg, corpo;
        zmq_msg_init(&topico_msg);
        if (zmq_msg_recv(&topico_msg, sub_socket, ZMQ_DONTWAIT) < 0) {
            zmq_msg_close(&topico_msg);
            return;
        }

        char topico[32] = "";
        size_t t_size = zmq_msg_size(&topico_msg);
        if (t_size >= sizeof topico) t_size = sizeof(topico) - 1;
        memcpy(topico, zmq_msg_data(&topico_msg), t_size);
        topico[t_size] = '\0';

        if (!zmq_msg_more(&topico_msg)) {
            zmq_msg_close(&topico_msg);
            continue;
        }
        zmq_msg_init(&corpo);
        zmq_msg_recv(&corpo, sub_socket, 0);

        if (strcmp(topico, "servers") == 0) {
            Publicacao *pub = publicacao__unpack(NULL, zmq_msg_size(&corpo), zmq_msg_data(&corpo));
            if (pub) {
                att_relogio(pub->relogio);
                const char *novo = (pub->mensagem && pub->mensagem[0]) ? pub->mensagem : pub->servidor;
                snprintf(coordenador, sizeof coordenador, "%s", novo);
                printf("%s: novo coordenador eleito: '%s'\n", NOME, coordenador);
                publicacao__free_unpacked(pub, NULL);
            }
        } else if (strcmp(topico, "replica") == 0) {
            Replica *rep_msg = replica__unpack(NULL, zmq_msg_size(&corpo), zmq_msg_data(&corpo));
            if (rep_msg) {
                att_relogio(rep_msg->relogio);
                if (strcmp(rep_msg->servidor, NOME) != 0) {
                    if (strcmp(rep_msg->tipo, "login") == 0) {
                        gravar_login_local(rep_msg->timestamp, rep_msg->usuario, rep_msg->servidor);
                    } else if (strcmp(rep_msg->tipo, "canal") == 0) {
                        gravar_canal_local(rep_msg->timestamp, rep_msg->canal, rep_msg->usuario, rep_msg->servidor);
                    } else if (strcmp(rep_msg->tipo, "publicacao") == 0) {
                        if (!canal_existe(rep_msg->canal)) {
                            gravar_canal_local(rep_msg->ts_requisicao, rep_msg->canal, "(via replica)", rep_msg->servidor);
                        }
                        gravar_publicacao_local(rep_msg->timestamp, rep_msg->ts_requisicao,
                                                rep_msg->canal, rep_msg->usuario,
                                                rep_msg->mensagem, rep_msg->servidor);
                    }
                    printf("%s: [REPLICA] %s de '%s' gravado no SQLite\n",
                           NOME, rep_msg->tipo, rep_msg->servidor);
                }
                replica__free_unpacked(rep_msg, NULL);
            }
        }

        zmq_msg_close(&topico_msg);
        zmq_msg_close(&corpo);
    }
}

// OPERAÇÕES 

static void op_login(const Requisicao *req, Resposta *resp)
{
    if (!usuario_valido(req->usuario)) { resp->erro = "nome de usuario invalido"; return; }
    if (!gravar_login_local(req->timestamp, req->usuario, NOME)) { resp->erro = "falha ao gravar login"; return; }
    enviar_replica("login", req->usuario, "", "", req->timestamp, 0);
    resp->sucesso = 1;
}

static void op_criar_canal(const Requisicao *req, Resposta *resp)
{
    if (!canal_valido(req->canal))      resp->erro = "nome de canal invalido";
    else if (canal_existe(req->canal))  resp->erro = "canal ja existe";
    else {
        if (n_canais >= MAX_CANAIS) { resp->erro = "limite de canais atingido"; return; }
        if (!gravar_canal_local(req->timestamp, req->canal, req->usuario, NOME)) {
            resp->erro = "falha ao gravar canal";
            return;
        }
        enviar_replica("canal", req->usuario, req->canal, "", req->timestamp, 0);
        resp->sucesso = 1;
    }
}

static void op_publicar(const Requisicao *req, Resposta *resp)
{
    if (!canal_valido(req->canal))              { resp->erro = "nome de canal invalido"; return; }
    const char *m = req->mensagem;
    while (*m == ' ') m++;
    if (*m == '\0')                             { resp->erro = "mensagem vazia"; return; }
    if (strlen(req->mensagem) > TAM_MAX_MENSAGEM) { resp->erro = "mensagem muito longa"; return; }

    if (!canal_existe(req->canal)) {
        if (n_canais >= MAX_CANAIS) { resp->erro = "limite de canais atingido"; return; }
        gravar_canal_local(req->timestamp, req->canal, "(via publicacao)", NOME);
        enviar_replica("canal", "(via publicacao)", req->canal, "", req->timestamp, 0);
    }

    relogio_contador++;
    Publicacao pub = PUBLICACAO__INIT;
    pub.canal = req->canal;
    pub.usuario = req->usuario;
    pub.mensagem = req->mensagem;
    pub.servidor = NOME;
    pub.timestamp = agora_ajst_ms();
    pub.relogio = relogio_contador;

    size_t tam = publicacao__get_packed_size(&pub);
    uint8_t *buf = malloc(tam ? tam : 1);
    publicacao__pack(&pub, buf);
    zmq_send(pub_socket, pub.canal, strlen(pub.canal), ZMQ_SNDMORE);
    zmq_send(pub_socket, buf, tam, 0);
    free(buf);
    mostrar_publicacao(NOME, ">>> ENVIADO ", &pub, -1);

    if (!gravar_publicacao_local(pub.timestamp, req->timestamp, pub.canal, pub.usuario, pub.mensagem, NOME)) {
        resp->erro = "falha ao gravar publicacao";
        return;
    }
    enviar_replica("publicacao", pub.usuario, pub.canal, pub.mensagem, pub.timestamp, req->timestamp);
    resp->sucesso = 1;
}

static char *lista_canais[MAX_CANAIS];

static void op_listar_canais(const Requisicao *req, Resposta *resp)
{
    (void)req;
    for (size_t i = 0; i < n_canais; i++) lista_canais[i] = canais[i];
    resp->n_canais = n_canais;
    resp->canais = lista_canais;
    resp->sucesso = 1;
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    char host[64] = "servidor_c";
    gethostname(host, sizeof host);
    snprintf(NOME, sizeof NOME, "%s", env_ou_padrao("NOME", host));
    const char *broker = env_ou_padrao("BROKER", "tcp://broker:5556");
    const char *proxy = env_ou_padrao("PROXY", "tcp://proxy:5557");
    const char *proxy_sub = env_ou_padrao("PROXY_SUB", "tcp://proxy:5558");
    const char *referencia = env_ou_padrao("REFERENCIA", "tcp://referencia:5559");
    const char *dados = env_ou_padrao("DADOS", "/data");
    PORTASERVER = atoi(env_ou_padrao("PORTASERVER", "5560"));

    criar_pasta(dados);
    snprintf(ARQ_DB, sizeof ARQ_DB, "%s/atlas.db", dados);
    snprintf(ARQ_LOGINS, sizeof ARQ_LOGINS, "%s/logins.tsv", dados);
    snprintf(ARQ_CANAIS, sizeof ARQ_CANAIS, "%s/canais.tsv", dados);
    snprintf(ARQ_PUBLICACOES, sizeof ARQ_PUBLICACOES, "%s/publicacoes.tsv", dados);
    iniciar_db();
    carregar_canais();

    ctx = zmq_ctx_new();
    void *rep = zmq_socket(ctx, ZMQ_REP);
    zmq_connect(rep, broker);

    pub_socket = zmq_socket(ctx, ZMQ_PUB);
    zmq_connect(pub_socket, proxy);

    sub_socket = zmq_socket(ctx, ZMQ_SUB);
    zmq_connect(sub_socket, proxy_sub);
    zmq_setsockopt(sub_socket, ZMQ_SUBSCRIBE, "servers", 7);
    zmq_setsockopt(sub_socket, ZMQ_SUBSCRIBE, "replica", 7);

    ref_socket = zmq_socket(ctx, ZMQ_REQ);
    zmq_connect(ref_socket, referencia);

    por_rep = zmq_socket(ctx, ZMQ_REP);
    char end_bind[64];
    snprintf(end_bind, sizeof end_bind, "tcp://*:%d", PORTASERVER);
    zmq_bind(por_rep, end_bind);

    printf("%s: servidor C conectado | SQLite em %s | %zu canais carregados\n",
           NOME, ARQ_DB, n_canais);

    RepReferencia *r_rank = comunicar_referencia("rank");
    if (r_rank) rep_referencia__free_unpacked(r_rank, NULL);

    while (1) {
        zmq_pollitem_t itens[] = {
            {por_rep,    0, ZMQ_POLLIN, 0},
            {sub_socket, 0, ZMQ_POLLIN, 0},
            {rep,        0, ZMQ_POLLIN, 0}
        };
        if (zmq_poll(itens, 3, -1) < 0) continue;

        if (itens[0].revents & ZMQ_POLLIN) {
            tratarpor_req();
        }
        if (itens[1].revents & ZMQ_POLLIN) {
            tratar_subservers();
        }
        if (itens[2].revents & ZMQ_POLLIN) {
            zmq_msg_t msg;
            zmq_msg_init(&msg);
            if (zmq_msg_recv(&msg, rep, 0) < 0) { zmq_msg_close(&msg); continue; }

            Requisicao *req = requisicao__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
            zmq_msg_close(&msg);

            Resposta resp = RESPOSTA__INIT;
            resp.servidor = NOME;

            if (!req) {
                resp.operacao = "desconhecida";
                resp.erro = "requisicao invalida";
            } else {
                att_relogio(req->relogio);
                mostrar_requisicao(NOME, "<<< RECEBIDO", req);
                resp.operacao = req->operacao;
                if (strcmp(req->operacao, "login") == 0)              op_login(req, &resp);
                else if (strcmp(req->operacao, "criar_canal") == 0)   op_criar_canal(req, &resp);
                else if (strcmp(req->operacao, "listar_canais") == 0) op_listar_canais(req, &resp);
                else if (strcmp(req->operacao, "publicar") == 0)      op_publicar(req, &resp);
                else resp.erro = "operacao desconhecida";

                msgs_recebidas++;
                if (msgs_recebidas % 10 == 0) {
                    RepReferencia *hb = comunicar_referencia("heartbeat");
                    if (hb) rep_referencia__free_unpacked(hb, NULL);
                }
                if (msgs_recebidas % 15 == 0) {
                    sincronizar_relogio();
                }
            }

            relogio_contador++;
            resp.timestamp = agora_ajst_ms();
            resp.relogio = relogio_contador;
            size_t tam = resposta__get_packed_size(&resp);
            uint8_t *buf = malloc(tam ? tam : 1);
            resposta__pack(&resp, buf);
            zmq_send(rep, buf, tam, 0);
            mostrar_resposta(NOME, ">>> ENVIADO ", &resp);
            free(buf);

            if (req) requisicao__free_unpacked(req, NULL);
        }
    }

    if (db) sqlite3_close(db);
    zmq_close(por_rep);
    zmq_close(ref_socket);
    zmq_close(sub_socket);
    zmq_close(pub_socket);
    zmq_close(rep);
    zmq_ctx_destroy(ctx);
    return 0;
}