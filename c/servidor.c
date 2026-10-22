
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zmq.h>

#include "comum.h"

static char NOME[64];
static char ARQ_LOGINS[512];
static char ARQ_CANAIS[512];

static char canais[MAX_CANAIS][TAM_NOME]; 
static size_t n_canais = 0;
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

static void carregar_canais(void)
{
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

static void criar_pasta(const char *caminho)
{
    char tmp[512];
    snprintf(tmp, sizeof tmp, "%s", caminho);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) perror("mkdir");
}

static int salvar_login(const Requisicao *r)
{
    char ts[32];
    fmt_ts(r->timestamp, ts, sizeof ts);
    FILE *f = fopen(ARQ_LOGINS, "a");
    if (!f) { perror("logins"); return 0; }
    fprintf(f, "%lld\t%s\t%s\n", (long long)r->timestamp, ts, r->usuario);
    fclose(f);
    return 1;
}

static int salvar_canal(const Requisicao *r)
{
    char ts[32];
    fmt_ts(r->timestamp, ts, sizeof ts);
    FILE *f = fopen(ARQ_CANAIS, "a");
    if (!f) { perror("canais"); return 0; }
    fprintf(f, "%lld\t%s\t%s\t%s\n", (long long)r->timestamp, ts, r->canal, r->usuario);
    fclose(f);
    return 1;
}

static void op_login(const Requisicao *req, Resposta *resp)
{
    if (!usuario_valido(req->usuario)) { resp->erro = "nome de usuario invalido"; return; }
    if (!salvar_login(req)) { resp->erro = "falha ao gravar login"; return; }
    resp->sucesso = 1;
}

static void op_criar_canal(const Requisicao *req, Resposta *resp)
{
    if (!canal_valido(req->canal))      resp->erro = "nome de canal invalido";
    else if (canal_existe(req->canal))  resp->erro = "canal ja existe";
    else {
        if (n_canais >= MAX_CANAIS) { resp->erro = "limite de canais atingido"; return; }
        if (!salvar_canal(req)) { resp->erro = "falha ao gravar canal"; return; }
        adicionar_canal(req->canal);
        resp->sucesso = 1;
    }
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
    const char *dados = env_ou_padrao("DADOS", "/data");

    criar_pasta(dados);
    snprintf(ARQ_LOGINS, sizeof ARQ_LOGINS, "%s/logins.tsv", dados);
    snprintf(ARQ_CANAIS, sizeof ARQ_CANAIS, "%s/canais.tsv", dados);
    carregar_canais();

    void *ctx = zmq_ctx_new();
    void *rep = zmq_socket(ctx, ZMQ_REP);
    zmq_connect(rep, broker);
    printf("%s: servidor C conectado ao broker %s | dados em %s | %zu canais carregados\n",
           NOME, broker, dados, n_canais);

    while (1) {
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
            mostrar_requisicao(NOME, "<<< RECEBIDO", req);
            resp.operacao = req->operacao;
            if (strcmp(req->operacao, "login") == 0)              op_login(req, &resp);
            else if (strcmp(req->operacao, "criar_canal") == 0)   op_criar_canal(req, &resp);
            else if (strcmp(req->operacao, "listar_canais") == 0) op_listar_canais(req, &resp);
            else resp.erro = "operacao desconhecida";
        }

        resp.timestamp = agora_ms();
        size_t tam = resposta__get_packed_size(&resp);
        uint8_t *buf = malloc(tam ? tam : 1);
        resposta__pack(&resp, buf);
        zmq_send(rep, buf, tam, 0);
        mostrar_resposta(NOME, ">>> ENVIADO ", &resp);
        free(buf);

        if (req) requisicao__free_unpacked(req, NULL);
    }

    zmq_close(rep);
    zmq_ctx_destroy(ctx);
    return 0;
}
