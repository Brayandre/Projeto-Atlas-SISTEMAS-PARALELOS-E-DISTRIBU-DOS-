
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zmq.h>

#include "comum.h"

static char NOME[64];
static void *req_socket;

static const char *SUGESTOES_CANAIS[] = {"geral", "noticias", "esportes", "musica", "jogos",
                                         "filmes", "tecnologia", "ciencia", "memes", "off-topic"};
#define N_SUGESTOES (sizeof SUGESTOES_CANAIS / sizeof SUGESTOES_CANAIS[0])

static Resposta *requisitar(const char *operacao, const char *canal)
{
    Requisicao req = REQUISICAO__INIT;
    req.operacao = (char *)operacao;
    req.usuario = NOME;
    req.canal = (char *)canal;
    req.timestamp = agora_ms();

    size_t tam = requisicao__get_packed_size(&req);
    uint8_t *buf = malloc(tam ? tam : 1);
    requisicao__pack(&req, buf);
    zmq_send(req_socket, buf, tam, 0);
    mostrar_requisicao(NOME, ">>> ENVIADO ", &req);
    free(buf);

    zmq_msg_t msg;
    zmq_msg_init(&msg);
    zmq_msg_recv(&msg, req_socket, 0);
    Resposta *resp = resposta__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
    zmq_msg_close(&msg);

    if (!resp) {
        fprintf(stderr, "%s: resposta mal formada\n", NOME);
        return resposta__unpack(NULL, 0, (const uint8_t *)"");
    }
    mostrar_resposta(NOME, "<<< RECEBIDO", resp);
    return resp;
}

static void liberar(Resposta *r)
{
    resposta__free_unpacked(r, NULL);
}

static void fazer_login(void)
{
    while (1) {
        Resposta *r = requisitar("login", "");
        int ok = r->sucesso;
        liberar(r);
        if (ok) return;
        sleep(1);
    }
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    char host[64] = "cliente_c";
    gethostname(host, sizeof host);
    snprintf(NOME, sizeof NOME, "%s", env_ou_padrao("NOME", host));
    const char *broker = env_ou_padrao("BROKER", "tcp://broker:5555");

    unsigned int semente = (unsigned int)agora_ms();
    for (const char *p = NOME; *p; p++) semente = semente * 31 + (unsigned char)*p;
    srand(semente);

    void *ctx = zmq_ctx_new();
    req_socket = zmq_socket(ctx, ZMQ_REQ);
    zmq_connect(req_socket, broker);
    printf("%s: bot C conectando ao broker %s\n", NOME, broker);

    fazer_login();

    while (1) {
        Resposta *lista = requisitar("listar_canais", "");
        size_t n = lista->n_canais;
        liberar(lista);
        if (n < 5) liberar(requisitar("criar_canal", SUGESTOES_CANAIS[rand() % N_SUGESTOES]));

        usleep(2000000 + (rand() % 2000) * 1000);
    }

    zmq_close(req_socket);
    zmq_ctx_destroy(ctx);
    return 0;
}
