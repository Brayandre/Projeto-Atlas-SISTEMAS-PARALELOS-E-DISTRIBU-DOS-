
#include <stdio.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <zmq.h>

#include "comum.h"

static char NOME[64];
static void *req;
static void *sub;
static int64_t relogio_contador = 0;

static const char *SUGESTOES_CANAIS[] = {"geral", "noticias", "esportes", "musica", "jogos",
                                         "filmes", "tecnologia", "ciencia"};
#define N_SUGESTOES (int)(sizeof SUGESTOES_CANAIS / sizeof SUGESTOES_CANAIS[0])

static const char *FRASES[] = {"salve", "opa, alguem ai?", "bom dia", "bah guri, que chuva",
                               "oh cabra da peste", "anjoletto é top",
                               "vamo tirr 10, será?", "até amanhã",
                               "ok, até a próxima", "beijo"};
#define N_FRASES (int)(sizeof FRASES / sizeof FRASES[0])

static char conhecidos[MAX_CANAIS][TAM_NOME];  
static int n_conhecidos = 0;
#define MAX_INSCRICOES 3
static char inscritos[MAX_INSCRICOES][TAM_NOME]; 
static int n_inscritos = 0;

static void att_relogio(int64_t relogio_rec)
{
    if (relogio_rec > relogio_contador) {
        relogio_contador = relogio_rec;
    }
}

static int contem(char lista[][TAM_NOME], int n, const char *nome)
{
    for (int i = 0; i < n; i++)
        if (strcmp(lista[i], nome) == 0) return 1;
    return 0;
}

static void conhecer(const char *nome)
{
    if (n_conhecidos < MAX_CANAIS && !contem(conhecidos, n_conhecidos, nome)) {
        snprintf(conhecidos[n_conhecidos], TAM_NOME, "%s", nome);
        n_conhecidos++;
    }
}

static void receber_publicacoes(void)
{
    while (1) {
        zmq_msg_t topico, corpo;
        zmq_msg_init(&topico);
        if (zmq_msg_recv(&topico, sub, ZMQ_DONTWAIT) < 0) { zmq_msg_close(&topico); return; }
        int64_t recebido = agora_ms();
        if (!zmq_msg_more(&topico)) { zmq_msg_close(&topico); continue; }
        zmq_msg_init(&corpo);
        zmq_msg_recv(&corpo, sub, 0);

        char canal[TAM_NOME] = "";
        size_t n = zmq_msg_size(&topico);
        if (n < TAM_NOME) { memcpy(canal, zmq_msg_data(&topico), n); canal[n] = '\0'; }

        if (canal[0] && contem(inscritos, n_inscritos, canal)) {
            Publicacao *p = publicacao__unpack(NULL, zmq_msg_size(&corpo), zmq_msg_data(&corpo));
            if (p) {
                att_relogio(p->relogio);
                mostrar_publicacao(NOME, "<<< RECEBIDO", p, recebido);
                publicacao__free_unpacked(p, NULL);
            }
        }
        zmq_msg_close(&topico);
        zmq_msg_close(&corpo);
    }
}

static void esperar(int ms)
{
    int64_t fim = agora_ms() + ms;
    int64_t resta;
    while ((resta = fim - agora_ms()) > 0) {
        zmq_pollitem_t itens[] = {{sub, 0, ZMQ_POLLIN, 0}};
        zmq_poll(itens, 1, (long)resta);
        if (itens[0].revents & ZMQ_POLLIN) receber_publicacoes();
    }
}

static Resposta *requisitar(const char *operacao, const char *canal, const char *mensagem)
{
    relogio_contador++;
    Requisicao r = REQUISICAO__INIT;
    r.operacao = (char *)operacao;
    r.usuario = NOME;
    r.canal = (char *)canal;
    r.mensagem = (char *)mensagem;
    r.timestamp = agora_ms();
    r.relogio = relogio_contador;

    size_t tam = requisicao__get_packed_size(&r);
    uint8_t *buf = malloc(tam ? tam : 1);
    requisicao__pack(&r, buf);
    zmq_send(req, buf, tam, 0);
    mostrar_requisicao(NOME, ">>> ENVIADO ", &r);
    free(buf);

    while (1) {
        zmq_pollitem_t itens[] = {{req, 0, ZMQ_POLLIN, 0}, {sub, 0, ZMQ_POLLIN, 0}};
        zmq_poll(itens, 2, -1);
        if (itens[1].revents & ZMQ_POLLIN) receber_publicacoes();
        if (itens[0].revents & ZMQ_POLLIN) break;
    }

    zmq_msg_t msg;
    zmq_msg_init(&msg);
    zmq_msg_recv(&msg, req, 0);
    Resposta *resp = resposta__unpack(NULL, zmq_msg_size(&msg), zmq_msg_data(&msg));
    zmq_msg_close(&msg);

    if (!resp) {
        fprintf(stderr, "%s: resposta mal formada\n", NOME);
        return resposta__unpack(NULL, 0, (const uint8_t *)""); /* resposta vazia = erro */
    }
    att_relogio(resp->relogio);
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
        Resposta *r = requisitar("login", "", "");
        int ok = r->sucesso;
        liberar(r);
        if (ok) return;
        esperar(1000);
    }
}

static void atualizar_canais(void)
{   
    Resposta *r = requisitar("listar_canais", "", "");
    for (size_t i = 0; i < r->n_canais; i++) conhecer(r->canais[i]);
    liberar(r);
}

static void novo_nome_canal(char *nome)
{
    const char *livres[N_SUGESTOES];
    int n = 0;
    for (int i = 0; i < N_SUGESTOES; i++)
        if (!contem(conhecidos, n_conhecidos, SUGESTOES_CANAIS[i])) livres[n++] = SUGESTOES_CANAIS[i];
    if (n > 0) snprintf(nome, TAM_NOME, "%s", livres[rand() % n]);
    else       snprintf(nome, TAM_NOME, "canal-%d", 100 + rand() % 900);
}

static void inscrever(const char *canal)
{
    zmq_setsockopt(sub, ZMQ_SUBSCRIBE, canal, strlen(canal));
    snprintf(inscritos[n_inscritos], TAM_NOME, "%s", canal);
    n_inscritos++;
    printf("%s: inscrito no canal '%s' (inscricoes: [", NOME, canal);
    for (int i = 0; i < n_inscritos; i++) printf("%s%s", i ? ", " : "", inscritos[i]);
    printf("])\n");
}

int main(void)
{
    setvbuf(stdout, NULL, _IOLBF, 0);

    char host[64] = "cliente_c";
    gethostname(host, sizeof host);
    snprintf(NOME, sizeof NOME, "%s", env_ou_padrao("NOME", host));
    const char *broker = env_ou_padrao("BROKER", "tcp://broker:5555");
    const char *proxy = env_ou_padrao("PROXY", "tcp://proxy:5558");

    unsigned int semente = (unsigned int)agora_ms();
    for (const char *p = NOME; *p; p++) semente = semente * 31 + (unsigned char)*p;
    srand(semente);

    void *ctx = zmq_ctx_new();
    req = zmq_socket(ctx, ZMQ_REQ);
    zmq_connect(req, broker);
    sub = zmq_socket(ctx, ZMQ_SUB);
    zmq_connect(sub, proxy);
    printf("%s: bot C | broker %s | proxy %s\n", NOME, broker, proxy);

    fazer_login();

    while (1) {
        atualizar_canais();

        if (n_conhecidos < 5) {
            char nome[TAM_NOME];
            novo_nome_canal(nome);
            Resposta *r = requisitar("criar_canal", nome, "");
            if (r->sucesso || strcmp(r->erro, "canal ja existe") == 0) conhecer(nome);
            liberar(r);
        }

        if (n_inscritos < MAX_INSCRICOES) {
            int livres[MAX_CANAIS], n = 0;
            for (int i = 0; i < n_conhecidos; i++)
                if (!contem(inscritos, n_inscritos, conhecidos[i])) livres[n++] = i;
            if (n > 0) inscrever(conhecidos[livres[rand() % n]]);
        }

        if (n_conhecidos == 0) { esperar(1000); continue; }

        const char *canal = conhecidos[rand() % n_conhecidos];
        for (int i = 0; i < 10; i++) {
            char texto[128];
            snprintf(texto, sizeof texto, "%s (%d)", FRASES[rand() % N_FRASES], rand() % 10000);
            liberar(requisitar("publicar", canal, texto));
            esperar(1000);
        }
    }

    zmq_close(sub);
    zmq_close(req);
    zmq_ctx_destroy(ctx);
    return 0;
}
