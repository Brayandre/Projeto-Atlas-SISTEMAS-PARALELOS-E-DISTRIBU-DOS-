#include "comum.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int64_t agora_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (int64_t)t.tv_sec * 1000 + t.tv_nsec / 1000000;
}

void fmt_ts(int64_t ms, char *buf, size_t tam)
{
    time_t s = (time_t)(ms / 1000);
    struct tm tm;
    localtime_r(&s, &tm);
    size_t n = strftime(buf, tam, "%Y-%m-%d %H:%M:%S", &tm);
    snprintf(buf + n, tam - n, ".%03d", (int)(ms % 1000));
}

const char *env_ou_padrao(const char *var, const char *padrao)
{
    const char *v = getenv(var);
    return (v && *v) ? v : padrao;
}

/* usuário: 1 a 32 caracteres [A-Za-z0-9_-] */
int usuario_valido(const char *s)
{
    size_t n = strlen(s);
    if (n < 1 || n > 32) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-'))
            return 0;
    }
    return 1;
}
int canal_valido(const char *s)
{
    size_t n = strlen(s);
    if (n < 1 || n > 32) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = s[i];
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-'))
            return 0;
    }
    return 1;
}

static void cabecalho(const char *quem, const char *direcao, const char *tipo)
{
    char agora[32];
    fmt_ts(agora_ms(), agora, sizeof agora);
    printf("[%s] %-14s %s %s {", agora, quem, direcao, tipo);
}

void mostrar_requisicao(const char *quem, const char *direcao, const Requisicao *r)
{
    char ts[32];
    fmt_ts(r->timestamp, ts, sizeof ts);
    cabecalho(quem, direcao, "REQ");
    printf("operacao: %s, usuario: %s", r->operacao, r->usuario);
    if (r->canal && *r->canal) printf(", canal: %s", r->canal);
    printf(", timestamp: %s}\n", ts);
    fflush(stdout);
}

void mostrar_resposta(const char *quem, const char *direcao, const Resposta *r)
{
    char ts[32];
    fmt_ts(r->timestamp, ts, sizeof ts);
    cabecalho(quem, direcao, "REP");
    printf("operacao: %s, sucesso: %s", r->operacao, r->sucesso ? "true" : "false");
    if (r->erro && *r->erro) printf(", erro: %s", r->erro);
    if (strcmp(r->operacao, "listar_canais") == 0) {
        printf(", canais: [");
        for (size_t i = 0; i < r->n_canais; i++)
            printf("%s%s", i ? ", " : "", r->canais[i]);
        printf("]");
    }
    printf(", servidor: %s, timestamp: %s}\n", r->servidor, ts);
    fflush(stdout);
}
