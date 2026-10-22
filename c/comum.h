
#ifndef COMUM_H
#define COMUM_H

#include <stdint.h>
#include <stddef.h>
#include "mensagens.pb-c.h"

#define TAM_NOME 33    
#define MAX_CANAIS 1024

int64_t agora_ms(void);
void fmt_ts(int64_t ms, char *buf, size_t tam);
const char *env_ou_padrao(const char *var, const char *padrao);

int usuario_valido(const char *s);
int canal_valido(const char *s);

void mostrar_requisicao(const char *quem, const char *direcao, const Requisicao *r);
void mostrar_resposta(const char *quem, const char *direcao, const Resposta *r);

#endif
