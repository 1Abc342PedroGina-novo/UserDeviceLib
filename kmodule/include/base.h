#ifndef _BASE_H_
#define _BASE_H_

#include <linux/types.h>
#include <linux/list.h>
#include <linux/kref.h>
#include <linux/spinlock.h>
#include <linux/completion.h>

#define REFLECTOR_MAX_NAME_LEN 32

/* 4.1 Tipos de Universos (Portas) */
enum reflector_port_type {
    REFLECTOR_PORT_KERNEL = 0,
    REFLECTOR_PORT_USER,
    REFLECTOR_PORT_SYSTEM
};

/* 4.1 Estrutura da Porta — Endereço IPC */
struct reflector_port {
    enum reflector_port_type type;
    char namespace_str[REFLECTOR_MAX_NAME_LEN];
    char name[REFLECTOR_MAX_NAME_LEN];
    u32 id;
};

/* Estados do Objeto Core */
enum reflector_obj_state {
    REFLECTOR_STATE_ACTIVE = 0,
    REFLECTOR_STATE_SUSPENDED,
    REFLECTOR_STATE_ERROR,
    REFLECTOR_STATE_REMOVED
};

/* Estrutura de uma Mensagem IPC */
struct reflector_msg {
    struct reflector_port src;
    struct reflector_port dst;
    u32 opcode;
    u32 payload_len;
    u8 payload[];
};

struct reflector_object;

/* 4.2 Tabela de operações que o Core implementa */
struct reflector_ops {
    int (*init)(struct reflector_object *obj);
    int (*handle_msg)(struct reflector_object *obj, struct reflector_msg *msg);
    void (*release)(struct reflector_object *obj);
};

/* 4.2 Objeto Refletor — Representação do Core no Kernel */
struct reflector_object {
    struct kref refcount;               /* Referência contada para destruição segura */
    struct list_head node;              /* Nó para a lista global de registro */
    spinlock_t lock;                    /* Trava própria contra concorrência */
    struct reflector_port port;         /* Endereço IPC (Invariante: deve ser KERNEL) */
    const struct reflector_ops *ops;    /* Tabela de operações (Invariante: handle_msg != NULL) */
    enum reflector_obj_state state;     /* Estado atual do Core */
    atomic_t in_flight_msgs;            /* Contador de mensagens em voo (observabilidade) */
    unsigned long last_activity;        /* Marca temporal (jiffies) para o watchdog */
    void *priv;                         /* Ponteiro privado para o contexto do driver físico */
};

/* 3.4 APIs Exportadas pelo Refletor para uso dos Cores (Kernel-Mode) */
int reflector_register_core(struct reflector_object *obj);
void reflector_unregister_core(struct reflector_object *obj);
int reflector_send_msg(struct reflector_msg *msg);

void reflector_obj_get(struct reflector_object *obj);
void reflector_obj_put(struct reflector_object *obj);

#endif /* _BASE_H_ */
