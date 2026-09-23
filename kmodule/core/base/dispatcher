#include <linux/slab.h>
#include <linux/rwlock.h>
#include <linux/jiffies.h>
#include <linux/hashtable.h>
#include <linux/crc32.h>
#include <base.h>

#define DUP_CACHE_BITS 8
#define DUP_CACHE_SIZE (1 << DUP_CACHE_BITS)
#define DUP_TTL_JIFFIES (HZ * 5) /* 5 segundos de retenção */

/* Estrutura para controle de deduplicação de mensagens */
struct reflector_dup_entry {
    struct hlist_node node;
    struct reflector_port src;
    u32 sequence;
    unsigned long timestamp;
};

/* Estado global do subsistema de roteamento */
static DEFINE_HASHTABLE(dup_cache, DUP_CACHE_BITS);
static DEFINE_SPINLOCK(dup_cache_lock);

extern struct list_head reflector_registry_list;
extern rwlock_t registry_lock;

/**
 * Calcula o hash para a tabela de deduplicação.
 */
static u32 calc_dup_hash(const struct reflector_port *src, u32 seq)
{
    u32 crc = crc32_le(0, (unsigned char *)src, sizeof(struct reflector_port));
    return hash_32(crc ^ seq, DUP_CACHE_BITS);
}

/**
 * 5.5 Deduplicação: Verifica se a mensagem já foi processada recentemente.
 * Remove registros expirados (TTL) durante a varredura para evitar vazamento de memória.
 */
static bool is_duplicate_and_track(const struct reflector_port *src, u32 seq)
{
    struct reflector_dup_entry *entry;
    struct hlist_node *tmp;
    u32 bucket = calc_dup_hash(src, seq);
    unsigned long now = jiffies;
    bool found = false;

    spin_lock(&dup_cache_lock);

    /* Limpeza sob demanda + Busca ativa */
    hash_for_each_possible_safe(dup_cache, entry, tmp, node, bucket) {
        if (time_after(now, entry->timestamp + DUP_TTL_JIFFIES)) {
            hash_del(&entry->node);
            kfree(entry);
            continue;
        }
        if (!found && entry->sequence == seq && 
            memcmp(&entry->src, src, sizeof(struct reflector_port)) == 0) {
            found = true;
        }
    }

    if (found) {
        spin_unlock(&dup_cache_lock);
        return true;
    }

    /* Aloca nova entrada caso não seja duplicada */
    entry = kmalloc(sizeof(*entry), GFP_ATOMIC);
    if (!entry) {
        spin_unlock(&dup_cache_lock);
        return false; /* Em falta de memória, permite passar para não quebrar a pipeline */
    }

    entry->src = *src;
    entry->sequence = seq;
    entry->timestamp = now;
    hash_add(dup_cache, &entry->node, bucket);

    spin_unlock(&dup_cache_lock);
    return false;
}

/**
 * 4.1 Função de comparação estrita de portas IPC.
 */
static inline bool port_equal(const struct reflector_port *p1, const struct reflector_port *p2)
{
    if (p1->type != p2->type || p1->id != p2->id)
        return false;
    if (strncmp(p1->namespace_str, p2->namespace_str, REFLECTOR_MAX_NAME_LEN) != 0)
        return false;
    return strncmp(p1->name, p2->name, REFLECTOR_MAX_NAME_LEN) == 0;
}

/**
 * 5.1/5.2 Dispatcher: Resolve a porta de destino e despacha a mensagem de forma síncrona.
 * Garante atomicidade, controle de mensagens em voo e atualização do watchdog.
 */
int reflector_send_msg(struct reflector_msg *msg, u32 seq)
{
    struct reflector_object *obj;
    struct reflector_object *target = NULL;
    unsigned long flags;
    int ret;

    if (unlikely(!msg))
        return -EINVAL;

    /* 5.5 Descarta duplicatas imediatamente */
    if (is_duplicate_and_track(&msg->src, seq)) {
        pr_debug("reflector: Mensagem duplicada ignorada de [%s] seq: %u\n", msg->src.name, seq);
        return -EALREADY;
    }

    /* 5.2 Consulta ao registry interno sob trava de leitura */
    read_lock_irqsave(&registry_lock, flags);
    list_for_each_entry(obj, &reflector_registry_list, node) {
        if (port_equal(&obj->port, &msg->dst)) {
            reflector_obj_get(obj); /* Garante validade do ponteiro (4.5) */
            target = obj;
            break;
        }
    }
    read_unlock_irqrestore(&registry_lock, flags);

    /* Erro de destino desconhecido */
    if (!target)
        return -ENXIO;

    /* Proteção de estado do objeto de destino */
    spin_lock_irqsave(&target->lock, flags);
    
    /* 5.2 Verifica se a instância está viva e apta a receber mensagens */
    if (unlikely(target->state == REFLECTOR_STATE_REMOVED)) {
        spin_unlock_irqrestore(&target->lock, flags);
        reflector_obj_put(target);
        return -ESHUTDOWN; /* Instância sendo descarregada */
    }
    
    if (unlikely(target->state == REFLECTOR_STATE_ERROR)) {
        spin_unlock_irqrestore(&target->lock, flags);
        reflector_obj_put(target);
        return -ECOMM; /* Operação em estado degradado */
    }

    /* 8.2 Se suspenso, recusa mensagens comuns, aceita apenas comandos de energia (Opcodes específicos) */
    if (unlikely(target->state == REFLECTOR_STATE_SUSPENDED && msg->opcode < 0xF0000000)) {
        spin_unlock_irqrestore(&target->lock, flags);
        reflector_obj_put(target);
        return -EHOSTDOWN;
    }

    /* Incrementa contador de mensagens em voo (Invariante 4.2) */
    atomic_inc(&target->in_flight_msgs);
    
    /* Atualiza marca temporal para o Watchdog (Invariante 4.2) */
    target->last_activity = jiffies;

    spin_unlock_irqrestore(&target->lock, flags);

    /* Executa o callback físico do Core no espaço de Kernel */
    ret = target->ops->handle_msg(target, msg);

    /* Atualiza marca temporal pós-execução e decrementa mensagens em voo */
    spin_lock_irqsave(&target->lock, flags);
    target->last_activity = jiffies;
    atomic_dec(&target->in_flight_msgs);
    spin_unlock_irqrestore(&target->lock, flags);

    /* Libera a referência adquirida pelo Dispatcher */
    reflector_obj_put(target);

    return ret;
}
EXPORT_SYMBOL(reflector_send_msg);

/**
 * Inicialização interna do subsistema de deduplicação.
 */
void reflector_dispatcher_init(void)
{
    hash_init(dup_cache);
}

/**
 * Destruição de runtime do cache de deduplicação.
 */
void reflector_dispatcher_exit(void)
{
    struct reflector_dup_entry *entry;
    struct hlist_node *tmp;
    int bkt;

    spin_lock(&dup_cache_lock);
    hash_for_each_safe(dup_cache, bkt, tmp, entry, node) {
        hash_del(&entry->node);
        kfree(entry);
    }
    spin_unlock(&dup_cache_lock);
}
