#include <linux/slab.h>
#include <linux/hashtable.h>
#include <linux/jiffies.h>
#include <linux/crc32.h>
#include <base.h>

/* Tabela Hash nativa para busca ultrarrápida de portas O(1) */
static DEFINE_HASHTABLE(port_registry_hash, 8); /* 256 Buckets */
static DEFINE_RWLOCK(port_hash_lock);

static u32 calc_port_hash(const struct reflector_port *port)
{
    u32 crc = crc32_le(0, (unsigned char *)port, sizeof(struct reflector_port));
    return hash_32(crc, 8);
}

static inline bool port_equal(const struct reflector_port *p1, const struct reflector_port *p2)
{
    if (p1->type != p2->type || p1->id != p2->id)
        return false;
    return (strncmp(p1->namespace_str, p2->namespace_str, REFLECTOR_MAX_NAME_LEN) == 0) &&
           (strncmp(p1->name, p2->name, REFLECTOR_MAX_NAME_LEN) == 0);
}

/**
 * Aloca internamente as filas circulares nativas da instância.
 */
int reflector_init_object_queues(struct reflector_object *obj)
{
    int ret;

    init_waitqueue_head(&obj->msg_wait);

    ret = kfifo_alloc(&obj->normal_fifo, REFLECTOR_FIFO_SIZE, GFP_KERNEL);
    if (ret) return ret;

    ret = kfifo_alloc(&obj->urgent_fifo, REFLECTOR_FIFO_SIZE, GFP_KERNEL);
    if (ret) {
        kfifo_free(&obj->normal_fifo);
        return ret;
    }

    return 0;
}

/**
 * Libera a memória das filas nativas.
 */
void reflector_free_object_queues(struct reflector_object *obj)
{
    kfree(obj->normal_fifo.kfifo.data);
    kfree(obj->urgent_fifo.kfifo.data);
}

/**
 * 5.1 / 5.3 Enfileiramento nativo com Backpressure e tratamento de prioridades.
 */
int reflector_enqueue_msg(struct reflector_msg *msg)
{
    struct reflector_object *obj;
    struct reflector_object *target = NULL;
    unsigned long flags;
    size_t total_size;
    struct kfifo *chosen_fifo;
    int ret = 0;

    if (!msg) return -EINVAL;

    total_size = sizeof(struct reflector_msg) + msg->payload_len;

    /* Resolução O(1) da porta pelo Hash */
    read_lock_irqsave(&port_hash_lock, flags);
    hash_for_each_possible(port_registry_hash, obj, node, calc_port_hash(&msg->dst)) {
        if (port_equal(&obj->port, &msg->dst)) {
            kref_get(&obj->refcount);
            target = obj;
            break;
        }
    }
    read_unlock_irqrestore(&port_hash_lock, flags);

    if (!target) return -ENXIO; /* Destino desconhecido (Seção 5.2) */

    spin_lock_irqsave(&target->lock, flags);

    /* Validações de Estado Críticas */
    if (target->state == REFLECTOR_STATE_REMOVED) {
        ret = -ESHUTDOWN;
        goto unlock;
    }
    if (target->state == REFLECTOR_STATE_ERROR) {
        ret = -ECOMM;
        goto unlock;
    }
    /* Se suspenso, barra mensagens normais. Só passam comandos de energia */
    if (target->state == REFLECTOR_STATE_SUSPENDED && msg->priority != REFLECTOR_PRI_POWER) {
        ret = -EHOSTDOWN;
        goto unlock;
    }

    /* 5.4 Seleção da fila por prioridade */
    chosen_fifo = (msg->priority >= REFLECTOR_PRI_URGENT) ? 
                  &target->urgent_fifo.kfifo : &target->normal_fifo.kfifo;

    /* 5.3 Verificação de Backpressure: A mensagem cabe no buffer físico da fila? */
    if (kfifo_avail(chosen_fifo) < total_size) {
        ret = -EAGAIN; /* Sinaliza sobrecarga imediatamente sem bloquear o kernel */
        goto unlock;
    }

    /* Escrita atômica serializada da mensagem para dentro da KFIFO */
    kfifo_in(chosen_fifo, (u8 *)msg, total_size);
    
    /* Atualiza metadados estruturais */
    target->last_activity = jiffies;
    atomic_inc(&target->in_flight_msgs);

    /* Acorda workers ou daemons parados na variável de condição */
    wake_up_interruptible(&target->msg_wait);

unlock:
    spin_unlock_irqrestore(&target->lock, flags);
    kref_put(&target->refcount, NULL); /* Solta a referência do roteamento rápido */
    return ret;
}
EXPORT_SYMBOL(reflector_enqueue_msg);

/**
 * 5.4 Consumo de Mensagens respeitando a prioridade estrita de filas (FIFO).
 */
struct reflector_msg *reflector_dequeue_msg(struct reflector_object *obj)
{
    unsigned long flags;
    struct reflector_msg header;
    struct reflector_msg *full_msg = NULL;
    struct kfifo *chosen_fifo = NULL;
    size_t total_size;

    if (!obj) return NULL;

    spin_lock_irqsave(&obj->lock, flags);

    /* 5.4 Varre prioritariamente a fila urgente. Se vazia, lê a normal */
    if (kfifo_len(&obj->urgent_fifo.kfifo) >= sizeof(struct reflector_msg)) {
        chosen_fifo = &obj->urgent_fifo.kfifo;
    } else if (kfifo_len(&obj->normal_fifo.kfifo) >= sizeof(struct reflector_msg)) {
        chosen_fifo = &obj->normal_fifo.kfifo;
    }

    if (!chosen_fifo) goto unlock;

    /* Copia o cabeçalho sem removê-lo para calcular o tamanho real do payload */
    if (kfifo_out_peek(chosen_fifo, (u8 *)&header, sizeof(struct reflector_msg)) != sizeof(struct reflector_msg))
        goto unlock;

    total_size = sizeof(struct reflector_msg) + header.payload_len;

    /* Aloca buffer dinâmico seguro para o processamento do Worker */
    full_msg = kmalloc(total_size, GFP_ATOMIC);
    if (!full_msg) goto unlock;

    /* Extrai definitivamente os dados da fila física */
    kfifo_out(chosen_fifo, (u8 *)full_msg, total_size);
    obj->last_activity = jiffies;

unlock:
    spin_unlock_irqrestore(&obj->lock, flags);
    return full_msg;
}
EXPORT_SYMBOL(reflector_dequeue_msg);
