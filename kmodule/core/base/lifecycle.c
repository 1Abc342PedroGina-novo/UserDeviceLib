// SPDX License Indentifier : GPL-2.0
#include <linux/module.h>
#include <linux/slab.h>
#include <linux/rwlock.h>
#include <linux/jiffies.h>
#include <core.h>

/* Lista global de Cores registrados no sistema */
static LIST_HEAD(reflector_registry_list);
static DEFINE_RWLOCK(registry_lock);

/**
 * 4.5 Referências: Função interna de liberação definitiva do Core.
 * Invocada automaticamente quando o kref zera.
 */
static void reflector_obj_release(struct kref *kref)
{
    struct reflector_object *obj = container_of(kref, struct reflector_object, refcount);
    
    pr_info("reflector: Destruindo contexto do objeto para a porta %s:%s\n",
            obj->port.namespace_str, obj->port.name);

    /* 4.4 Invocação de desanexação e destruição de contexto do hardware */
    if (obj->ops && obj->ops->release) {
        obj->ops->release(obj);
    }

    /* Define estado final como destruído e libera memória */
    obj->state = REFLECTOR_STATE_REMOVED; // Mapeia para Destruída/Removida
    kfree(obj);
}

/**
 * Incrementa a referência de forma segura.
 */
void reflector_obj_get(struct reflector_object *obj)
{
    if (obj) {
        kref_get(&obj->refcount);
    }
}
EXPORT_SYMBOL(reflector_obj_get);

/**
 * Decrementa a referência. Se chegar a zero, libera o objeto.
 */
void reflector_obj_put(struct reflector_object *obj)
{
    if (obj) {
        kref_put(&obj->refcount, reflector_obj_release);
    }
}
EXPORT_SYMBOL(reflector_obj_put);

/**
 * 4.4 / 7.3 Registro de um novo Core. Valida invariantes de arquitetura.
 */
int reflector_register_core(struct reflector_object *obj)
{
    unsigned long flags;

    /* Invariante: Todo objeto deve possuir uma porta KERNEL válida */
    if (!obj || obj->port.type != REFLECTOR_PORT_KERNEL) {
        return -EINVAL;
    }

    /* Invariante: Manipulador de mensagens obrigatório */
    if (!obj->ops || !obj->ops->handle_msg) {
        return -EINVAL;
    }

    /* Inicializa estruturas internas do objeto */
    kref_init(&obj->refcount);
    spin_lock_init(&obj->lock);
    atomic_set(&obj->in_flight_msgs, 0);
    obj->last_activity = jiffies;
    obj->state = REFLECTOR_STATE_ACTIVE;

    /* Inicialização opcional do driver */
    if (obj->ops->init) {
        int ret = obj->ops->init(obj);
        if (ret) {
            pr_err("reflector: Falha na inicializacao do Core (%d)\n", ret);
            return ret;
        }
    }

    /* Insere no registro global de forma atômica */
    write_lock_irqsave(&registry_lock, flags);
    list_add_tail(&obj->node, &reflector_registry_list);
    write_unlock_irqrestore(&registry_lock, flags);

    pr_info("reflector: Core registrado com sucesso na porta [%s/%s/%u]\n",
            obj->port.namespace_str, obj->port.name, obj->port.id);

    
    ret = reflector_init_object_queues(obj);
    if (ret) return ret;

    write_lock_irqsave(&port_hash_lock, flags);
    hash_add(port_registry_hash, &obj->node, calc_port_hash(&obj->port));
    write_unlock_irqrestore(&port_hash_lock, flags);
    
    return 0;
}
EXPORT_SYMBOL_GPL(reflector_register_core);

/**
 * 4.4 Descarga Simétrica: Desregistra, isola e inicia o processo de drenagem.
 */
void reflector_unregister_core(struct reflector_object *obj)
{
    unsigned long flags;

    write_lock_irqsave(&port_hash_lock, flags);
    hash_del(&obj->node);
    write_unlock_irqrestore(&port_hash_lock, flags);
    
    bool found = false;
    struct reflector_object *entry;

    if (!obj) return;

    /* 1. Modifica o estado para impedir novas mensagens (Isolamento) */
    spin_lock_irqsave(&obj->lock, flags);
    obj->state = REFLECTOR_STATE_REMOVED;
    spin_unlock_irqrestore(&obj->lock, flags);

    /* 2. Remove o objeto do Registry global */
    write_lock_irqsave(&registry_lock, flags);
    list_for_each_entry(entry, &reflector_registry_list, node) {
        if (entry == obj) {
            list_del(&obj->node);
            found = true;
            break;
        }
    }
    write_unlock_irqrestore(&registry_lock, flags);

    if (!found) return;

    /* 3. Drenagem de mensagens em voo (Seção 4.4 e 7.5 Quiescência) */
    pr_info("reflector: Aguardando mensagens em voo (drenagem) para %s\n", obj->port.name);
    while (atomic_read(&obj->in_flight_msgs) > 0) {
        cpu_relax(); // Espera ativa de baixo consumo até os workers terminarem
    }

    /* 4. Solta a referência de posse do Registry */
    reflector_obj_put(obj);
     reflector_free_object_queues(obj);
}
EXPORT_SYMBOL_GPL(reflector_unregister_core);
