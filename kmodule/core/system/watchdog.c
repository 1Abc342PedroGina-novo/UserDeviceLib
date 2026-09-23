#include <linux/sched.h>
#include <linux/kthread.h>
#include <linux/delay.h>
#include <linux/jiffies.h>
#include <linux/slab.h>
#include <linux/rwlock.h>
#include "../../include/reflector.h"

#define WATCHDOG_INTERVAL_MS 500
#define TRAVAMENTO_THRESHOLD_JIFFIES (5 * HZ) /* 5 segundos (Seção 9.3) */

static struct task_struct *watchdog_kthread = NULL;
extern struct list_head reflector_registry_list;
extern rwlock_t registry_lock;

/**
 * 9.2/9.3 Varredura Periódica e Detecção de Travamentos por Instância.
 */
static int reflector_watchdog_fn(void *data)
{
    struct reflector_object *obj;
    unsigned long flags;
    unsigned long now;

    pr_info("reflector: Subsistema Watchdog de Supervisao ativo.\n");

    while (!kthread_should_stop()) {
        msleep_interruptible(WATCHDOG_INTERVAL_MS);
        
        if (kthread_should_stop())
            break;

        now = jiffies;

        /* Bloqueio de leitura para varrer de forma segura o registro */
        read_lock_irqsave(&registry_lock, flags);
        list_for_each_entry(obj, &reflector_registry_list, node) {
            
            spin_lock(&obj->lock);
            
            /* Se possui mensagens em voo e ultrapassou o limiar de inatividade (Seção 9.3) */
            if (atomic_read(&obj->in_flight_msgs) > 0 && 
                time_after(now, obj->last_activity + TRAVAMENTO_THRESHOLD_JIFFIES)) {
                
                pr_emerg("reflector: [WATCHDOG] Instancia TRAVADA detectada: %s/%s. Mensagens em voo: %d. Forçando isolamento.\n",
                         obj->port.namespace_str, obj->port.name, atomic_read(&obj->in_flight_msgs));
                
                /* Política de Recuperação (Seção 9.3): Força o estado para erro e rejeita novas requisições */
                obj->state = REFLECTOR_STATE_ERROR;
            }
            
            spin_unlock(&obj->lock);
        }
        read_unlock_irqrestore(&registry_lock, flags);
    }

    return 0;
}

int reflector_watchdog_init(void)
{
    watchdog_kthread = kthread_run(reflector_watchdog_fn, NULL, "reflector_wd");
    if (IS_ERR(watchdog_kthread)) {
        pr_err("reflector: Falha ao inicializar a thread do Watchdog.\n");
        return PTR_ERR(watchdog_kthread);
    }
    return 0;
}

void reflector_watchdog_exit(void)
{
    if (watchdog_kthread) {
        kthread_stop(watchdog_kthread);
        watchdog_kthread = NULL;
    }
}
