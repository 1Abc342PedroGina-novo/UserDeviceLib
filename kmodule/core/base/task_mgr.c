#include <linux/sched.h>
#include <linux/kthread.h>
#include <linux/cpumask.h>
#include <linux/slab.h>
#include <linux/delay.h>
#include "../../include/reflector.h"

/* Estrutura estrutural do Worker mapeado por CPU */
struct reflector_worker {
    struct task_struct *thread;      /* Instância do kthread no escalonador */
    unsigned int cpu_id;             /* ID físico da CPU parceira */
    struct list_head assigned_objs;   /* Lista de objetos pertencentes a este núcleo (6.3) */
    spinlock_t worker_lock;          /* Proteção da lista de instâncias associadas */
    wait_queue_head_t work_wait;     /* Multiplexador por evento central do worker */
};

/* Pool de Workers global (Alocado estritamente baseado no número de CPUs online) */
static struct reflector_worker **reflector_pool = NULL;
static unsigned int nr_workers_active = 0;
static atomic_t next_worker_index = ATOMIC_INIT(0);

extern struct list_head reflector_registry_list;
extern rwlock_t registry_lock;

/**
 * 6.4 Modelo de Execução: Ciclo de vida interno da thread de processamento física.
 * Varre as filas associadas à CPU via Multiplexação por Eventos.
 */
static int reflector_worker_fn(void *data)
{
    struct reflector_worker *worker = (struct reflector_worker *)data;
    struct reflector_object *obj;
    struct reflector_msg *msg;
    bool work_done;

    pr_info("reflector: Worker iniciado e fixado na CPU [%u]\n", worker->cpu_id);

    while (!kthread_should_stop()) {
        work_done = false;

        /* Aguarda de forma segura (Bloqueio sem consumo - 6.5) até haver mensagens ou parada */
        wait_event_interruptible(worker->work_wait, kthread_should_stop() || ({
            bool has_msg = false;
            spin_lock(&worker->worker_lock);
            list_for_each_entry(obj, &worker->assigned_objs, node) {
                if (kfifo_len(&obj->urgent_fifo.kfifo) >= sizeof(struct reflector_msg) ||
                    kfifo_len(&obj->normal_fifo.kfifo) >= sizeof(struct reflector_msg)) {
                    has_msg = true;
                    break;
                }
            }
            spin_unlock(&worker->worker_lock);
            has_msg;
        }));

        if (kthread_should_stop())
            break;

        /* Execução Round-Robin das instâncias pertencentes à CPU */
        spin_lock(&worker->worker_lock);
        list_for_each_entry(obj, &worker->assigned_objs, node) {
            spin_unlock(&worker->worker_lock);

            /* Desenfileira e consome todas as mensagens da instância respeitando a prioridade */
            while ((msg = reflector_dequeue_msg(obj)) != NULL) {
                work_done = true;
                
                /* Invocação direta do manipulador do Core no espaço de Kernel */
                if (obj->ops && obj->ops->handle_msg) {
                    obj->ops->handle_msg(obj, msg);
                }

                kfree(msg);
                atomic_dec(&obj->in_flight_msgs);
            }

            spin_lock(&worker->worker_lock);
        }
        spin_unlock(&worker->worker_lock);
    }

    return 0;
}

/**
 * 6.3 Afinidade por Round-Robin: Vincula uma instância a um worker/CPU específico na criação.
 */
void reflector_task_bind_object(struct reflector_object *obj)
{
    unsigned int idx;
    struct reflector_worker *worker;

    if (!reflector_pool || nr_workers_active == 0) return;

    /* Distribuição Round-Robin baseada no contador atômico global */
    idx = (unsigned int)atomic_inc_return(&next_worker_index) % nr_workers_active;
    worker = reflector_pool[idx];

    spin_lock(&worker->worker_lock);
    /* Sobrescreve a fila de espera do objeto para escutar diretamente o sinalizador do worker */
    obj->msg_wait = worker->work_wait; 
    list_add_tail(&obj->node, &worker->assigned_objs);
    spin_unlock(&worker->worker_lock);

    pr_info("reflector: Objeto %s vinculado com afinidade estrita ao Worker/CPU [%u]\n", 
            obj->port.name, worker->cpu_id);
}
EXPORT_SYMBOL(reflector_task_bind_object);

/**
 * Desvincula o objeto do pool de workers durante o processo de descarga.
 */
void reflector_task_unbind_object(struct reflector_object *obj)
{
    unsigned int i;
    struct reflector_worker *worker;
    struct reflector_object *entry, *tmp;

    if (!reflector_pool) return;

    for (i = 0; i < nr_workers_active; i++) {
        worker = reflector_pool[i];
        spin_lock(&worker->worker_lock);
        list_for_each_entry_safe(entry, tmp, &worker->assigned_objs, node) {
            if (entry == obj) {
                list_del(&entry->node);
                spin_unlock(&worker->worker_lock);
                return;
            }
        }
        spin_unlock(&worker->worker_lock);
    }
}
EXPORT_SYMBOL(reflector_task_unbind_object);

/**
 * 6.1/6.2 Inicialização e Dimensionamento Dinâmico do Pool de Threads de produção.
 */
int reflector_task_mgr_init(void)
{
    unsigned int cpu;
    unsigned int idx = 0;

    nr_workers_active = num_online_cpus();
    reflector_pool = kmalloc_array(nr_workers_active, sizeof(struct reflector_worker *), GFP_KERNEL);
    if (!reflector_pool)
        return -ENOMEM;

    /* Instancia e fixa um kthread nativo para cada núcleo online de CPU */
    for_each_online_cpu(cpu) {
        struct reflector_worker *worker = kmalloc(sizeof(*worker), GFP_KERNEL);
        if (!worker) goto err_clean;

        worker->cpu_id = cpu;
        INIT_LIST_HEAD(&worker->assigned_objs);
        spin_lock_init(&worker->worker_lock);
        init_waitqueue_head(&worker->work_wait);

        /* Criação da Thread do Kernel */
        worker->thread = kthread_create_on_node(reflector_worker_fn, worker, 
                                                cpu_to_node(cpu), "reflector_wk/%u", cpu);
        if (IS_ERR(worker->thread)) {
            kfree(worker);
            goto err_clean;
        }

        /* 6.3 Força a afinidade estrita de hardware no escalonador do Linux */
        kthread_bind(worker->thread, cpu);
        wake_up_process(worker->thread);

        reflector_pool[idx++] = worker;
    }

    return 0;

err_clean:
    while (idx > 0) {
        struct reflector_worker *w = reflector_pool[--idx];
        kthread_stop(w->thread);
        kfree(w);
    }
    kfree(reflector_pool);
    reflector_pool = NULL;
    return -ENOMEM;
}

/**
 * 6.6 Encerramento síncrono e limpeza completa de threads antes da descarga do módulo.
 */
void reflector_task_mgr_exit(void)
{
    unsigned int i;
    struct reflector_worker *worker;

    if (!reflector_pool) return;

    /* Sinaliza a parada e realiza o join de todas as kthreads de forma ordenada */
    for (i = 0; i < nr_workers_active; i++) {
        worker = reflector_pool[i];
        if (worker && worker->thread) {
            kthread_stop(worker->thread);
            /* Nota: kthread_stop aguarda internamente a conclusão da execução atual */
            kfree(worker);
        }
    }

    kfree(reflector_pool);
    reflector_pool = NULL;
    nr_workers_active = 0;
}
