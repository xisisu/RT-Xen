/******************************************************************************
 * Preemptive Global EDF scheduler for xen
 *
 * by Sisu Xi, 2013, Washington University in Saint Louis
 * based on code of George Dunlap's credit2 Scheduler
 */

#include <xen/config.h>
#include <xen/init.h>
#include <xen/lib.h>
#include <xen/sched.h>
#include <xen/domain.h>
#include <xen/delay.h>
#include <xen/event.h>
#include <xen/time.h>
#include <xen/perfc.h>
#include <xen/sched-if.h>
#include <xen/softirq.h>
#include <asm/atomic.h>
#include <xen/errno.h>
#include <xen/trace.h>
#include <xen/cpu.h>
#include <xen/keyhandler.h>
#include <xen/trace.h>

/*
 * TODO:
 *
 * Defeferrable server -> slack stealing periodic server
 * Split RunQ?
 * How to show individual VCPU info?
 * Migration compensation and resist like credit2
 * Locking overhead measuremnets
 * Lock Holder Problem, using yield?
 * Self switch problem?
 * More testing with xentrace and xenanalyze
 */


/*
 * Design: 
 *
 * Follows the pre-emptive Global-EDF theory.
 * Each VCPU can have a dedicated period/budget pair of parameter. When a VCPU is running, it burns its budget, and when there are no budget, the VCPU need to wait unitl next period to get replensihed. Any unused budget is discarded in the end of period.
 * Server mechanism: deferrable server is used here. Therefore, when a VCPU has no task but with budget left, the budget is preserved. 
 * Priority scheme: dynamic priority is used here. The earlier the absolute deadline (equals period), the higher the priority.
 * Queue scheme: a global runqueue is used here. It holds all runnable VCPUs. VCPUs are divided into two parts: with and without remaining budget. Among each part, VCPUs are sorted by their current deadlines.
 * Scheduling quanta: 1ms is picked as the scheduling quanta, but the accounting is done in microsecond level.
 * Extra: cpumask is also supported, as a result, although the runq is sorted, the scheduler also need to verify whether the cpumask is allowed or not.
 */

/*
 * Locking:
 * Just like credit2, a global system lock is used to protect the RunQ.
 * 
 * The lock is already grabbed when calling wake/sleep/schedule/ functions in schedule.c
 *
 * The functions involes RunQ and needs to grab locks are:
 *    dump, vcpu_insert, vcpu_remove, context_saved, 
 */

/*
 * Default parameters
 */
#define GEDF_DOM0_BUDGET        10 /* Dom0 always have budget to run, dedicate to core 0 */
#define GEDF_DOM0_PERIOD        10
#define GEDF_DEFAULT_BUDGET     4
#define GEDF_DEFAULT_PERIOD     10
#define GEDF_IDLE_BUDGET        10  /* ILDE VCPU always have budget to run */
#define GEDF_IDLE_PERIOD        10

/*
 * Useful macros
 */
#define GEDF_PRIV(_ops)     ((struct gedf_private *)((_ops)->sched_data))
#define GEDF_VCPU(_vcpu)    ((struct gedf_vcpu *)(_vcpu)->sched_priv)
#define GEDF_DOM(_dom)      ((struct gedf_dom *)(_dom)->sched_priv)
#define CUR_VCPU(_cpu)      (per_cpu(schedule_data, _cpu).curr)
#define RUNQ(_ops)          (&GEDF_PRIV(_ops)->runq)

/*
 * Flags
 */
#define __GEDF_scheduled            1
#define GEDF_scheduled (1<<__GEDF_scheduled)
#define __GEDF_delayed_runq_add     2
#define GEDF_delayed_runq_add (1<<__GEDF_delayed_runq_add)

/*
 * Used to limit debug output
 */
// #define RTXEN_DEBUG
#ifdef RTXEN_DEBUG
#define RTXEN_WAKE      0
#define RTXEN_SLEEP     1
#define RTXEN_PICK      2
#define RTXEN_SCHED     3
#define RTXEN_MIGRATE   4
#define RTXEN_CONTEXT   5
#define RTXEN_YIELD     6
#define RTXEN_MAX       10
static int rtxen_counter[7] = {0};
#endif

/* 
 * Used to printout debug information
 */
#define printtime()     ( printk("%d : %3ld.%3ld : %-19s ", smp_processor_id(), NOW()/MILLISECS(1), NOW()%MILLISECS(1)/1000, __func__) )

/*
 * Systme-wide private data, include a global RunQueue
 * The global lock is referenced by schedule_data.schedule_lock from all physical cpus.
 * It can be grabbed via vcpu_schedule_lock_irq()
 */
struct gedf_private {
    spinlock_t lock;        /* The global coarse grand lock */
    struct list_head sdom;  /* list of availalbe domains, used for dump */
    struct list_head runq;  /* Ordered list of runnable VMs */
    cpumask_t cpus;         /* cpumask_t of available physical cpus */
    cpumask_t tickled;      /* another cpu in the queue already ticked this one */
};

/*
 * Virtual CPU
 */
struct gedf_vcpu {
    struct list_head runq_elem; /* On the runqueue list */
    struct list_head sdom_elem; /* On the domain VCPU list */

    /* Up-pointers */
    struct gedf_dom *sdom;
    struct vcpu *vcpu;

    /* GEDF parameters, in milliseconds */
    int period;
    int budget;
    
    /* VCPU current infomation */
    long cur_budget;             /* current budget in microseconds, if none, should not schedule unless extra */
    s_time_t last_start;        /* last start time, used to calculate budget */
    s_time_t cur_deadline;      /* current deadline, used to do EDF */
    unsigned flags;             /* mark __GEDF_scheduled, etc.. */
};

/*
 * Domain
 */
struct gedf_dom {
    struct list_head vcpu;      /* link its VCPUs */
    struct list_head sdom_elem; /* link list on gedf_priv */
    struct domain *dom;         /* pointer to upper domain */
    int    extra;               /* not implemented */
};

/*
 * RunQueue helper functions
 */
static int
__vcpu_on_runq(struct gedf_vcpu *svc) 
{
   return !list_empty(&svc->runq_elem);
}

static struct gedf_vcpu *
__runq_elem(struct list_head *elem)
{
    return list_entry(elem, struct gedf_vcpu, runq_elem);
}

/* lock is grabbed before calling this function */
static inline void
__runq_remove(struct gedf_vcpu *svc)
{
    if ( __vcpu_on_runq(svc) )
        list_del_init(&svc->runq_elem);
}

/* lock is grabbed before calling this function */
static void
__runq_insert(const struct scheduler *ops, struct gedf_vcpu *svc)
{
    struct list_head *runq = RUNQ(ops);
    struct list_head *iter;
    ASSERT( spin_is_locked(per_cpu(schedule_data, svc->vcpu_processor).schedule_lock) );

    if ( __vcpu_on_runq(svc) )
        return;

    list_for_each(iter, runq) {
        struct gedf_vcpu * iter_svc = __runq_elem(iter);

        if ( svc->cur_budget > 0 && 
             (iter_svc->cur_budget == 0 ||
              svc->cur_deadline < iter_svc->cur_deadline) ) {
            break;
        }
        if ( svc->cur_budget == 0 &&
             iter_svc->cur_budget == 0 &&
             svc->cur_deadline < iter_svc->cur_deadline ) {
            break;
        }
    }

    list_add_tail(&svc->runq_elem, iter);
}

/*
 * Debug related code, dump vcpu/pcpu
 */
static void
gedf_dump_vcpu(struct gedf_vcpu *svc)
{
    if ( svc == NULL ) {
        printk("NULL!\n");
        return;
    }
// #define cpustr keyhandler_scratch 
    // cpumask_scnprintf(cpustr, sizeof(cpustr), svc->vcpu->cpu_affinity);
    printk("[%5d.%-2d] cpu %d, (%-2d, %-2d), cur_b=%ld cur_d=%lu last_start=%lu onR=%d runnable=%d\n",
        // , affinity=%s\n",
            svc->vcpu->domain->domain_id,
            svc->vcpu->vcpu_id,
            svc->vcpu->processor,
            svc->period,
            svc->budget,
            svc->cur_budget,
            svc->cur_deadline,
            svc->last_start,
            __vcpu_on_runq(svc),
            vcpu_runnable(svc->vcpu) );
            // cpustr);
// #undef cpustr
}

static void
gedf_dump_pcpu(const struct scheduler *ops, int cpu)
{
    struct gedf_vcpu *svc = GEDF_VCPU(CUR_VCPU(cpu));

    printtime();
    gedf_dump_vcpu(svc);
}

/* should not need lock here. only showing stuff */
static void 
gedf_dump(const struct scheduler *ops)
{
    struct list_head *iter_sdom, *iter_svc, *runq, *iter;
    struct gedf_private *prv = GEDF_PRIV(ops);
    struct gedf_vcpu *svc;
    int cpu = 0;
    int loop = 0;

    printtime();
    printk("\n");

    printk("PCPU info: \n");
    for_each_cpu_mask(cpu, prv->cpus) {
        gedf_dump_pcpu(ops, cpu);
    }

    printk("Domain info: \n");
    loop = 0;
    list_for_each( iter_sdom, &prv->sdom ) {
        struct gedf_dom *sdom;
        sdom = list_entry(iter_sdom, struct gedf_dom, sdom_elem);
        printk("\tdomain: %d\n", sdom->dom->domain_id);

        list_for_each( iter_svc, &sdom->vcpu ) {
            svc = list_entry(iter_svc, struct gedf_vcpu, sdom_elem);
            printk("\t\t%3d: ", ++loop);
            gedf_dump_vcpu(svc);
        }
    }

    printk("RunQueue info: \n");
    loop = 0;
    runq = RUNQ(ops);
    list_for_each( iter, runq ) {
        svc = __runq_elem(iter);
        printk("\t%3d: ", ++loop);
        gedf_dump_vcpu(svc);
    }
}

/*
 * Init/Free related code
 */
static int
gedf_init(struct scheduler *ops)
{
    struct gedf_private *prv;

    prv = xmalloc(struct gedf_private);
    if ( prv == NULL ) {
        printk("malloc failed at gedf_private\n");
        return -ENOMEM;
    }
    memset(prv, 0, sizeof(*prv));

    ops->sched_data = prv;
    spin_lock_init(&prv->lock);
    INIT_LIST_HEAD(&prv->sdom);
    INIT_LIST_HEAD(&prv->runq);
    cpus_clear(prv->tickled);
    cpus_clear(prv->cpus);

    printk("This is the Deferrable Server version of the preemptive GEDF scheduler\n");
    printk("If you want to use it as a periodic server, please run a background busy CPU task\n");

    printtime();
    printk("\n");

    return 0;
}

static void
gedf_deinit(const struct scheduler *ops)
{
    struct gedf_private *prv;

    printtime();
    printk("\n");

    prv = GEDF_PRIV(ops);
    if ( prv )
        xfree(prv);
}

/* point per_cpu spinlock to the global system lock */
static void *
gedf_alloc_pdata(const struct scheduler *ops, int cpu)
{
    struct gedf_private *prv = GEDF_PRIV(ops);

    cpu_set(cpu, prv->cpus);

    per_cpu(schedule_data, cpu).schedule_lock = &prv->lock;

    printtime();
#define cpustr keyhandler_scratch 
    cpumask_scnprintf(cpustr, sizeof(cpustr), prv->cpus);
    printk("cpu=%d system_cpus=%s\n", cpu, cpustr);
#undef cpustr
    return (void *)1;
}

static void
gedf_free_pdata(const struct scheduler *ops, void *pcpu, int cpu)
{
    struct gedf_private * prv = GEDF_PRIV(ops);
    cpu_clear(cpu, prv->cpus);
    printtime();
    printk("cpu=%d\n", cpu);
}

static void *
gedf_alloc_domdata(const struct scheduler *ops, struct domain *dom)
{
    unsigned long flags;
    struct gedf_dom *sdom;
    struct gedf_private * prv = GEDF_PRIV(ops);

    printtime();
    printk("dom=%d\n", dom->domain_id);

    sdom = xmalloc(struct gedf_dom);
    if ( sdom == NULL ) {
        printk("%s, xmalloc failed\n", __func__);
        return NULL;
    }
    memset(sdom, 0, sizeof(*sdom));

    INIT_LIST_HEAD(&sdom->vcpu);
    INIT_LIST_HEAD(&sdom->sdom_elem);
    sdom->dom = dom;
    sdom->extra = 0;         /* by default, should not allow extra time */

    /* spinlock here to insert the dom */
    spin_lock_irqsave(&prv->lock, flags);
    list_add_tail(&sdom->sdom_elem, &(prv->sdom));
    spin_unlock_irqrestore(&prv->lock, flags);

    return (void *)sdom;
}

static void
gedf_free_domdata(const struct scheduler *ops, void *data)
{
    unsigned long flags;
    struct gedf_dom *sdom = data;
    struct gedf_private * prv = GEDF_PRIV(ops);

    printtime();
    printk("dom=%d\n", sdom->dom->domain_id);

    spin_lock_irqsave(&prv->lock, flags);
    list_del_init(&sdom->sdom_elem);
    spin_unlock_irqrestore(&prv->lock, flags);
    xfree(data);
}

static int
gedf_dom_init(const struct scheduler *ops, struct domain *dom)
{
    struct gedf_dom *sdom;

    printtime();
    printk("dom=%d\n", dom->domain_id);

    /* IDLE Domain does not link on gedf_private */
    if ( is_idle_domain(dom) ) { return 0; }

    sdom = gedf_alloc_domdata(ops, dom);
    if ( sdom == NULL ) {
        printk("%s, failed\n", __func__);
        return -ENOMEM;
    }
    dom->sched_priv = sdom;

    return 0;
}

static void
gedf_dom_destroy(const struct scheduler *ops, struct domain *dom)
{
    printtime();
    printk("dom=%d\n", dom->domain_id);

    gedf_free_domdata(ops, GEDF_DOM(dom));
}

static void *
gedf_alloc_vdata(const struct scheduler *ops, struct vcpu *vc, void *dd)
{
    struct gedf_vcpu *svc;
    s_time_t now = NOW();
    long count;

    /* Allocate per-VCPU info */
    svc = xmalloc(struct gedf_vcpu);
    if ( svc == NULL ) {
        printk("%s, xmalloc failed\n", __func__);
        return NULL;
    }
    memset(svc, 0, sizeof(*svc));

    INIT_LIST_HEAD(&svc->runq_elem);
    INIT_LIST_HEAD(&svc->sdom_elem);
    svc->flags = 0U;
    svc->sdom = dd;
    svc->vcpu = vc;
    svc->last_start = 0;            /* init last_start is 0 */

    if ( !is_idle_vcpu(vc) ) {
        if ( vc->domain->domain_id == 0) {
            svc->period = GEDF_DOM0_PERIOD;
            svc->budget = GEDF_DOM0_BUDGET;
        } else {
            svc->period = GEDF_DEFAULT_PERIOD;
            svc->budget = GEDF_DEFAULT_BUDGET;    
        }
        
        count = (now/MILLISECS(svc->period)) + 1;
        svc->cur_deadline += count*MILLISECS(svc->period);  /* sync all VCPU's start time to 0 */
    } else {
        svc->period = GEDF_IDLE_PERIOD;
        svc->budget = GEDF_IDLE_BUDGET;
    }

    svc->cur_budget = svc->budget*1000; /* counting in microseconds level */
    printtime();
    gedf_dump_vcpu(svc);

    return svc;
}

static void
gedf_free_vdata(const struct scheduler *ops, void *priv)
{
    struct gedf_vcpu *svc = priv;
    printtime();
    gedf_dump_vcpu(svc);
    xfree(svc);
}

/* lock is grabbed before calling this function */
static void
gedf_vcpu_insert(const struct scheduler *ops, struct vcpu *vc)
{
    struct gedf_vcpu *svc = GEDF_VCPU(vc);

    printtime();
    gedf_dump_vcpu(svc);

    /* IDLE VCPU not allowed on RunQ */
    if ( is_idle_vcpu(vc) )
        return;

    list_add_tail(&svc->sdom_elem, &svc->sdom->vcpu);   /* add to dom vcpu list */
}

/* lock is grabbed before calling this function */
static void
gedf_vcpu_remove(const struct scheduler *ops, struct vcpu *vc)
{
    struct gedf_vcpu * const svc = GEDF_VCPU(vc);
    struct gedf_dom * const sdom = svc->sdom;

    printtime();
    gedf_dump_vcpu(svc);

    BUG_ON( sdom == NULL );
    BUG_ON( __vcpu_on_runq(svc) );

    if ( !is_idle_vcpu(vc) ) {
        list_del_init(&svc->sdom_elem);
    }
}

/*
 * Other important functions
 */
/* do we need the lock here? */
/* TODO: How to return the per VCPU parameters? Right now return the sum of budgets */
static int
gedf_dom_cntl(const struct scheduler *ops, struct domain *d, struct xen_domctl_scheduler_op *op) 
{
    struct gedf_dom * const sdom = GEDF_DOM(d);
    struct list_head *iter;

    if ( op->cmd == XEN_DOMCTL_SCHEDOP_getinfo ) {
        /* for debug use, whenever adjust Dom0 parameter, do global dump */
        if ( d->domain_id == 0 ) {
            gedf_dump(ops);
        }

        /* TODO: how to return individual VCPU parameters? */
        op->u.gedf.budget = 0;
        op->u.gedf.extra = sdom->extra;
        list_for_each( iter, &sdom->vcpu ) {
            struct gedf_vcpu * svc = list_entry(iter, struct gedf_vcpu, sdom_elem);
            op->u.gedf.budget += svc->budget;
            op->u.gedf.period = svc->period;
        }
    } else {
        ASSERT(op->cmd == XEN_DOMCTL_SCHEDOP_putinfo);
        sdom->extra = op->u.gedf.extra;
        list_for_each( iter, &sdom->vcpu ) {
            struct gedf_vcpu * svc = list_entry(iter, struct gedf_vcpu, sdom_elem);
            if ( op->u.gedf.vcpu == svc->vcpu->vcpu_id ) { /* adjust per VCPU parameter */
                svc->period = op->u.gedf.period;
                svc->budget = op->u.gedf.budget;
                break;
            }
        }
    }

    return 0;
}

/* return a VCPU considering affinity */
static int
gedf_cpu_pick(const struct scheduler *ops, struct vcpu *vc)
{
    cpumask_t cpus;
    int cpu;

    cpus_and(cpus, GEDF_PRIV(ops)->cpus, vc->cpu_affinity);
    cpu = cpu_isset(vc->processor, cpus)? vc->processor : cycle_cpu(vc->processor, cpus);
    ASSERT( !cpus_empty(cpus) && cpu_isset(cpu, cpus) );

// #ifdef RTXEN_DEBUG
//     if ( vc->domain->domain_id != 0 && rtxen_counter[RTXEN_PICK] < RTXEN_MAX ) {
//         printtime();
//         gedf_dump_vcpu(GEDF_VCPU(vc));
//         rtxen_counter[RTXEN_PICK]++;
//     }
// #endif
    
    return cpu;
}

/* TODO: right now implemented as deferrable server. */
/* burn budget at microsecond level */
static void
burn_budgets(const struct scheduler *ops, struct gedf_vcpu *svc, s_time_t now) {
    s_time_t delta;
    unsigned int consume;
    long count = 0;

    /* first time called for this svc, update last_start */
    if ( svc->last_start == 0 ) {
        svc->last_start = now;
        return;
    }

    /* don't burn budget for idle VCPU */
    if ( is_idle_vcpu(svc->vcpu) ) {
        return;
    }

    /* update deadline info */
    delta = now - svc->cur_deadline;
    if ( delta >= 0 ) {
        count = ( delta/MILLISECS(svc->period) ) + 1;
        svc->cur_deadline += count * MILLISECS(svc->period);
        svc->cur_budget = svc->budget * 1000;
        return;
    }

    delta = now - svc->last_start;
    if ( delta < 0 ) {
        printk("%s, delta = %ld for ", __func__, delta);
        gedf_dump_vcpu(svc);
        svc->last_start = now;  /* update last_start */
        svc->cur_budget = 0;    
        return;
    }

    if ( svc->cur_budget == 0 ) return;

    /* burn at microseconds level */
    consume = ( delta/MICROSECS(1) );
    if ( delta%MICROSECS(1) > MICROSECS(1)/2 ) consume++;
    
    svc->cur_budget -= consume;
    if ( svc->cur_budget < 0 ) svc->cur_budget = 0;
}

/* RunQ is sorted. Pick first one within cpumask. If no one, return NULL */
/* lock is grabbed before calling this function */
static struct gedf_vcpu *
__runq_pick(const struct scheduler *ops, cpumask_t mask)
{
    struct list_head *runq = RUNQ(ops);
    struct list_head *iter;
    struct gedf_vcpu *svc = NULL;
    struct gedf_vcpu *iter_svc = NULL;
    cpumask_t cpu_common;

    list_for_each(iter, runq) {
        iter_svc = __runq_elem(iter);

        cpus_clear(cpu_common);
        cpus_and(cpu_common, mask, iter_svc->vcpu->cpu_affinity);
        if ( cpus_empty(cpu_common) )
            continue;

        if ( iter_svc->cur_budget <= 0 && iter_svc->sdom->extra == 0 )
            continue;

        svc = iter_svc;
        break;
    }

    return svc;
}

/* lock is grabbed before calling this function */
static void
__repl_update(const struct scheduler *ops, s_time_t now)
{
    struct list_head *runq = RUNQ(ops);
    struct list_head *iter;
    struct list_head *tmp;
    struct gedf_vcpu *svc = NULL;

    s_time_t diff;
    long count;

    list_for_each_safe(iter, tmp, runq) {
        svc = __runq_elem(iter);

        diff = now - svc->cur_deadline;
        if ( diff > 0 ) {
            count = (diff/MILLISECS(svc->period)) + 1;
            svc->cur_deadline += count * MILLISECS(svc->period);
            svc->cur_budget = svc->budget * 1000;
            __runq_remove(svc);
            __runq_insert(ops, svc);
        }
    }
}

/* The lock is already grabbed in schedule.c, no need to lock here */
static struct task_slice
gedf_schedule(const struct scheduler *ops, s_time_t now, bool_t tasklet_work_scheduled)
{
    const int cpu = smp_processor_id();
    struct gedf_private * prv = GEDF_PRIV(ops);
    struct gedf_vcpu * const scurr = GEDF_VCPU(current);
    struct gedf_vcpu * snext = NULL;
    struct task_slice ret;

    /* clear ticked bit now that we've been scheduled */
    if ( cpu_isset(cpu, prv->tickled) )
        cpu_clear(cpu, prv->tickled);

    /* burn_budget would return for IDLE VCPU */
    burn_budgets(ops, scurr, now);

#ifdef RTXEN_DEBUG
    if ( !is_idle_vcpu(scurr->vcpu) && scurr->vcpu->domain->domain_id != 0 && rtxen_counter[RTXEN_SCHED] < RTXEN_MAX ) {
    // if ( rtxen_counter[RTXEN_SCHED] < RTXEN_MAX ) {
        printtime();
        printk("from: ");
        gedf_dump_vcpu(scurr);
        rtxen_counter[RTXEN_SCHED]++;
    }
#endif

    __repl_update(ops, now);

    if ( tasklet_work_scheduled ) {
        snext = GEDF_VCPU(idle_vcpu[cpu]);
    } else {
        cpumask_t cur_cpu;
        cpus_clear(cur_cpu);
        cpu_set(cpu, cur_cpu);
        snext = __runq_pick(ops, cur_cpu);
        if ( snext == NULL )
            snext = GEDF_VCPU(idle_vcpu[cpu]);

        /* if scurr has higher priority and budget, still pick scurr */
        if ( !is_idle_vcpu(current) && 
             vcpu_runnable(current) && 
             scurr->cur_budget > 0 &&
             ( is_idle_vcpu(snext->vcpu) ||
               scurr->cur_deadline <= snext->cur_deadline ) ) {
               // scurr->vcpu->domain->domain_id == snext->vcpu->domain->domain_id ) ) {
            snext = scurr;
        }
    }

    /* Trace switch self problem */
    if ( snext != scurr &&
         !is_idle_vcpu(snext->vcpu) &&
         !is_idle_vcpu(scurr->vcpu) &&
         snext->vcpu->domain->domain_id == scurr->vcpu->domain->domain_id &&
         scurr->cur_budget > 0 &&
         vcpu_runnable(current) &&
         snext->cur_deadline < scurr->cur_deadline ) {
        TRACE_3D(TRC_SCHED_GEDF_SWITCHSELF, scurr->vcpu->domain->domain_id, scurr->vcpu->vcpu_id, snext->vcpu->vcpu_id);
    }

    if ( snext != scurr && 
         !is_idle_vcpu(current) &&
         vcpu_runnable(current) ) {
        set_bit(__GEDF_delayed_runq_add, &scurr->flags);
    }

    snext->last_start = now;
    ret.migrated = 0;
    if ( !is_idle_vcpu(snext->vcpu) ) {
        if ( snext != scurr ) {
            __runq_remove(snext);
            set_bit(__GEDF_scheduled, &snext->flags);
        }
        if ( snext->vcpu->processor != cpu ) {
            snext->vcpu->processor = cpu;
            ret.migrated = 1;
        }
    }

    if ( is_idle_vcpu(snext->vcpu) || snext->cur_budget > MILLISECS(1)) {
        ret.time = MILLISECS(1);
    } else if ( snext->cur_budget > 0 ) {
        ret.time = MICROSECS(snext->budget);
    } else {
        ret.time = MICROSECS(500);
    }
    
    ret.task = snext->vcpu;

#ifdef RTXEN_DEBUG
    if ( !is_idle_vcpu(snext->vcpu) && snext->vcpu->domain->domain_id != 0 && rtxen_counter[RTXEN_SCHED] < RTXEN_MAX ) {
    // if ( rtxen_counter[RTXEN_SCHED] < RTXEN_MAX ) {
        printtime();
        printk(" to : ");
        gedf_dump_vcpu(snext);
    }
#endif

    return ret;
}

/* Remove VCPU from RunQ */
/* The lock is already grabbed in schedule.c, no need to lock here */
static void
gedf_vcpu_sleep(const struct scheduler *ops, struct vcpu *vc)
{
    struct gedf_vcpu * const svc = GEDF_VCPU(vc);

#ifdef RTXEN_DEBUG
    if ( vc->domain->domain_id != 0 && rtxen_counter[RTXEN_SLEEP] < RTXEN_MAX ) {
        printtime();
        gedf_dump_vcpu(svc);    
        rtxen_counter[RTXEN_SLEEP]++;
    }
#endif
    
    BUG_ON( is_idle_vcpu(vc) );

    if ( CUR_VCPU(vc->processor) == vc ) {
        cpu_raise_softirq(vc->processor, SCHEDULE_SOFTIRQ);
        return;
    }

    if ( __vcpu_on_runq(svc) ) {
        __runq_remove(svc);
    }

    clear_bit(__GEDF_delayed_runq_add, &svc->flags);
}

/*
 * called by wake() and context_saved()
 * we have a running candidate here, the kick logic is:
 * Among all the cpus that are within the cpu affinity
 * 1) if the new->cpu is idle, kick it. This could benefit cache hit
 * 2) if there are any idle vcpu, kick it.
 * 3) now all pcpus are busy, among all the running vcpus, pick lowest priority one 
 *    if snext has higher priority, kick it.
 * TODO: what if these two vcpus belongs to the same domain?
 * replace a vcpu belonging to the same domain does not make sense
 */
/* lock is grabbed before calling this function */
static void
runq_tickle(const struct scheduler *ops, struct gedf_vcpu *new)
{
    struct gedf_private * prv = GEDF_PRIV(ops);
    struct gedf_vcpu * scheduled = NULL;    /* lowest priority scheduled */
    struct gedf_vcpu * iter_svc;
    struct vcpu * iter_vc;
    int cpu = 0;
    cpumask_t not_tickled;                  /* not tickled cpus */

    if ( new == NULL || is_idle_vcpu(new->vcpu) ) return;

    cpus_clear(not_tickled);
    cpus_andnot(not_tickled, new->vcpu->cpu_affinity, prv->tickled);

    /* 1) if new cpu is idle, kick it for cache */
    if ( is_idle_vcpu(CUR_VCPU(new->vcpu->processor)) ) {
        cpu_set(new->vcpu->processor, prv->tickled);
        cpu_raise_softirq(new->vcpu->processor, SCHEDULE_SOFTIRQ);
        return;
    }

    /* 2) if there are any idle pcpu, kick it */
    /* the same loop also found the one with lowest priority */
    for_each_cpu_mask(cpu, not_tickled) {
        iter_vc = CUR_VCPU(cpu);
        if ( is_idle_vcpu(iter_vc) ) {
            cpu_set(cpu, prv->tickled);
            cpu_raise_softirq(cpu, SCHEDULE_SOFTIRQ);
            return;
        }
        iter_svc = GEDF_VCPU(iter_vc);
        if ( scheduled == NULL || iter_svc->cur_deadline < scheduled->cur_deadline ) {
            scheduled = iter_svc;
        }
    }

    /* 3) new has higher priority, kick it */
    if ( scheduled != NULL && new->cur_deadline < scheduled->cur_deadline ) {
        cpu_set(scheduled->vcpu->processor, prv->tickled);
        cpu_raise_softirq(scheduled->vcpu->processor, SCHEDULE_SOFTIRQ);
    }
    return;
}

/* Should always wake up runnable, put it back to RunQ. Check priority to raise interrupt */
/* The lock is already grabbed in schedule.c, no need to lock here */
/* TODO: what if these two vcpus belongs to the same domain? */
static void
gedf_vcpu_wake(const struct scheduler *ops, struct vcpu *vc)
{
    struct gedf_vcpu * const svc = GEDF_VCPU(vc);
    s_time_t diff;
    s_time_t now = NOW();
    long count = 0;
    struct gedf_private * prv = GEDF_PRIV(ops);
    struct gedf_vcpu * snext = NULL;        /* highest priority on RunQ */

#ifdef RTXEN_DEBUG
    if ( vc->domain->domain_id != 0 && rtxen_counter[RTXEN_WAKE] < RTXEN_MAX ) {
        printtime();
        gedf_dump_vcpu(svc);
        rtxen_counter[RTXEN_WAKE]++;
    }
#endif
    
    BUG_ON( is_idle_vcpu(vc) );

    if ( unlikely(CUR_VCPU(vc->processor) == vc) ) return;

    /* on RunQ, just update info is ok */
    if ( unlikely(__vcpu_on_runq(svc)) ) return;    

    /* if context hasn't been saved yet, set flag so it will add later */
    if ( unlikely(test_bit(__GEDF_scheduled, &svc->flags)) ) {
        set_bit(__GEDF_delayed_runq_add, &svc->flags);
        return;
    }

    /* update deadline info */
    diff = now - svc->cur_deadline;
    if ( diff >= 0 ) {
        count = ( diff/MILLISECS(svc->period) ) + 1;
        svc->cur_deadline += count * MILLISECS(svc->period);
        svc->cur_budget = svc->budget * 1000;
    }

    __runq_insert(ops, svc);
    __repl_update(ops, now);
    snext = __runq_pick(ops, prv->cpus);    /* pick snext from ALL cpus */
    runq_tickle(ops, snext);

    return;
}

/* scurr has finished context switch, insert it back to the RunQ*/
static void
gedf_context_saved(const struct scheduler *ops, struct vcpu *vc)
{
    struct gedf_vcpu * svc = GEDF_VCPU(vc);
    struct gedf_vcpu * snext = NULL;
    struct gedf_private * prv = GEDF_PRIV(ops);

#ifdef RTXEN_DEBUG
    if ( vc->domain->domain_id != 0 && rtxen_counter[RTXEN_CONTEXT] < RTXEN_MAX ) {
        printtime();
        gedf_dump_vcpu(svc);    
        rtxen_counter[RTXEN_CONTEXT]++;
    }
#endif

    clear_bit(__GEDF_scheduled, &svc->flags);
    if ( is_idle_vcpu(vc) ) return;

    vcpu_schedule_lock_irq(vc);
    if ( test_and_clear_bit(__GEDF_delayed_runq_add, &svc->flags) && likely(vcpu_runnable(vc)) ) {
        __runq_insert(ops, svc);
        __repl_update(ops, NOW());
        snext = __runq_pick(ops, prv->cpus);    /* pick snext from ALL cpus */
        runq_tickle(ops, snext);
    } 
    vcpu_schedule_unlock_irq(vc);
}


// /*
//  * Since we only have one RunQ here, do we still need this function?
//  */
// static void
// gedf_vcpu_migrate(const struct scheduler *ops, struct vcpu *vc, unsigned int new_cpu)
// {
// #ifdef RTXEN_DEBUG
//     struct gedf_vcpu * svc = GEDF_VCPU(vc);
//     if ( rtxen_counter[RTXEN_MIGRATE] < RTXEN_MAX ) {
//         printtime();
//         gedf_dump_vcpu(svc);
//         printk("\tnew_cpu=%d\n", new_cpu);
//         rtxen_counter[RTXEN_MIGRATE]++;
//     }
// #endif
// }

// /*
//  * Not implemented now.
//  */
// static void
// gedf_vcpu_yield(const struct scheduler *ops, struct vcpu *vc)
// {
// #ifdef RTXEN_DEBUG
//     struct gedf_vcpu * svc = GEDF_VCPU(vc);
//     if ( rtxen_counter[RTXEN_YIELD] < RTXEN_MAX ) {
//         printtime();
//         gedf_dump_vcpu(svc);
//         rtxen_counter[RTXEN_YIELD]++;
//     }
// #endif
// }

static struct gedf_private _gedf_priv;

const struct scheduler sched_gedf_def = {
    .name           = "SMP GEDF Scheduler",
    .opt_name       = "gedf",
    .sched_id       = XEN_SCHEDULER_GEDF,
    .sched_data     = &_gedf_priv,

    .dump_cpu_state = gedf_dump_pcpu,
    .dump_settings  = gedf_dump,
    .init           = gedf_init,
    .deinit         = gedf_deinit,
    .alloc_pdata    = gedf_alloc_pdata,
    .free_pdata     = gedf_free_pdata,
    .alloc_domdata  = gedf_alloc_domdata,
    .free_domdata   = gedf_free_domdata,
    .init_domain    = gedf_dom_init,
    .destroy_domain = gedf_dom_destroy,
    .alloc_vdata    = gedf_alloc_vdata,
    .free_vdata     = gedf_free_vdata,
    .insert_vcpu    = gedf_vcpu_insert,
    .remove_vcpu    = gedf_vcpu_remove,

    .adjust         = gedf_dom_cntl,
    .pick_cpu       = gedf_cpu_pick,
    .do_schedule    = gedf_schedule,
    .sleep          = gedf_vcpu_sleep,
    .wake           = gedf_vcpu_wake,
    .context_saved  = gedf_context_saved,

    .yield          = NULL,
    .migrate        = NULL,
};
