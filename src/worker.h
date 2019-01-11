/**
 * Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
 * 
 * Data structures used by worker threads, declaration of a worker entry point
 */

#include "config.h"
#include "common.h"
#include "time.h"

#include <stdlib.h>

/**
 * Structure to keep shared high-level summary of work done by worker so far
 *
 * The way the workers update these structures depends on whether progress monitoring was
 * requested by a user:
 *     1) if monitoring is requested, then the monitor thread reads execution summary of
 *        each worker once per every accumulation period (the length of the period is
 *        given by a user). Thus, the statistics summary structures are not private to
 *        workers in presence of monitoring. Access to them is protected by locks in this
 *        case. Locks make access to the structures expensive. To mitigate this negative
 *        impact, workers don't update their execution summary too often. Each worker can
 *        update its summary not more than 2 times during the accumulation period
 *     2) in absence of monitoring, the summary structures are private to workers, and the
 *        workers update them whenever thay have something to say
 */
typedef struct
{
    /* Number of tasks completed by worker so far */
    int64_t tasks_completed;
} pef_WorkerSharedSummary_t;

/**
 * Structure to keep private high-level execution summary of worker
 *
 * While a worker is running, only it is allowed to access its private execution summary
 * structure. After the worker is finished, the structure can be accessed by other
 * threads.
 *
 * In constrast to "shared execution summary", workers can update their "private execution
 * summary" as frequently as they like. That's because accessing this structure is cheap.
 * (While accessing "shared execution summary" may be expensive because it may require
 * acquiring a lock).
 *
 * Example: both "private" and "shared" execution summaries have "tasks_completed" field.
 *          But in the first case this field is always equal to the actual number of
 *          tasks completed by the worker so far. While in the second case, if monitoring
 *          is enabled, the field may lag significantly behind the actual number of
 *          completed tasks.
 *          The shared version of "tasks_completed" counter is used for progress tracking
 *          purporses. The precise number of completed tasks is not absolutely necessary
 *          for progress tracking.
 *          PEF relies on the private version of "tasks_completed" when emitting the
 *          detailed execution report. Workers no longer exist by that time. Hence, all
 *          their execution artifacts may be accessed freely. After a worker ceases to
 *          exist, its private summary structure still keeps the exact number of tasks
 *          completed by the worker. This is particularly useful when a worker fails or
 *          gets cancelled before it completes ALL assigned tasks.
 */
typedef struct
{
    /* Number of tasks completed by worker so far */
    int64_t tasks_completed;
} pef_WorkerPrivateSummary_t;

/**
 * Single-task execution statistics
 *
 * Each worker has a pointer to its own pre-allocated array of structures of this type.
 * When a worker completes a task, it stores execution statistics related to this task in
 * the corresponding element of the array
 */
typedef struct
{
    /* Time when task was completed */
    pef_time_t completion_time;
} pef_WorkerTaskStat_t;

/**
 * Structure passed as an argument to a worker thread
 *
 * Workers must not modify their argument structures unless they fail (in which case a
 * failed worker stores an error message in its argument structure). The argument
 * structures are currently subject to false memory sharing. It is currently expected that
 * all modifiable data structures are referenced from the argument structure, but the
 * argument structure itself stays (almost) read-only. If that's to change - and the
 * argument structure is to become modifiable itself - allocation of the argument
 * structures must change appropriately to avoid false memory sharing
 */
typedef struct
{
    /* ID assigned to worker */
    int worker_id;
    /* PEF configuration */
    const pef_Opts_t *opts;
    /* Pointer to shared summary statistics structure */
    pef_WorkerSharedSummary_t *shared_summary;
    /* Pointer to private summary statistics structure */
    pef_WorkerPrivateSummary_t *private_summary;
    /* Array of per-task statistics structures */
    pef_WorkerTaskStat_t *stat_tasks;
    /* A lock that guards access to data structures that are not private to worker */
    pthread_mutex_t *lock;
    /* Function that supplies workers with data that they write to data files. The
       function gets the size of data that a worker needs and returns a pointer to a
       buffer with the corresponding data. Currently workers are not required neither to
       deallocate the buffer nor signal that they no longer need it. "get_data()" always
       returns a pointer to the same pre-generated data. So, "get_data()" is currently an
       abstraction that is not strictly required. But we introduced it keeping possible
       future development in mind */
    int (*get_data) ( void *common_arg,
                      const pef_Opts_t* const opts,
                      int64_t size,
                      char **buf,
                      char *err_msg,
                      int err_msg_size);
    /* Pointer to a common argument (as opposed to context-specific arguments)
       of "get_data()" function */
    void *get_data_arg;
    /* A buffer to store an error message returned by worker (if any) */
    char err_msg[PEF_ERR_MSG_SIZE];
} pef_WorkerArg_t;

/* Worker thread */
void *pef_DoWork( void *arg);
