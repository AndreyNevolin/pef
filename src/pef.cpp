/**
 * Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
 *
 * Performance Evaluation Framework
 */

/* PEF includes */
#include "common.h"
#include "worker.h"
#include "time.h"
#include "config.h"

/* Standard C/C++ includes */
#include <unistd.h>
#include <errno.h>

/* Linux, POSIX, GNU includes */
#include <fcntl.h>

/* This "include" is required for "getrandom()" system call and - correspondingly - a
 * version of "pef_GenerateInitialData()" that relies on this system call. Currently we
 * don't use this version of "pef_GenerateInitialData()" by default because at the time
 * of this writing most Linux installations were not fresh enough and didn't support
 * "getrandom()".
 * Uncomment this "include" and also the corresponding version of
 * "pef_GenerateInitialData()" if you want to enable random data generation based on
 * "getrandom()"
 *
 * #include <sys/random.h>
 */

/**
 * Structure that represents a context-invariant argument (as opposed to context-specific
 * arguments) for "pef_GetData()" function
 */
typedef struct
{
    /* Buffer with pre-generated data */
    char *data_buf;
} pef_GetDataArg_t;

/**
 * Global - i.e. not related to particular tasks or workers - execution statistics
 */
typedef struct
{
    /* Time when actual benchmarking was started */
    pef_time_t benchmark_start_time;
    /* Whether there were errors during benchmarking */
    bool is_errors;
} pef_GlobalStat_t;

/**
 * References to data structures pre-allocated for workers
 */
typedef struct
{
    /* Array of references to structures with shared summary statistics */
    pef_WorkerSharedSummary_t **shared_summary;
    /* Array of references to structures with private summary statistics */
    pef_WorkerPrivateSummary_t **private_summary;
    /* Array of references to arrays of structures with per-task statistics. Amount of
       tasks to do by each worker is known in advance. So, all the memory needed to keep
       the statistics can be pre-allocated in advance */
    pef_WorkerTaskStat_t **stat_tasks;
    /* Array of references to locks that guard access to data structures that are not
       private to workers. One lock per each worker */
    pthread_mutex_t **locks;
} pef_WorkerData_t;

/**
 * Generate initial data
 *
 * (We call this step "initial" generation of data because it happens before the worker
 * threads start doing their job. In future "in-flight" generation of data or some other
 * strategies may be added)
 *
 * Return value: a pointer to a buffer with generated data. The buffer must be deallocated
 * after use by caller
 */
static int pef_GenerateInitialData( const pef_Opts_t* const opts,
                                    char **buf_ret,
                                    std::string indent,
                                    char *err_msg,
                                    int err_msg_size)
{
    PEF_ASSERT( opts);
    PEF_VERBOSE( opts, "%sGenerating %ld bytes of initial data", indent.c_str(),
                 opts->write_chunk_size);

    int ret = 0;
    char local_err_msg[PEF_ERR_MSG_SIZE];
    const char dev_urandom[] = "/dev/urandom";
    /* Allocate memory for the data buffer */
    char *data_buf = (char *)malloc( opts->write_chunk_size);

    if ( !data_buf )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for the data "
                      "buffer");

        return PEF_RET_GENERIC_ERR;
    }

    int fd = open( dev_urandom, O_RDONLY);
    int64_t bytes_read = -1;

    if ( fd == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't open \"%s\": %s", dev_urandom,
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_generate_initial_data_out;
    }

    bytes_read = read( fd, data_buf, opts->write_chunk_size);

    if ( bytes_read == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error reading \"%s\": %s", dev_urandom,
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_generate_initial_data_out;
    }

    if ( bytes_read < opts->write_chunk_size )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size,  "\"%s\" returned less data than it was "
                      "requested (%ld < %ld)", dev_urandom, bytes_read,
                      opts->write_chunk_size);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_generate_initial_data_out;
    }

pef_generate_initial_data_out:
    /* Don't care of a value returned by "close()" */
    if ( fd >= 0 ) close( fd);

    if ( ret || !buf_ret )
    {
        if ( data_buf ) free( data_buf);
    } else
    {
        PEF_ASSERT( data_buf);
        *buf_ret = data_buf;
    }

    return ret;
}

#if 0
/**
 * This version of "pef_GenerateInitialData()" relies on "getrandom()" system call.
 * Currently we don't use this version by default because at the time of this writing most
 * Linux installations were not fresh enough and didn't support "getrandom()".
 * Uncomment this function and also a corresponding "include" at the beginning of the file
 * if you want to enable random data generation based on "getrandom()"
 */
static int pef_GenerateInitialData( const pef_Opts_t* const opts,
                                    char **buf_ret,
                                    std::string indent,
                                    char *err_msg,
                                    int err_msg_size)
{
    PEF_ASSERT( opts);
    PEF_VERBOSE( opts, "%sGenerating %ld bytes of initial data", indent.c_str(),
                 opts->write_chunk_size);

    int ret = 0;
    char local_err_msg[PEF_ERR_MSG_SIZE];
    int64_t num_bytes = -1;
    /* Allocate memory for the data buffer */
    char *data_buf = (char *)malloc( opts->write_chunk_size);

    if ( !data_buf )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for the data "
                      "buffer");

        return PEF_RET_GENERIC_ERR;
    }

    num_bytes = getrandom( data_buf, opts->write_chunk_size, GRND_NONBLOCK);

    if ( num_bytes == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Generation of data by means of "
                      "\"getrandom()\" failed: %s",
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_generate_initial_data_out;
    }

    if ( num_bytes < opts->write_chunk_size )
    {
        PEF_ERROR( "\"getrandom()\" returned less data than it was requested "
                   "(%ld < %ld)", num_bytes, opts->write_chunk_size);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_generate_initial_data_out;
    }

pef_generate_initial_data_out:
    if ( ret || !buf_ret )
    {
        if ( data_buf ) free( data_buf);
    } else
    {
        PEF_ASSERT( data_buf);
        *buf_ret = data_buf;
    }

    return ret;
}
#endif

/**
 * Provide with data of requested size
 *
 * Currently this function just returns a pointer to the data pre-generated by
 * "pef_GenerateInitialData()" routine. The function extracts this pointer from
 * "common_arg"
 *
 * Return value: pointer to a buffer with data
 */
static int pef_GetData( void *common_arg,
                        const pef_Opts_t* const opts,
                        int64_t size,
                        char **buf,
                        char *err_msg,
                        int err_msg_size)
{
    PEF_ASSERT( common_arg && opts);

    if ( (size < 0) || (size > opts->write_chunk_size) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Size of requested data is invalid: %ld "
                      "(must be from 0 to %ld)", size, opts->write_chunk_size);

        return PEF_RET_GENERIC_ERR;
    }

    if ( buf ) *buf = ((pef_GetDataArg_t*)common_arg)->data_buf;

    return 0;
}

/**
 * Get cache line size
 *
 * The implementation provided here is platform-specific. It's expected to work seamlessly
 * on Linux
 *
 * It's expected that PEF will be executed on a system with homogenous CPUs (or on a
 * system with a single CPU). There is no mechanism in PEF to resolve false memory
 * sharing issues on systems with heterogenous CPUs
 */
static int pef_GetCacheLineSize( const pef_Opts_t* const opts,
                                 int *cline_size_ret,
                                 std::string indent,
                                 char *err_msg,
                                 int err_msg_size)
{
    PEF_ASSERT( opts);
    PEF_VERBOSE( opts, "%sGetting cache line size", indent.c_str());
    indent += "\t";

    const char *ind = indent.c_str();
    char local_err_msg[PEF_ERR_MSG_SIZE];
    long cline_size = -1;
    /* Get cache line size using "sysconf" */
    long cline_size_sysconf = sysconf( _SC_LEVEL1_DCACHE_LINESIZE);

    if ( cline_size_sysconf == -1 )
    {
        PEF_VERBOSE( opts, "%sCouldn't get cache line size by means of \"sysconf()\"",
                     ind);
    } else
    {
        PEF_VERBOSE( opts, "%sCache line size reported by \"sysconf()\": %ld", ind,
                     cline_size_sysconf);
    }

    /* Get cache line size using sysfs */
    long cline_size_sysfs = -1;
    const char *sysfs_path = "/sys/devices/system/cpu/cpu0/cache/index0/"
                             "coherency_line_size";
    const char *sysfs_err_prefix = "Couldn't get cache line size from SysFS: ";
    FILE *file = fopen( sysfs_path, "r");

    if ( !file )
    {
        PEF_VERBOSE( opts, "%s%serror opening \"%s\": %s", ind, sysfs_err_prefix,
                     sysfs_path,
                     PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
    } else
    {
        errno = 0;

        if ( fscanf( file, "%ld", &cline_size_sysfs) != 1 )
        {
            /* Restore the initial value of the variable. We do that because it's unclear
               from the C++ documentation whether "fscanf()" may modify the argument in
               case of errors */
            cline_size_sysfs = -1;

            if ( feof( file) )
            {
                PEF_VERBOSE( opts, "%s%sfile \"%s\" is empty", ind, sysfs_err_prefix,
                             sysfs_path);
            } else
            {
                PEF_ASSERT( errno);
                PEF_VERBOSE( opts, "%s%serror extracting a value from \"%s\": %s", ind,
                             sysfs_err_prefix, sysfs_path,
                             PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
            }
        } else
        {
            PEF_VERBOSE( opts, "%sCache line size obtained from SysFS: %ld", ind,
                         cline_size_sysfs);
        }
    }

    /* Don't care of possible errors returned by "fclose()" */
    fclose( file);

    if ( cline_size_sysconf == -1 && cline_size_sysfs == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't get cache line size by neither "
                      "of the methods");

        return PEF_RET_GENERIC_ERR;
    }

    if ( cline_size_sysconf != -1 && cline_size_sysfs != -1 &&
         cline_size_sysconf != cline_size_sysfs )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Cache line sizes obtained by different "
                      "methods are different");

        return PEF_RET_GENERIC_ERR;
    }

    if ( cline_size_sysconf != -1 )
    {
        PEF_VERBOSE( opts, "%sUsing cache line size returned by \"sysconf()\"", ind);
        cline_size = cline_size_sysconf;
    } else
    {
        PEF_VERBOSE( opts, "%sUsing cache line size obtained from SysFS\n", ind);
        cline_size = cline_size_sysfs;
    }

    if ( cline_size_ret ) *cline_size_ret = cline_size;

    return 0;
}

/**
 * Allocate memory for references to workers' data
 */
static int pef_AllocReferences( const pef_Opts_t* const opts,
                                pef_WorkerData_t *wdata,
                                std::string indent,
                                char *err_msg,
                                int err_msg_size)
{
    PEF_ASSERT( opts);

    int num_workers = opts->num_threads;
    const char *wdata_error_prefix = "Couldn't allocate memory for an array of "
                                     "references to workers' ";
    pef_WorkerSharedSummary_t **shared_summary = 0;
    pef_WorkerPrivateSummary_t **private_summary = 0;
    pef_WorkerTaskStat_t **stat_tasks = 0;
    pthread_mutex_t **locks = 0;
    int ret = 0;

    PEF_VERBOSE( opts, "%sAllocating memory for storing references to per-worker data "
                 "structures", indent.c_str());
    shared_summary = (pef_WorkerSharedSummary_t**)
                     calloc( num_workers, sizeof( pef_WorkerSharedSummary_t*));

    if ( !shared_summary )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "%s%s", wdata_error_prefix,
                      "shared summary structures");
        ret = PEF_RET_GENERIC_ERR;

        goto pef_alloc_references_out;
    }

    private_summary = (pef_WorkerPrivateSummary_t**)
                      calloc( num_workers, sizeof( pef_WorkerPrivateSummary_t*));

    if ( !private_summary )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "%s%s", wdata_error_prefix,
                      "private summary structures");
        ret = PEF_RET_GENERIC_ERR;

        goto pef_alloc_references_out;
    }

    stat_tasks = (pef_WorkerTaskStat_t**)
                 calloc( num_workers, sizeof( pef_WorkerTaskStat_t*));

    if ( !stat_tasks )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "%s%s", wdata_error_prefix,
                      "per-task statistics data");
        ret = PEF_RET_GENERIC_ERR;

        goto pef_alloc_references_out;
    }

    if ( opts->screen_stat_timer != -1 )
    {
        locks = (pthread_mutex_t**)calloc( num_workers, sizeof( pthread_mutex_t*));

        if ( !locks )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "%s%s", wdata_error_prefix, "locks");
            ret = PEF_RET_GENERIC_ERR;

            goto pef_alloc_references_out;
        }
    }

pef_alloc_references_out:
    if ( !ret && wdata )
    {
        wdata->shared_summary = shared_summary;
        wdata->private_summary = private_summary;
        wdata->stat_tasks = stat_tasks;
        wdata->locks = locks;
    } else
    {
        if ( shared_summary ) free( shared_summary);

        if ( private_summary ) free( private_summary);

        if ( stat_tasks ) free( stat_tasks);

        if ( locks ) free( locks);
    }

    return ret;
}

/**
 * Allocate memory for workers' summary statistics
 */
static int pef_AllocSummaryStructs( pef_Opts_t *opts,
                                    pef_WorkerData_t *wdata,
                                    bool is_shared,
                                    int cline_size,
                                    std::string indent,
                                    char *err_msg,
                                    int err_msg_size)
{
    PEF_ASSERT( wdata);

    const char *summary_type = is_shared ? "shared" : "private";

    PEF_VERBOSE( opts, "%sAllocating memory to store %s execution summary of each "
                 "worker", indent.c_str(), summary_type);
    indent += "\t";

    char local_err_msg[PEF_ERR_MSG_SIZE];
    const char *ind = indent.c_str();
    int struct_size = is_shared ? sizeof( pef_WorkerSharedSummary_t) :
                      sizeof( pef_WorkerPrivateSummary_t);
    /* Calculate number of cache lines required to keep summary statistics */
    int num_clines = struct_size / cline_size;

    if ( struct_size % cline_size ) num_clines++;

    PEF_VERBOSE( opts, "%s%d cache lines is required to keep each worker's %s summary "
                 "structure", ind, num_clines, summary_type);

    int64_t i = 0;

    for ( i = 0; i < opts->num_threads; i++ )
    {
        void *ptr = 0;
        
        ptr = aligned_alloc( cline_size, num_clines * cline_size);

        if ( !ptr ) break;

        /* Initialize the allocated structure */
        if ( is_shared )
        {
            pef_WorkerSharedSummary_t *shared_p = (pef_WorkerSharedSummary_t*)ptr;

            *shared_p = {.tasks_completed = 0};
            wdata->shared_summary[i] = shared_p;
        } else
        {
            pef_WorkerPrivateSummary_t *private_p = (pef_WorkerPrivateSummary_t*)ptr;

            *private_p = {.tasks_completed = 0};
            wdata->private_summary[i] = private_p;
        }
    }

    if ( i != opts->num_threads )
    {
        PEF_ASSERT( i < opts->num_threads);
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for a %s summary "
                      "structure of worker #%ld: %s", summary_type, i,
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));

        while ( --i >= 0 )
        {
            if ( is_shared )
            {
                free( wdata->shared_summary[i]);
            } else
            {
                free( wdata->private_summary[i]);
            }
        }

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

/**
 * Allocate memory for workers' detailed statistics
 */
static int pef_AllocTaskStatStructs( pef_Opts_t *opts,
                                     pef_WorkerData_t *wdata,
                                     int cline_size,
                                     std::string indent,
                                     char *err_msg,
                                     int err_msg_size)
{
    PEF_ASSERT( opts && wdata);
    PEF_VERBOSE( opts, "%sAllocating memory to store detailed execution statistics of "
                 "each worker", indent.c_str());
    indent += '\t';

    const char *ind = indent.c_str();
    int64_t num_files = opts->num_files;
    int64_t num_workers = opts->num_threads;

    if ( opts->is_verbose )
    {
        /* Calculate size of memory required to keep the detailed statistics */
        int64_t group1_size = num_files % num_workers;
        int64_t group2_size = num_workers - group1_size;
        int64_t tasks_per_worker = num_files / num_workers;
        /* Memory per worker from the first group */
        /* Check that we will not have overflow on the multiplication below */
        PEF_ASSERT( INT64_MAX / (int64_t)sizeof( pef_WorkerTaskStat_t) >=
                    tasks_per_worker + 1);
        int64_t mem_sz1 = (tasks_per_worker + 1) * sizeof( pef_WorkerTaskStat_t);
        /* Memory per worker from the second group */
        int64_t mem_sz2 = tasks_per_worker * sizeof( pef_WorkerTaskStat_t);

        /* Account for alignment */
        /* Check that we will not have overflow on the multiplications below */
        PEF_ASSERT( INT64_MAX - cline_size >= mem_sz1);
        PEF_ASSERT( INT64_MAX - cline_size >= mem_sz2);
        mem_sz1 = ((mem_sz1 / cline_size) + (mem_sz1 % cline_size ? 1 : 0)) * cline_size;
        mem_sz2 = ((mem_sz2 / cline_size) + (mem_sz2 % cline_size ? 1 : 0)) * cline_size;

        /* Check that we will not get overflow while calulating "memory_needed" */
        /* "group1_size" may be equal to zero */
        if ( group1_size ) PEF_ASSERT( INT64_MAX / group1_size >= mem_sz1);

        PEF_ASSERT( INT64_MAX / group2_size >= mem_sz2);
        PEF_ASSERT( INT64_MAX - group1_size * mem_sz1 >= group2_size * mem_sz2);

        int64_t memory_needed = group1_size * mem_sz1 + group2_size * mem_sz2;

        PEF_OUT( "%s%ld bytes of memory is required to keep detailed statistics of "
                 "all worker threads", ind, memory_needed);
    }

    int64_t i = 0;

    for ( i = 0; i < num_workers; i++ )
    {
        int64_t tasks_to_do = num_files / num_workers;

        if ( i < (num_files % num_workers ) ) tasks_to_do++;

        int64_t mem_size = tasks_to_do * sizeof( pef_WorkerTaskStat_t);

        /* Check that we didn't get overflow on the multiplication above */
        PEF_ASSERT( INT64_MAX / (int64_t)sizeof( pef_WorkerTaskStat_t) >= tasks_to_do);
        /* Take alignement into account */
        mem_size = ((mem_size / cline_size) + (mem_size % cline_size ? 1 : 0)) *
                   cline_size;
        /* Check that we didn't get overflow on the multiplication above */
        PEF_ASSERT( INT64_MAX - cline_size >= mem_size);
        /* Finally allocate memory */
        (wdata->stat_tasks)[i] = (pef_WorkerTaskStat_t*)aligned_alloc( cline_size,
                                                                       mem_size);

        if ( (wdata->stat_tasks)[i] == 0 ) break;

        /* Initialize allocated structures */
        for ( int64_t j = 0; j < tasks_to_do; j++ )
        {
            (wdata->stat_tasks)[i][j] = {.completion_time = PEF_TIME_ZERO};
        }
    }

    if ( i < num_workers )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for the detailed "
                      "execution statistics of worker #%ld", i);

        while ( --i >= 0 ) free( (wdata->stat_tasks)[i]);

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

/**
 * Allocate locks used to guard access to workers' shared data
 */
static int pef_AllocLocks( pef_Opts_t *opts,
                           pef_WorkerData_t *wdata,
                           int cline_size,
                           std::string indent,
                           char *err_msg,
                           int err_msg_size)
{
    PEF_ASSERT( opts && wdata);
    PEF_VERBOSE( opts, "%sAllocating memory for locks used to guard access to "
                 "workers' shared execution summary", indent.c_str());
    indent += '\t';

    const char *ind = indent.c_str();
    /* Calculate number of cache lines needed to store one lock */
    /* Most of the code below is redundant because "pthread_mutex_t" is a really small
       data structure, while cache lines are more or less big. But we keep the code
       universal */
    int num_clines = sizeof( pthread_mutex_t) / cline_size;

    if ( sizeof( pthread_mutex_t) % cline_size ) num_clines++;

    PEF_VERBOSE( opts, "%s%d cache lines is required to keep one lock", ind, num_clines);

    int64_t i = 0;

    for ( i = 0; i < opts->num_threads; i++ )
    {
        pthread_mutex_t *lock_p = 0;

        /* Definitely there will be no overflow on the multiplication below */
        lock_p = (pthread_mutex_t*)aligned_alloc( cline_size, num_clines * cline_size);

        if ( !lock_p ) break;

        *lock_p = PTHREAD_MUTEX_INITIALIZER;
        wdata->locks[i] = lock_p;
    }

    if ( i < opts->num_threads )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for the lock of "
                      "worker #%ld", i);

        while ( --i >= 0 ) free( wdata->locks[i]);

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

/**
 * Deallocate memory occupied by workers' data structures
 */
static void pef_DeallocateWorkerStructs( pef_Opts_t *opts,
                                         pef_WorkerData_t *wdata,
                                         std::string indent)
{
    PEF_ASSERT( opts && wdata);
    PEF_VERBOSE( opts, "%sDeallocating memory occupied by workers' data structures",
                 indent.c_str());

    /* The code below assumes that (*wdata) and all the arrays referenced from (*wdata)
       were properly initialized right during allocation (namely be zeros) */
    if ( wdata->shared_summary )
    {
        for ( int i = 0; i < opts->num_threads; i++ )
        {
            if ( wdata->shared_summary[i] ) free( wdata->shared_summary[i]);
        }

        free( wdata->shared_summary);
        wdata->shared_summary = 0;
    }

    if ( wdata->private_summary )
    {
        for ( int i = 0; i < opts->num_threads; i++ )
        {
            if ( wdata->private_summary[i] ) free( wdata->private_summary[i]);
        }

        free( wdata->private_summary);
        wdata->private_summary = 0;
    }

    if ( wdata->stat_tasks )
    {
        for ( int i = 0; i < opts->num_threads; i++ )
        {
            if ( wdata->stat_tasks[i] ) free( wdata->stat_tasks[i]);
        }

        free( wdata->stat_tasks);
        wdata->stat_tasks = 0;
    }

    if ( wdata->locks )
    {
        /* Memory locks are allocated only if progress monitoring is requested */
        PEF_ASSERT( opts->screen_stat_timer != -1);

        for ( int i = 0; i < opts->num_threads; i++ )
        {
            if ( wdata->locks[i] )
            {
                pthread_mutex_destroy( wdata->locks[i]);
                free( wdata->locks[i]);
            }
        }

        free( wdata->locks);
        wdata->locks = 0;
    }

    return;
}

/**
 * Allocate data structures that scale in the same way as workers scale (i.e. each worker
 * will have its own set of those structures):
 *  - structures used to store per-worker execution statistics
 *  - locks that exist per worker
 *
 * All data structures allocated inside this function are aligned to cache line size to
 * avoid false memory sharing issues
 */
static int pef_AllocateWorkerStructs( pef_Opts_t *opts,
                                      pef_WorkerData_t *wdata_ret,
                                      std::string indent,
                                      char *err_msg,
                                      int err_msg_size)
{
    PEF_ASSERT( opts);

    int ret = 0;
    char local_err_msg[PEF_ERR_MSG_SIZE];
    int cline_size = -1;
    pef_WorkerData_t wdata = {.shared_summary = 0, .private_summary = 0, .stat_tasks = 0,
                              .locks = 0};

    PEF_VERBOSE( opts, "%sAllocating memory to store per-worker execution statistics "
                 "and locks", indent.c_str());
    indent += "\t";

    const char *ind = indent.c_str();

    ret = pef_GetCacheLineSize( opts, &cline_size, indent, local_err_msg,
                                sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error getting cache line size: %s",
                      local_err_msg);

        return PEF_RET_GENERIC_ERR;
    }

    ret = pef_AllocReferences( opts, &wdata, indent, local_err_msg,
                               sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error allocating arrays of references to "
                      "per-worker data structures: %s", local_err_msg);

        return PEF_RET_GENERIC_ERR;
    }

    ret = pef_AllocSummaryStructs( opts, &wdata, true, cline_size, indent, local_err_msg,
                                   sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error allocating shared execution summary "
                      "structs: %s", local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_allocate_per_worker_structs_out;
    }

    ret = pef_AllocSummaryStructs( opts, &wdata, false, cline_size, indent,
                                   local_err_msg, sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error allocating private execution "
                      "summary structs: %s", local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_allocate_per_worker_structs_out;
    }

    ret = pef_AllocTaskStatStructs( opts, &wdata, cline_size, indent, local_err_msg,
                                    sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error allocating structs for storing "
                      "detailed execution statistics: %s", local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_allocate_per_worker_structs_out;
    }

    if ( opts->screen_stat_timer != -1 )
    {
        ret = pef_AllocLocks( opts, &wdata, cline_size, indent, local_err_msg,
                              sizeof( local_err_msg));

        if ( ret )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "Error allocating memory for locks "
                          "used to guard access to workers' shared data: %s",
                          local_err_msg);
            ret = PEF_RET_GENERIC_ERR;

            goto pef_allocate_per_worker_structs_out;
        }
    } else
    {
        PEF_VERBOSE( opts, "%sSkipped allocating memory for locks. Workers are "
                     "expected to have exclusive access to all their structures", ind);
    }

pef_allocate_per_worker_structs_out:
    if ( !ret && wdata_ret ) *wdata_ret = wdata;

    if ( ret || !wdata_ret ) pef_DeallocateWorkerStructs( opts, &wdata, indent);

    return ret;
}

/**
 * Compose array of worker arguments
 *
 * Return: array of worker arguments (should be deallocated by caller)
 */
static int pef_PrepareWorkerArgs( const pef_Opts_t* const opts,
                                  pef_GetDataArg_t *get_data_arg,
                                  pef_WorkerData_t *wdata,
                                  pef_WorkerArg_t **args_ret,
                                  std::string indent,
                                  char *err_msg,
                                  int err_msg_size)
{
    PEF_ASSERT( opts && wdata);
    PEF_VERBOSE( opts, "%sPreparing worker arguments", indent.c_str());

    const char *ind = (indent += "\t").c_str();
    int num_threads = opts->num_threads;
    pef_WorkerArg_t *worker_args = 0;
    
    worker_args = (pef_WorkerArg_t*)calloc( num_threads, sizeof( pef_WorkerArg_t));

    if ( !worker_args )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate array of worker "
                      "arguments");

        return PEF_RET_GENERIC_ERR;
    }

    PEF_VERBOSE( opts, "%sAllocated array of worker arguments", ind);

    for ( int i = 0; i < num_threads; i++ )
    {
        worker_args[i].worker_id = i;
        worker_args[i].opts = opts;
        worker_args[i].shared_summary = wdata->shared_summary[i];
        worker_args[i].private_summary = wdata->private_summary[i];
        worker_args[i].stat_tasks = wdata->stat_tasks[i];
        worker_args[i].get_data = pef_GetData;
        worker_args[i].get_data_arg = get_data_arg;
        worker_args[i].err_msg[0] = '\0';

        if ( wdata->locks ) worker_args[i].lock = wdata->locks[i];
    }

    PEF_VERBOSE( opts, "%sInitialized all the arguments", ind);

    if ( args_ret ) *args_ret = worker_args; else free( worker_args);

    return 0;
}

/**
 * Spawn worker threads
 *
 * Return: array of thread descriptors, array of booleans indicating whether the
 *         corresponding worker was created
 *
 * If the function succeeds, both arrays must be deallocated by caller
 *
 * The array of boolean indicators is needed only by "pef_JoinWithAllTheThreads()"
 * function for tracking which threads have been already joined with. But it's allocated
 * here - and not inside "pef_JoinWithAllTheThreads()" - because allocation of memory can
 * fail. Thus, before the threads are started, it's better to make sure that it will be
 * possible to track their execution status
 */
static int pef_SpawnWorkerThreads( const pef_Opts_t* const opts,
                                   pef_WorkerArg_t *worker_args,
                                   pthread_t **threads_ret,
                                   bool **is_thread_started_ret,
                                   std::string indent,
                                   char *err_msg,
                                   int err_msg_size)
{
    PEF_ASSERT( opts && worker_args && threads_ret && is_thread_started_ret);
    PEF_VERBOSE( opts, "%sSpawning worker threads", indent.c_str());

    int ret = 0;
    const char *ind = (indent += "\t").c_str();
    int64_t num_threads = opts->num_threads;
    pthread_t *threads = (pthread_t*)calloc( num_threads, sizeof( pthread_t));

    if ( !threads )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for an array of "
                      "worker thread descriptors");

        return PEF_RET_GENERIC_ERR;
    }

    PEF_VERBOSE( opts, "%sAllocated memory for worker thread descriptors", ind);

    bool *is_thread_started = (bool*)calloc( num_threads, sizeof( bool));

    if ( !is_thread_started )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for an array of "
                      "booleans indicating which workers have been created");
        free( threads);

        return PEF_RET_GENERIC_ERR;
    }

    PEF_VERBOSE( opts, "%sAllocated memory for booleans indicating which workers have "
                 "been created", ind);
    PEF_VERBOSE( opts, "%sCreating threads...", ind);

    int64_t i = 0;

    for ( i = 0; i < num_threads; i++ )
    {
        if ( pthread_create( &threads[i], NULL, pef_DoWork, &worker_args[i]) ) break;

        is_thread_started[i] = true;
    }

    if ( i < num_threads )
    {
        PEF_EMIT_ERR_MSG( "Creation of worker #%ld failed", i);
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error creating worker thread #%ld", i);
        ret = PEF_RET_GENERIC_ERR;
    } else
    {
        PEF_VERBOSE( opts, "%s\tAll worker threads started", ind);
    }

    /* Even if the thread-creation loop failed to create ALL the threads, we still need to
       join with the threads that WERE created. That's why this function returns a pointer
       to the array of thread desctiptors (and also a pointer to the array of boolean
       indicators) as long as at least one thread was created */
    if ( i > 0 )
    {
        *threads_ret = threads;
        *is_thread_started_ret = is_thread_started;
    } else
    {
        free( threads);
        free( is_thread_started);
    }

    return ret;
}

/**
 * Send cancellation request to all the worker threads that were started
 */
static void pef_CancelAllThreads( const pef_Opts_t* const opts,
                                  pthread_t *worker_threads,
                                  bool *is_worker_running,
                                  pthread_t *monitor_thread,
                                  std::string indent)
{
    PEF_ASSERT( opts && worker_threads && is_worker_running);
    PEF_VERBOSE( opts, "%sCancelling all the running threads...", indent.c_str());

    /* Number of times that sending cancellation request to a worker failed */
    int64_t failed_for_worker = 0;
    /* Whether sending cancellation request to the monitor thread failed */
    bool is_failed_for_monitor = false;
    /* The number of running workers */
    int64_t running_workers = 0;
    const char *ind = (indent += "\t").c_str();

    for ( int64_t i = 0; i < opts->num_threads; i++ )
    {
        if ( !is_worker_running[i] ) continue;

        running_workers++;

        if ( pthread_cancel( worker_threads[i]) )
        {
            failed_for_worker++;
            PEF_EMIT_ERR_MSG( "Error cancelling worker #%ld", i);
        }
    }

    if ( !running_workers && !monitor_thread )
    {
        PEF_VERBOSE( opts, "%sSeems, there is no threads to cancel", ind);

        return;
    }

    if ( monitor_thread && pthread_cancel( *monitor_thread) )
    {
        PEF_EMIT_ERR_MSG( "Error cancelling the monitor thread");
        is_failed_for_monitor = true;
    }

    if ( failed_for_worker || is_failed_for_monitor )
    {
        std::string msg = std::string( "%sSending cancellation request failed for ") +
                          (is_failed_for_monitor ? "the monitor thread" : "") +
                          (failed_for_worker && is_failed_for_monitor ? " and " : "") +
                          (failed_for_worker ? "%ld workers" : "");

        if ( failed_for_worker )
        {
            PEF_VERBOSE( opts, msg.c_str(), ind, failed_for_worker);
        } else
        {
            PEF_VERBOSE( opts, msg.c_str(), ind);
        }
    } else
    {
        PEF_ASSERT( running_workers || monitor_thread);

        std::string msg = std::string( "%sCancellation request successfully sent to ") +
                          (running_workers ? "%ld workers" : "") +
                          (running_workers && monitor_thread ? " and " : "") +
                          (monitor_thread ? "the monitor thread" : "");

        if ( running_workers )
        {
            PEF_VERBOSE( opts, msg.c_str(), ind, running_workers);
        } else
        {
            PEF_VERBOSE( opts, msg.c_str(), ind);
        }
    }

    return;
}

/**
 * Value used to describe current status of a thread
 */
typedef enum
{
    /* Returned zero */
    PEF_THREAD_SUCCEEDED,
    PEF_THREAD_RUNNING,
    PEF_THREAD_CANCELLED,
    /* Thread returned non-zero value */
    PEF_THREAD_FAILED,
    PEF_THREAD_DETACHED,
    PEF_THREAD_STATUS_COUNT
} pef_join_status_t;

/**
 * Convert numeric representation of a thread status to string
 */
static std::string pef_ConvThreadStatus2String( pef_join_status_t status)
{
    switch ( status )
    {
        case PEF_THREAD_SUCCEEDED:
            return "success";
        case PEF_THREAD_RUNNING:
            return "running";
        case PEF_THREAD_CANCELLED:
            return "cancelled";
        case PEF_THREAD_FAILED:
            return "failed";
        case PEF_THREAD_DETACHED:
            return "detached";
        default:
            return "UNKNOWN";
    }

    /* Must never be here */
    PEF_ASSERT( 0);
}

/**
 * Try to join with a thread, return the result of the attempt and emit error messages
 * when needed
 */
static pef_join_status_t pef_TryJoin( const pef_Opts_t* const opts,
                                      pthread_t thread,
                                      const char* const thread_type,
                                      int64_t thread_id,
                                      const char *thread_err_msg,
                                      /* Use reference here to avoid object copying.
                                         This function must be performance-efficient */
                                      const std::string& indent)
{
    PEF_ASSERT( opts && thread_type);

    /* Whether thread ID should be added to verbose and error prints */
    bool is_emit_id = (thread_id != -1);
    void *thread_ret = 0;
    int tryjoin_ret = pthread_tryjoin_np( thread, &thread_ret);

    /* Was the join operation successful? */
    if ( !tryjoin_ret )
    {
        if ( thread_ret )
        {
            if ( thread_ret == PTHREAD_CANCELED )
            {
                std::string fmt_str = std::string( "%sJoined with cancelled ") +
                                      thread_type + (is_emit_id ? " #%ld" : "");

                if ( is_emit_id )
                {
                    PEF_VERBOSE( opts, fmt_str.c_str(), indent.c_str(), thread_id);
                } else
                {
                    PEF_VERBOSE( opts, fmt_str.c_str(), indent.c_str());
                }

                return PEF_THREAD_CANCELLED;
            } else
            {
                std::string fmt_str = std::string( thread_type) +
                                      (is_emit_id ? " #%ld" : "" ) +
                                      " exited with error code %p and error message: %s";

                thread_err_msg = thread_err_msg ? thread_err_msg : "\0";

                if ( is_emit_id )
                {
                    PEF_EMIT_ERR_MSG( fmt_str.c_str(), thread_id, thread_ret,
                                      thread_err_msg);
                } else
                {
                    PEF_EMIT_ERR_MSG( fmt_str.c_str(), thread_ret, thread_err_msg);
                }

                return PEF_THREAD_FAILED;
            }
        }

        return PEF_THREAD_SUCCEEDED;
    }

    /* At this point it's known that "pthread_tryjoin_np()" returned non-zero */
    if ( tryjoin_ret == EBUSY )
    {
        return PEF_THREAD_RUNNING;
    } else
    {
        std::string msg_fmt = std::string( "Enexpected error when trying to join ") +
                              "with %s" + (is_emit_id ? " #%ld" : "") +
                              ". It will be detached";

        if ( is_emit_id )
        {
            PEF_EMIT_ERR_MSG( msg_fmt.c_str(), thread_type, thread_id);
        } else
        {
            PEF_EMIT_ERR_MSG( msg_fmt.c_str(), thread_type);
        }

        pthread_detach( thread);

        return PEF_THREAD_DETACHED;
    }

    /* Must never be here */
    PEF_ASSERT( 0);
}

/**
 * Structure passed as an argument to a monitor thread
 */
typedef struct
{
    /* PEF configuration */
    const pef_Opts_t *opts;
    /* Array of references to structures with summary statistics. One structure per each
       worker */
    pef_WorkerSharedSummary_t **shared_summary;
    /* Array of references to locks that guard access to data structures that are not
       private to workers. One lock per each worker */
    pthread_mutex_t **locks;
    /* A buffer to store an error message returned by the monitor (if any) */
    char err_msg[PEF_ERR_MSG_SIZE];
} pef_MonitorArg_t;

/**
 * Join with all the worker threads and the progress-tracking thread
 *
 * The function may cancel all the running threads. That may happen in two cases:
 *  1) at least one of the worker threads wasn't created or the progress-tracking thread
 *     wasn't created, while it was expected to be
 *  2) any of the threads returned an error
 *
 * The function doesn't return before it joins with all the threads that were created,
 * regardless of whether they were cancelled or not (i.e. it's not enough for the function
 * to know that a cancellation request was successfully sent to a thread. The thread still
 * must to be joined with)
 *
 * The function returns zero only if all the following conditions are met:
 *  1) all the required threads were started
 *  2) all of them returned naturally
 *  3) return value of each thread is zero
 */
static int pef_JoinWithAllTheThreads( const pef_Opts_t* const opts,
                                      pthread_t *worker_threads,
                                      pef_WorkerArg_t *worker_args,
                                      bool *is_worker_running,
                                      pthread_t *monitor_thread,
                                      pef_MonitorArg_t *monitor_arg,
                                      std::string indent,
                                      char *err_msg,
                                      int err_msg_size)
{
    PEF_ASSERT( opts && worker_threads && worker_args && is_worker_running);
    PEF_ASSERT( !monitor_thread || monitor_arg);
    PEF_VERBOSE( opts, "%sWaiting for all the started threads to complete...",
                  indent.c_str());

    const char *ind = (indent += "\t").c_str();
    /* Whether cancellation request was sent to all the running threads */
    bool is_cancelled = false;
    /* Whether the monitor thread is still running */
    bool is_monitor_running = (monitor_thread != 0);
    /* The smallest worker ID such that the worker with this ID is still running */
    int64_t first_running_worker = 0;
    /* Each element of this array stores the number of workers that finished with the
       corresponding join status (except an element that corresponds to
       PEF_THREAD_RUNNING status; this status is not final; it means that a worker hasn't
       finished yet) */
    int64_t worker_status[PEF_THREAD_STATUS_COUNT];
    pef_join_status_t monitor_status = PEF_THREAD_STATUS_COUNT;
    /* Number of worker threads that were not started */
    int64_t workers_not_started = 0;
    bool is_monitor_required = opts->screen_stat_timer != -1;

    /* Initialize "worker_status" array */
    for ( int i = 0; i < PEF_THREAD_STATUS_COUNT; i++ ) worker_status[i] = 0;

    PEF_VERBOSE( opts, "%sChecking whether all the required threads were created", ind);

    if ( !worker_threads )
    {
        /* If no workers were created, the monitor thread must not have been even
           attempted to create */
        PEF_ASSERT( !is_monitor_running);
        PEF_BUFF_MSG( err_msg, err_msg_size, "No threads to join with");

        return PEF_RET_GENERIC_ERR;
    }

    for ( int64_t i = 0; i < opts->num_threads; i++ )
    {
        if ( !is_worker_running[i] ) workers_not_started++;
    }

    if ( workers_not_started )
    {
        /* If at least one worker wasn't created, the monitor thread must not have
           been even attempted to create */
        PEF_ASSERT( !is_monitor_running);
        PEF_VERBOSE( opts, "%s\tAt least one worker thread wasn't created", ind);
    } else
    {
        if ( is_monitor_required && !is_monitor_running )
        {
            PEF_VERBOSE( opts, "%s\tOnly the monitor thread wasn't created", ind);
        } else
        {
            PEF_VERBOSE( opts, "%s\tAll the required threads WERE created", ind);
        }
    }

    if ( workers_not_started || (is_monitor_required && !is_monitor_running) )
    {
        pef_CancelAllThreads( opts, worker_threads, is_worker_running,
                              is_monitor_running ? monitor_thread : 0, indent);
        is_cancelled = true;
    }

    PEF_VERBOSE( opts, "%sJoining with the threads...", ind);

    /* If somewhere in the below loop it's decided that all the running threads must
       be cancelled, this variable is set to "true" */
    bool need_cancel = false;

    while ( first_running_worker < opts->num_threads || is_monitor_running )
    {
        int64_t i = first_running_worker;

        first_running_worker = opts->num_threads;

        for ( ; i < opts->num_threads; i++ )
        {
            if ( !is_worker_running[i] ) continue;

            pef_join_status_t join_res = pef_TryJoin( opts, worker_threads[i], "Worker",
                                                      i, worker_args[i].err_msg,
                                                      indent + "\t");

            PEF_ASSERT( join_res < PEF_THREAD_STATUS_COUNT);
            worker_status[join_res]++;

            switch ( join_res )
            {
                case PEF_THREAD_FAILED:
                case PEF_THREAD_DETACHED:
                    if ( !is_cancelled && !need_cancel ) need_cancel = true;

                    /* Fall-through */
                case PEF_THREAD_CANCELLED:
                case PEF_THREAD_SUCCEEDED:
                    is_worker_running[i] = false;

                    break;
                case PEF_THREAD_RUNNING:
                    if ( first_running_worker == opts->num_threads )
                    {
                        first_running_worker = i;
                    }

                    break;
                default:
                    PEF_ASSERT( 0);
            }
        }

        if ( is_monitor_running )
        {
            monitor_status = pef_TryJoin( opts, *monitor_thread, "Monitor thread", -1,
                                          monitor_arg->err_msg, indent + "\t");
            PEF_ASSERT( monitor_status < PEF_THREAD_STATUS_COUNT);

            switch ( monitor_status )
            {
                case PEF_THREAD_FAILED:
                case PEF_THREAD_DETACHED:
                    if ( !is_cancelled && !need_cancel ) need_cancel = true;

                    /* Fall-through */
                case PEF_THREAD_CANCELLED:
                case PEF_THREAD_SUCCEEDED:
                    is_monitor_running = false;

                    break;
                case PEF_THREAD_RUNNING:
                    break;
                default:
                    PEF_ASSERT( 0);
            }
        }

        if ( need_cancel )
        {
            pef_CancelAllThreads( opts, worker_threads, is_worker_running,
                                  is_monitor_running ? monitor_thread : 0,
                                  indent + "\t");
            is_cancelled = true;
            need_cancel = false;
        }

        sleep( PEF_PAUSE_BEFORE_JOIN_RETRY);
    }

    if ( worker_status[PEF_THREAD_SUCCEEDED] != opts->num_threads ||
         (is_monitor_required && monitor_status != PEF_THREAD_SUCCEEDED) )
    {
        PEF_ASSERT( workers_not_started + worker_status[PEF_THREAD_SUCCEEDED] +
                    worker_status[PEF_THREAD_CANCELLED] +
                    worker_status[PEF_THREAD_FAILED] +
                    worker_status[PEF_THREAD_DETACHED] == opts->num_threads);

        std::string monitor_status_str = "";

        if ( monitor_thread )
        {
            PEF_ASSERT( monitor_status < PEF_THREAD_STATUS_COUNT &&
                        monitor_status != PEF_THREAD_RUNNING);
            monitor_status_str = pef_ConvThreadStatus2String( monitor_status);
        } else
        {
            monitor_status_str = "not started";
        }

        PEF_BUFF_MSG( err_msg, err_msg_size, "Worker threads: %ld not started, "
                      "%ld succeeded, %ld cancelled, %ld failed, %ld detached. "
                      "Monitor thread: %s", workers_not_started,
                      worker_status[PEF_THREAD_SUCCEEDED],
                      worker_status[PEF_THREAD_CANCELLED],
                      worker_status[PEF_THREAD_FAILED],
                      worker_status[PEF_THREAD_DETACHED], monitor_status_str.c_str());

        return PEF_RET_GENERIC_ERR;
    } else
    {
        PEF_VERBOSE( opts, "%sAll the threads successfully returned", ind);
    }

    return 0;
}

/**
 * Collect execution summary of all the workers, emit the overall execution progress to
 * the screen
 *
 * The monitor doesn't assume any initial indentation for verbose output
 */
static void *pef_Monitor( void *arg)
{
    PEF_ASSERT( arg && ((pef_MonitorArg_t*)arg)->opts);
    pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, 0);
    pthread_setcanceltype( PTHREAD_CANCEL_DEFERRED, 0);

    pef_MonitorArg_t *monitor_arg = (pef_MonitorArg_t*)arg;
    const pef_Opts_t *opts = monitor_arg->opts;
    char *err_msg = monitor_arg->err_msg;
    int err_msg_size = sizeof( monitor_arg->err_msg);
    /* Progress of the most lagging worker (in percentage) */
    double least_progress = 101.0;
    /* Number of tasks completed by the most lagging worker */
    int64_t least_tasks_done = INT64_MAX;
    /* ID of the most lagging worker */
    int64_t outsider_id = -1;
    /* Progress of the leading worker (among those workers that still have tasks to do; in
       percentage) */
    double best_progress = -1.0;
    /* Number of tasks completed by the leading worker (among those workers that still
       have tasks to do) */
    int64_t best_tasks_done = 0;
    /* ID of the leading worker */
    int64_t leader_id = -1;
    /* Number of workers that have completed all their tasks */
    int64_t num_finished = 0;
    int64_t tasks_overall = opts->num_files;
    int64_t num_threads = opts->num_threads;
    /* Base number of tasks assigned to a worker. It's assumed in this function that each
       worker is assigned at least this number of tasks. And possibly some workers must do
       one task more. Also it's assumed here that it is workers with lower IDs who must do
       the "extra" work (if any). Maybe it's better to avoid these assumptions by
       explicitly storing somewhere the number of tasks assigned to each worker */
    int64_t base_tasks_num = tasks_overall / num_threads;

    PEF_VERBOSE( opts, "The monitor has started");

    if ( opts->screen_stat_timer == -1 )
    {
        PEF_VERBOSE( opts, "\tThe monitoring was not requested according to the "
                     "configuration options. The monitor is exiting");

        return 0;
    }

    while ( num_finished < num_threads )
    {
        num_finished = 0;
        least_progress = 101.0;
        best_progress = -1.0;

        for ( int64_t i = 0; i < num_threads; i++ )
        {
            if ( pthread_mutex_lock( monitor_arg->locks[i]) )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't acquire lock to read "
                              "execution summary of worker %ld", i);

                return (void*)PEF_RET_GENERIC_ERR;
            }

            int64_t tasks_completed = monitor_arg->shared_summary[i]->tasks_completed;

            if ( pthread_mutex_unlock( monitor_arg->locks[i]) )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't release lock used to "
                              "read execution summary of worker %ld", i);

                return (void*)PEF_RET_GENERIC_ERR;
            }

            int64_t tasks_num = base_tasks_num;

            if ( i < (tasks_overall % num_threads) ) tasks_num++;

            if ( tasks_completed == tasks_num )
            {
                num_finished++;

                continue;
            }

            PEF_ASSERT( tasks_num);

            double progress = ((double)tasks_completed / (double)tasks_num) * 100;

            if ( progress > best_progress )
            {
                best_progress = progress;
                leader_id = i;
                best_tasks_done = tasks_completed;
            }

            if ( progress < least_progress )
            {
                least_progress = progress;
                outsider_id = i;
                least_tasks_done = tasks_completed;
            }
        }

        PEF_ASSERT( num_finished == num_threads ||
                    (leader_id != -1 && outsider_id != -1));

        std::string out_msg = std::string( num_finished ? "%ld workers finished. " : "") +
                              (num_finished == num_threads ? "" : "Outsider "
                               "worker: %ld (%0.0f%% %ld/%ld tasks). Leading worker: %ld "
                              "(%0.0f%% %ld/%ld tasks).");

        if ( num_finished < num_threads )
        {
            int64_t tasks_per_leader = leader_id < tasks_overall % num_threads ?
                                       base_tasks_num + 1 : base_tasks_num;
            int64_t tasks_per_outsider = outsider_id < tasks_overall % num_threads ?
                                         base_tasks_num + 1 : base_tasks_num;

            if ( num_finished )
            {
                PEF_OUT( out_msg.c_str(), num_finished, outsider_id, least_progress,
                         least_tasks_done, tasks_per_outsider, leader_id, best_progress,
                         best_tasks_done, tasks_per_leader);
            } else
            {
                PEF_OUT( out_msg.c_str(), outsider_id, least_progress, least_tasks_done,
                         tasks_per_outsider, leader_id, best_progress, best_tasks_done,
                         tasks_per_leader);
            }
        } else
        {
            PEF_OUT( out_msg.c_str(), num_finished);
        }

        /* An artificial cancellation point. Added, because there is no natural
           guaranteed cancellation points inside the loop (in fact, there is one - fprintf
           hidden inside PEF_OUT macro. But it's not guaranteed to be a cancellation
           point. Though in practice it IS a cancellation point) */
        pthread_testcancel();
        sleep( opts->screen_stat_timer);
    }

    PEF_VERBOSE( opts, "\tMonitor is exiting. All the workers completed all their "
                 "tasks");

    return 0;
}

/**
 * Safe version of "strlen()"
 *
 * The function is safe as long as "buff_size" is not bigger than actual size of "buff"
 */
static int64_t inline pef_Strlen( const char *buff,
                                  int64_t buff_size)
{
    int64_t len = 0;

    for ( int64_t i = 0; i < buff_size; i++ )
    {
        if ( buff[i] == '\0' ) break;

        len++;
    }

    return len;
}

/**
 * Emit a null-terminated string to file
 */
static int pef_EmitStringToFile( int fd,
                                 const char *buff,
                                 int64_t buff_size,
                                 char *err_msg,
                                 int err_msg_size)
{
    PEF_ASSERT( buff);

    char local_err_msg[PEF_ERR_MSG_SIZE];
    int64_t str_size = pef_Strlen( buff, buff_size);
    int64_t bytes_written = write( fd, buff, str_size);

    if ( bytes_written == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Write request failed: %s",
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));

        return PEF_RET_GENERIC_ERR;
    }

    if ( bytes_written < str_size )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't write a complete string (was "
                      "trying to write %ld bytes, but only %ld bytes were written)",
                      str_size, bytes_written);

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

/**
 * Size of a buffer used to write execution report files. The buffer must be big enough
 * to hold any line of any of the files
 */
#define PEF_REPORT_BUFF_SIZE 500

/**
 * Emit a line to the execution summary file
 */
static bool pef_EmitSummaryLine( int fd,
                                 const char *prop_name,
                                 const char *val,
                                 char *err_msg,
                                 int err_msg_size)
{
    char local_err_msg[PEF_ERR_MSG_SIZE];
    char buff[PEF_REPORT_BUFF_SIZE];

    PEF_ASSERT( prop_name && val);
    snprintf( buff, sizeof( buff), "%s: %s\n", prop_name, val);

    int ret = pef_EmitStringToFile( fd, buff, sizeof( buff), local_err_msg,
                                    sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting property %s: %s", prop_name,
                      local_err_msg);

        return false;
    }

    return true;
}

/**
 * Emit int64 property to the execution summary file
 */
static bool pef_EmitSummaryInt64Prop( bool is_ok,
                                      int fd,
                                      const char *prop_name,
                                      int64_t val,
                                      char *err_msg,
                                      int err_msg_size)
{
    if ( !is_ok ) return is_ok;

    char buff[PEF_REPORT_BUFF_SIZE];

    PEF_ASSERT( prop_name);
    snprintf( buff, sizeof( buff), "%ld", val);

    return pef_EmitSummaryLine( fd, prop_name, buff, err_msg, err_msg_size);
}

/**
 * Emit uint64 property to the execution summary file
 */
static bool pef_EmitSummaryUInt64Prop( bool is_ok,
                                       int fd,
                                       const char *prop_name,
                                       uint64_t val,
                                       char *err_msg,
                                       int err_msg_size)
{
    if ( !is_ok ) return is_ok;

    char buff[PEF_REPORT_BUFF_SIZE];

    PEF_ASSERT( prop_name);
    snprintf( buff, sizeof( buff), "%lu", val);

    return pef_EmitSummaryLine( fd, prop_name, buff, err_msg, err_msg_size);
}

/**
 * Emit string property to the execution summary file
 */
static bool pef_EmitSummaryStrProp( bool is_ok,
                                    int fd,
                                    const char *prop_name,
                                    const char *val,
                                    char *err_msg,
                                    int err_msg_size)
{
    if ( !is_ok ) return is_ok;

    PEF_ASSERT( prop_name && val);

    return pef_EmitSummaryLine( fd, prop_name, val, err_msg, err_msg_size);
}

/**
 * Emit execution summary and configurations parameters to the designated statistics
 * directory
 */
static int pef_EmitSummary( const pef_Opts_t* const opts,
                            pef_GlobalStat_t *global_stat,
                            std::string indent,
                            char *err_msg,
                            int err_msg_size)
{
    PEF_ASSERT( opts && global_stat);
    PEF_VERBOSE( opts, "%sEmitting execution summary and configuration properties",
                 indent.c_str());

    char local_err_msg[PEF_ERR_MSG_SIZE];
    int ret = 0;
    const char *sync_err_msg = "Couldn't sync execution summary file \"%s\" to the "
                               "backing store: %s";
    std::string summary_file_name = opts->stat_output_path + "/" + opts->file_base_name +
                                    "stat.summary";
    int fd = open( summary_file_name.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                   PEF_STAT_FILE_PERMISSIONS);

    if ( fd == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't create execution summary file "
                      "\"%s\": %s", summary_file_name.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));

        return PEF_RET_GENERIC_ERR;
    }

    bool is_ok = true;

    is_ok = pef_EmitSummaryInt64Prop( is_ok, fd, "Number of workers", opts->num_threads,
                                      local_err_msg, sizeof( local_err_msg));
    is_ok = pef_EmitSummaryInt64Prop( is_ok, fd, "Tasks", opts->num_files,
                                      local_err_msg, sizeof( local_err_msg));
    is_ok = pef_EmitSummaryInt64Prop( is_ok, fd, "Size of data file", opts->file_size,
                                      local_err_msg, sizeof( local_err_msg));
    is_ok = pef_EmitSummaryInt64Prop( is_ok, fd, "Size of write chunk",
                                      opts->write_chunk_size, local_err_msg,
                                      sizeof( local_err_msg));
    is_ok = pef_EmitSummaryInt64Prop( is_ok, fd, "Files per directory",
                                      opts->num_files_per_dir, local_err_msg,
                                      sizeof( local_err_msg));
    is_ok = pef_EmitSummaryUInt64Prop( is_ok, fd, "TSC ticks per second",
                                       opts->tsc_ticks_per_sec, local_err_msg,
                                       sizeof( local_err_msg));

    if ( is_ok )
    {
        char buff[PEF_TIME_BUFF_SIZE];

        PEF_TIME2STR( buff, sizeof( buff), global_stat->benchmark_start_time);
        is_ok = pef_EmitSummaryStrProp( is_ok, fd, "Workload start time", buff,
                                        local_err_msg, sizeof( local_err_msg));
    }

    is_ok = pef_EmitSummaryStrProp( is_ok, fd, "Errors during benchmarking",
                                    global_stat->is_errors ? "yes" : "no", local_err_msg,
                                    sizeof( local_err_msg));

    if ( !is_ok )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting a property: %s",
                      local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_summary_out;
    }

    /* Flush the file to the backing store before emitting "report completeness"
       statement. That's done to ensure that "report completeness" statement doesn't get
       to the backing store before all the other contents gets */
    if ( fsync( fd) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, sync_err_msg, summary_file_name.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_summary_out;
    }

    /* Emit "report completeness" statement */
    is_ok = pef_EmitSummaryStrProp( true, fd, "Report is complete", "yes", local_err_msg,
                                    sizeof( local_err_msg));

    if ( !is_ok )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting \"report completeness\" "
                      "statement: %s", local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_summary_out;
    }

    /* Finalize the file */
    if ( fsync( fd) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, sync_err_msg, summary_file_name.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_summary_out;
    }

pef_emit_summary_out:
    /* Don't care of errors returned by "close()" (if any) */
    close( fd);

    return ret;
}

/**
 * Emit header of a table in which task-granular execution statistics will be stored
 */
static int pef_EmitDetailsHeader( int fd,
                                  char *err_msg,
                                  int err_msg_size)
{
    char buff[PEF_REPORT_BUFF_SIZE];

    snprintf( buff, sizeof( buff), "%s,%s,%s,%s,%s\n", "Worker ID", "Task number",
              "Task completion time (absolute)", "Task completion time (from bencmark "
              "start)", "Task completion time in ns (from benchmark start)");

    return pef_EmitStringToFile( fd, buff, sizeof( buff), err_msg, err_msg_size);
}

/**
 * Emit execution statistics of a single task to the designated file
 */
static int pef_EmitTaskStat( const pef_Opts_t* const opts,
                             int fd,
                             pef_GlobalStat_t *global_stat,
                             int64_t worker_id,
                             int64_t task_num,
                             pef_WorkerTaskStat_t *task_stat,
                             char *err_msg,
                             int err_msg_size)
{
    PEF_ASSERT( opts && global_stat && task_stat);

    char abs_time_buff[PEF_TIME_BUFF_SIZE];
    char time_delta_buff[PEF_TIME_BUFF_SIZE];
    char out_buff[PEF_REPORT_BUFF_SIZE];
    pef_time_t task_end_time = task_stat->completion_time;
    pef_time_t time_delta = PEF_TIME_ZERO;
    uint64_t nsec = 0;

    PEF_TIME2STR( abs_time_buff, sizeof( abs_time_buff), task_end_time);
    PEF_ASSERT( PEF_TIME_LESS( global_stat->benchmark_start_time, task_end_time));
    PEF_TIME_SUB( time_delta, task_end_time, global_stat->benchmark_start_time);
    PEF_TIME2STR( time_delta_buff, sizeof( time_delta_buff), time_delta);
    PEF_TIME2NSEC( nsec, time_delta, opts->tsc_ticks_per_sec);
    snprintf( out_buff, sizeof( out_buff), "%ld,%ld,%s,%s,%lu\n", worker_id, task_num,
              abs_time_buff, time_delta_buff, nsec);

    return pef_EmitStringToFile( fd, out_buff, sizeof( out_buff), err_msg, err_msg_size);
}

/**
 * Emit detailed task-granular execution statistics to the designated statistics directory
 */
static int pef_EmitDetails( const pef_Opts_t* const opts,
                            pef_GlobalStat_t *global_stat,
                            pef_WorkerPrivateSummary_t **private_summary,
                            pef_WorkerTaskStat_t **stat_tasks,
                            std::string indent,
                            char *err_msg,
                            int err_msg_size)
{
    PEF_ASSERT( opts && private_summary && stat_tasks);
    PEF_VERBOSE( opts, "%sEmitting task-granular execution statistics", indent.c_str());

    int ret = 0;
    char local_err_msg[PEF_ERR_MSG_SIZE];
    /* Completion time of the last emitted task */
    pef_time_t prev_end_time = PEF_TIME_ZERO;
    /* Total number of tasks completed by all workers */
    int64_t tasks_total = 0;
    /* Indexes into workers' task statistics arrays. One index per worker */
    int64_t *indexes = (int64_t*)calloc( opts->num_threads, sizeof( int64_t));

    if ( !indexes )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory for indexes into "
                      "workers' task statistics arrays");

        return PEF_RET_GENERIC_ERR;
    }

    std::string details_file_name = opts->stat_output_path + "/" + opts->file_base_name +
                                    "stat.details";
    int fd = open( details_file_name.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                   PEF_STAT_FILE_PERMISSIONS);

    if ( fd == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't create file \"%s\" for storing "
                      "the statistics: %s", details_file_name.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_details_out;
    }

    if ( pef_EmitDetailsHeader( fd, local_err_msg, sizeof( local_err_msg)) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting the header of the table: "
                      "%s", local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_details_out;
    }

    for ( int64_t i = 0; i < opts->num_threads; i++ )
    {
        tasks_total += private_summary[i]->tasks_completed;
    }

    PEF_ASSERT( tasks_total <= opts->num_files);

    for ( int64_t emitted = 0; emitted < tasks_total; emitted++ )
    {
        pef_time_t min_time = PEF_TIME_ZERO;
        /* ID of a worker whose task is to be emitted */
        int64_t worker_id = -1;

        for ( int64_t i = 0; i < opts->num_threads; i++ )
        {
            int64_t tasks_num = private_summary[i]->tasks_completed;

            if ( indexes[i] >= tasks_num ) continue;

            pef_WorkerTaskStat_t *stat = stat_tasks[i];
            pef_time_t task_end_time = stat[indexes[i]].completion_time;

            if ( PEF_TIME_LESS( task_end_time, prev_end_time) )
            {
                PEF_ASSERT( indexes[i] > 0);
                PEF_ASSERT( PEF_TIME_LESS( task_end_time,
                                           stat[indexes[i] - 1].completion_time));
                PEF_BUFF_MSG( err_msg, err_msg_size, "Task %ld done by worker %ld has "
                              "completion time smaller than that of the previous task "
                              "done by this worker (%ld < %ld)", indexes[i], i,
                              task_end_time, stat[indexes[i] - 1].completion_time);
                ret = PEF_RET_GENERIC_ERR;

                goto pef_emit_details_out;
            }

            if ( worker_id == -1 || PEF_TIME_LESS( task_end_time, min_time) )
            {
                min_time = task_end_time;
                worker_id = i;
            }
        }

        /* Exactly "tasks_total" tasks must be emitted. The loop must exit naturally */
        PEF_ASSERT( worker_id != -1);
        ret = pef_EmitTaskStat( opts, fd, global_stat, worker_id, indexes[worker_id],
                                &stat_tasks[worker_id][indexes[worker_id]],
                                local_err_msg, sizeof( local_err_msg));

        if ( ret )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting task %ld of worker "
                          "%ld: %s", indexes[worker_id], worker_id, local_err_msg);
            ret = PEF_RET_GENERIC_ERR;

            goto pef_emit_details_out;
        }

        PEF_ASSERT( indexes[worker_id] < INT64_MAX);
        indexes[worker_id]++;
    }

    /* Finalize the file */
    if ( fsync( fd) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't sync file with task-granular "
                      "execution statistics \"%s\" to the backing store: %s",
                      details_file_name.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_emit_details_out;
    }

pef_emit_details_out:
    /* Don't care of "close()" return value */
    close( fd);

    if ( indexes ) free( indexes);

    return ret;
}

/**
 * Emit execution report to the designated statistics directory
 */
static int pef_EmitExecutionReport( const pef_Opts_t* const opts,
                                    pef_GlobalStat_t *global_stat,
                                    pef_WorkerPrivateSummary_t **private_summary,
                                    pef_WorkerTaskStat_t **stat_tasks,
                                    std::string indent,
                                    char *err_msg,
                                    int err_msg_size)
{
    PEF_ASSERT( opts && global_stat && private_summary && stat_tasks);
    PEF_VERBOSE( opts, "%sEmitting execution report to the designated statistics "
                 "directory", indent.c_str());

    char local_err_msg[PEF_ERR_MSG_SIZE];
    int ret = 0;

    /* Detailed execution statistics is emitted first, execution summary and configuration
       parameters are emitted last. This ordering is significant. That's because the last
       line of a summary file is expected to hold a report completeness statement. If this
       statement is present, that means that the execution report was emitted fully. If
       this statement is absent - or even the summary file itself is absent - that means
       that problems occured while emitting the report or even earlier. In this case the
       report files alone (if any) are not sufficient to acquire an understanding of how
       successful the corresponding PEF run was and whether all collected task-granular
       execution statistics is on disk. Of course, all errors occuring during PEF
       execution must be reported by the tool before it exits. But this is a runtime
       machanism. "Report completeness" feature instead allows learning - at least
       partially - PEF error status from on-disk artifacts. For example, assume that
       artifacts from several PEF runs were collected in a single directory. Thanks to
       "report completeness" feature, inspecting just the artifacts alone allows to
       understand which runs have been successful (and therefore could be analyzed
       further) and which have failed */
    ret = pef_EmitDetails( opts, global_stat, private_summary, stat_tasks, indent + "\t",
                           local_err_msg, sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting detailed task-granular "
                      "execution statistics: %s", local_err_msg);

        return PEF_RET_GENERIC_ERR;
    }

    ret = pef_EmitSummary( opts, global_stat, indent + "\t", local_err_msg,
                           sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Error emitting execution summary and "
                      "configuration parameters: %s", local_err_msg);

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

int main( int argc, char *argv[])
{
    pef_Opts_t opts;
    pef_GlobalStat_t global_stat = {.benchmark_start_time = PEF_TIME_ZERO,
                                    .is_errors = false};
    pef_WorkerData_t worker_data = {.shared_summary = 0, .private_summary = 0,
                                    .stat_tasks = 0, .locks = 0};
    /* Using old-style initializers here, because the new initializers cannot be used
       with char arrays */
    pef_MonitorArg_t monitor_arg = {0, 0, 0, "\0"};
    pthread_t *worker_threads = 0, monitor_thread = 0;
    /* Pointer to the monitor thread descriptor. If the monitor thread is not created,
       this pointer will stay zero */
    pthread_t *monitor_thread_p = 0;
    /* Array of booleans showing what worker threads still occupy system resources */
    bool *is_worker_running = 0;
    pef_WorkerArg_t *worker_args = 0;
    pef_GetDataArg_t get_data_arg = {.data_buf = 0};
    char err_msg[PEF_ERR_MSG_SIZE];
    int ret = 0;

    pef_InitOpts( &opts);
    pef_ParseCmdLine( argc, argv, &opts);

    if ( ret = pef_CheckOptsAndFillFurther( &opts, "", err_msg, sizeof( err_msg)), ret )
    {
        PEF_EMIT_ERR_MSG( "Error while checking command-line arguments and obtaining/"
                          "calculating further configuration properties: %s", err_msg);

        goto pef_main_out;
    }

    ret = pef_GenerateInitialData( &opts, &get_data_arg.data_buf, "", err_msg,
                                   sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error generating initial data: %s", err_msg);

        goto pef_main_out;
    }

    ret = pef_AllocateWorkerStructs( &opts, &worker_data, "", err_msg,
                                     sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error allocating data structures that exist per worker: "
                          "%s", err_msg);

        goto pef_main_out;
    }

    ret = pef_PrepareWorkerArgs( &opts, &get_data_arg, &worker_data, &worker_args, "",
                                 err_msg, sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error preparing arguments for worker threads: %s", err_msg);

        goto pef_main_out;
    }

    PEF_GETTIME( global_stat.benchmark_start_time);
    ret = pef_SpawnWorkerThreads( &opts, worker_args, &worker_threads,
                                  &is_worker_running,"", err_msg, sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error spawning worker threads: %s", err_msg);

        /* Are there any threads to join with? */
        if ( !worker_threads ) goto pef_main_out;
    } else if ( opts.screen_stat_timer != -1 )
    {
        monitor_arg.opts = &opts;
        monitor_arg.shared_summary = worker_data.shared_summary;
        monitor_arg.locks = worker_data.locks;

        /* Create the monitor thread. The thread is created only if all the worker threads
           were successfully created */
        if ( pthread_create( &monitor_thread, NULL, pef_Monitor, &monitor_arg) )
        {
            PEF_EMIT_ERR_MSG( "Couldn't create the monitor thread");
        } else
        {
            monitor_thread_p = &monitor_thread;
        }
    }

    ret = pef_JoinWithAllTheThreads( &opts, worker_threads, worker_args,
                                     is_worker_running, monitor_thread_p,
                                     &monitor_arg, "", err_msg, sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error waiting for the created threads to complete: %s",
                          err_msg);
    }

    /* Emit execution report to the designated statistics directory. The report is emitted
       as long as at least one worker was created and regardless of errors that might
       have happened afterwards */
    global_stat.is_errors = ret != 0;
    ret = pef_EmitExecutionReport( &opts, &global_stat, worker_data.private_summary,
                                   worker_data.stat_tasks, "", err_msg,
                                   sizeof( err_msg));

    if ( ret )
    {
        PEF_EMIT_ERR_MSG( "Error emitting execution report: %s", err_msg);

        goto pef_main_out;
    }

pef_main_out:
    if ( worker_threads ) free( worker_threads);

    if ( is_worker_running ) free( is_worker_running);

    if ( worker_args ) free( worker_args);

    pef_DeallocateWorkerStructs( &opts, &worker_data, "");

    if ( get_data_arg.data_buf ) free( get_data_arg.data_buf);

    pef_DeallocateOpts( &opts, "");

    if ( !ret && !global_stat.is_errors ) exit( EXIT_SUCCESS); else exit( EXIT_FAILURE);
}
