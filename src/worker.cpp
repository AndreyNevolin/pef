/**
 * Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
 * 
 * Reference implementation of worker thread. If you want workers to do a different job
 * just change the worker thread implementation according to your needs
 *
 * NOTE: worker implemented here grabs the following process-wide resources:
 *          - heap memory
 *          - regular file descriptor
 *          - one or two directory descriptors
 *          - lock
 *       these resources are NOT released properly if a thread is cancelled. Proper
 *       deallocation of resources during/after thread cancellation is tricky. It's not
 *       implemented for now in a hope that PEF will be executed as a standalone
 *       application and will exit immediately after the worker thread(s) is cancelled
 *       (should the cancellation occur).
 */

/* PEF includes */
#include "worker.h"
#include "common.h"
#include "time.h"

/* Linux, POSIX, GNU includes */
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

#define MIN( _a, _b) (_a) < (_b) ? (_a) : (_b)

/**
 * Allocate memory to keep data directory name and data file name
 *
 * The allocated memory is initialized by "zeros" so that the buffers could be safely used
 * in calls to "strlen()" and other routines
 *
 * The allocated memory must be deallocated after use by a caller
 */
static int pef_AllocMemForFileNameAndPath( const pef_Opts_t* const opts,
                                           int path_size,
                                           char **path_ret,
                                           int file_name_size,
                                           char **file_name_ret,
                                           char *err_msg,
                                           int err_msg_size)
{
    PEF_ASSERT( opts);
    PEF_ASSERT( path_size > 0 && file_name_size > 0);

    char *path = 0, *file_name = 0;

    /* Allocate space to keep path to a data file */
    path = (char*)calloc( sizeof( char), path_size);

    if ( !path )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory to keep path to "
                      "a data file");

        return PEF_RET_GENERIC_ERR;
    }

    /* Allocate space to keep data file name */
    file_name = (char *)calloc( sizeof( char), file_name_size);

    if ( !file_name )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't allocate memory to keep a data "
                      "file name");
        free( path);

        return PEF_RET_GENERIC_ERR;
    }

    /* Return variables are modified only at this point to make sure that if errors occur
       inside the function then the return variables stay unchanged */
    if ( path_ret ) *path_ret = path; else free( path);

    if ( file_name_ret ) *file_name_ret = file_name; else free( file_name);

    return 0;
}

/**
 * Create a directory for data files
 */
static int pef_CreateDataDir( const pef_Opts_t* const opts,
                              int64_t worker_id,
                              char *name_buf,
                              int buf_size,
                              int64_t dir_num,
                              char *err_msg,
                              int err_msg_size)
{
    PEF_ASSERT( opts && name_buf);

    int buf_offset = opts->data_output_path.length();
    int chars_to_write = snprintf( name_buf + buf_offset, buf_size - buf_offset,
                                   "/%sworker%0*ld_%0*ld", opts->file_base_name,
                                   opts->num_digits_threads, worker_id,
                                   opts->num_digits_dirs, dir_num);

    if ( chars_to_write < 0 || chars_to_write >= buf_size - buf_offset )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't compose data directory name");

        return PEF_RET_GENERIC_ERR;
    }

    char local_err_msg[PEF_ERR_MSG_SIZE];

    if ( mkdir( name_buf, PEF_DATA_DIR_PERMISSIONS) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't create data directory \"%s\": %s",
                      name_buf, PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));

        return PEF_RET_GENERIC_ERR;
    }

    return 0;
}

/**
 * Write a data file
 */
static int pef_WriteDataFile( pef_WorkerArg_t *worker_arg,
                              char *name_buf,
                              int buf_size,
                              int64_t file_num,
                              char *dir_name,
                              char *err_msg,
                              int err_msg_size)
{
    PEF_ASSERT( worker_arg && name_buf);
    PEF_ASSERT( worker_arg->get_data && worker_arg->get_data_arg);
    PEF_ASSERT( dir_name);

    const pef_Opts_t* const opts = worker_arg->opts;
    int64_t worker_id = worker_arg->worker_id;
    int ret = 0;

    /* compose file name */
    int chars_to_write = snprintf( name_buf, buf_size, "%sworker%0*ld__%0*ld.data",
                                   opts->file_base_name,
                                   opts->num_digits_threads,
                                   worker_id,
                                   opts->num_digits_files,
                                   file_num);

    if ( chars_to_write < 0 || chars_to_write >= buf_size )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't compose data file name");

        return PEF_RET_GENERIC_ERR;
    }

    /* compose full path */
    std::string full_path = std::string( dir_name) + "/" + name_buf;
    char local_err_msg[PEF_ERR_MSG_SIZE];
    /* create the file */
    int fd = open( full_path.c_str(), O_CREAT | O_EXCL | O_WRONLY,
                   PEF_DATA_FILE_PERMISSIONS);

    if ( fd == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't create data file \"%s\": %s",
                      full_path.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));

        return PEF_RET_GENERIC_ERR;
    }

    /* write data to the file */
    void *get_data_arg = worker_arg->get_data_arg;
    int64_t bytes_remaining = opts->file_size;

    while ( bytes_remaining > 0 )
    {
        int64_t bytes_to_write = MIN( bytes_remaining, opts->write_chunk_size);
        char *data_buf = 0;

        ret = worker_arg->get_data( get_data_arg, opts, bytes_to_write, &data_buf,
                                    local_err_msg, sizeof( local_err_msg));

        if ( ret )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "Error getting data: %s",
                          local_err_msg);
            ret = PEF_RET_GENERIC_ERR;

            goto pef_write_data_file_out;
        }

        int64_t bytes_written = write( fd, data_buf, bytes_to_write);

        if ( bytes_written == -1 )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "Error writing to data file \"%s\": %s",
                          full_path.c_str(),
                          PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
            ret = PEF_RET_GENERIC_ERR;

            goto pef_write_data_file_out;
        }

        if ( bytes_written < bytes_to_write )
        {
            PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't write the whole data chunk "
                          "at once to file \"%s\" (was trying to write %ld bytes, but "
                          "only %ld bytes were written)", full_path.c_str(),
                          bytes_to_write, bytes_written);
            ret = PEF_RET_GENERIC_ERR;

            goto pef_write_data_file_out;
        }

        PEF_ASSERT( bytes_written == bytes_to_write);
        bytes_remaining -= bytes_written;
    }

    /* finalize the file */
    if ( fsync( fd) )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Couldn't sync data file \"%s\" to the "
                      "backing store: %s", full_path.c_str(),
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_write_data_file_out;
    }

pef_write_data_file_out:
    /* don't care of "close()" return value */
    close( fd);

    return ret;
}

/**
 * Worker thread
 *
 * Workers are allowed to produce verbose output only if the code was built in DEBUG mode
 * (because the output is likely to affect execution metrics).
 * Workers should assume no initial indentation for their verbose output
 */
void *pef_DoWork( void *arg)
{
    PEF_ASSERT( arg && ((pef_WorkerArg_t*)arg)->opts);

    pef_WorkerArg_t *worker_arg = (pef_WorkerArg_t*)arg;
    const pef_Opts_t *opts = worker_arg->opts;
    int64_t worker_id = worker_arg->worker_id;
    int path2file_sz = -1, file_name_sz = -1;
    char *path2file = 0, *file_name = 0;
    int ret = 0;
    int64_t files_to_write = -1;
    char *err_msg = worker_arg->err_msg;
    int err_msg_size = sizeof( worker_arg->err_msg);
    char local_err_msg[PEF_ERR_MSG_SIZE] = "";
    int64_t files_per_dir = -1;
    /* Number of data directories to store files to */
    int64_t output_dirs_num = -1;
    /* Number of already written data files */
    int64_t file_num = 0;
    /* Directory descriptors */
    int outermost_dd = -1, inner_dd = -1;
    const char *cant_open_dir_err = "Worker %d: Couldn't open data directory \"%s\": %s";
    /* Time when the most recent task was completed */
    pef_time_t current_time = PEF_TIME_ZERO;
    /* Time when the previous task was completed */
    pef_time_t prev_time = PEF_TIME_ZERO;
    /* Time when execution summary was last updated */
    pef_time_t prev_summary_update = PEF_TIME_ZERO;

    pthread_setcancelstate( PTHREAD_CANCEL_ENABLE, 0);
    pthread_setcanceltype( PTHREAD_CANCEL_DEFERRED, 0);
    PEF_GETTIME( prev_summary_update);
    file_name_sz = strlen( opts->file_base_name) + strlen( "worker") +
                   opts->num_digits_threads + strlen( "__") +
                   opts->num_digits_files + strlen( ".data") + 1;
    path2file_sz = opts->data_output_path.length() + 1;

    if ( opts->num_files_per_dir != -1 )
    {
        path2file_sz += strlen( "/") + strlen( opts->file_base_name) + strlen( "worker") +
                        opts->num_digits_threads + strlen( "_") + opts->num_digits_dirs;
    }

    ret = pef_AllocMemForFileNameAndPath( opts, path2file_sz, &path2file, file_name_sz,
                                          &file_name, local_err_msg,
                                          sizeof( local_err_msg));

    if ( ret )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn'd allocate memory to "
                      "store data file name and path to a data file: %s", worker_id,
                      local_err_msg);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_do_work_out;
    }

    /* Path to data files starts with this obligatory prefix */
    ret = snprintf( path2file, path2file_sz, "%s", opts->data_output_path.c_str());

    if ( ret < 0 || ret >= path2file_sz )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn't copy data output "
                      "path", worker_id);
        ret = PEF_RET_GENERIC_ERR;

        goto pef_do_work_out;
    }

    /* Get file descriptor of the data output directory. It's needed to flush this
       directory to the backing store each time a new object is created inside it */
    if ( (outermost_dd = open( path2file, O_DIRECTORY | O_RDONLY)) == -1 )
    {
        PEF_BUFF_MSG( err_msg, err_msg_size, cant_open_dir_err, worker_id, path2file,
                      PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
        ret = PEF_RET_GENERIC_ERR;

        goto pef_do_work_out;
    }

    /* Calculate number of files to write */
    files_to_write = opts->num_files / opts->num_threads;

    /* NOTE: need to be careful here. Function "pef_AllocateTaskStatStructs()" that
             allocates memory for workers' per-task statistics assumes that "extra" work
             (if any) will be done by workers with lower IDs. We need to satisfy this
             assumption here.
             Safer approaches are:
                1) allocate the same amount of memory for all workers as though all of
                   them do "extra" work
                2) transfer the amount of tasks to do to a worker explicitly (through the
                   worker's argument) */
    if ( worker_id < (opts->num_files % opts->num_threads) ) files_to_write++;

    /* Calculate the number of output directories and the number of files per directory */
    if ( opts->num_files_per_dir == -1 )
    {
        output_dirs_num = 1;
        files_per_dir = files_to_write;
    } else
    {
        PEF_ASSERT( opts->num_files_per_dir > 0);
        output_dirs_num = files_to_write / opts->num_files_per_dir;

        if ( files_to_write % opts->num_files_per_dir ) output_dirs_num++;

        files_per_dir = opts->num_files_per_dir;
    }

    /* Main worker loop. Create directories and fill them with files */
    for ( int64_t i = 0; i < output_dirs_num; i++ )
    {
        if ( opts->num_files_per_dir != -1 )
        {
            /* Create new directory. NOTE: "path2file" is updated as a result of the below
               call; the name of the new directory is appended to the data output path */
            ret = pef_CreateDataDir( opts, worker_id, path2file, path2file_sz, i,
                                     local_err_msg, sizeof( local_err_msg));

            if ( ret )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Error creating a data "
                              "directory: %s", worker_id, local_err_msg);
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }

            /* Flush parent directory to the backing store */
            if ( fsync( outermost_dd) )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn't sync the "
                              "outermost data directory to the backing store: %s",
                              worker_id,
                              PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }

            /* Release descriptor of the previous directory. Don't care of "close()"
               return value */
            if ( i > 0 ) close( inner_dd);

            /* Get new directory descriptor */
            if ( (inner_dd = open( path2file, O_DIRECTORY | O_RDONLY)) == -1 )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, cant_open_dir_err, worker_id,
                              path2file,
                              PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }
        }

        for ( int64_t j = 0; j < files_per_dir && file_num < files_to_write; j++ )
        {
            /* Write the new data file */
            ret = pef_WriteDataFile( worker_arg, file_name, file_name_sz, file_num,
                                     path2file, local_err_msg, sizeof( local_err_msg));

            if ( ret )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Error writing a data "
                              "file: %s", worker_id, local_err_msg);
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }

            /* Ensure that the directory reference to the new file is committed to the
               backing store */
            if ( fsync( inner_dd) )
            {
                PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn't sync data "
                              "directory \"%s\" to the backing store: %s", worker_id,
                              path2file,
                              PEF_STRERROR_R( local_err_msg, sizeof( local_err_msg)));
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }

            PEF_GETTIME( current_time);

            if ( PEF_TIME_LESS( current_time, prev_time) )
            {
                char buff1[PEF_TIME_BUFF_SIZE], buff2[PEF_TIME_BUFF_SIZE];

                PEF_TIME2STR( buff1, sizeof( buff1), current_time);
                PEF_TIME2STR( buff2, sizeof( buff2), prev_time);
                PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: time when own task "
                              "%ld was completed is smaller than time when the previous "
                              "own task was completed (%s < %s)", worker_id, file_num,
                              buff1, buff2);
                ret = PEF_RET_GENERIC_ERR;

                goto pef_do_work_out;
            }

            prev_time = current_time;
            worker_arg->stat_tasks[file_num].completion_time = current_time;

            /* Update execution summary */
            if ( opts->screen_stat_timer != -1 )
            {
                pef_time_t since_last_update = PEF_TIME_ZERO;

                PEF_TIME_SUB( since_last_update, current_time, prev_summary_update);

                if ( PEF_TIME_LEQ( opts->summary_update_timer, since_last_update) ||
                     /* Need to update execution summary before returning. Otherwise the
                        monitor thread may never detect that the worker has finished */
                     file_num == files_to_write - 1 )
                {
                    if ( pthread_mutex_lock( worker_arg->lock) )
                    {
                        PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn't "
                                      "acquire a lock for updating execution summary",
                                      worker_id);
                        ret = PEF_RET_GENERIC_ERR;

                        goto pef_do_work_out;
                    }

                    worker_arg->shared_summary->tasks_completed = file_num + 1;

                    if ( pthread_mutex_unlock( worker_arg->lock) )
                    {
                        PEF_BUFF_MSG( err_msg, err_msg_size, "Worker %ld: Couldn't "
                                      "release a lock after updating execution summary",
                                      worker_id);
                        ret = PEF_RET_GENERIC_ERR;

                        goto pef_do_work_out;
                    }

                    prev_summary_update = current_time;
                }
            } else
            {
                worker_arg->shared_summary->tasks_completed = file_num + 1;
            }

            worker_arg->private_summary->tasks_completed = file_num + 1;
            file_num++;
        }
    }

pef_do_work_out:
    /* Release the directory descriptors. Don't care of "close()" return value */
    close( outermost_dd);
    close( inner_dd);

    if ( path2file ) free( path2file);

    if ( file_name ) free( file_name);

    return (void*)(int64_t)ret;
}
