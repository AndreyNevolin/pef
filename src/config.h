/**
 * Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
 *
 * Data structures and utilities to work with the PEF configuration:
 *     - initialization of the configuration
 *     - parsing and validation of command line arguments
 *     - collection and validation of properties from other sources
 *     - calculation of derived properties
 */

#ifndef _PEF_CONFIG_H_
#define _PEF_CONFIG_H_

#include "time.h"

#include <string>

/**
 * Macro to perform verbose output
 */
#define PEF_VERBOSE( opts_, ...) \
    if ( opts_->is_verbose )     \
    {                            \
        PEF_OUT( __VA_ARGS__);   \
    }

/**
 * Structure to keep configuration parameters
 */
typedef struct
{
    /* Number of worker threads */
    int64_t num_threads;
    /* Path to the data directory. Must exist */
    std::string data_output_path;
    /* Path to the statistics directory. Must exist */
    std::string stat_output_path;
    /* Number of files to create*/
    int64_t num_files;
    /* Size of each file (in bytes) */
    int64_t file_size;
    /* Size of chunks written at once to data files */
    int64_t write_chunk_size;
    /* Number of files per directory */
    int64_t num_files_per_dir;
    /* Time period (in seconds) between updates of on-screen statistics */
    int64_t screen_stat_timer;
    /* Time period after which workers are allowed (but are not obliged) to update
       their execution summary */
    pef_time_t summary_update_timer;
    /* Verbose output */
    bool is_verbose;
    /* Base name for output files and directories */
    char *file_base_name;
    /* Number of decimal digits used when emitting worker IDs */
    int num_digits_threads;
    /* Number of decimal digits used when emitting task numbers */
    int num_digits_files;
    /* Number of decimal digits used when emitting directory numbers */
    int num_digits_dirs;
    /* Number of TSC ticks in a second */
    uint64_t tsc_ticks_per_sec;
} pef_Opts_t;

/* Permissions for data directories created by workers */
#define PEF_DATA_DIR_PERMISSIONS 0700
/* Permissions for data files created by workers */
#define PEF_DATA_FILE_PERMISSIONS 0600
/* Permissions for statistics files created as parts of PEF execution report */
#define PEF_STAT_FILE_PERMISSIONS 0600

/* Minimal number of seconds that PEF requires to remain before the earliest TSC wrap.
   Currently it's 1 month's worth */
#define PEF_SECS_TO_REMAIN_BEFORE_TSC_WRAP (long)60*60*24*30

/* Time period (in seconds) that must pass before PEF tries again to join with each
   running thread */
#define PEF_PAUSE_BEFORE_JOIN_RETRY 1

/* Initialize options */
int pef_InitOpts( pef_Opts_t *opts);
/* Parse command line */
int pef_ParseCmdLine( int argc, char *argv[], pef_Opts_t *opts);
/* Validate command-line arguments, obtain configuration properties from other sources and
   calculate derived properties */
int pef_CheckOptsAndFillFurther( pef_Opts_t *opts,
                                 std::string indent,
                                 char *err_msg,
                                 int err_msg_size);
/* Deallocated memory used to store configuration parameters */
void pef_DeallocateOpts( pef_Opts_t* const opts, std::string indent);

#endif /* _PEF_CONFIG_H_ */
