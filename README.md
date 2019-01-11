# Performance Evaluation Framework (PEF)


## Table of contents
- [Description](#description)
- [Reference implementation](#reference-implementation)
- [Project structure](#project-structure)
- [Design notes](#design-notes)
- [License](#license)

## Description
PEF is a framework for evaluating performance of... almost everything. The framework is
NOT a ready-to-use tool. Instead, it's a canvas one can fill with appropriate details to
get a complete ready-to-use tool. The resulting tool is called "PEF-tool".

PEF is intended for those wanting to execute very specific performance evaluation
scenarios. Namely, scenarios that cannot be mimicked by means of state-of-the-art generic
synthetic benchmarks.

PEF takes the following approach to performance evaluation:
 - it's assumed that a workload used to evaluate performance consists of a number of
   "tasks". PEF user must define what the task actually is. This is done by coding the
   task in plain C/C++
 - PEF allows distributing the tasks between 1 or more concurrently running threads. The
   distribution strategy must also be coded by an user. The number of tasks to do in a
   particular run of the PEF-tool and the number of threads to spawn are passed to the
   tool as command-line arguments
 - each thread - "worker" in PEF terminology - spawned by the PEF-tool performs tasks
   assigned to it one-by-one. Simultaneously the worker collects execution statistics
   of every task it performs
 - when the workers complete, the PEF-tool emits overall execution report
 - this report can then be processed by custom-written scripts or in Excel, or somewhere
   else to derive higher-level reports intended to highlight specific performance metrics

Below is a list of capabilities that PEF provides to PEF-tool developers:
 - measurement of wall-clock time
 - abstractions that allow easy switching to different time representations and time
   measurement methods
 - generation of execution reports
 - progress monitoring
 - instruments for self-verification to make debugging of PEF-tools easier
 - error reporting and verbose prints
 - canvas for configuration part of PEF-tools
 - canvas for implementing multi-threaded performance benchmarks

As was already said, PEF is a canvas one might fill to get a ready-to-use PEF-tool. To
make this filling easier, PEF comes in a form of reference implementation. This reference
implementation is a ready-to-use PEF-tool intended for evaluating File System performance
in a very specific way. To get his/her own PEF-tool, one needs to take this reference
implementation and replace details specific to the File System evaluation scenario by
his/her own details.

## Reference implementation
The reference PEF-tool implements a very specific File System performance evaluation
scenario. The tool writes the specified amount of files to a locally mounted file system.
All the files are of the same size which is passed to the tool as a command-line argument.
Every file is written sequentially.

Different files can be written concurrently. In this case every single file will still be
written sequentially, but the files themselves will be distributed evenly between the
specified amount of workers.

If requested, each worker may put every "so-many" files it creates to a separate
directory. "So-many" amount is passed to the tool as a command-line argument. If the
workers ARE required to create the directories, they do that on on-demand basis. When a
worker starts, it creates a directory and begins filling it with the files. When a "files
per directory" limit is reached, the worker creates a new directory, which becomes a home
to the new files being created by the worker. Again, only until the limit is reached. So
on... All the directories are created at the same level inside the same pre-existing
directory. The workers don't share directories they create. Each worker creates
directories for its files by its own. If "files per directory" limit is not set, then all
the workers create their files inside the same pre-existing directory.

The reference PEF-tool performs "sync()" operation on the following objects:
 - on every data file after it's written
 - on a data file's parent directory after the data file is written
 - on a perent directory of a newly created directory (if the tool is requested to create
   intermediate directories)

After the workload is completed, the refernce tool emits an execution report in a form of
two files named "pef_TIMESTAMP__stat.summary" and "pef_TIMESTAMP__stat.details", where
"TIMESTAMP" is date and time when the tool was started. The only execution statistics
collected by the tool is time when a data file was written. The time is collected for
every data file written.

To build the tool:
1. build [WTMLIB](https://github.com/AndreyNevolin/wtmlib) first. The reference tool
   relies on it for measuring wall-clock time. It's assumed in the reference build
   scenario that WTMLIB and PEF-tool itself are cloned to the same parent directory
2. execute `make` or `make debug` to build release or debug version of the tool
   accordingly. The debug version comes with build-in consistency checks. Some of these
   checks may potentially exist inside performance-critical sections of the code. Thus,
   this build mode is not intended for obtaining trusted performance evaluation results,
   but only for the purpose of finding bugs in the code

Then you can run `./pfe` to read a help message on using the tool.

The reference implementation also comes with an auxiliary
[script](scripts/avg_bw_every_n_tasks.py) intended for producing bandwidth report from
PEF-tool execution report (i.e. how many megabytes per second was the tool able to write
to the File System while it was progressing). Execute
`python3 avg_bw_every_n_tasks.py help` to learn how to use the script.

## Project structure
 - [src/common.h](src/common.h) - common tools that can be used by any code module:
   assertions, debugging, logging
 - [src/config.cpp](src/config.cpp) and [src/config.h](src/config.h) - configuration part
   of PEF-tool. Change this file to switch to your own configuration (command-line
   options, environment variables, so on)
 - [src/pef.cpp](src/pef.cpp) - main PEF driver. Normally needs to be changed only
   slightly when implementing a new PEF-tool (report-emitting part, memory allocation part
   an maybe other minor parts)
 - [src/time.h](src/time.h) - this is where time representation and time manipulation
   routines are encapsulated. Change this module if you want to add a different time
   representation and/or different time-measurement method
 - [src/worker.cpp](src/worker.cpp) and [src/worker.h](src/worker.h) - worker thread
   implementation. The actual performance benchmark's logic. This is where one should
   place code of his/her own performance benchmark
 - [scripts/avg_bw_every_n_tasks.py](scripts/avg_bw_every_n_tasks.py) - auxiliary script
   to produce bandwidth report from the PEF-tool's execution report

## Design notes
1. The main underlying design principle of PEF is "staying as non-invasive as possible".
   I.e. PEF-tool's orchestration logic must not interfere with performance benchmark's
   logic. For example, in reference implementation execution statistics is fully collected
   in memory first. And only when the benchmark is completed, the statistics is stored to
   persistent location. Emitting statistics on-the-fly might distort performance results.
   Also by default the reference PEF-tool doesn't print its progress. That's because
   progress reporting may potentially interfere with the actual workload. If a user (or - 
   more generally speaking - a client) does want to have progress reporting, he/she can
   set the progress update interval. The bigger is the interval, the less probable and
   less harmful will be the interference. Don't use progress reporting if you don't
   tolerate any interference
2. There is the following build-in usability feature. Execution report is emitted
   regardless of errors that might have happened while the workers were running. There are
   two reasons for that:
     - debugging purposes. Execution statistics collected before (at least one of) workers
       failed may aid in investigating the issue
     - convenience. For example, assume one wants the reference PEF-tool to fill the File
       System with data entirely. It's hard to pre-calculate the amount of tasks needed
       for that. To do precise calculations one needs to know what fraction of space will
       be occupied with the data and what fraction of the space will be filled with the
       corresponding metadata. So, instead of doing this calculation, one may ask the tool
       to write deliberately more files than the File System can hold. The tool will write
       the data until the File System is full. When that happens (i.e. the tool gets an
       error while trying to write the data), the tool stops writing files and emits all
       the statistics collected so far. Thus, this is an example of a case when a client
       deliberately wants the tool to fail.
3. One more usability feature of the reference implementation. Execution report consists
   of two parts: 1) execution summary and configuration properties; 2) detailed per-task
   execution statistics. Each part is stored in its own file. Detailed execution
   statistics is emitted first, execution summary and configuration parameters are emitted
   last. This ordering is significant. That's because the last line of summary file is
   expected to hold a report completeness statement. If this statement is present, that
   means that the overall execution report was emitted fully. If this statement is
   absent - or even the summary file itself is absent - that means that problems occured
   while emitting the report or even earlier. In this case the report files alone (if any)
   are not sufficient to acquire an understanding of how successful the corresponding
   PEF-tool run was and whether all collected task-granular execution statistics is on
   disk. Of course, all errors occuring during PEF-tool execution must be reported by the
   tool before it exits. But this is a runtime machanism. "Report completeness" feature
   instead allows learning - at least partially - PEF-tool error status from on-disk
   artifacts. For example, assume that artifacts from several runs of the same PEF-tool
   were collected in a single directory. Thanks to "report completeness" feature,
   inspecting just the artifacts alone allows to understand which runs have been
   successful (and therefore could be analyzed further) and which have failed

## License
Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
 * Twitter: @Andrey_Nevolin
 * LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
  
This software is provided under the Apache 2.0 Software license provided in
the [LICENSE.md](LICENSE.md) file
