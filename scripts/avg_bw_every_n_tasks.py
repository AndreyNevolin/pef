#!/usr/bin/env python

##
# Copyright © 2019 Andrey Nevolin, https://github.com/AndreyNevolin
# Twitter: @Andrey_Nevolin
# LinkedIn: https://www.linkedin.com/in/andrey-nevolin-76387328
#
# This script produces bandwidth report from PEF execution report
#
# The script assumes that:
#   1) PEF summary report contains the size of task in bytes. The size is the same for
#      all tasks
#   2) each line of the detailed PEF report - except the first one - describes a
#      single task. The lines are sorted increasingly by the absolute task completion
#      time
#
# The script produces one output file. The name of the file is based on the name of the
# PEF report files. While the names of the PEF report files have the form
# "pef_YYYY-MM-DD__HH:MM:SS__stat" (with the corresponding extensions), the output file
# of this script will have the name of the form "pef_YYYY-MM-DD__HH:MM:SS__bw.report"
#
# The output file contains a table. The table has a row for every N records of the
# detailed PEF report. N is given to the script as a command-line argument. There are two
# columns in the table. The meaning of the columns is the following:
#   1) the first column contains time in seconds since the beginning of the PEF
#      benchmark. This is time when the last task in a portion of N tasks was completed
#   2) bandwidth averaged over the corresponding period of time (i.e. the period during
#      which the corresponding N tasks were running)
##

import sys

if sys.version_info[0] != 3:
    print( "This script requires Python version 3.x or higher")
    sys.exit( 1)

import os
import re

def printUsage( script_name):
    basename = os.path.basename( os.path.normpath( script_name))
    print( "Usage:", basename,
           "<path_to_report> <report_name> <tasks_per_bw_point> [<output_path>]\n"
           "\tor\n"
           "      ", basename, "help\n")
    print( "Where: \n"
           "\tpath_to_report - path to a PEF execution report\n\n"
           "\treport_name - name of a PEF execution report. It must be of form "
           "\"pef_YYYY-MM-DD__HH:MM:SS__stat\". For example: "
           "pef_2018-12-14__17:50:21__stat\n\n"
           "\ttasks_per_bw_point - number of tasks that bandwidth averaging periods "
           "are based on. Bandwidth is averaged over a period of time that was needed to "
           "do this number of tasks. For example, if this number is 5, then bandwidth "
           "will be calculated for every 5 tasks. For each chunk of 5 tasks an averaging "
           "time period may be different (because it might take different time to do "
           "jobs belonging to different chunks)\n\n"
           "\toutput_path - non-obligatory output path. If it is present, then the "
           "output file of this script will be stored in the specified location. By "
           "default, the output file is stored to the same directory where the input "
           "PEF report is stored")

    return

def printHelp( script_name):
    print( "Produce bandwidth report from PEF execution report. The bandwidth report "
           "is stored in a file which has a name based on the name of the PEF report "
           "files: \"pef_YYYY-MM-DD__HH:MM:SS__bw.report\". By default, the file is "
           "stored to the same location where the input PEF report is stored.\n"
           "The file contains a table. There is a row in this table for every chunk of "
           "N tasks, where N is passed to the script as a command-line argument. The "
           "table has two columns. The first column stores time in seconds from the "
           "beginning of the PEF benchmark. This is time when the last task in the "
           "chunk has finished. The second column stores bandwidth in megabytes per "
           "second averaged over a period of time that was needed to do all the tasks in "
           "the chunk\n")
    printUsage( script_name)

    return

# Get task size from an PEF summary file
def getTaskSize( input_dir, pef_report_name):
    summary_name = input_dir + "/" + pef_report_name + ".summary"

    try:
        f = open( summary_name)
    except IOError as e:
        print ( "Error opening PEF summary file \"{0}\": {1}"
                .format( summary_name, e.strerror))
        sys.exit( -1)

    line = "Just to enter the loop below"
    match = None

    while ( line ):
        try:
            line = f.readline()
        except IOError as e:
            print( "Error reading line from PEF summary file \"{0}\": {1}"
                   .format( summary_name, e.strerror))
            sys.exit( -1)

        match = re.fullmatch( r'Size of data file: (\d+)\n', line)

        if ( match ): break

    if ( not match ):
        print( "Couldn't find task size in the PEF summary file")
        sys.exit( -1)

    try:
        bytes_per_task = int( match.group( 1))
    except ValueError:
        print( "Couldn't convert size of PEF task to integer")
        sys.exit( -1)

    if ( bytes_per_task < 1 ):
        print( "Unexpected size of PEF task. Must be at least 1 byte")
        sys.exit( -1)

    f.close()

    return bytes_per_task

# Given a header of an PEF "details" file, return a number of a column in which time in
# nanoseconds from the beginning of the PEF benchmark is stored
def getTimeColumnNum( details_header):
    time_column_num = 0;
    # Remove the last character from the header (it's expected to be a "newline"
    # character)
    details_header = details_header[:-1]
    column_names = details_header.split( ",")
    search_name = "Task completion time in ns (from benchmark start)"

    for name in column_names:
        if ( name == search_name ):
            break
        else:
            time_column_num += 1

    if ( time_column_num >= len( column_names) ):
        print( "Couldn't find a column named \"{0}\" in the PEF \"details\" file"
               .format( search_name))
        sys.exit( -1)

    return time_column_num

# Create output file
def createOutputFile( output_dir, pef_report_name):
    output_name = output_dir + "/" + pef_report_name[:-4] + "bw.report"

    try:
        output_f = open( output_name, 'x')
    except IOError as e:
        print ( "Error creating output file \"{0}\": {1}"
                .format( output_name, e.strerror))
        sys.exit( -1)

    return output_f

# Emit the header to the output file
def emitOutputHeader( output_f):
    try:
        output_f.write( "Time in seconds from the beginning of the benchmark,Bandwidth "
                        "in megabytes per second\n")
    except IOError as e:
        print ( "Error emitting the header to the output file: {0}"
                .format( e.strerror))
        sys.exit( -1)

    return

# Given a task record from an PEF "details" file, return time in nanoseconds from the
# beginning of the benchmark
def extractTimeFromTaskRecord( record, time_column_num, line_num):
    # trailing "newline" character is removed before splitting
    stats = (record[:-1]).split( ",")

    if ( len( stats) < time_column_num + 1 ):
        print( "Line {0} of the PEF \"details\" file has less columns than expected"
               # 2 added to "line_num" to account for the header and the fact that people
               # normally enumerate file lines from 1
               .format( line_num + 2))
        sys.exit( -1)

    try:
        time = int( stats[time_column_num])
    except ValueError:
        print( "Couldn't convert a value from line {0}, column {1} of the PEF "
               "\"details\" file to integer"
               .format( line_num + 2, time_column_num + 1))
        sys.exit( -1)

    return time

# Emit bandwidth record to the output file
def emitBWRecordToOutputFile( output_f, time, bw):
    try:
        output_f.write( "{0},{1}\n".format( time, bw))
    except IOError as e:
        print ( "Error emitting a bandwidth record to the output file: {0}"
                .format( e.strerror))
        sys.exit( -1)

    return

##########################################################################################
####                             The script's main logic                              ####
##########################################################################################

if ( len( sys.argv) == 2 and sys.argv[1] == "help" ):
    printHelp( sys.argv[0])
    sys.exit( 0)

if ( len( sys.argv) < 4 or len( sys.argv) > 5 ):
    print( "The number of arguments must be 3, or 4, or 1. In the latter case the "
           "argument must be \"help\"\n")
    printUsage( sys.argv[0])
    sys.exit( 1)

try:
    tasks_per_bw_point = int( sys.argv[3])
except ValueError:
    print ( "\"Tasks per BW point\" must be an integer")
    sys.exit( -1)

if ( tasks_per_bw_point < 1 ):
    print( "\"Tasks per BW point\" must be bigger or equal to 1")
    sys.exit( -1)

pef_report_name = sys.argv[2]
input_dir = sys.argv[1]
output_dir = ""

if ( len( sys.argv) == 5 ):
    output_dir = sys.argv[4]
else:
    output_dir = input_dir

pef_report_name_regexp = r'pef_\d\d\d\d-\d\d-\d\d__\d\d:\d\d:\d\d__stat'

if ( not re.fullmatch( pef_report_name_regexp, pef_report_name) ):
    print( "PEF report name must be of the form \"pef_YYYY-MM-DD__HH:MM:SS__stat\"")
    sys.exit( 1)

bytes_per_task = getTaskSize( input_dir, pef_report_name)
output_f = createOutputFile( output_dir, pef_report_name)
emitOutputHeader( output_f)

# Open the "details" file and find a column that stores time in nanoseconds from the
# beginning of the benchmark
details_name = input_dir + "/" + pef_report_name + ".details"

try:
    details_f = open( details_name)
except IOError as e:
    print ( "Error opening PEF \"details\" file \"{0}\": {1}"
            .format( details_name, e.strerror))
    sys.exit( -1)

try:
    details_header = details_f.readline()
except IOError as e:
    print( "Error reading the header from PEF \"details\" file \"{0}\": {1}"
           .format( details_name, e.strerror))
    sys.exit( -1)

time_column_num = getTimeColumnNum( details_header)

# Loop over records of the "details" file and calculate bandwidth every
# "tasks_per_bw_point" records
line = "Just to enter the loop below"
line_num = 0
prev_time = 0
prev_emitted_time = 0
# a factor used to convert (bytes/ns) into (megabytes/sec)
factor = 1000000000 / 1024 / 1024

while ( line ):
    try:
        line = details_f.readline()
    except IOError as e:
        print( "Error reading a line from PEF \"details\" file \"{0}\": {1}"
               .format( details_name, e.strerror))
        sys.exit( -1)

    if ( not line ): break

    current_time = extractTimeFromTaskRecord( line, time_column_num, line_num)

    if ( current_time < prev_time ):
        print( "Time on line {0} of the PEF \"details\" file is smaller than time on "
               "the previous line"
               # 2 added to "line_num" to account for the header and the fact that people
               # normally enumerate file lines from 1
               .format( line_num + 2))
        sys.exit( -1)

    # Is it time to calculate and emit bandwidth?
    if ( line_num % tasks_per_bw_point == tasks_per_bw_point - 1 ):
        bw = (bytes_per_task * tasks_per_bw_point) / (current_time - prev_emitted_time)
        bw = bw * factor
        emitBWRecordToOutputFile( output_f, current_time / 1000000000, bw)
        prev_emitted_time = current_time

    prev_time = current_time
    line_num += 1

details_f.close()
output_f.close()
