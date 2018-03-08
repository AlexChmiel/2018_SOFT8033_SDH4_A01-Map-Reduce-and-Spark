#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    dict_of_langs = {}
    for each_line in input_stream:
        words = each_line.split() # Split by empty space.
        lang_prefix = words[0] # Grab language
        
        if words[1].isdigit():
            visits = int(words[1])
        else:
            for each in words:
                if each.isdigit(): 
                    visits = int(each)
                    break
           
        # Create dictionary where key = lang_prefix and value = visits
        if lang_prefix not in dict_of_langs:
            dict_of_langs[lang_prefix] = visits
        else:
            dict_of_langs[lang_prefix] = dict_of_langs.get(lang_prefix) + visits
        
    # Write to the output
    for key in sorted(dict_of_langs, key=dict_of_langs.get, reverse=True):
        value = dict_of_langs[key]
        total_petitions_ratio = '{:.10f}%'.format((value/total_petitions) * 100)
        value = value, total_petitions_ratio
        
        output_stream.write(str(key) + "\t" + str(value) + "\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_reduce(my_input_stream, total_petitions, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    # This variable must be computed in the first stage
    total_petitions = 21996631

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
