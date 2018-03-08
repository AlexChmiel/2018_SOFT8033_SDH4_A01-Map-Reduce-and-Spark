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

# -----------------------------------------
# FUNCTION  get_lang_or_project
def get_lang_or_project(per_language_or_project, words):
    
    if per_language_or_project == False: # Per project
            if '.' in words: 
                return words.split('.')[1]
            else:
                return 'wikipedia'
        # If per project
    if per_language_or_project == True: # Per Language
        if '.' in words:
            return words.split('.')[0]
        else:
            return words[0]
# ------------------------------------------

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    
    # If per_language_or_project: # When true, process for language.

    dict_of_langs = {}
    for each_line in input_stream:
        words = each_line.split()
        
        # Gets language or project based on per_language_or_project value
        specifier = get_lang_or_project(per_language_or_project, words[0]) 
    
        # Check if the string at position 2 is a digit, before being converted to integer.
        # This caused errors when special characters were involved in one of the files.
        if words[2].isdigit():
            visits = int(words[2])
        else:
            for each in words:
                if each.isdigit(): 
                    visits = int(each)
                    break
        
        # Create dictionary where key = lang_prefix and value = visits
        if specifier not in dict_of_langs:
            dict_of_langs[specifier] = visits
        else:
            dict_of_langs[specifier] = dict_of_langs.get(specifier) + visits
        
    # Write to the output
    for key, value in dict_of_langs.items():
        output_stream.write(str(key) + "\t" + str(value) + "\n")

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
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
    my_map(my_input_stream, per_language_or_project, my_output_stream)

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

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    per_language_or_project = True # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
