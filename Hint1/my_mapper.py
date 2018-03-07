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





# ---------------------------------------
# OBJECT Record(language, site_name, visits)
# ---------------------------------------
class Record:
    def __init__(self, language, site_name, visits):
        self.language = language
        self.site_name = site_name
        self.visits = visits
    
    def __str__(self):
        return self.language + "\t" + "(" + self.site_name + "," + str(self.visits) + ")"

#def is_larger_value(value, list_of_top_5):
    
# ------------------------------------------
# FUNCTION clean_dataset
# ------------------------------------------
def get_dict_of_top5_records(input_stream, languages,num_top_entries):
    # Split the lines into 3 words, language, site_name and visits
    # Create a top 5 list with 0 values, check if the current value is larger than the smallest value in the list, 
    # then, add to dictionary of lists
    dict_of_records = {}
    
    for each_line in input_stream:
        words = each_line.split()
        lang = words[0]       
        # Using startswith() method I select only the objects that start with EN/ES/FR languages and store them a list of Record objects
        if any([lang.startswith(s) for s in languages]):
           site_name = words[1] 
           visits = int(words[2])
           if(lang in dict_of_records):
               # Get the lowest value in the list of 5 top 5 records
               min_val = min(dict_of_records.get(lang), key=lambda x: x.visits).visits
               #If current visits larger than smallest in the list, replace it with current record.
               if(visits > min_val or visits == min_val):
                   list_top_5 = dict_of_records.get(lang)
                   for index, value in enumerate(list_top_5):
                       if(value.visits == min_val):
                          record = Record(lang, site_name, visits)
                          list_top_5[index] = record
                          break
           else:
               list_of_top_5 = [Record("","",0) for i in range(num_top_entries-1)]
               record = Record(lang, site_name, visits)
               list_of_top_5.append(record)
               dict_of_records[lang] = list_of_top_5
              
                
    
    return dict_of_records

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    dict_of_top_5 = get_dict_of_top5_records(input_stream, languages, num_top_entries)
    for key,value in dict_of_top_5.items():
        for each in value:
            if(each.visits != 0):
                record = each.__str__()
                output_stream.write(record + "\n")
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)
    my_output_stream.close()    
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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)