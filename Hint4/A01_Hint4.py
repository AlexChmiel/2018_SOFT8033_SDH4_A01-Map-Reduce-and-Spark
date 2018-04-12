
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
# FUNCTION map_by_kv
# ------------------------------------------
def map_by_kv(line):
  res = ""
  stringSplit = line.split(" ")
  res = (stringSplit[0], (stringSplit[1], stringSplit[2]))
  
  
  return res

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
# FUNCTION my_reduce()
# ------------------------------------------
def my_reduce(x, y):
  site_visit = x[1]
  visits = site_visit[1]
  
  
  return 
          
# ------------------------------------------
# FUNCTION my_main
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
from __future__ import division



# -----------------------------------------
# FUNCTION  get_lang_or_project
# -----------------------------------------
def get_lang_or_project(words):
  # Split the line and extract site, lang and visits
  wordsSplit = words.split()
  lang = wordsSplit[0]
  site = wordsSplit[1]
  
  # Check if visits is an int, if its not, its non utf-8 line which has visits number pushed few bytes further
  # Grab the visits value then
  if wordsSplit[2].isdigit():
    visits = int(wordsSplit[2])
  else:
    for each in wordsSplit:
      if each.isdigit(): 
        visits = int(each)
        break
  
  # Per project - return (project, visits, visits/total_visits)                  
  if per_language_or_project == False:
    # If there is a dot in lang, 
    if '.' in lang: 
      return (lang.split('.')[1], visits) 
    else:
      return ('wikipedia', visits)
  
  # Per site - return (language prefix, visits, visits/total_visits)
  if per_language_or_project == True: # Per Language
    if '.' in lang:
      return (lang.split('.')[0], visits)
    else:
      return (lang, visits)
        
# ------------------------------------------
# FUNCTION my_mapper()
# ------------------------------------------
def my_mapper(line):
  wordsSplit = line.split()

  # Extract visits
  if wordsSplit[2].isdigit():
    visits = int(wordsSplit[2])
  else:
    for each in wordsSplit:
      if each.isdigit(): 
        visits = int(each)
        break
  
  
  return visits

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_total(line, total_visits):
  # Add total_visits ratio
  lang = line[0]
  visits = int(line[1])
  
  return (lang, visits, (visits/total_visits)*100.0)
  
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

    # Read dataset into RDD. Cache it because we are reusing it.
    dataset = sc.textFile(dataset_dir).cache()
    
    # Map total_visits
    total_visits_mapped = dataset.map(my_mapper)
    
    # Get the total visits as a number
    total_visits = total_visits_mapped.reduce(lambda x, y: x + y)   
    
    # Map language or project RDD
    mappedRDD = dataset.map(get_lang_or_project)
    
    # Reduce per language or project
    reducedRDD = mappedRDD.reduceByKey(lambda x, y: x + y)
    
    # Map the total_visits onto the reducedRDD
    finalRDD = reducedRDD.map(lambda f: my_total(f, total_visits))

    for w in finalRDD.collect():
        print(w)

	# Complete the Spark Job

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
