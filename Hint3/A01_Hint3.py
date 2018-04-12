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
# FUNCTION filter_by_language
# ------------------------------------------
def filter_by_language(line):
  res = False
  
  # Check if current line contains the language defined in languages in main and return that line
  lang = line[0]
  if any([lang.startswith(s) for s in languages]):
    res = True
  
  return res

# ------------------------------------------
# FUNCTION map_by_kv
# ------------------------------------------
def map_by_kv(line):
  # Map the line to a tuple inside a tuple (en, (site, visits))
  res = ""
  stringSplit = line.split()
  res = (stringSplit[0], (stringSplit[1], stringSplit[2]))
  
  
  return res
# ------------------------------------------
# FUNCTION group_func
# ------------------------------------------
def group_func(x):
  # Group by language
  lang = x[0]
  
  
  return lang

# ------------------------------------------
# FUNCTION map_values
# ------------------------------------------
def map_values(x):
  # Create Empty list of ("", 0)
  list_of_top_5 = [("",0) for i in range(num_top_entries)]
  
  # Iterate through pyspark result iterable object 
  for each in x:
    site = each[1][0]
    visits = int(each[1][1])
    # Get the smallest value in the list, if current visits value is larger than the smallest, replace it.
    min_val = min(list_of_top_5, key = lambda f: f[1])
    min_val_visits = int(min_val[1])
    if visits > min_val_visits:
      min_val_index = list_of_top_5.index(min(list_of_top_5, key = lambda g: g[1]))
      list_of_top_5[min_val_index] = (site, visits)

      
  return list_of_top_5

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # 1. We remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)
    
    # Load the dataset
    datasetRDD = sc.textFile(dataset_dir)
    
    # Map dataset by key, value. Format = (lang, site, visits)
    mappedRDD = datasetRDD.map(map_by_kv)
    
    # Filter by language 
    filteredRDD = mappedRDD.filter(filter_by_language)
    
    # Group by language
    groupedRDD = filteredRDD.groupBy(group_func)
    
    # Get the top 5 most visited sites
    resultRDD = groupedRDD.mapValues(map_values)
    
    # Save results to the text file
    resultRDD.saveAsTextFile(o_file_dir)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main ent ry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
