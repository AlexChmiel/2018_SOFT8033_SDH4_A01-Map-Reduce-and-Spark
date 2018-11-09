"# 2018_SOFT8033_SDH4_A01_AleksanderChmiel" 

Wikimedia is a global movement whose mission is to bring free educational content to the
world. Their projects include Wikipedia, Wiktionary, Wikibooks and Wikinews, among others.
For more information on any of these projects please visit: https://www.wikimedia.org/
Since May of 2015 Wikimedia provides dumps with page view statistics, which are publicly
available at: https://dumps.wikimedia.org/other/pageviews/. These dumps are accumulated
daily, on an hourly basis, and contain about 50MB of compressed entries. 

Each entry is unique (there are no entry repetitions) and is provided in a single line.
- The first word of an entry represents:
- The language (e.g., en - English, es - Spanish (Español), fr - French (Français)).
- The project (e.g., no extension - Wikipedia, .d - Wiktionary, .b - Wikibooks).
- The second word represents the title of the page viewed.
- The third word represents the number of page views.
- The fourth word is not relevant to us and thus is to be discard. 

my_mapper.py:
- This program runs a Map process for a slice of the dataset.
Given a single sub-file, the program computes the Map stage for it.

- my_mapper_simulation.py:
- This program runs the Map stage for the entire dataset.
Given the folder containing all the slices of the dataset (all the sub-files), the
program runs (sequentially) the Map stage for each slice (sub-file), aggregating
all results into the desired file (by default map_simulation.txt). 

- my_sort_simulation.py:
- This program sorts all (key, value) pairs generated by the Map stage.
Given the Map aggregated-results file (by default map_simulation.txt) it stores
its sorted (key, value) pairs into the desired file (by default sort_simulation.txt).

- my_reducer.py:
- This program runs a Reduce process for a slice of the sorted (key, value) pairs
generated after Map and Sort stages.
Without loss of generality, we can assume our Map-Reduce job will trigger 1
Reduce process. Thus, given the Sort aggregated-results file (by default
sort_simulation.txt) the program computes the Reduce stage for it, storing the
results into the desired file (by default reduce_simulation.txt).
Note: In case of n > 1 Reduce processes, each of them will provide a file
part-xxxxx with the solutions computed. The final solution to the MapReduce job
requires to merge all part-xxxx files into a single file. This merge can be done by
a daemon (a simple Python program) and its not part of the MapReduce job.

Hint 1:
GOAL.
Given a Wikimedia dump (e.g., the pageviews-20180219-100000 being provided):
- Create a MapReduce job to compute the 5 entries with most page views for all
Wikimedia projects in the languages English, Spanish and French. 

Hint 2:
GOAL.
Given a Wikimedia dump (e.g., the pageviews-20180219-100000 being provided):
- Create a MapReduce job to compute the number and percentage of petitions per
language and per project.

Hint 3:
GOAL.
Given a Wikimedia dump (e.g., the pageviews-20180219-100000 being provided):
- Create a Spark job to compute the 5 entries with most page views for all Wikimedia
projects in the languages English, Spanish and French. 
