#!/usr/local/bin/python

# This code is provided in the Udacity course "Intro to Hadoop and MapReduce":
# https://www.udacity.com/course/intro-to-hadoop-and-mapreduce--ud617
# It has been slightly adjusted for compatibility with the course "Scaling Database Systems"

# Your task is to finish this reducer, so it correctly processes the data passed by the mapper.

import sys

def reducer():
    salesTotal = 0
    oldKey = None
    
    for line in sys.stdin:
        data = line.strip().split("\t")

        # Defensive programming: must have exactly 2 fields: store, cost
    
        if len(data) != 2:
            continue

        store, cost = data
    
        # if cost is not a number, skip this line
        try:
            cost = float(cost)
        except:
            continue

        # if we have moved to a new store, output the total for the old store
        if oldKey and oldKey != store:
            print(f"{oldKey}\t{salesTotal}")
            salesTotal = 0

        oldKey = store
        salesTotal += cost

        # dont forget to output the last store if needed
        if oldKey:
            print(f"{oldKey}\t{salesTotal}")

        # HINT: The solution discussed in the Udacity videos targets Python 2
        #       You may need to adjust the syntax to work with Python 3,
        #       e.g, add parentheses to the print statements.

if __name__ == "__main__":
    reducer()