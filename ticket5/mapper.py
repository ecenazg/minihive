#!/usr/local/bin/python

# This code is provided in the Udacity course "Intro to Hadoop and MapReduce":
# https://www.udacity.com/course/intro-to-hadoop-and-mapreduce--ud617
# It has been slightly adjusted for compatibility with the course "Scaling Database Systems"

# Your task is to make sure that this mapper code does not fail on corrupt data lines,
# but instead just ignores them and continues working
import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split("\t")

        # This is the place you need to do some defensive programming
        # what if there are not exactly 6 fields in that line?
        # if the line does not have exactly 6 fields, skip it
        if len(data) != 6:
            continue
        # this next line is called 'multiple assignment' in Python
        # this is not really necessary, we could access the data
        # with data[2] and data[5], but we do this for convenience
        # and to make the code easier to read

        date, time, store, item, cost, payment = data

        # print out the data that will be passed to the reducer
        print("{0}\t{1}".format(store, cost))

if __name__ == "__main__":
    mapper()
