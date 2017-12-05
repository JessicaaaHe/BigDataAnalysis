# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 13:00:42 2017

@author: mw352

count frequencies of different values in each attribute
"""
from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader


sc = SparkContext.getOrCreate()
data = sc.textFile(sys.argv[1], 1)

head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))\
        .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))
        
for i in range(24):
    statistic = line.map(lambda x: (x[i].encode('utf-8'), 1))\
                        .reduceByKey(add)\
                        .sortByKey()\
                        .map(lambda x: str(x[0])+'\t'+str(x[1]))\
                        .saveAsTextFile("statistic_"+str(i)+".out")

