# -*- coding: utf-8 -*-
"""
Created on Sun Dec  3 19:16:20 2017

@author: mw352

statistic the integrity quality of the whole dataset by missing value
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
        
def encode(x):
    for i in range(len(x)):
        x[i] = x[i].encode('utf-8')
    return x

def count_missing(x):
    count = 0
    for i in range(len(x)):
        if x[i] == '':
            count += 1
    return str(count)+' ,number of missing value'

mapreduce = line.map(lambda x: (encode(x)))\
            .map(lambda x: (count_missing(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("integrity.out")
