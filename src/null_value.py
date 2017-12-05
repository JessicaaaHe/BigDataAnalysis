
# coding: utf-8
# check null, n/a, unspecified ... non-empty value in the dataset
# In[1]:


import sys
from operator import add
from pyspark import SparkContext
from csv import reader


# In[2]:


sc = SparkContext.getOrCreate()
data = sc.textFile(sys.argv[1], 1)

head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))\
    .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


# In[8]:


def null_value_specify(content):
    try:
        content = content.lower()
    except AttributeError:
        content = content
    if content == '':
        return "Missing"
    elif content == 'n/a' or content == 'na' or content == "null":
        return "non-empty missing"
    return "not Missing"


# In[9]:


for i in range(24):
    mapreduce = line.map(lambda x: (x[i].encode('utf-8')))\
        .map(lambda x: (null_value_specify(x), 1))\
        .reduceByKey(add)\
        .map(lambda x: x[0]+'\t'+str(x[1]))\
        .saveAsTextFile("nullvalue_"+str(i)+".out")

