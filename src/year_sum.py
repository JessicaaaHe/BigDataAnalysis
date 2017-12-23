
# coding: utf-8

# In[1]:


import sys
import re
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


# In[2]:


filepath='NYPD_Complaint_Data_Historic.csv'
sc = SparkContext.getOrCreate()
data = sc.textFile(filepath, 1)

head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))\
        .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


# In[3]:


def normal_validate(content):
    try:
        content = content.lower()
    except AttributeError:
        content = content
    if content == '':
        return "Missing"
    elif content == 'n/a' or content == 'na':
        return "N/A"
    elif content == 'unspecified':
        return "Unspecified"
    elif content == 'null':
        return "Null"
    return "Valid"


# In[4]:


def get_year_total(date):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016 or int(d.year) < 2006:
                date = "Invalid"
            else:
                date =  str(d.year)
        except ValueError:
            date = "Invalid"
    return date


# In[5]:


year_total = line.map(lambda x: (x[5].encode('utf-8')))\
            .map(lambda x:(get_year_total(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("year_sum.out")

