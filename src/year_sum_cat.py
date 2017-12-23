
# coding: utf-8

# In[1]:


import sys
import re
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


# In[2]:


filepath='D:/tandon/big data analytics/project/NYPD_Complaint_Data_Historic.csv'
sc = SparkContext.getOrCreate()
data = sc.textFile(filepath, 1)

head = data.first() 
lines = data.filter(lambda row: row != head) 
line = lines.map(lambda x:(x.encode('ascii','ignore')))    .mapPartitions(lambda x: (reader(x, delimiter = ',', quotechar = '"')))


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


def get_year_cat(date, cat):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016 or int(d.year) < 2006:
                date = "Invalid"
            else:
                date =  str(d.year) + " " + cat
        except ValueError:
            date = "Invalid"
    return date


# In[5]:


def get_year_cat_sta(date, cat, sta):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016 or int(d.year) < 2006:
                date = "Invalid"
            else:
                date =  str(d.year) + " " + cat + " " + sta
        except ValueError:
            date = "Invalid"
    return date


# In[9]:


year_sum_cat = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8')])            .map(lambda x:(get_year_cat(x[0],x[1]), 1))            .reduceByKey(add)            .map(lambda x: x[0]+'\t'+str(x[1]))            .saveAsTextFile("year_sum_cat.out")


# In[10]:


year_sum_cat_sta = line.map(lambda x: [x[5].encode('utf-8'),x[11].encode('utf-8'),x[10].encode('utf-8')])            .map(lambda x:(get_year_cat(x[0],x[1],x[2]), 1))            .reduceByKey(add)            .map(lambda x: x[0]+'\t'+str(x[1]))            .saveAsTextFile("year_sum_cat_sta.out")

