
# coding: utf-8
# count missing values in each attribute
# In[1]:

import sys
import re
from operator import add
from pyspark import SparkContext
from csv import reader
import datetime


# In[2]:


sc = SparkContext.getOrCreate()
data = sc.textFile(sys.argv[1], 1)

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


for i in range(12,23):
    mapreduce = line.map(lambda x: (x[i].encode('utf-8')))\
		.map(lambda x: (normal_validate(x), 1))\
		.reduceByKey(add)\
		.map(lambda x: x[0]+'\t'+str(x[1]))\
        	.saveAsTextFile("validate_"+str(i)+".out")


# In[5]:


def approximate_coordinate_range(content):
	try:
    		coordinator = filter(None,re.split('[(),]',content))
    		latitude = float(coordinator[0].strip())
    		longitude = float(coordinator[1].strip())
    		# reference from 'New York City Borough Boundary MetaData'
    		west = -74.257159
    		east = -73699215
    		north = 40.915568
    		south = 40.495992
    		if latitude < south or latitude > north:
        		return "Coordinator Invalid"
    		if longitude < west or longitude > east:
        		return "Coordinator Invalid"
    		return "Valid"
	except ValueError:
		return "Missing"


# In[6]:


mapreduce = line.map(lambda x: (x[23].encode('utf-8')))\
            .map(lambda x: (normal_validate(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+str(23)+".out")


# In[ ]:


# check the length of CMPLNT_NUM
def verificate_unique_key(unique_key):
    nv = normal_validate(unique_key) 
    if nv != "Valid":
        unique_key = nv
    else:
        try:
            int(unique_key)
            if len(unique_key) != 9 :
                unique_key = "DigitNumberError"
            else:
                unique_key = "Valid"
        except ValueError:
            unique_key = "Invalid"
    return unique_key


# In[5]:


# Filter CMPLNT_NUM
unique_key = line.map(lambda x: (x[0].encode('utf-8')))\
            .map(lambda x:(verificate_unique_key(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"0"+".out")


# In[6]:


# check format and range of date for CMPLNT_FR_DT, CMPLNT_TO_DT and RPT_DT
def verificate_date(date):
    nv = normal_validate(date) 
    if nv != "Valid":
        date =  nv
    else:
        try:
            d = datetime.datetime.strptime(date, "%m/%d/%Y")
            if int(d.year) > 2016:
                date = date + " " +  "YearOutOfRangeError"
            else:
                date =  "Valid"
        except ValueError:
            date = date + "Invalid"
    return date


# In[7]:


# Filter CMPLNT_FR_DT
start_date = line.map(lambda x: (x[1].encode('utf-8')))\
            .map(lambda x:(verificate_date(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"1"+".out")
                
# Filter CMPLNT_TO_DT
end_date = line.map(lambda x: (x[3].encode('utf-8')))\
            .map(lambda x:(verificate_date(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"3"+".out")
            
# Filter RPT_DT
report_date = line.map(lambda x: (x[5].encode('utf-8')))\
            .map(lambda x:(verificate_date(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"5"+".out")


# In[8]:


# check the format of time for CMPLNT_FR_TM, CMPLNT_TO_TM
def verificate_time(time):
    nv = normal_validate(time) 
    if nv != "Valid":
        time = nv
    else:
        try:
            t = time[0:8].split(":")
            if int(t[0])>24 or int(t[0])<0 or int(t[1])>60 or int(t[1])<0 or int(t[2])>60 or int(t[2])<0:
               time = time + " " +"Invalid"
            else:
               time = "Valid"
        except ValueError:
            time = time + " " + "InValid"
    return time


# In[9]:


# Filter CMPLNT_FR_TM, CMPLNT_TO_TM 
from_time = line.map(lambda x: (x[2].encode('utf-8')))\
            .map(lambda x:(verificate_time(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"2"+".out")
            
            
to_time = line.map(lambda x: (x[4].encode('utf-8')))\
            .map(lambda x:(verificate_time(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"4"+".out")
            


# In[10]:


# check the range of classify code KY_CD
def verificate_classify_code(code):
    nv = normal_validate(code) 
    if nv != "Valid":
        code =  code + " " +nv
    else:
        try:
            int(code)
            if code[0] == "0":
                code = code+ " " +"OutOfRangeError"
            elif len(code) != 3:
                code = code+ " " +"DigitNumberError"    
            else:
                code = code+ " " +"Valid"
        except ValueError:
            code = code+ " " +"TypeError"
    return code


# In[11]:


# Filter KY_CD
classify_code = line.map(lambda x: (x[6].encode('utf-8')))\
            .map(lambda x:(verificate_classify_code(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"6"+".out")


# In[12]:


# check the range of intenal classify code PD_CD
def verificate_internal_classify_code(code):
    nv = normal_validate(code) 
    if nv != "Valid":
        code = code + " " + nv
    else:
        try:
            int(code)
            if code[0] == "0":
                code = code+ " " +"OutOfRangeError"
            elif len(code) != 3:
                code = code+ " " +"DigitNumberError"
            else:
                code = code+ " " +"Valid"
        except ValueError:
            code = code+ " " +"TypeError"
    return code


# In[13]:


# Filter PD_CD
internal_classify_code = line.map(lambda x: (x[8].encode('utf-8')))\
            .map(lambda x:(verificate_internal_classify_code(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"8"+".out")


# In[14]:


#check the classification of indicator for CRM_ATPT_CPTD_CD
def verificate_indicator(indicator):
    nv = normal_validate(indicator) 
    if nv != "Valid":
        indicator = indicator + " " + nv
    else:
        if indicator == "ATTEMPTED" or indicator == "COMPLETED":
            indicator = indicator + " " + "Valid"
        else:
            indicator = indicator + " " + "Invalid"
    return indicator   


# In[15]:


# Filter CRM_ATPT_CPTD_CD
indicator = line.map(lambda x: (x[10].encode('utf-8')))\
            .map(lambda x:(verificate_indicator(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"10"+".out")


# In[16]:


# check the classification of level for CRM_ATPT_CPTD_CD
def verificate_level(level):
    nv = normal_validate(level) 
    if nv != "Valid":
        level = level + " " + nv
    else:
        if level == "FELONY" or level == "MISDEMEANOR" or level == "VIOLATION":
            level = level + " " + "Valid"
        else:
            level = level + " " + "Invalid"
    return level
   


# In[17]:


#Filter LAW_CAT_CD
level = line.map(lambda x: (x[11].encode('utf-8')))\
            .map(lambda x:(verificate_level(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"11"+".out")


# In[18]:
def verificate_off_descrip(des):
	nv = normal_validate(des)
	if nv != "Valid":
	   des = nv
        else:
           des = "Valid"
        return des

def verificate_in_descrip(des):
        nv = normal_validate(des)
        if nv != "Valid":
           des = nv
        else:
           des = "Valid"
        return des

# Filter OFNS_DESC
offen_descrip = line.map(lambda x: (x[8].encode('utf-8')))\
            .map(lambda x:(verificate_off_descrip(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"7"+".out")


# In[19]:


# Filter PD_DESC
internal_descrip = line.map(lambda x: (x[10].encode('utf-8')))\
            .map(lambda x:(verificate_in_descrip(x), 1))\
            .reduceByKey(add)\
            .map(lambda x: x[0]+'\t'+str(x[1]))\
            .saveAsTextFile("validate_"+"9"+".out")

