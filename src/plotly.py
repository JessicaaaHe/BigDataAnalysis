
# coding: utf-8

# In[ ]:


import plotly
plotly.__version__


# In[ ]:


from plotly.graph_objs import Scatter, Layout


# In[ ]:


plotly.offline.init_notebook_mode(connected=True)


# In[ ]:


plotly.offline.iplot({
    "data": [Scatter(x=[1, 2, 3, 4], y=[4, 3, 2, 1])],
    "layout": Layout(title="hello world")
})


# In[8]:



import plotly
import plotly.graph_objs as go
plotly.offline.init_notebook_mode(connected=True)
trace1 = go.Bar(
    x=['Manhattan', 'Brooklyn', 'Bronx', 'Queens', 'Staten Island'],
    y=[422189, 541841, 325145,370347,57453],
    name='Felony',
    opacity=0.8
)
trace2 = go.Bar(
    x=['Manhattan', 'Brooklyn', 'Bronx', 'Queens', 'Staten Island'],
    y=[771325, 919218, 737972,593282,158701],
    name='Misdemeanor',
    opacity=0.8
)
trace3 = go.Bar(
    x=['Manhattan', 'Brooklyn', 'Bronx', 'Queens', 'Staten Island'],
    y=[138246, 205844, 146530,141992,49487],
    name='Violation',
    opacity=0.8
)

data = [trace1, trace2, trace3]
layout = go.Layout(
    barmode='group',
    title='Distribution of Complaints by Borough'
)

fig = go.Figure(data=data, layout=layout)
plotly.offline.iplot(fig, filename='grouped-bar')


# In[13]:


import plotly
import plotly.graph_objs as go
plotly.offline.init_notebook_mode(connected=True)
month=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
trace1 = go.Scatter(
    x=month,
    y=[140323, 119858, 137027,135282,146472,144576,
      153112,155453,148897,153134,141457,141674],
    name='Felony',
    line = dict(width = 3)
)
trace2 = go.Scatter(
    x=month,
    y=[257008, 231741, 270787,266797,282345,275247,
      281771,284365,268448,274608,248142,239385],
    name='Misdemeanor',
    line = dict(width = 3)
)
trace3 = go.Scatter(
    x=month,
    y=[53249, 47303, 56105,55449,61563,61686,
      61883,60165,60487,59559,53418,51259],
    name='Violation',
    line = dict(width = 3)
)

data = [trace1, trace2, trace3]
layout = go.Layout(
    barmode='group',
    title='Distribution of Complaints by Month'
)
layout = dict(title = 'Trends of Complaints by Month',
              xaxis = dict(title = 'Month'),
              yaxis = dict(title = 'Comlaints'),
              )

fig = go.Figure(data=data, layout=layout)
plotly.offline.iplot(fig, filename='month-trend')


# In[14]:


weekday=['Monday', 'Tuesday', 'Wednesday', 'Thursday',
       'Friday', 'Saturday','Sunday']
import plotly
import plotly.graph_objs as go
plotly.offline.init_notebook_mode(connected=True)

trace1 = go.Scatter(
    x=weekday,
    y=[249668, 263631, 265017,257978,255942,224297,
      200732],
    name='Felony',
    line = dict(width = 3)
)
trace2 = go.Scatter(
    x=weekday,
    y=[430441, 474884, 486289,475972,472354,442858,
      397846],
    name='Misdemeanor',
    line = dict(width = 3)
)
trace3 = go.Scatter(
    x=weekday,
    y=[104136, 104431, 103574,102295,100388,84064,
      83238],
    name='Violation',
    line = dict(width = 3)
)

data = [trace1, trace2, trace3]
layout = go.Layout(
    barmode='group',
    title='Trends of Complaints by Weekday'
)
layout = dict(title = 'Trends of Complaints by Weekday',
              xaxis = dict(title = 'Weekday'),
              yaxis = dict(title = 'Comlaints'),
              )

fig = go.Figure(data=data, layout=layout)
plotly.offline.iplot(fig, filename='weekday-trend')


# In[3]:


import plotly.plotly as py
from plotly.graph_objs import *
#plotly.offline.init_notebook_mode(connected=True)
mapbox_access_token = 'pk.eyJ1IjoiZ29kYXBwbGVtYW4iLCJhIjoiY2piMWx4dnM3N2N4YjMzbnFtcjgzejdoaiJ9.GD_zceZhZzm9dE3sRWZTTA'

data = Data([
    Scattermapbox(
        lat=latitude[:20000],
        lon=longitude[:20000],
        mode='markers',
        marker=Marker(
            size=2
        )
        
    )
])
layout = Layout(
    autosize=True,
    hovermode='closest',
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=40.75,
            lon=-73.91
        ),
        pitch=0,
        zoom=10
    ),
)

fig = dict(data=data, layout=layout)
py.iplot(fig, filename='Multiple Mapbox')


# In[2]:


import csv

# open the file in universal line ending mode 
with open('NYPD_Complaint_Data_Historic.csv', 'rU') as infile:
  # read the file as a dictionary for each row ({header : value})
  reader = csv.DictReader(infile)
  data = {}
  for row in reader:
    for header, value in row.items():
      try:
        data[header].append(value)
      except KeyError:
        data[header] = [value]


latitude = data['Latitude']
longitude = data['Longitude']


# In[24]:





# In[15]:




