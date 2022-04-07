#!/usr/bin/env python
# coding: utf-8

# In[19]:


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

import pyspark.sql.functions as func


# In[20]:


# Create a SparkSession (the config bit is only for Windows!)
spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

# Monitor the logs directory for new log data, and read in the raw lines as accessLines
accessLines = spark.readStream.text("logs")
accessLines


# In[22]:


# Parse out the common log format to a DataFrame
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                         regexp_extract('value', timeExp, 1).alias('timestamp'),
                         regexp_extract('value', generalExp, 1).alias('method'),
                         regexp_extract('value', generalExp, 2).alias('endpoint'),
                         regexp_extract('value', generalExp, 3).alias('protocol'),
                         regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                         regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))
logsDF = logsDF.withColumn("eventTime", func.current_timestamp())


# In[23]:


# Keep a running count of every access by status code
topUrlsDF = logsDF.groupBy(func.window(func.col("eventTime"), 
                                            windowDuration="30 seconds", slideDuration="10 seconds"), 
                                            func.col("endpoint")).count()


# In[24]:


statusCountsDF = topUrlsDF.orderBy(func.col("count").desc())


# In[28]:


query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )


# In[ ]:


# Run forever until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()


# In[ ]:




