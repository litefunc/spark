import pandas as pd
import numpy as np
import cytoolz.curried
import os
import sys
if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
from common.env import PG_PWD, PG_PORT, PG_USER

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
df = spark.read.json("/usr/local/spark/examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()

# spark, df are from the previous example
# Print the schema in a tree format
df.printSchema()


# Select only the "name" column
df.select("name").show()

# Select everybody, but increment the age by 1
df.select(df['name'], df['age'] + 1).show()


# Select people older than 21
df.filter(df['age'] > 21).show()


# Count people by age
df.groupBy("age").count().show()

# to pandas dataframe
df.toPandas().dtypes


df.createGlobalTempView("people")

# Global temporary view is tied to a system preserved database `global_temp`
spark.sql("SELECT * FROM global_temp.people").show()


# Global temporary view is cross-session
spark.newSession().sql("SELECT * FROM global_temp.people").show()

# read from postgres
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, DataFrameReader
os.environ['SPARK_CLASSPATH'] = "/home/david/Downloads/postgresql-42.2.1.jar"
sparkClassPath = os.getenv('SPARK_CLASSPATH')

# Populate configuration
conf = SparkConf()
conf.setAppName('application')
conf.set('spark.jars', 'file:%s' % sparkClassPath)
conf.set('spark.executor.extraClassPath', sparkClassPath)
conf.set('spark.driver.extraClassPath', sparkClassPath)
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
url = f'postgresql://localhost:{PG_PORT}/tse'
properties = {'user':PG_USER, 'password':PG_PWD}

df = DataFrameReader(sqlContext).jdbc(url='jdbc:%s' % url, table='"每日收盤行情(全部(不含權證、牛熊證))"', properties=properties)

df.printSchema()
df.show(truncate = False)

