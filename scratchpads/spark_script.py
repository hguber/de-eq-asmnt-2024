import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName('eqdataprocessing') \
    .getOrCreate()

df = spark.read.csv("gs://de-eq-asmnt-2024-raw-bucket/*/*")

df.registerTempTable('eq_events')





