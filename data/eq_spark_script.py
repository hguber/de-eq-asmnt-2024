import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import date_format
gs_bucket_raw = 'gs://de-eq-asmnt-2024-raw-bucket'
gs_bucket_stage = 'gs://de-eq-asmnt-2024-staging-bucket'
spark = SparkSession.builder \
    .config(conf=sc.getConf()) \
    .getOrCreate()

df = spark.read.csv(gs_bucket_raw + '/eq_events/raw/*/*', header='true')
df = df.withColumn("year", date_format(df.date, "yyyy")).withColumn("month", date_format(df.date, "MM"))
df = df.drop('_c0')

df.registerTempTable('eq_events')

df_final = spark.sql("""
select *
from eq_events
cluster by country
""")

df_monthly = spark.sql("""
select 
    country, 
    date_trunc('month', date) as eq_month,
    COUNT(event_id) as events_occ,
    avg(depth) as avg_depth_month,
    avg(magnitude) as avg_mag_month,
    avg(significance) as avg_sig_month,
    max(depth) as max_depth_month,
    max(magnitude) as max_mag_month,
    max(significance) as max_sig_month,
    min(depth) as min_depth_month,
    min(magnitude) as min_mag_month,
    min(significance) as min_sig_month
from eq_events
group by 1,2
cluster by country
""")

df_week = spark.sql("""
select 
    country, 
    date_trunc('week', date) as eq_week,
    COUNT(event_id) as events_occ,
    avg(depth) as avg_depth_week,
    avg(magnitude) as avg_mag_week,
    avg(significance) as avg_sig_week,
    max(depth) as max_depth_week,
    max(magnitude) as max_mag_week,
    max(significance) as max_sig_week,
    min(depth) as min_depth_week,
    min(magnitude) as min_mag_week,
    min(significance) as min_sig_week
from eq_events
group by 1,2
cluster by country
""")

df_daily = spark.sql("""
select 
    country, 
    date,
    COUNT(event_id) as events_occ,
    avg(depth) as avg_depth_day,
    avg(magnitude) as avg_mag_day,
    avg(significance) as avg_sig_day,
    max(depth) as max_depth_day,
    max(magnitude) as max_mag_day,
    max(significance) as max_sig_day,
    min(depth) as min_depth_day,
    min(magnitude) as min_mag_day,
    min(significance) as min_sig_day
from eq_events
group by 1,2 
cluster by country
""")

df.coalesce(1).write.option("header", "true").partitionBy('year', 'month').parquet(gs_bucket + '/eq_events/processed/final', mode='overwrite')
df_week.coalesce(1).write.option("header", "true").partitionBy('eq_week').parquet(gs_bucket + '/eq_events/processed/weekly/', mode='overwrite')
df_monthly.coalesce(1).write.option("header", "true").partitionBy('eq_month').parquet(gs_bucket + '/eq_events/processed/monthly/', mode='overwrite')
df_daily.coalesce(1).write.option("header", "true").parquet(gs_bucket + '/eq_events/processed/daily/', mode='overwrite')

