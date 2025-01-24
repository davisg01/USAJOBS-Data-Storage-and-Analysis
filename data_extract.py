import sys
import os

# Dynamically set the Python executable for both the Spark driver and worker
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, count, month, year, date_format, col, row_number
from pyspark.sql import Window
from pyspark.sql import SparkSession
import mysql.connector

# Create SparkSession use your own my-sql-connector file location for .config
spark = SparkSession.builder \
    .appName("PySpark MySQL Example") \
    .master("local[*]") \
    .config("spark.jars", "file:///C:/Users/davis/Documents/Pyspark/mysql-connector-j-9.1.0/mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

mysql_db_driver_class = "com.mysql.cj.jdbc.Driver"  # Updated driver class
table_name = "job_details"
host_name = "localhost"
port_no = "3306"
user_name = "davis"
password = "#####" # use your own Mysql database password
database_name = "usa_jobs"

# JDBC connection properties
mysql_jdbc_url = f"jdbc:mysql://{host_name}:{port_no}/{database_name}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
connection_properties = {
    "user": "root",
    "password": "######",  # use your own Mysql database password
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=mysql_jdbc_url,
    table="job_details",  # Table name in your database
    properties=connection_properties
)


# Highest paying jobs with location
(df.filter(
    "pay_type = 'per year' and country = 'United States'"
).groupBy('region').agg(round(avg('salary_avg'),2).alias('avg_salary')).orderBy('avg_salary', ascending=False).limit(800).show(800, truncate=False))

# How many jobs available per state
df.filter(
    "country = 'United States'"
).groupby('region').agg((count('job_id')).alias('job_count')).orderBy('job_count', ascending=False).show(100)

# How many jobs released per month
# first adding new columns month and year
df = df.withColumn('month', month(df['date_posted']))
df = df.withColumn('year', year(df['date_posted']))

df.groupby('year', 'month').agg((count('job_id')).alias('job_count')).orderBy('year','month').show()

# Which org has the most openings?
df.groupby('organization_name').agg((count('job_id')).alias('job_count')).orderBy('job_count', ascending=False).show(100)

# The 5 top available jobs per region in the US

country_filter = df.filter(
    "country = 'United States' and region != 'Any State'"
).groupby('region','job_title').agg(avg('salary_avg').alias('avg_salary'))
parti = Window.partitionBy('region').orderBy(col('avg_salary').desc())
rank_jobs = country_filter.withColumn('rank',row_number().over(parti))
top_5_jobs = rank_jobs.filter(col('rank')<=5).orderBy('region','rank').show(300, truncate=False)

# Find data, anaylst or admin position in washington, oregon and california (can be remote as well)

df.filter(
    (col('region').isin('Washington','Oregon','California','Any State')) &
    (col('job_title').rlike('(?i)analyst') | col('job_title').rlike('(?i)data') | col('job_title').rlike('(?i)admin'))
).select('job_title','organization_name','region','salary_avg','url').show(200, truncate=False)


# Stop the SparkSession
spark.stop()




