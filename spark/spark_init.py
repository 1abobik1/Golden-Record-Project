from pyspark.sql import SparkSession
import os


os.environ['PYSPARK_PYTHON'] = r"C:\Users\dima1\PycharmProjects\golden_record_project\.venv\Scripts\python.exe"
os.environ['PYSPARK_DRIVER_PYTHON'] = r"C:\Users\dima1\PycharmProjects\golden_record_project\.venv\Scripts\python.exe"

spark = SparkSession.builder \
    .appName("APP") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()
