from pyspark.sql import SparkSession
import os
import sys

# Установить пути для PySpark
python_executable = sys.executable  # Текущий Python, запускающий скрипт
os.environ['PYSPARK_PYTHON'] = python_executable
os.environ['PYSPARK_DRIVER_PYTHON'] = python_executable

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("APP") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

