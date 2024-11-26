from pyspark.sql import SparkSession

# Инициализация единственного экземпляра SparkSession
spark = SparkSession.builder \
    .appName("GoldenRecordProject") \
    .master("local[*]") \
    .getOrCreate()