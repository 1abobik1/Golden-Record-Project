from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("APP") \
    .config("spark.sql.shuffle.partitions", "100") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "2") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()
