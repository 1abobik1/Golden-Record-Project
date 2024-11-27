import os
from pyspark.sql.functions import col, struct, collect_list, concat_ws, udf
from pyspark.sql.types import StringType, DoubleType
from jellyfish import jaro_winkler_similarity, levenshtein_distance
from config.config import PROCESSED_DATA_FOLDER
from spark.spark_init import spark


@udf(DoubleType())
def calculate_jaro(record1, record2):
    return jaro_winkler_similarity(record1, record2)


@udf(DoubleType())
def calculate_levenshtein(record1, record2):
    return levenshtein_distance(record1, record2)


def find_similar_records(input_path, output_file, jaro_threshold=0.9, levenshtein_max_dist=50):
    """
    Оптимизированная версия поиска похожих записей.
    """
    # Загрузка данных
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Объединяем все данные записи в одну строку для сравнения
    columns = df.columns
    df = df.withColumn("record_string", concat_ws("|", *columns))

    # Сгруппируем данные по ключу блокировки
    grouped = df.groupBy("block_key").agg(collect_list("record_string").alias("records"))

    # Разворачиваем записи для попарного сравнения
    exploded = grouped.select(
        col("block_key"),
        col("records")
    ).rdd.flatMap(lambda row: [
        (row["block_key"], rec1, rec2)
        for i, rec1 in enumerate(row["records"])
        for rec2 in row["records"][i + 1:]
    ]).toDF(["block_key", "record1", "record2"])

    # Добавляем метрики сходства
    result = exploded \
        .withColumn("jaro_score", calculate_jaro("record1", "record2")) \
        .withColumn("levenshtein_score", calculate_levenshtein("record1", "record2")) \
        .filter((col("jaro_score") > jaro_threshold) & (col("levenshtein_score") < levenshtein_max_dist))

    # Сохранение в CSV
    result.write.csv(output_file, header=True, mode="overwrite")
    print(f"Найденные похожие записи сохранены в: {output_file}")


if __name__ == "__main__":
    input_file = os.path.join(PROCESSED_DATA_FOLDER, "data_with_blocks.csv")
    output_file = os.path.join(PROCESSED_DATA_FOLDER, "optimized_similar_records.csv")

    # Поиск похожих записей
    find_similar_records(input_file, output_file)