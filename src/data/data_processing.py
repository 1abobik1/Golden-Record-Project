import os
from pyspark.sql.functions import col, trim, lower, when
from pyspark.sql.types import StringType
from spark.spark_init import spark

def clean_data(input_path):
    # Загрузка и обработка данных
    df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how='all')

    # Определяем текстовые столбцы
    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

    # Обработка текстовых столбцов
    for col_name in string_columns:
        # Приведение к нижнему регистру, удаление пробелов, удаление `Ъ` или `Ь` в начале строки
        df = df.withColumn(
            col_name,
            when(col(col_name).rlike(r"^[ЪЬъь]"), col(col_name).substr(2, 1000000))  # Удаляем первый символ, если он Ъ/Ь
            .otherwise(col(col_name))  # Иначе оставляем строку без изменений
        )
        df = df.withColumn(col_name, lower(trim(col(col_name))))  # Приведение к нижнему регистру и удаление пробелов

    return df
