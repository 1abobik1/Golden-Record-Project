import os
from pyspark.sql.functions import col, substring, concat, lit
from spark.spark_init import spark
from config.config import RAW_DATA_PATH, PROCESSED_DATA_FOLDER
from src.data.data_processing import clean_data


def create_block_key(df):
    name_columns = [col_name for col_name in df.columns if "name" in col_name.lower()]
    if len(name_columns) < 2:
        raise ValueError("Недостаточно столбцов для генерации ключа. Нужно хотя бы два столбца с подстрокой 'name'.")
    first_name_key = substring(col(name_columns[0]), 1, 4)
    last_name_key = substring(col(name_columns[1]), 1, 4)
    return df.withColumn("block_key", concat(first_name_key, lit("-"), last_name_key))


def split_into_blocks(input_path, output_dir):
    df = spark.read.csv(clean_data(RAW_DATA_PATH, PROCESSED_DATA_FOLDER), header=True, inferSchema=True)
    df_with_blocks = create_block_key(df)
    blocked_file_name = os.path.join(output_dir, f"{os.path.basename(input_path).replace('.csv', '_with_blocks.csv')}")
    df_with_blocks.write.csv(blocked_file_name, header=True, mode="overwrite")
    print(f"Файл с блоками сохранен в: {blocked_file_name}")
    return blocked_file_name


if __name__ == "__main__":

    # Разбиение на блоки
    blocked_file_path = split_into_blocks(RAW_DATA_PATH, PROCESSED_DATA_FOLDER)
    print(f"Файл с блоками готов: {blocked_file_path}")