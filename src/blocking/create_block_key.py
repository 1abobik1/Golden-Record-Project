import os
from pyspark.sql.functions import col, concat, lit, when, date_format
from config.config import RAW_DATA_PATH, PROCESSED_DATA_FOLDER
from src.data.data_processing import clean_data


def create_block_key(df):
    """
    Создание ключей блоков на основе create_date и source_cd.
    """
    # Извлекаем дату до дня включительно
    create_date_key = date_format(col("create_date"), "yyyyMMdd")

    # Проверяем наличие source_cd и формируем ключ
    block_key = when(
        col("source_cd").isNotNull(),
        concat(create_date_key, lit("-"), col("source_cd"))  # create_date + source_cd
    ).otherwise(create_date_key)  # Только create_date, если source_cd отсутствует

    return df.withColumn("block_key", block_key)


def split_into_blocks(input_path, output_dir):
    """
    Разбиение очищенных данных на блоки.
    """
    # Очистка данных и создание ключей блокировки
    df_cleaned = clean_data(input_path)
    df_with_blocks = create_block_key(df_cleaned)

    # Сохранение данных с ключами блокировки
    blocked_file_name = os.path.join(output_dir, "data_with_blocks.csv")
    df_with_blocks.write.csv(blocked_file_name, header=True, mode="overwrite")
    print(f"Файл с блоками сохранен в: {blocked_file_name}")
    return blocked_file_name


if __name__ == "__main__":
    # Разбиение на блоки
    blocked_file_path = split_into_blocks(RAW_DATA_PATH, PROCESSED_DATA_FOLDER)
    print(f"Файл с блоками готов: {blocked_file_path}")
