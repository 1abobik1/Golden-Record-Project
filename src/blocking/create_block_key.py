import os
from pyspark.sql.functions import col, substring, concat, lit
from spark.spark_init import spark
from config.config import RAW_DATA_PATH, PROCESSED_DATA_FOLDER
from src.data.data_processing import clean_data


def find_name_columns(df):
    """
    Автоматический поиск столбцов, содержащих подстроку 'name'.
    """
    name_columns = [col_name for col_name in df.columns if "name" in col_name.lower()]
    return name_columns

def create_block_key(df):
    """
    Генерация ключа блокировки: Имя + Фамилия (поиск по подстроке 'name').
    """
    # Найти столбцы с подстрокой 'name'
    name_columns = find_name_columns(df)

    # Проверяем наличие хотя бы двух столбцов
    if len(name_columns) < 2:
        raise ValueError("Недостаточно столбцов для генерации ключа. Нужно хотя бы два столбца с подстрокой 'name'.")

    # Берем первые два столбца с подстрокой 'name'
    first_name_col = name_columns[0]
    last_name_col = name_columns[1]

    # Формируем ключ как комбинацию первых 4 символов имени и фамилии
    first_name_key = substring(col(first_name_col), 1, 4)
    last_name_key = substring(col(last_name_col), 1, 4)

    # Формируем полный ключ
    block_key = concat(first_name_key, lit("-"), last_name_key)
    return df.withColumn("block_key", block_key)

def split_into_blocks(cleaned_file_path, output_dir):
    """
    Разбиение очищенных данных на блоки.
    """
    # Загрузка очищенных данных
    df = spark.read.csv(clean_data(RAW_DATA_PATH, PROCESSED_DATA_FOLDER), header=True, inferSchema=True)

    try:
        # Генерация ключей блокировки
        df_with_blocks = create_block_key(df)
    except ValueError as e:
        print(f"Ошибка: {e}")
        return

    # Сохранение данных с ключами блокировки
    output_file = os.path.join(output_dir, "blocked_data.csv")
    df_with_blocks.coalesce(1).write.csv(output_file, header=True, mode="overwrite")

    print(f"Данные с блоками сохранены в: {output_file}")
    return output_file

if __name__ == "__main__":

    # Разбиение на блоки
    blocked_file_path = split_into_blocks(RAW_DATA_PATH, PROCESSED_DATA_FOLDER)
    print(f"Файл с блоками готов: {blocked_file_path}")
