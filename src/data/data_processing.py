import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, to_date, year
from pyspark.sql.types import StringType, DateType, IntegerType, FloatType

# Инициализация SparkSession
spark = SparkSession.builder \
    .appName("UniversalDataCleaning") \
    .master("local[*]") \
    .getOrCreate()

def clean_data(input_path, output_dir):
    # Извлечение имени исходного файла
    dirty_file_name = os.path.basename(input_path)  # Получаем имя файла (без пути)
    cleaned_file_name = f"cleared_data_file_name_{dirty_file_name}"  # Формируем имя выходного файла

    # Временная директория для Spark
    temp_output_path = os.path.join(output_dir, "temp_output")

    # Загрузка данных
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Очистка данных
    # 1. Удаление строк, где все значения NULL
    df_cleaned = df.dropna(how='all')

    # 2. Определение типов столбцов
    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    date_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, DateType)]
    numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, FloatType))]

    # 3. Приведение текстовых колонок к нижнему регистру и удаление пробелов
    for col_name in string_columns:
        df_cleaned = df_cleaned.withColumn(col_name, lower(trim(col(col_name))))

    # 4. Обработка датовых колонок
    for col_name in date_columns:
        df_cleaned = df_cleaned.withColumn(col_name, to_date(col(col_name), "yyyy-MM-dd"))

    # 5. Добавление новых колонок
    if date_columns:
        df_cleaned = df_cleaned.withColumn("create_year", year(col(date_columns[0])))

    # Сохранение очищенных данных во временную директорию
    df_cleaned.coalesce(1).write.csv(temp_output_path, header=True, mode="overwrite")

    # Перемещение файла из временной директории в целевую директорию
    final_csv_path = os.path.join(output_dir, cleaned_file_name)
    for file in os.listdir(temp_output_path):
        if file.endswith(".csv"):
            shutil.move(os.path.join(temp_output_path, file), final_csv_path)

    # Удаление временной директории
    shutil.rmtree(temp_output_path)

    print(f"Обработанный файл сохранен в: {final_csv_path}")
    return final_csv_path

if __name__ == "__main__":
    input_path = "../../data/raw/raw_test_file.csv"
    output_dir = "../../data/processed/"  # Папка для сохранения выходных файлов
    cleaned_file_path = clean_data(input_path, output_dir)

    # Используйте путь очищенного файла для дальнейшей обработки
    print(f"Путь очищенного файла: {cleaned_file_path}")
