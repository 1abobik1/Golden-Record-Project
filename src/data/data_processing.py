import os
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StringType
from spark.spark_init import spark


def clean_data(input_path, output_dir):
    dirty_file_name = os.path.basename(input_path)
    cleaned_file_name = f"cleared_data_file_name_{dirty_file_name}"
    final_csv_path = os.path.join(output_dir, cleaned_file_name)

    # Загрузка и обработка данных
    df = spark.read.csv(input_path, header=True, inferSchema=True).dropna(how='all')

    string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
    for col_name in string_columns:
        df = df.withColumn(col_name, lower(trim(col(col_name))))

    df.write.csv(final_csv_path, header=True, mode="overwrite")
    print(f"Обработанный файл сохранен в: {final_csv_path}")
    return final_csv_path
