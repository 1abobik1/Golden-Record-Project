# from jellyfish import jaro_winkler_similarity, levenshtein_distance
#
# def combined_similarity(string1, string2, weight_jaro=0.7, weight_levenshtein=0.3):
#     # Jaro-Winkler Similarity (значение от 0 до 1)
#     jaro_score = jaro_winkler_similarity(string1, string2)
#
#     # Levenshtein Distance (нормализуем расстояние к значению от 0 до 1)
#     max_len = max(len(string1), len(string2))  # Максимальная длина строк
#     levenshtein_score = 1 - (levenshtein_distance(string1, string2) / max_len)
#
#     # Объединяем метрики
#     combined_score = (weight_jaro * jaro_score) + (weight_levenshtein * levenshtein_score)
#     return combined_score


# # Пример использования
# string1 = "АбдусаламДворянниковdvorynnikov"
# string2 = "АбдукаюмКамалитдинов"
# print(jaro_winkler_similarity(string1, string2))
#
# combined_score = combined_similarity(string1, string2)
#
# print(f"Объединенная метрика сходства: {combined_score:.2f}")

# import pandas as pd
#
# # Загрузка данных
# data = pd.read_csv("../data/raw/ds_dirty_fin_202410041147.csv", low_memory=False)
#
# # Найти дубликаты (включая оригиналы)
# duplicates = data[data.duplicated(keep=False)]
#
# # Подсчет количества строк-дубликатов (включая оригинальные записи)
# duplicates_count = len(duplicates)
#
# # Сортировка дубликатов для наглядности
# duplicates_sorted = duplicates.sort_values(by=data.columns.tolist(), ascending=True)
#
# # Сохранение дубликатов в CSV файл
# duplicates_file = "duplicates.csv"
# duplicates_sorted.to_csv(duplicates_file, index=False)
#
# # Сохранение количества строк-дубликатов в текстовый файл
# duplicates_count_file = "duplicates_count.txt"
# with open(duplicates_count_file, "w") as f:
#     f.write(f"Количество строк-дубликатов (включая оригиналы): {duplicates_count}\n")
#
# print(f"Дубликаты сохранены в: {duplicates_file}")
# print(f"Количество строк-дубликатов сохранено в: {duplicates_count_file}")
#


# import pandas as pd
#
# # Загрузка данных из CSV
# file_path = "../data/raw/ds_dirty_fin_202410041147.csv"  # Замените на путь к вашему файлу
# df = pd.read_csv(file_path, low_memory=False)
#
# # Преобразование столбца update_date в формат datetime
# df['update_date'] = pd.to_datetime(df['update_date'], errors='coerce')
#
# # Сортировка по столбцу update_date в порядке убывания (более поздние даты — более актуальные)
# df_sorted = df.sort_values(by='update_date', ascending=False)
#
# # Сохранение отсортированных данных в новый CSV
# output_file = "sorted_file.csv"  # Укажите путь для сохранения отсортированного файла
# df_sorted.to_csv(output_file, index=False)
#
# print(f"Данные успешно отсортированы и сохранены в: {output_file}")

# import pandas as pd
#
# # Загрузка данных из CSV
# file_path = "../data/raw/ds_dirty_fin_202410041147.csv"  # Укажите путь к вашему файлу
# df = pd.read_csv(file_path, low_memory=False)
#
# # Преобразование столбца create_date в формат datetime
# df['create_date'] = pd.to_datetime(df['create_date'], errors='coerce')
#
# # Сортировка по столбцу create_date в порядке возрастания (ранние даты будут первыми)
# df_sorted = df.sort_values(by='create_date', ascending=True)
#
# # Сохранение отсортированных данных в новый CSV
# output_file = "sorted_by_create_date.csv"  # Укажите путь для сохранения отсортированного файла
# df_sorted.head(5000).to_csv("top_5000_sorted_by_create_date.csv", index=False)
