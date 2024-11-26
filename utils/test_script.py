from jellyfish import jaro_winkler_similarity, levenshtein_distance

def combined_similarity(string1, string2, weight_jaro=0.7, weight_levenshtein=0.3):
    # Jaro-Winkler Similarity (значение от 0 до 1)
    jaro_score = jaro_winkler_similarity(string1, string2)

    # Levenshtein Distance (нормализуем расстояние к значению от 0 до 1)
    max_len = max(len(string1), len(string2))  # Максимальная длина строк
    levenshtein_score = 1 - (levenshtein_distance(string1, string2) / max_len)

    # Объединяем метрики
    combined_score = (weight_jaro * jaro_score) + (weight_levenshtein * levenshtein_score)
    return combined_score

# Пример использования
string1 = "Ivanov"
string2 = "Ivanof"

combined_score = combined_similarity(string1, string2)

print(f"Объединенная метрика сходства: {combined_score:.2f}")


import pandas as pd

# Загрузка данных
data = pd.read_csv("../data/raw/ds_dirty_fin_202410041147.csv", low_memory=False)

# Найти дубликаты (включая оригиналы)
duplicates = data[data.duplicated(keep=False)]

# Подсчет количества строк-дубликатов (включая оригинальные записи)
duplicates_count = len(duplicates)

# Сортировка дубликатов для наглядности
duplicates_sorted = duplicates.sort_values(by=data.columns.tolist(), ascending=True)

# Установим параметры отображения, чтобы показать все строки и столбцы
pd.set_option('display.max_columns', None)  # Показать все столбцы
pd.set_option('display.max_rows', None)     # Показать все строки
pd.set_option('display.width', 2000)        # Увеличить ширину для отображения

# Вывод количества строк-дубликатов
print(f"Количество строк-дубликатов (включая оригиналы): {duplicates_count}")

# Вывод самих дубликатов
print(duplicates_sorted)

