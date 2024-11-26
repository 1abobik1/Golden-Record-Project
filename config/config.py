import os

# Папка для хранения исходных файлов
RAW_DATA_FOLDER = r'..\\..\\data\\raw'
PROCESSED_DATA_FOLDER = r'..\\..\\data\\processed'

os.makedirs(RAW_DATA_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_DATA_FOLDER, exist_ok=True)

# Переменная для пути к последнему загруженному файлу
RAW_DATA_PATH = r'..\\\\..\\\\data\\\\raw\\asd.csv'
