import sys
import os
from PyQt5.QtWidgets import QApplication
from src.dekstop.app import FileDownloaderApp  # Импорт из вашего GUI приложения
from src.data.data_processing import clean_data  # Импорт функции очистки данных

# Пути к папкам и файлам в конфигурации
from config.config import RAW_DATA_FOLDER, RAW_DATA_PATH, PROCESSED_DATA_FOLDER


# Функция для запуска GUI приложения
def start_file_downloader_app():
    app = QApplication(sys.argv)
    window = FileDownloaderApp()
    window.show()
    app.exec_()  # Запуск графического интерфейса программы


# Главная функция для последовательного запуска
def main():
    # 1. Запуск GUI приложения для загрузки файлов
    print("Шаг 1: Запуск приложения для загрузки файлов.")
    start_file_downloader_app()

    # Проверяем, существует ли загруженный файл после завершения GUI
    if not os.path.exists(RAW_DATA_PATH):
        print(f"Ошибка: Файл для обработки не найден по пути: {RAW_DATA_PATH}")
        return

    # 2. Запуск обработки данных
    print(f"Шаг 2: Запуск очистки данных. Обрабатывается файл: {RAW_DATA_PATH}")

    try:
        # Чистим данные и получаем путь к очищенному файлу
        cleaned_file_path = clean_data(RAW_DATA_PATH, PROCESSED_DATA_FOLDER)
        print(f"Очищенные данные сохранены по пути: {cleaned_file_path}")
    except Exception as e:
        print(f"Ошибка при очистке данных: {str(e)}")
        return

    # Дополнительные шаги
    print("Процесс завершен. Все шаги успешно выполнены.")


if __name__ == "__main__":
    main()
