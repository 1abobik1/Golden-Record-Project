import sys
import os
import zipfile
import tarfile
import requests
import shutil
from PyQt5.QtWidgets import (QApplication, QWidget, QLabel, QLineEdit,
                             QPushButton, QVBoxLayout, QStackedWidget,
                             QMessageBox, QFileDialog, QListWidget)


# Папка для сохранения загруженных файлов
DOWNLOAD_FOLDER = "data/raw"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)


class FileDownloaderApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.last_downloaded_file_path = None

    def initUI(self):
        self.setWindowTitle('Загрузчик файлов')
        self.setGeometry(100, 100, 800, 500)
        self.setStyleSheet("background-color: #f0f0f5;")

        # Основные дисплеи
        self.file_download_page = QWidget()
        self.file_conversion_page = QWidget()

        self.setup_download_page()
        self.setup_conversion_page()

        # Главный стекномовой виджет для переключения страниц
        self.stacked_widget = QStackedWidget(self)
        self.stacked_widget.addWidget(self.file_download_page)
        self.stacked_widget.addWidget(self.file_conversion_page)

        main_layout = QVBoxLayout()
        main_layout.addWidget(self.stacked_widget)
        self.setLayout(main_layout)

    def setup_download_page(self):
        """Настройка страницы загрузки файлов."""
        layout = QVBoxLayout()

        self.title_label = QLabel("Загрузите или выберите файл", self)
        self.title_label.setStyleSheet("font-size: 24px; font-weight: bold;")
        self.url_label = QLabel("Введите URL файла:", self)
        self.url_entry = QLineEdit(self)
        self.url_entry.setPlaceholderText("http://example.com/file.zip")
        self.url_entry.setStyleSheet("padding: 10px; border-radius: 5px; border: 1px solid #ccc;")

        self.download_button = QPushButton("Скачать файл", self)
        self.download_button.setStyleSheet(self.button_style())
        self.choose_file_button = QPushButton("Выбрать файл на устройстве", self)
        self.choose_file_button.setStyleSheet(self.button_style())

        # Список для отображения загруженных файлов
        self.file_list = QListWidget(self)
        self.file_list.setStyleSheet("border: 1px solid #ccc; border-radius: 5px;")
        self.file_list.itemDoubleClicked.connect(self.on_item_double_clicked)

        # Подключаем кнопки к функциям
        self.download_button.clicked.connect(self.download_file)
        self.choose_file_button.clicked.connect(self.choose_file)

        # Устанавливаем макет
        layout.addWidget(self.title_label)
        layout.addWidget(self.url_label)
        layout.addWidget(self.url_entry)
        layout.addWidget(self.download_button)
        layout.addWidget(self.choose_file_button)
        layout.addWidget(self.file_list)

        self.file_download_page.setLayout(layout)

    def setup_conversion_page(self):
        """Настройка страницы преобразования."""
        layout = QVBoxLayout()

        self.conversion_label = QLabel("Выберите файл для преобразования:", self)

        self.convert_button = QPushButton("Преобразовать", self)
        self.convert_button.setStyleSheet(self.button_style())
        self.convert_button.clicked.connect(self.start_conversion)

        layout.addWidget(self.conversion_label)
        layout.addWidget(self.convert_button)

        self.file_conversion_page.setLayout(layout)

    def button_style(self):
        """Стиль для кнопок."""
        return """
            QPushButton {
                background-color: #348eda;
                color: white;
                border: none;
                padding: 10px;
                border-radius: 5px;
                font-size: 16px;
            }
            QPushButton:hover {
                background-color: #4a90e2;
            }
        """

    def on_item_double_clicked(self, item):
        """Обработка двойного клика по элементу списка файлов."""
        self.last_downloaded_file_path = os.path.join(DOWNLOAD_FOLDER, item.text().replace("Загружен: ", ""))
        self.conversion_label.setText(f"Файл для преобразования: {self.last_downloaded_file_path}")
        self.stacked_widget.setCurrentWidget(self.file_conversion_page)

    def download_file(self):
        url = self.url_entry.text()
        if not url:
            QMessageBox.warning(self, "Ошибка", "Введите ссылку на файл!")
            return

        try:
            # Получаем имя файла из URL
            filename = url.split("/")[-1]
            file_path = os.path.join(DOWNLOAD_FOLDER, filename)

            # Отправляем запрос на скачивание файла
            response = requests.get(url, stream=True)
            if response.status_code != 200:
                QMessageBox.warning(self, "Ошибка", f"Невозможно скачать файл. Код ошибки: {response.status_code}")
                return

            # Сохраняем файл на диск
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.file_list.addItem(f"Загружен: {filename}")

            # Проверяем, является ли файл архивом
            if self.is_valid_zipfile(file_path):
                self.extract_archive(file_path)
                QMessageBox.information(self, "Успех", f"Файл успешно загружен и разархивирован\nСохранён в папку: {DOWNLOAD_FOLDER}")
            else:
                QMessageBox.information(self, "Успех", f"Файл успешно загружен\nСохранён в: {file_path}")

        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Произошла ошибка при загрузке файла: {e}")

    def choose_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Выберите файл", "", "Все файлы (*.*);;Архивы (*.zip *.tar *.gz)")
        if not file_path:
            return  # Если файл не выбран, ничего не делаем

        try:
            # Копируем выбранный файл в папку "downloaded_files"
            filename = os.path.basename(file_path)
            destination_path = os.path.join(DOWNLOAD_FOLDER, filename)
            shutil.copy(file_path, destination_path)

            self.last_downloaded_file_path = destination_path
            self.file_list.addItem(f"{filename}")

            # Проверяем, является ли файл архивом и обрабатываем его при необходимости
            if self.is_valid_zipfile(destination_path) or tarfile.is_tarfile(destination_path):
                self.extract_archive(destination_path)
                QMessageBox.information(self, "Успех", f"Файл успешно скопирован и разархивирован\nСодержимое сохранено в папку: {DOWNLOAD_FOLDER}")
            else:
                QMessageBox.information(self, "Файл выбран", f"Файл успешно скопирован:\n{destination_path}")

        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Произошла ошибка при обработке файла: {e}")

    @staticmethod
    def is_valid_zipfile(file_path):
        try:
            with open(file_path, "rb") as f:
                magic_number = f.read(4)
                return magic_number == b'PK\x03\x04'  # Проверка на ZIP
        except Exception:
            return False

    @staticmethod
    def extract_archive(file_path):
        try:
            if zipfile.is_zipfile(file_path):
                with zipfile.ZipFile(file_path, "r") as archive:
                    archive.extractall(DOWNLOAD_FOLDER)
            elif tarfile.is_tarfile(file_path):
                with tarfile.open(file_path, "r:*") as archive:
                    archive.extractall(DOWNLOAD_FOLDER)
            else:
                raise ValueError("Файл не является поддерживаемым архивом.")
        except Exception as e:
            QMessageBox.warning(None, "Ошибка", f"Ошибка при разархивировании: {e}")

    def path_to_file(self):
        # Возвращает путь к последнему загруженному файлу
        return self.last_downloaded_file_path

    def start_conversion(self):
        if self.last_downloaded_file_path:
            # Выводим путь к загруженному файлу
            QMessageBox.information(self, "Информация", f"Путь к загруженному файлу: {self.last_downloaded_file_path}")
        else:
            QMessageBox.warning(self, "Ошибка", "Нет загруженного файла для преобразования.")



if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = FileDownloaderApp()
    window.show()
    sys.exit(app.exec_())

