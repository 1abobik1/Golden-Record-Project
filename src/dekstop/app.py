import sys
import os
import zipfile
import tarfile
import requests
import shutil
import subprocess
from PyQt5.QtWidgets import (QApplication, QWidget, QLabel, QLineEdit,
                             QPushButton, QVBoxLayout, QStackedWidget,
                             QMessageBox, QFileDialog, QListWidget)
from config.config import RAW_DATA_FOLDER, RAW_DATA_PATH

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
        self.last_downloaded_file_path = os.path.join(RAW_DATA_FOLDER, item.text().replace("Загружен: ", ""))
        self.update_raw_data_path(self.last_downloaded_file_path)
        self.conversion_label.setText(f"Файл для преобразования: {self.last_downloaded_file_path}")
        self.stacked_widget.setCurrentWidget(self.file_conversion_page)

    def download_file(self):
        url = self.url_entry.text()
        if not url:
            QMessageBox.warning(self, "Ошибка", "Введите ссылку на файл!")
            return

        try:
            filename = url.split("/")[-1]
            file_path = os.path.join(RAW_DATA_FOLDER, filename)

            response = requests.get(url, stream=True)
            if response.status_code != 200:
                QMessageBox.warning(self, "Ошибка", f"Невозможно скачать файл. Код ошибки: {response.status_code}")
                return

            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.file_list.addItem(f"Загружен: {filename}")
            self.update_raw_data_path(file_path)
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Произошла ошибка при загрузке файла: {e}")

    def choose_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Выберите файл", "", "Все файлы (*.*)")
        if not file_path:
            return

        try:
            filename = os.path.basename(file_path)
            destination_path = os.path.join(RAW_DATA_FOLDER, filename)
            shutil.copy(file_path, destination_path)
            self.last_downloaded_file_path = destination_path
            self.file_list.addItem(f"{filename}")
            self.update_raw_data_path(destination_path)
        except Exception as e:
            QMessageBox.warning(self, "Ошибка", f"Произошла ошибка при обработке файла: {e}")

    def update_raw_data_path(self, file_path):
        """Обновляет RAW_DATA_PATH в config.py с корректными путями."""
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "config.py"))

        with open(config_path, "r") as f:
            config_content = f.readlines()

        with open(config_path, "w") as f:
            for line in config_content:
                if line.startswith("RAW_DATA_PATH"):
                    # Преобразуем путь в формат с двойными обратными слешами
                    normalized_path = file_path.replace("\\", "\\\\")
                    f.write(f"RAW_DATA_PATH = r'{normalized_path}'\n")
                else:
                    f.write(line)

    def start_conversion(self):
        if self.last_downloaded_file_path:
            try:
                script_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "blocking", "create_block_key.py"))
                # Запуск скрипта с захватом stdout и stderr
                python_executable = r"C:\Users\maksi\PycharmProjects\Golden-Record-Project\.venv\Scripts\python.exe"
                result = subprocess.run([python_executable, script_path], check=True,
                capture_output=True,
                text=True
                )

                QMessageBox.information(self, "Информация",
                                        f"Скрипт '{script_path}' успешно выполнен.\n\nВывод:\n{result.stdout}")
            except subprocess.CalledProcessError as e:
                QMessageBox.warning(self, "Ошибка", f"Ошибка при выполнении скрипта: {e}\n\nЛог ошибки:\n{e.stderr}")
            except Exception as e:
                QMessageBox.warning(self, "Ошибка", f"Произошла непредвиденная ошибка: {e}")
        else:
            QMessageBox.warning(self, "Ошибка", "Нет загруженного файла для преобразования.")


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = FileDownloaderApp()
    window.show()
    sys.exit(app.exec_())
