# файл возможно не понадобиться, если мы не будем использовать логгер

import logging

def setup_logging():
    # Настройка базового конфигуратора логирования
    logging.basicConfig(
        level=logging.DEBUG,  # Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # Формат вывода
        handlers=[
            logging.FileHandler("app.log"),  # Логирование в файл
            logging.StreamHandler()  # Логирование в консоль
        ]
    )

# Пример использования
setup_logging()

logger = logging.getLogger(__name__)
logger.debug("Это сообщение отладки.")
logger.info("Это информационное сообщение.")
logger.warning("Это предупреждение.")
