from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Настройки подключения
DATABASE_URL = "postgresql://postgres:dima15042004@localhost:5432/golden_record_db"

# Создание движка для подключения
engine = create_engine(DATABASE_URL, echo=True)

# Базовый класс для моделей
Base = declarative_base()

# Создание сессии
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Функция для получения сессии
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
