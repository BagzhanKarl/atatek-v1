import mysql.connector
import json
from dotenv import load_dotenv
import os

# Загрузка переменных из .env файла
load_dotenv()

def save_json_to_mariadb(json_data):
    try:
        # Подключение к базе данных с использованием переменных из .env
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor()

        # Создание таблицы
        create_table_query = """
        CREATE TABLE IF NOT EXISTS json_data (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            locked INT,
            author INT,
            image VARCHAR(255),
            birth_year INT,
            death_year INT,
            meta_status VARCHAR(50),
            checked INT
        );
        """
        cursor.execute(create_table_query)

        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO json_data (id, name, locked, author, image, birth_year, death_year, meta_status, checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)
        """

        # Проход по данным и вставка в базу данных
        for item in json_data:
            cursor.execute(insert_query, (
                item['id'],
                item['name'],
                item['locked'],
                item['author'],
                item['image'],
                item['birth_year'],
                item['death_year'],
                item['meta_status']
            ))

        # Подтверждение изменений
        conn.commit()

        print("Данные успешно сохранены в базе данных.")

    except mysql.connector.Error as err:
        print(f"Ошибка: {err}")
    finally:
        # Закрытие соединения
        if conn.is_connected():
            cursor.close()
            conn.close()

# Ваш JSON
data = [
    {"id": "1", "name": "Ұлы жүз", "locked": "1", "author": "1", "image": None, "birth_year": None, "death_year": None, "meta_status": "draft"},
    {"id": "2", "name": "Орта жүз", "locked": "1", "author": "1", "image": None, "birth_year": None, "death_year": None, "meta_status": "rejected"},
    {"id": "3", "name": "Кіші жүз", "locked": "3", "author": "1", "image": None, "birth_year": None, "death_year": None, "meta_status": "rejected"},
    {"id": "4", "name": "Жүзден тыс", "locked": "0", "author": "1", "image": None, "birth_year": None, "death_year": None, "meta_status": "rejected"}
]

# Вызов функции для сохранения данных
save_json_to_mariadb(data)
