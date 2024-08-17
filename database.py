import mysql.connector
from dotenv import load_dotenv
import os
import requests

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

# save_json_to_mariadb(data)
def get_first_unchecked_record():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        cursor = conn.cursor(dictionary=True)

        # SQL-запрос для получения первой записи с checked = 0
        cursor.execute("SELECT * FROM json_data WHERE checked = 0 ORDER BY id LIMIT 1")
        record = cursor.fetchone()

        if record:
            record_id = record['id']

            # Обновляем статус checked на 1
            update_query = "UPDATE json_data SET checked = 1 WHERE id = %s"
            cursor.execute(update_query, (record_id,))
            conn.commit()  # Подтверждаем изменения в базе данных

            print(f"Статус записи с id = {record_id} обновлен на 1.")
            return record_id
        else:
            print("Нет записей с checked = 0")

    finally:
        cursor.close()
        conn.close()


def getDateFromApi(id):
    url = f"https://tumalas.kz/wp-admin/admin-ajax.php?action=tuma_cached_childnew_get&nodeid=14&id={id}"

    response = requests.get(url)
    return response.json()

# Вызов функции

while True:
    save_json_to_mariadb(getDateFromApi(get_first_unchecked_record()))

