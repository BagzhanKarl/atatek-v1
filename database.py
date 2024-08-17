import mysql.connector
from dotenv import load_dotenv
import os
import requests

# Загрузка переменных из .env файла
load_dotenv()

# Создание глобальных переменных для подключения
conn = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME")
)
cursor = conn.cursor(dictionary=True)


def save_json_to_mariadb(json_data):
    try:
        # SQL-запрос для вставки данных
        insert_query = """
        INSERT INTO json_data (id, name, locked, author, image, birth_year, death_year, meta_status, checked)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            locked = VALUES(locked),
            author = VALUES(author),
            image = VALUES(image),
            birth_year = VALUES(birth_year),
            death_year = VALUES(death_year),
            meta_status = VALUES(meta_status)
        """

        # Подготовка данных для пакетной вставки
        data_to_insert = [
            (
                item['id'],
                item['name'],
                item['locked'],
                item['author'],
                item['image'],
                item['birth_year'],
                item['death_year'],
                item['meta_status']
            )
            for item in json_data
        ]

        cursor.executemany(insert_query, data_to_insert)
        conn.commit()

        print("Данные успешно сохранены в базе данных.")

    except mysql.connector.Error as err:
        print(f"Ошибка: {err}")


def get_first_unchecked_record():
    try:
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
            return None

    except mysql.connector.Error as err:
        print(f"Ошибка: {err}")


def getDateFromApi(id):
    url = f"https://tumalas.kz/wp-admin/admin-ajax.php?action=tuma_cached_childnew_get&nodeid=14&id={id}"

    response = requests.get(url)
    return response.json()


# Основной цикл
while True:
    record_id = get_first_unchecked_record()
    if record_id:
        json_data = getDateFromApi(record_id)
        save_json_to_mariadb(json_data)
    else:
        break

# Закрытие соединения и курсора
cursor.close()
conn.close()
